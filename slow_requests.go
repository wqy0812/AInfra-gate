package aigate

// Slow request capture is observational: original input is retained only for
// validated streaming requests, and storage failures never change a response.
import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type SlowRequestConfig struct {
	Enabled          bool    `json:"enabled"`
	ThresholdSeconds float64 `json:"threshold_seconds"`
	Directory        string  `json:"directory"`
	RetentionHours   int     `json:"retention_hours"`
	MaxDiskBytes     int64   `json:"max_disk_bytes"`
	MaxQueueBytes    int64   `json:"max_queue_bytes"`
}

func (c *SlowRequestConfig) defaults(path string) {
	if c.ThresholdSeconds == 0 {
		c.ThresholdSeconds = 300
	}
	if c.Directory == "" {
		c.Directory = "slow-requests"
	}
	if !filepath.IsAbs(c.Directory) {
		absolute, _ := filepath.Abs(path)
		c.Directory = filepath.Join(filepath.Dir(absolute), c.Directory)
	}
	if c.RetentionHours == 0 {
		c.RetentionHours = 72
	}
	if c.MaxDiskBytes == 0 {
		c.MaxDiskBytes = 10 << 30
	}
	if c.MaxQueueBytes == 0 {
		c.MaxQueueBytes = 64 << 20
	}
}

func (c SlowRequestConfig) validate() error {
	if !c.Enabled {
		return nil
	}
	if math.IsNaN(c.ThresholdSeconds) || math.IsInf(c.ThresholdSeconds, 0) || c.ThresholdSeconds <= 0 || c.ThresholdSeconds >= float64(math.MaxInt64)/float64(time.Second) || seconds(c.ThresholdSeconds) <= 0 {
		return fmt.Errorf("slow_requests.threshold_seconds must be a positive duration")
	}
	if c.Directory == "" || c.RetentionHours <= 0 || int64(c.RetentionHours) > math.MaxInt64/int64(time.Hour) || c.MaxDiskBytes <= 0 || c.MaxQueueBytes <= 0 {
		return fmt.Errorf("invalid slow_requests storage limits")
	}
	return nil
}

type slowRequestMeta struct {
	Schema           string    `json:"schema"`
	RequestID        string    `json:"request_id"`
	StartedAt        time.Time `json:"started_at"`
	EndedAt          time.Time `json:"ended_at"`
	DurationSeconds  float64   `json:"duration_seconds"`
	Backend          *string   `json:"backend"`
	Model            *string   `json:"model"`
	Traffic          string    `json:"traffic,omitempty"`
	Status           int       `json:"status"`
	Outcome          string    `json:"outcome"`
	ClientCancelled  bool      `json:"client_cancelled"`
	GenerationResult *string   `json:"generation_result"`
	UpstreamStatus   *int      `json:"upstream_status"`
	ErrorClass       *string   `json:"error_class"`
	InputBytes       int64     `json:"input_bytes"`
	InputSHA256      string    `json:"input_sha256"`
}

const slowRequestSchema = "aigate-slow-request-v1"

type slowRequestJob struct {
	meta slowRequestMeta
	body []byte // immutable original input, owned until the writer finishes
}

type slowRequestStore struct {
	c          SlowRequestConfig
	log        *log.Logger
	directory  *os.File // flock prevents another writer/cleaner using this directory
	queue      chan slowRequestJob
	done       chan struct{}
	mu         sync.Mutex
	closed     bool
	queueBytes int64 // includes the currently writing job
	write      func(slowRequestJob) error
}

func newSlowRequestStore(c SlowRequestConfig, logger *log.Logger) (*slowRequestStore, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(c.Directory, 0700); err != nil {
		return nil, fmt.Errorf("slow_requests directory: %w", err)
	}
	info, err := os.Lstat(c.Directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("slow_requests.directory must be a dedicated real directory with mode 0700")
	}
	dir, err := os.Open(c.Directory)
	if err != nil {
		return nil, err
	}
	if err = lockState(dir); err != nil {
		dir.Close()
		return nil, fmt.Errorf("slow_requests directory lock: %w", err)
	}
	s := &slowRequestStore{c: c, log: logger, directory: dir, queue: make(chan slowRequestJob, 1024), done: make(chan struct{})}
	if err := s.cleanup(time.Now(), 0); err != nil {
		unlockState(dir)
		dir.Close()
		return nil, fmt.Errorf("slow_requests startup cleanup: %w", err)
	}
	s.write = s.writeRecord
	go s.run()
	return s, nil
}

func (s *slowRequestStore) diagnostic(id, result, reason string) {
	s.log.Print(string(encodeJSON(map[string]any{"event": "slow_request_capture", "request_id": id, "result": result, "reason": reason})))
}

func (s *slowRequestStore) capture(entry *auditEntry, started, ended time.Time, cancelled bool) {
	if entry.slowInput == nil || ended.Sub(started) <= seconds(s.c.ThresholdSeconds) {
		return
	}
	m := slowRequestMeta{Schema: slowRequestSchema, RequestID: entry.RequestID, StartedAt: started.UTC(), EndedAt: ended.UTC(), DurationSeconds: ended.Sub(started).Seconds(), Backend: entry.Backend, Model: entry.Model, Traffic: entry.Traffic, Status: entry.Status, Outcome: entry.Outcome, ClientCancelled: cancelled, InputBytes: int64(len(entry.slowInput))}
	if r := entry.profile; r != nil && r.outcome != "" {
		m.Outcome = r.outcome
	}
	if cancelled {
		m.Outcome = "client_cancelled"
	}
	if g := entry.generation; g != nil && g.started {
		if g.result != "" {
			value := g.result
			m.GenerationResult = &value
		}
		if g.status != 0 {
			value := g.status
			m.UpstreamStatus = &value
		}
		if g.errorClass != "" {
			value := g.errorClass
			m.ErrorClass = &value
		}
	}
	s.enqueue(slowRequestJob{meta: m, body: entry.slowInput})
}

func (s *slowRequestStore) enqueue(job slowRequestJob) bool {
	s.mu.Lock()
	reason := ""
	n := int64(len(job.body))
	switch {
	case s.closed:
		reason = "closed"
	case n > s.c.MaxQueueBytes:
		reason = "input_exceeds_queue_limit"
	case n > s.c.MaxQueueBytes-s.queueBytes:
		reason = "queue_bytes_full"
	default:
		select {
		case s.queue <- job:
			s.queueBytes += n
		default:
			reason = "queue_events_full"
		}
	}
	s.mu.Unlock()
	if reason != "" {
		s.diagnostic(job.meta.RequestID, "skipped", reason)
	}
	return reason == ""
}

func (s *slowRequestStore) close() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
	s.mu.Unlock()
	<-s.done
}

func (s *slowRequestStore) run() {
	ticker := time.NewTicker(archiveInterval)
	defer func() {
		ticker.Stop()
		unlockState(s.directory)
		s.directory.Close()
		close(s.done)
	}()
	for {
		select {
		case job, ok := <-s.queue:
			if !ok {
				return
			}
			if err := s.write(job); err != nil {
				s.diagnostic(job.meta.RequestID, "failed", err.Error())
			} else {
				s.diagnostic(job.meta.RequestID, "saved", "")
			}
			s.mu.Lock()
			s.queueBytes -= int64(len(job.body))
			s.mu.Unlock()
		case now := <-ticker.C:
			if err := s.cleanup(now, 0); err != nil {
				s.diagnostic("", "cleanup_failed", err.Error())
			}
		}
	}
}

func slowRequestID(id string) bool {
	if len(id) != 32 || strings.ToLower(id) != id {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func slowWriteFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

func (s *slowRequestStore) writeRecord(job slowRequestJob) error {
	diagnosticArchiveMu.RLock()
	defer diagnosticArchiveMu.RUnlock()
	if !slowRequestID(job.meta.RequestID) {
		return fmt.Errorf("invalid request ID")
	}
	hash := sha256.Sum256(job.body)
	job.meta.InputSHA256 = hex.EncodeToString(hash[:])
	meta := encodeJSON(job.meta)
	if len(meta) > 16<<10 {
		return fmt.Errorf("metadata exceeds 16 KiB limit")
	}
	final := filepath.Join(s.c.Directory, "request-"+job.meta.RequestID)
	if _, err := os.Lstat(final); !os.IsNotExist(err) {
		return fmt.Errorf("record already exists or cannot be inspected")
	}
	tmp := filepath.Join(s.c.Directory, ".pending-"+job.meta.RequestID)
	if err := os.Mkdir(tmp, 0700); err != nil {
		return err
	}
	defer slowRemoveFiles(tmp)
	if err := slowWriteFile(filepath.Join(tmp, "request.json"), job.body); err != nil {
		return err
	}
	if err := slowWriteFile(filepath.Join(tmp, "meta.json"), meta); err != nil {
		return err
	}
	dir, err := os.Open(tmp)
	if err != nil {
		return err
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return s.directory.Sync()
}

type slowStoredRecord struct {
	path  string
	ended time.Time
	bytes int64
}

// Never traverse links or remove unexpected files, even inside a reserved name.
func slowRecordFiles(path string) ([]os.DirEntry, int64, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, 0, err
	}
	var size int64
	for _, entry := range entries {
		if entry.Name() != "meta.json" && entry.Name() != "request.json" && entry.Name() != diagnosticRefName {
			return nil, 0, fmt.Errorf("unexpected file in slow request directory")
		}
		info, err := entry.Info()
		if err != nil {
			return nil, 0, err
		}
		if !info.Mode().IsRegular() {
			return nil, 0, fmt.Errorf("non-regular slow request file")
		}
		size += info.Size()
	}
	return entries, size, nil
}

func slowRemoveFiles(path string) error {
	entries, _, err := slowRecordFiles(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := os.Remove(filepath.Join(path, entry.Name())); err != nil {
			return err
		}
	}
	return os.Remove(path)
}

func slowStoredRecords(directory string) ([]slowStoredRecord, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var records []slowStoredRecord
	for _, entry := range entries {
		name := entry.Name()
		pending := strings.HasPrefix(name, ".pending-")
		if !pending && !strings.HasPrefix(name, "request-") {
			continue
		}
		id := strings.TrimPrefix(name, "request-")
		if pending {
			id = strings.TrimPrefix(name, ".pending-")
		}
		if !entry.IsDir() || !slowRequestID(id) {
			continue
		}
		path := filepath.Join(directory, name)
		files, size, err := slowRecordFiles(path)
		if err != nil { // foreign content is protected, never recursively deleted
			return nil, err
		}
		if pending {
			if err := slowRemoveFiles(path); err != nil {
				return nil, err
			}
			continue
		}
		if len(files) != 2 {
			return nil, fmt.Errorf("incomplete slow request record: %s", name)
		}
		info, err := os.Lstat(filepath.Join(path, "meta.json"))
		if err != nil || info.Size() > 16<<10 {
			return nil, fmt.Errorf("invalid slow request metadata: %s", name)
		}
		data, err := os.ReadFile(filepath.Join(path, "meta.json"))
		var meta slowRequestMeta
		if err != nil || decodeJSON(data, &meta) != nil || meta.Schema != slowRequestSchema || meta.RequestID != id || meta.EndedAt.IsZero() {
			return nil, fmt.Errorf("invalid slow request metadata: %s", name)
		}
		records = append(records, slowStoredRecord{path, meta.EndedAt, size})
	}
	return records, nil
}

func (s *slowRequestStore) cleanup(now time.Time, incoming int64) error {
	root := s.c.Directory
	if observationPolicyDir(filepath.Base(root)) {
		root = filepath.Dir(root)
	}
	return maintainSlowArchives(root, now, s.c.MaxDiskBytes)
}

func maintainSlowArchives(root string, now time.Time, budget int64) error {
	diagnosticArchiveMu.Lock()
	dirs, err := diagnosticFolders(root)
	if err == nil {
		for _, dir := range dirs {
			_, err = slowStoredRecords(dir)
			if err != nil {
				break
			}
		}
	}
	diagnosticArchiveMu.Unlock()
	if err != nil {
		return err
	}
	return maintainDiagnosticArchives(root, now, budget, nil)
}
