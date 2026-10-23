package aigate

// Byte-prefix observations of the adapted upstream request. This is not token
// or KV telemetry. No prompt text or token IDs are persisted by this store.
import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const prefixSchema = "prefill-prefix-v1"
const prefixAlgorithm = "hmac-sha256-chain-utf8-json-v1"
const prefixBlockBytes = 1024
const prefixQueueBytes int64 = 64 << 20
const prefixRecordLimit = 8 << 20

type prefixMark struct {
	EndBytes int    `json:"end_bytes"`
	Digest   string `json:"digest"`
}
type prefixField struct {
	Bytes  int    `json:"bytes"`
	Digest string `json:"digest"`
}
type prefixRecord struct {
	Schema        string                 `json:"schema"`
	Kind          string                 `json:"kind"`
	Algorithm     string                 `json:"algorithm"`
	KeyID         string                 `json:"key_id"`
	BlockBytes    int                    `json:"block_bytes"`
	RequestID     string                 `json:"request_id"`
	BootID        string                 `json:"boot_id"`
	Sequence      uint64                 `json:"sequence"`
	ArrivedAt     float64                `json:"arrived_at"`
	At            float64                `json:"at"`
	RouterID      string                 `json:"router_id"`
	ServiceID     string                 `json:"service_id"`
	ConfigDigest  string                 `json:"config_digest"`
	RouteVersion  uint64                 `json:"route_version"`
	Backend       string                 `json:"backend"`
	Model         string                 `json:"model"`
	Traffic       string                 `json:"traffic"`
	Stream        bool                   `json:"stream"`
	PayloadBytes  int                    `json:"payload_bytes,omitempty"`
	MessagesBytes int                    `json:"messages_bytes,omitempty"`
	Marks         []prefixMark           `json:"prefix_marks,omitempty"`
	Fields        map[string]prefixField `json:"fields,omitempty"`
	Reason        string                 `json:"reason,omitempty"`
	Outcome       string                 `json:"outcome,omitempty"`
	Status        int                    `json:"status,omitempty"`
	PromptTokens  *uint64                `json:"prompt_tokens"`
	CachedTokens  *uint64                `json:"cached_tokens"`
	InputPrepared bool                   `json:"input_prepared"`
	UsageQuality  string                 `json:"usage_quality,omitempty"`
}
type prefixGap struct {
	Start  float64 `json:"start"`
	End    float64 `json:"end"`
	Reason string  `json:"reason"`
	Count  uint64  `json:"count"`
}
type prefixState struct {
	RouterID   string            `json:"router_id"`
	ServiceID  string            `json:"service_id"`
	BootStart  float64           `json:"boot_start"`
	Started    float64           `json:"started_at"`
	Floor      float64           `json:"retained_start"`
	Checkpoint float64           `json:"checkpoint"`
	Clean      bool              `json:"clean_shutdown"`
	Counters   map[string]uint64 `json:"counters"`
	Gaps       []prefixGap       `json:"gaps"`
}
type prefixSegment struct {
	path    string
	size    int64
	maximum float64
	readers int
}
type prefixJob struct {
	record  prefixRecord
	payload []byte
	charge  int64
}
type prefillPrefixStore struct {
	mu                   sync.Mutex
	queueMu              sync.Mutex // never held across disk or network I/O
	pendingDrop          prefixGap
	root                 string
	owner                *os.File
	key                  []byte
	keyID                string
	router, service, cfg string
	maxBytes             int64
	state                prefixState
	segments             []*prefixSegment
	active               *os.File
	activeSegment        *prefixSegment
	opened               time.Time
	stored               int64
	queued               int64
	queue                chan prefixJob
	done                 chan struct{}
	closed               bool
	exports              int
	closeOnce            sync.Once
}

func newPrefillPrefixStore(root string, maxBytes int64, router, service, cfg string) (*prefillPrefixStore, error) {
	if maxBytes == 0 {
		maxBytes = 10 << 30
	}
	if maxBytes < 1<<20 {
		return nil, fmt.Errorf("prefill prefix disk limit must be at least 1 MiB")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("invalid prefill prefix directory")
	}
	owner, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	if err = lockState(owner); err != nil {
		owner.Close()
		return nil, err
	}
	s := &prefillPrefixStore{root: root, owner: owner, maxBytes: maxBytes, router: router, service: service, cfg: cfg,
		queue: make(chan prefixJob, 256), done: make(chan struct{})}
	fail := func(err error) (*prefillPrefixStore, error) { unlockState(owner); owner.Close(); return nil, err }
	keyPath := filepath.Join(root, "key")
	if _, err = os.Lstat(keyPath); os.IsNotExist(err) {
		if _, stateErr := os.Lstat(filepath.Join(root, "state.json")); !os.IsNotExist(stateErr) {
			return fail(fmt.Errorf("prefill prefix key missing for existing state"))
		}
		key := make([]byte, 32)
		if _, err = rand.Read(key); err != nil {
			return fail(err)
		}
		f, err := os.OpenFile(keyPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return fail(err)
		}
		_, err = f.Write(key)
		if err == nil {
			err = f.Sync()
		}
		f.Close()
		if err != nil {
			return fail(err)
		}
	}
	info, err = os.Lstat(keyPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return fail(fmt.Errorf("invalid prefill prefix key"))
	}
	s.key, err = os.ReadFile(keyPath)
	if err != nil || len(s.key) != 32 {
		return fail(fmt.Errorf("invalid prefill prefix key length"))
	}
	id := sha256.Sum256(s.key)
	s.keyID = hex.EncodeToString(id[:])
	now := profileNow()
	s.state = prefixState{RouterID: router, ServiceID: service, Started: now, Floor: now, Clean: true, Counters: map[string]uint64{}}
	statePath := filepath.Join(root, "state.json")
	if raw, readErr := os.ReadFile(statePath); readErr == nil {
		if json.Unmarshal(raw, &s.state) != nil || s.state.Started <= 0 || s.state.Counters == nil {
			return fail(fmt.Errorf("invalid prefill prefix state"))
		}
		if s.state.RouterID != router || s.state.ServiceID != service {
			return fail(fmt.Errorf("prefill prefix storage belongs to another source"))
		}
		if !s.state.Clean {
			// Pending input/result work can belong to any request in this boot.
			s.gapLocked(s.state.BootStart, now, "unclean_restart", 1)
		}
	} else if !os.IsNotExist(readErr) {
		return fail(readErr)
	}
	paths, err := filepath.Glob(filepath.Join(root, "trace-*.jsonl"))
	if err != nil {
		return fail(err)
	}
	sort.Strings(paths)
	for _, path := range paths {
		info, err = os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			return fail(fmt.Errorf("invalid prefill prefix segment"))
		}
		segment := &prefixSegment{path: path, size: info.Size()}
		meta, _ := os.ReadFile(path + ".meta.json")
		var saved profileSegmentMeta
		if json.Unmarshal(meta, &saved) == nil && saved.Bytes == info.Size() && saved.Max > 0 {
			segment.maximum = saved.Max
		} else {
			f, err := os.Open(path)
			if err != nil {
				return fail(err)
			}
			scanner := bufio.NewScanner(f)
			scanner.Buffer(make([]byte, 65536), prefixRecordLimit+1)
			for scanner.Scan() {
				var record prefixRecord
				if json.Unmarshal(scanner.Bytes(), &record) == nil && record.ArrivedAt > 0 {
					segment.maximum = max(segment.maximum, record.ArrivedAt)
				} else {
					s.gapLocked(s.state.Started, now, "invalid_records", 1)
				}
			}
			if scanner.Err() != nil {
				s.gapLocked(s.state.Started, now, "invalid_records", 1)
			}
			f.Close()
			if segment.maximum == 0 {
				segment.maximum = now
			}
		}
		s.segments = append(s.segments, segment)
		s.stored += info.Size()
	}
	if len(paths) > 0 && s.state.Checkpoint == 0 {
		return fail(fmt.Errorf("prefill prefix state missing for retained segments"))
	}
	s.state.Clean = false
	s.state.BootStart = now
	if err = s.saveLocked(); err != nil {
		return fail(err)
	}
	go s.writer()
	return s, nil
}

func (s *prefillPrefixStore) gapLocked(start, end float64, reason string, count uint64) {
	s.state.Counters[reason] += count
	gaps := s.state.Gaps
	if len(gaps) > 0 && gaps[len(gaps)-1].Reason == reason && start <= gaps[len(gaps)-1].End+60 {
		g := &gaps[len(gaps)-1]
		g.Start, g.End, g.Count = min(g.Start, start), max(g.End, end), g.Count+count
		return
	}
	if len(gaps) >= 1024 {
		// Bound quality metadata while conservatively retaining the covered gap.
		a, b := gaps[0], gaps[1]
		gaps[1] = prefixGap{min(a.Start, b.Start), max(a.End, b.End), "multiple_losses", a.Count + b.Count}
		gaps = gaps[1:]
	}
	s.state.Gaps = append(gaps, prefixGap{start, end, reason, count})
}

func (s *prefillPrefixStore) base(e profileEvent) prefixRecord {
	traffic := e.Traffic
	if traffic == "" {
		traffic = "business"
		if e.TestTask != "" {
			traffic = "evaluation"
		}
	}
	return prefixRecord{Schema: prefixSchema, Algorithm: prefixAlgorithm, KeyID: s.keyID, BlockBytes: prefixBlockBytes,
		RequestID: e.ID, BootID: e.Boot, Sequence: e.Seq, ArrivedAt: e.Arrived, At: e.At,
		RouterID: s.router, ServiceID: s.service, ConfigDigest: s.cfg, RouteVersion: e.RouteVersion,
		Backend: e.Backend, Model: e.Model, Traffic: traffic, Stream: e.Stream}
}

func (s *prefillPrefixStore) enqueue(job prefixJob) {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if s.closed {
		return
	}
	job.charge = int64(len(job.payload)) + 4096
	if s.queued+job.charge > prefixQueueBytes {
		s.dropLocked(job.record.ArrivedAt)
		return
	}
	select {
	case s.queue <- job:
		s.queued += job.charge
	default:
		s.dropLocked(job.record.ArrivedAt)
	}
}
func (s *prefillPrefixStore) dropLocked(arrived float64) {
	if s.pendingDrop.Count == 0 {
		s.pendingDrop.Start = arrived
	}
	s.pendingDrop.Start = min(s.pendingDrop.Start, arrived)
	s.pendingDrop.End = profileNow()
	s.pendingDrop.Count++
}
func (s *prefillPrefixStore) flushDrops() {
	s.queueMu.Lock()
	gap := s.pendingDrop
	s.pendingDrop = prefixGap{}
	s.queueMu.Unlock()
	if gap.Count != 0 {
		s.mu.Lock()
		s.gapLocked(gap.Start, gap.End, "queue_dropped", gap.Count)
		s.mu.Unlock()
	}
}
func (s *prefillPrefixStore) queueStatus() (int64, uint64) {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	return s.queued, s.pendingDrop.Count
}
func (s *prefillPrefixStore) input(e profileEvent, payload []byte) {
	r := s.base(e)
	r.Kind, r.PayloadBytes, r.InputPrepared = "input", len(payload), true
	// makePayload owns this immutable byte slice. The queue retains it only in
	// memory until fingerprinting; the writer never writes the request payload.
	s.enqueue(prefixJob{record: r, payload: payload})
}
func (s *prefillPrefixStore) end(e profileEvent, status int) {
	r := s.base(e)
	r.Kind, r.Outcome, r.Status, r.InputPrepared = "result", e.Outcome, status, e.Bytes > 0
	u := profileUsage(e.Usage)
	if n, ok := u["prompt_tokens"]; ok {
		r.PromptTokens = &n
	} else {
		r.UsageQuality = "prompt_tokens_missing"
	}
	if n, ok := u["cached_tokens"]; ok {
		r.CachedTokens = &n
	}
	if !r.InputPrepared {
		r.Reason = "input_not_prepared"
	}
	s.enqueue(prefixJob{record: r})
}

// encoding/json sorts string object keys; UseNumber preserves numeric lexemes.
// HTML escaping is disabled. Version 1 deliberately keeps 1 and 1.0 distinct.
func prefixCanonical(v any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte{'\n'}), nil
}
func (s *prefillPrefixStore) digest(parts ...[]byte) []byte {
	h := hmac.New(sha256.New, s.key)
	for _, part := range parts {
		h.Write(part)
	}
	return h.Sum(nil)
}
func (s *prefillPrefixStore) fingerprint(r *prefixRecord, payload []byte) {
	var fields map[string]any
	if decodeJSON(payload, &fields) != nil || fields == nil {
		r.Reason = "invalid_payload"
		return
	}
	messages, ok := fields["messages"].([]any)
	if !ok || len(messages) == 0 {
		r.Reason = "unsupported_input_structure"
		return
	}
	for _, message := range messages {
		if _, ok := message.(map[string]any); !ok {
			r.Reason = "unsupported_input_structure"
			return
		}
	}
	data, err := prefixCanonical(messages)
	if err != nil {
		r.Reason = "canonicalization_failed"
		return
	}
	r.MessagesBytes = len(data)
	previous := s.digest([]byte(prefixAlgorithm + ":messages"))
	for start := 0; start < len(data); start += prefixBlockBytes {
		end := min(start+prefixBlockBytes, len(data))
		var offset [8]byte
		binary.BigEndian.PutUint64(offset[:], uint64(end))
		previous = s.digest([]byte("block:"), previous, offset[:], data[start:end])
		r.Marks = append(r.Marks, prefixMark{end, hex.EncodeToString(previous)})
	}
	r.Fields = map[string]prefixField{}
	for name, value := range fields {
		if name == "messages" {
			continue
		}
		data, err := prefixCanonical(value)
		if err != nil {
			r.Reason = "canonicalization_failed"
			return
		}
		// Length-prefixed field names make field/domain separation unambiguous.
		tag := []byte(fmt.Sprintf("field:%d:%s:", len(name), name))
		r.Fields[name] = prefixField{len(data), hex.EncodeToString(s.digest(tag, data))}
	}
}

func (s *prefillPrefixStore) saveLocked() error {
	s.state.Checkpoint = profileNow()
	data, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	path := filepath.Join(s.root, "state.json")
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(path+".tmp", path)
}
func (s *prefillPrefixStore) closeActiveLocked() {
	if s.active != nil {
		if err := s.active.Sync(); err != nil {
			s.gapLocked(s.state.Floor, profileNow(), "write_errors", 1)
		}
		s.saveSegmentLocked()
		s.active.Close()
		s.active, s.activeSegment = nil, nil
	}
}
func (s *prefillPrefixStore) saveSegmentLocked() {
	if s.activeSegment == nil {
		return
	}
	segment := s.activeSegment
	data, _ := json.Marshal(profileSegmentMeta{Max: segment.maximum, Bytes: segment.size})
	path := segment.path + ".meta.json"
	if os.WriteFile(path+".tmp", data, 0600) != nil || os.Rename(path+".tmp", path) != nil {
		s.gapLocked(s.state.Floor, profileNow(), "write_errors", 1)
	}
}
func (s *prefillPrefixStore) roomLocked(bytesNeeded int64) bool {
	for s.stored+bytesNeeded > s.maxBytes {
		if len(s.segments) == 0 || s.segments[0] == s.activeSegment || s.segments[0].readers > 0 {
			return false
		}
		old := s.segments[0]
		previousFloor := s.state.Floor
		s.state.Floor = max(s.state.Floor, math.Nextafter(old.maximum, math.Inf(1)))
		// Persist the coverage boundary before deleting data. A crash must not
		// leave a manifest claiming that an already removed interval is complete.
		if s.saveLocked() != nil {
			s.state.Floor = previousFloor
			s.gapLocked(s.state.Started, profileNow(), "write_errors", 1)
			return false
		}
		if err := os.Remove(old.path); err != nil {
			s.gapLocked(s.state.Floor, profileNow(), "write_errors", 1)
			return false
		}
		s.stored -= old.size
		s.segments = s.segments[1:]
		os.Remove(old.path + ".meta.json")
		os.Remove(old.path + ".meta.json.tmp")
		s.state.Counters["evicted_segments"]++
		if s.saveLocked() != nil {
			s.gapLocked(s.state.Started, profileNow(), "write_errors", 1)
		}
	}
	return true
}
func (s *prefillPrefixStore) writeLocked(record prefixRecord, data []byte) {
	if len(data) > prefixRecordLimit || int64(len(data)) > s.maxBytes {
		s.gapLocked(record.ArrivedAt, profileNow(), "oversized_records", 1)
		return
	}
	if s.active != nil && (s.activeSegment.size+int64(len(data)) > min(8<<20, s.maxBytes/2) || time.Since(s.opened) > time.Minute) {
		s.closeActiveLocked()
	}
	if !s.roomLocked(int64(len(data))) {
		s.gapLocked(record.ArrivedAt, profileNow(), "capacity_dropped", 1)
		return
	}
	if s.active == nil {
		s.opened = time.Now()
		path := filepath.Join(s.root, fmt.Sprintf("trace-%013d-%s.jsonl", s.opened.UnixMilli(), randomID()))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			s.gapLocked(record.ArrivedAt, profileNow(), "write_errors", 1)
			return
		}
		s.active, s.activeSegment = f, &prefixSegment{path: path}
		s.segments = append(s.segments, s.activeSegment)
	}
	n, err := s.active.Write(data)
	s.activeSegment.size += int64(n)
	s.activeSegment.maximum = max(s.activeSegment.maximum, record.ArrivedAt)
	s.stored += int64(n)
	if err != nil || n != len(data) {
		s.gapLocked(record.ArrivedAt, profileNow(), "write_errors", 1)
		s.closeActiveLocked()
		return
	}
	s.state.Counters[record.Kind+"_records"]++
	if record.Reason != "" {
		s.gapLocked(record.ArrivedAt, record.At, record.Reason, 1)
	}
}
func (s *prefillPrefixStore) writer() {
	defer close(s.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case job, ok := <-s.queue:
			if !ok {
				s.flushDrops()
				s.mu.Lock()
				s.closeActiveLocked()
				s.state.Clean = true
				if s.saveLocked() != nil {
					s.state.Clean = false
				}
				s.mu.Unlock()
				return
			}
			if job.record.Kind == "input" {
				s.fingerprint(&job.record, job.payload)
			}
			data, err := json.Marshal(job.record)
			s.mu.Lock()
			if err != nil {
				s.gapLocked(job.record.ArrivedAt, profileNow(), "encoding_failed", 1)
			} else {
				s.writeLocked(job.record, append(data, '\n'))
			}
			s.mu.Unlock()
			s.queueMu.Lock()
			s.queued -= job.charge
			s.queueMu.Unlock()
			s.flushDrops()
		case <-ticker.C:
			s.flushDrops()
			s.mu.Lock()
			s.roomLocked(0)
			if s.active != nil && s.active.Sync() != nil {
				s.gapLocked(s.state.Floor, profileNow(), "write_errors", 1)
			}
			s.saveSegmentLocked()
			if s.saveLocked() != nil {
				s.gapLocked(s.state.Started, profileNow(), "write_errors", 1)
			}
			s.mu.Unlock()
		}
	}
}
func (s *prefillPrefixStore) close() {
	s.closeOnce.Do(func() {
		s.queueMu.Lock()
		s.closed = true
		close(s.queue)
		s.queueMu.Unlock()
		<-s.done
		unlockState(s.owner)
		s.owner.Close()
	})
}
func (s *prefillPrefixStore) statusLocked() map[string]any {
	queued, pendingDrops := s.queueStatus()
	return map[string]any{"schema": prefixSchema, "status": "collecting", "algorithm": prefixAlgorithm,
		"canonicalization": "go-json-sorted-keys-use-number-no-html-escape-v1", "unit": "utf8_bytes", "block_bytes": prefixBlockBytes,
		"observation": "proxy_upstream_payload", "approximate": true, "key_id": s.keyID,
		"router_id": s.router, "service_id": s.service, "config_digest": s.cfg,
		"started_at": s.state.Started, "retained_start": s.state.Floor, "retained_end": profileNow(),
		"retention": "capacity_only", "max_disk_bytes": s.maxBytes, "stored_bytes": s.stored,
		"queue_bytes": queued, "queue_records": len(s.queue), "queue_max_bytes": prefixQueueBytes, "pending_queue_drops": pendingDrops,
		"counters": s.state.Counters, "gaps": s.state.Gaps, "quality_scope": "collector_lifetime",
		"result_policy": "join_by_boot_id_request_id_sequence; missing result is unknown"}
}

func (s *prefillPrefixStore) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path == "/profile/v1/prefill-prefix/status" {
		s.flushDrops()
		s.mu.Lock()
		data, _ := json.Marshal(s.statusLocked())
		s.mu.Unlock()
		w.Write(data)
		return
	}
	if r.URL.Path != "/profile/v1/prefill-prefix/export" {
		http.NotFound(w, r)
		return
	}
	start, e1 := strconv.ParseFloat(r.URL.Query().Get("start"), 64)
	end, e2 := strconv.ParseFloat(r.URL.Query().Get("end"), 64)
	backend, model := r.URL.Query().Get("backend"), r.URL.Query().Get("model")
	if e1 != nil || e2 != nil || math.IsNaN(start) || math.IsNaN(end) || math.IsInf(start, 0) || math.IsInf(end, 0) ||
		start <= 0 || start >= end || end > profileNow()+5 || len(backend) > 128 || len(model) > 256 {
		http.Error(w, "invalid window or filters", http.StatusBadRequest)
		return
	}
	s.export(w, r, start, end, backend, model)
}

func (s *prefillPrefixStore) export(w http.ResponseWriter, r *http.Request, start, end float64, backend, model string) {
	type snapshot struct {
		segment *prefixSegment
		size    int64
	}
	s.flushDrops()
	s.mu.Lock()
	// At most two exports can pin the entire retained set. This bounds handles
	// and prevents unlimited concurrent readers from defeating capacity cleanup.
	if s.exports >= 2 {
		s.mu.Unlock()
		http.Error(w, "export busy", http.StatusTooManyRequests)
		return
	}
	s.exports++
	files := []snapshot{}
	for _, segment := range s.segments {
		segment.readers++
		files = append(files, snapshot{segment, segment.size})
	}
	// Deep copy mutable state before releasing the writer lock.
	raw, _ := json.Marshal(s.statusLocked())
	var status map[string]any
	json.Unmarshal(raw, &status)
	partial := start < s.state.Floor
	gaps := []prefixGap{}
	for _, gap := range s.state.Gaps {
		if gap.Start < end && gap.End >= start {
			gaps = append(gaps, gap)
		}
	}
	queued, pendingDrops := s.queueStatus()
	asof := profileNow()
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.exports--
		for _, item := range files {
			item.segment.readers--
		}
	}()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", `attachment; filename="prefill-prefix.ndjson"`)
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	if enc.Encode(map[string]any{"kind": "manifest", "schema": prefixSchema, "source": status,
		"start": start, "end": end, "snapshot_at": asof, "backend": backend, "model": model,
		"window_end_after_snapshot": end > asof,
		"window_partially_expired":  partial, "gaps": gaps, "queued_bytes_at_snapshot": queued,
		"complete": false, "completion_marker_required": true, "pending_queue_drops": pendingDrops}) != nil {
		return
	}
	checksum := sha256.New()
	counts := map[string]int{"input": 0, "result": 0, "invalid": 0}
	for _, item := range files {
		file, err := os.Open(item.segment.path)
		if err != nil {
			return
		}
		readErr := func() error {
			defer file.Close()
			scanner := bufio.NewScanner(io.NewSectionReader(file, 0, item.size))
			scanner.Buffer(make([]byte, 65536), prefixRecordLimit+1)
			for scanner.Scan() {
				if r.Context().Err() != nil {
					return r.Context().Err()
				}
				line := scanner.Bytes()
				var record prefixRecord
				if json.Unmarshal(line, &record) != nil || record.Schema != prefixSchema || (record.Kind != "input" && record.Kind != "result") {
					counts["invalid"]++
					continue
				}
				if record.ArrivedAt < start || record.ArrivedAt >= end || (backend != "" && backend != record.Backend) || (model != "" && model != record.Model) {
					continue
				}
				// Files are append ordered, not arrival ordered. Consumers sort by
				// arrived_at, boot_id, sequence and join result records independently.
				line = append(append([]byte{}, line...), '\n')
				if _, err := w.Write(line); err != nil {
					return err
				}
				checksum.Write(line)
				counts[record.Kind]++
			}
			return scanner.Err()
		}()
		if readErr != nil {
			return
		} // no completion marker on read failure
	}
	enc.Encode(map[string]any{"kind": "complete", "schema": prefixSchema, "complete": true,
		"data_complete":            !partial && end <= asof && len(gaps) == 0 && queued == 0 && pendingDrops == 0 && counts["invalid"] == 0,
		"window_partially_expired": partial, "counts": counts, "records_sha256": hex.EncodeToString(checksum.Sum(nil)),
		"results_may_be_pending": true})
}
