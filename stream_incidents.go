package aigate

// Public summaries contain only the first/last ten question runes. Complete
// incident evidence is held by a separate bounded, asynchronous capture store.
import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const incidentThreshold = 60 * time.Second
const incidentRetention = 72 * time.Hour
const incidentMaxDisk int64 = 256 << 20

type incidentQuestionKey struct{}
type incidentQuestion struct {
	head, tail string
	length     int
}

type streamIncident struct {
	ConnectionPeer   string     `json:"connection_peer,omitempty"`
	ID               string     `json:"id"`
	RequestID        string     `json:"request_id"`
	GatewayID        string     `json:"gateway_id"`
	BootID           string     `json:"boot_id"`
	Router           string     `json:"router"`
	Model            string     `json:"model"`
	BindingRevision  uint64     `json:"binding_revision"`
	QuestionHead     string     `json:"question_head"`
	QuestionTail     string     `json:"question_tail"`
	QuestionLength   int        `json:"question_length"`
	RequestStartedAt time.Time  `json:"request_started_at"`
	Kind             string     `json:"kind"`
	StartedAt        time.Time  `json:"started_at"`
	TriggeredAt      time.Time  `json:"triggered_at"`
	EndedAt          *time.Time `json:"ended_at,omitempty"`
	ThresholdSeconds float64    `json:"threshold_seconds"`
	DurationSeconds  float64    `json:"duration_seconds"`
	ReadWaitSeconds  float64    `json:"read_wait_seconds"`
	OutputGapSeconds float64    `json:"output_gap_seconds"`
	LastByteAt       *time.Time `json:"last_byte_at,omitempty"`
	ReceivedBytes    int64      `json:"received_bytes"`
	Operation        string     `json:"operation,omitempty"`
	PlannedBytes     int        `json:"planned_bytes"`
	WrittenBytes     *int       `json:"written_bytes,omitempty"`
	WriteSeconds     float64    `json:"write_seconds"`
	FlushSeconds     float64    `json:"flush_seconds"`
	State            string     `json:"state"`
	EndReason        string     `json:"end_reason,omitempty"`
	Observation      string     `json:"observation"`
}

type incidentRequest struct {
	store                            *incidentStore
	base                             streamIncident
	lastUseful, lastByte             time.Time
	readStart                        time.Time
	reading, terminal, unknown       bool
	readWait                         time.Duration
	received                         int64
	writeStart, flushStart           time.Time
	writeReturnedAt, flushReturnedAt time.Time
	written                          *int
	planned                          int
	stall, write                     *streamIncident
	capture                          *streamCapture
	responseOffset                   int64
	lastEvent, lastUsefulEvent       sseEventPosition
	writePosition                    captureEvent
}

type incidentStore struct {
	mu                    sync.Mutex // tracker state only; never held across disk or network I/O
	active                map[*incidentRequest]bool
	root, gatewayID, boot string
	queue                 chan streamIncident
	stop, done, watched   chan struct{}
	once                  sync.Once
	closed                bool
	diskMu                sync.Mutex
	directory             *os.File
	querySlots            chan struct{}
	logger                *log.Logger
	dropped, failures     atomic.Uint64
	maxDisk               int64
	retention             time.Duration
}

func questionEdges(text string) (head, tail string, length int) {
	// Slice UTF-8 at rune boundaries without retaining the complete input buffer.
	end := 0
	for i := range text {
		if length < 10 {
			end = i
		}
		length++
	}
	if length <= 10 {
		return strings.Clone(text), strings.Clone(text), length
	}
	_, n := utf8.DecodeRuneInString(text[end:])
	head = strings.Clone(text[:end+n])
	start := len(text)
	for i := 0; i < 10; i++ {
		_, n = utf8.DecodeLastRuneInString(text[:start])
		start -= n
	}
	return head, strings.Clone(text[start:]), length
}

// Extract only literal, balanced outer tags. An unfinished trailing group does
// not hide the last complete group. The adapter's original question is untouched.
func incidentQuestionText(text string) string {
	const open, close = "<user_query>", "</user_query>"
	depth, start, foundStart, foundEnd := 0, 0, -1, -1
	for offset := 0; offset < len(text); {
		a, b := strings.Index(text[offset:], open), strings.Index(text[offset:], close)
		if a < 0 && b < 0 {
			break
		}
		if a >= 0 && (b < 0 || a < b) {
			offset += a + len(open)
			if depth == 0 {
				start = offset
			}
			depth++
		} else {
			end := offset + b
			offset = end + len(close)
			if depth > 0 {
				depth--
				if depth == 0 {
					foundStart, foundEnd = start, end
				}
			}
		}
	}
	if foundStart >= 0 {
		return strings.TrimSpace(text[foundStart:foundEnd])
	}
	return text
}

func newIncidentStore(root, gatewayID, boot string, audit io.Writer) (*incidentStore, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("invalid incident directory")
	}
	dir, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	if err = lockState(dir); err != nil {
		dir.Close()
		return nil, err
	}
	if audit == nil {
		audit = io.Discard
	}
	s := &incidentStore{root: root, gatewayID: gatewayID, boot: boot, queue: make(chan streamIncident, 1024), stop: make(chan struct{}), done: make(chan struct{}), watched: make(chan struct{}), active: map[*incidentRequest]bool{}, directory: dir, querySlots: make(chan struct{}, 2), logger: log.New(audit, "", 0), maxDisk: incidentMaxDisk, retention: incidentRetention}
	if err = s.cleanup(time.Now()); err != nil {
		unlockState(dir)
		dir.Close()
		return nil, err
	}
	go s.run()
	go s.watch()
	return s, nil
}
func (s *incidentStore) close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		close(s.stop)
		<-s.watched
		s.mu.Lock()
		s.closed = true
		close(s.queue)
		s.mu.Unlock()
		<-s.done
		unlockState(s.directory)
		s.directory.Close()
	})
}
func (s *incidentStore) watch() {
	defer close(s.watched)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-t.C:
			s.tick(now)
		}
	}
}
func (s *incidentStore) tick(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for r := range s.active {
		r.check(now)
	}
}
func (s *incidentStore) enqueue(v streamIncident) { // called under mu
	if s.closed {
		s.dropped.Add(1)
		return
	}
	select {
	case s.queue <- v:
	default:
		s.dropped.Add(1)
	}
}
func (s *incidentStore) begin(base streamIncident, capture ...*streamCapture) *incidentRequest {
	if s == nil {
		return nil
	}
	base.GatewayID = s.gatewayID
	base.BootID = s.boot
	base.ThresholdSeconds = 60
	base.Observation = "known"
	r := &incidentRequest{store: s, base: base}
	if len(capture) > 0 {
		r.capture = capture[0]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.active[r] = true
	return r
}
func (r *incidentRequest) mutate(f func()) {
	if r == nil {
		return
	}
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	if r.store.closed {
		return
	}
	f()
}
func (r *incidentRequest) newRecord(kind string, start, now time.Time) *streamIncident {
	v := r.base
	v.ID = randomID()
	v.Kind = kind
	v.StartedAt = start.UTC()
	v.TriggeredAt = now.UTC()
	v.State = "ongoing"
	return &v
}
func elapsed(now, start time.Time) float64 {
	if start.IsZero() {
		return 0
	}
	return max(0, now.Sub(start).Seconds())
}
func (r *incidentRequest) snapshot(v *streamIncident, now time.Time) streamIncident {
	out := *v
	if r.unknown {
		out.Observation = "unknown"
	}
	if v.Kind == "output_stall" {
		wait := r.readWait
		if r.reading {
			wait += now.Sub(r.readStart)
		}
		out.ReadWaitSeconds = max(0, wait.Seconds())
		out.DurationSeconds = out.ReadWaitSeconds
		out.OutputGapSeconds = elapsed(now, r.lastUseful)
		out.ReceivedBytes = r.received
		if !r.lastByte.IsZero() {
			t := r.lastByte.UTC()
			out.LastByteAt = &t
		}
	} else {
		out.DurationSeconds = r.writeWait(now).Seconds()
		out.PlannedBytes = r.planned
		out.WrittenBytes = r.written
		out.Operation = "write"
		out.WriteSeconds = out.DurationSeconds
		if !r.flushStart.IsZero() {
			out.Operation = "flush"
			writeEnd := r.writeReturnedAt
			if writeEnd.IsZero() {
				writeEnd = r.flushStart
			}
			out.WriteSeconds = elapsed(writeEnd, r.writeStart)
			flushEnd := r.flushReturnedAt
			if flushEnd.IsZero() {
				flushEnd = now
			}
			out.FlushSeconds = elapsed(flushEnd, r.flushStart)
		}
	}
	return out
}
func (r *incidentRequest) check(now time.Time) {
	if !r.terminal && !r.unknown && !r.lastUseful.IsZero() {
		wait := r.readWait
		if r.reading {
			wait += now.Sub(r.readStart)
		}
		if wait > incidentThreshold && r.stall == nil {
			r.stall = r.newRecord("output_stall", r.lastUseful, now)
			r.captureIncident(r.stall, now, "incident_trigger", "")
			r.store.enqueue(r.snapshot(r.stall, now))
		}
	}
	if !r.writeStart.IsZero() && r.writeWait(now) > incidentThreshold && r.write == nil {
		r.write = r.newRecord("client_write_stall", r.writeStart, now)
		r.captureIncident(r.write, now, "incident_trigger", "")
		r.store.enqueue(r.snapshot(r.write, now))
	}
}

func (r *incidentRequest) writeWait(now time.Time) time.Duration {
	if r.writeReturnedAt.IsZero() {
		return max(0, now.Sub(r.writeStart))
	}
	wait := max(0, r.writeReturnedAt.Sub(r.writeStart))
	if !r.flushStart.IsZero() {
		end := r.flushReturnedAt
		if end.IsZero() {
			end = now
		}
		wait += max(0, end.Sub(r.flushStart))
	}
	return wait
}
func (r *incidentRequest) endRecord(v **streamIncident, now time.Time, reason string) {
	if *v == nil {
		return
	}
	out := r.snapshot(*v, now)
	t := now.UTC()
	out.EndedAt = &t
	out.State = "ended"
	out.EndReason = reason
	r.captureIncident(*v, now, "incident_end", reason)
	r.store.enqueue(out)
	*v = nil
}
func (r *incidentRequest) startRead(now time.Time) {
	r.mutate(func() { r.reading = true; r.readStart = now })
}
func (r *incidentRequest) readReturned(now time.Time, n int) {
	r.mutate(func() {
		if r.reading {
			r.readWait += max(0, now.Sub(r.readStart))
			r.reading = false
		}
		if n > 0 {
			r.lastByte = now
			r.received += int64(n)
			r.responseOffset += int64(n)
		}
		r.check(now)
	})
}

func (r *incidentRequest) captureIncident(v *streamIncident, now time.Time, eventType, reason string) {
	if r.capture == nil {
		return
	}
	e := captureEvent{Type: eventType, At: now, IncidentID: v.ID, Reason: reason,
		Phase: v.Kind, Start: r.responseOffset, End: r.responseOffset,
		LastEventIndex: r.lastEvent.Index, LastEventStart: r.lastEvent.Start, LastEventEnd: r.lastEvent.End,
		LastEventKnown: r.lastEvent.Known, LastUsefulEventIndex: r.lastUsefulEvent.Index,
		LastUsefulStart: r.lastUsefulEvent.Start, LastUsefulEnd: r.lastUsefulEvent.End,
		LastUsefulAt: r.lastUsefulEvent.At, PositionKnown: !r.unknown}
	e.StartedAt = v.StartedAt
	if v.Kind == "output_stall" {
		e.ReadWaitSeconds = r.snapshot(v, now).ReadWaitSeconds
		if reason == "output_resumed" {
			e.HasRange = true
			e.Start = r.lastEvent.Start
			e.End = r.lastEvent.End
			e.EventIndex = r.lastEvent.Index
		}
	} else {
		e.WriteStartedAt = r.writeStart
		e.FlushStartedAt = r.flushStart
		e.FlushReturned = !r.flushReturnedAt.IsZero()
		e.Origin = r.writePosition.Origin
		e.Phase = r.writePosition.Phase
		e.IOSequence = r.writePosition.IOSequence
		e.HasRange = r.writePosition.HasRange
		e.Start = r.writePosition.Start
		e.End = r.writePosition.End
		e.PlannedBytes = r.planned
		e.TotalWritten = r.writePosition.TotalWritten
		if r.written != nil {
			e.WrittenKnown = true
			e.WrittenBytes = *r.written
		}
	}
	if eventType == "incident_trigger" {
		r.capture.trigger(v.ID, v.Kind, now, e)
	} else {
		r.capture.event(e)
	}
}

func (r *incidentRequest) eventPosition(p sseEventPosition) {
	r.mutate(func() {
		r.lastEvent = p
		if p.Known && p.Useful {
			r.endRecord(&r.stall, p.At, "output_resumed")
			r.lastUsefulEvent = p
			r.lastUseful = p.At
			r.readWait = 0
			r.received = 0
		}
	})
}

func (r *incidentRequest) setWritePosition(e captureEvent) { r.mutate(func() { r.writePosition = e }) }

func (r *incidentRequest) observe(now time.Time, o *sseObserver) {
	r.mutate(func() {
		if o.Unknown || o.Invalid {
			r.unknown = true
			r.endRecord(&r.stall, now, "observation_unknown")
		}
		if !o.LastUsefulAt.IsZero() && !o.LastUsefulAt.Equal(r.lastUseful) {
			r.endRecord(&r.stall, now, "output_resumed")
			r.lastUseful = o.LastUsefulAt
			r.readWait = 0
			r.received = 0
		}
		if o.HasError || o.valid() {
			r.terminal = true
			reason := "generation_completed"
			if o.HasError {
				reason = "upstream_error"
			}
			r.endRecord(&r.stall, now, reason)
		}
	})
}
func (r *incidentRequest) startWrite(now time.Time, n int) {
	r.mutate(func() {
		r.writeStart = now
		r.flushStart = time.Time{}
		r.writeReturnedAt = time.Time{}
		r.flushReturnedAt = time.Time{}
		r.written = nil
		r.planned = n
	})
}
func (r *incidentRequest) startFlush(now time.Time, n int) {
	r.mutate(func() {
		r.flushStart = now
		v := n
		r.written = &v
		if r.writePosition.Phase == "body_write" {
			r.writePosition.Phase = "body_flush"
		}
		r.check(now)
	})
}
func (r *incidentRequest) endWrite(now time.Time, err error) {
	r.mutate(func() {
		r.check(now)
		reason := "write_resumed"
		if err != nil {
			reason = "write_error"
			var timeout net.Error
			if errors.Is(err, context.Canceled) {
				reason = "client_cancelled"
			} else if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
				reason = "timeout"
			}
		}
		r.endRecord(&r.write, now, reason)
		r.writeStart = time.Time{}
		r.flushStart = time.Time{}
		r.writeReturnedAt = time.Time{}
		r.flushReturnedAt = time.Time{}
	})
}

func (r *incidentRequest) wrote(n int) {
	r.mutate(func() { v := n; r.written = &v })
}

func (r *incidentRequest) wroteAt(at time.Time, n int) {
	r.mutate(func() { v := n; r.written = &v; r.writeReturnedAt = at })
}
func (r *incidentRequest) flushedAt(at time.Time) { r.mutate(func() { r.flushReturnedAt = at }) }
func (r *incidentRequest) finish(now time.Time, reason string) {
	r.mutate(func() {
		r.check(now)
		r.endRecord(&r.stall, now, reason)
		r.endRecord(&r.write, now, reason)
		delete(r.store.active, r)
	})
}

func (s *incidentStore) recordPath(v streamIncident) string {
	return filepath.Join(s.root, v.TriggeredAt.UTC().Format("2006010215"), fmt.Sprintf("%019d-%s.json", v.TriggeredAt.UnixNano(), v.ID))
}
func (s *incidentStore) save(v streamIncident) error {
	s.diskMu.Lock()
	defer s.diskMu.Unlock()
	if v.TriggeredAt.Before(time.Now().Add(-s.retention)) {
		return nil
	}
	path := s.recordPath(v)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	data := encodeJSON(v)
	if len(data) > 16384 {
		return fmt.Errorf("incident too large")
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}
func incidentDirs(root string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	out := entries[:0]
	for _, e := range entries {
		if !e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			continue
		}
		if _, err := time.Parse("2006010215", e.Name()); err == nil && len(e.Name()) == 10 {
			out = append(out, e)
		}
	}
	return out, nil
}
func (s *incidentStore) cleanup(now time.Time) error {
	s.diskMu.Lock()
	defer s.diskMu.Unlock()
	dirs, err := incidentDirs(s.root)
	if err != nil {
		return err
	}
	type file struct {
		path string
		size int64
		at   time.Time
	}
	files := []file{}
	var total int64
	for _, d := range dirs {
		entries, e := os.ReadDir(filepath.Join(s.root, d.Name()))
		if e != nil {
			return e
		}
		for _, f := range entries {
			if !f.Type().IsRegular() {
				continue
			}
			path := filepath.Join(s.root, d.Name(), f.Name())
			if strings.HasSuffix(f.Name(), ".tmp") {
				if e = os.Remove(path); e != nil {
					return e
				}
				continue
			}
			if !strings.HasSuffix(f.Name(), ".json") || len(f.Name()) < 20 {
				continue
			}
			stamp, e := strconv.ParseInt(f.Name()[:19], 10, 64)
			if e != nil {
				continue
			}
			info, e := f.Info()
			if e != nil {
				return e
			}
			files = append(files, file{path, info.Size(), time.Unix(0, stamp)})
			total += info.Size()
		}
	}
	// ReadDir traversals already sort timestamp-prefixed directories and files.
	for _, f := range files {
		if f.at.Before(now.Add(-s.retention)) || total > s.maxDisk {
			if err = os.Remove(f.path); err != nil {
				return err
			}
			total -= f.size
		}
	}
	for _, d := range dirs {
		_ = os.Remove(filepath.Join(s.root, d.Name()))
	} // removes empty directories only
	return nil
}
func (s *incidentStore) run() {
	defer close(s.done)
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case v, ok := <-s.queue:
			if !ok {
				return
			}
			if err := s.save(v); err != nil {
				s.failures.Add(1)
				s.logger.Print(`{"event":"stream_incident_write_failed"}`)
			}
		case now := <-t.C:
			if err := s.cleanup(now); err != nil {
				s.failures.Add(1)
				s.logger.Print(`{"event":"stream_incident_cleanup_failed"}`)
			}
		}
	}
}

type incidentCursor struct {
	Time  time.Time `json:"time"`
	ID    string    `json:"id"`
	Query string    `json:"query"`
}

func incidentBefore(a, b streamIncident) bool {
	if a.TriggeredAt.Equal(b.TriggeredAt) {
		return a.ID > b.ID
	}
	return a.TriggeredAt.After(b.TriggeredAt)
}
func (s *incidentStore) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", 405)
		return
	}
	q := r.URL.Query()
	start, e1 := strconv.ParseFloat(q.Get("start"), 64)
	end, e2 := strconv.ParseFloat(q.Get("end"), 64)
	keyword := strings.TrimSpace(q.Get("keyword"))
	limit := 50
	var err error
	if q.Has("limit") {
		limit, err = strconv.Atoi(q.Get("limit"))
	}
	if e1 != nil || e2 != nil || err != nil || !(start > 0 && start < end && end <= float64(time.Now().Unix())+5) || end-start > 30*86400 || utf8.RuneCountInString(keyword) > 256 || limit < 1 || limit > 100 {
		http.Error(w, "invalid incident query", 400)
		return
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%g/%g/%s", start, end, strings.ToLower(keyword)))))
	var cursor incidentCursor
	if raw := q.Get("cursor"); raw != "" {
		b, e := base64.RawURLEncoding.DecodeString(raw)
		if len(raw) > 1024 || e != nil || json.Unmarshal(b, &cursor) != nil || cursor.Query != digest || cursor.Time.IsZero() || cursor.ID == "" {
			http.Error(w, "invalid cursor", 400)
			return
		}
	}
	select {
	case s.querySlots <- struct{}{}:
		defer func() { <-s.querySlots }()
	default:
		http.Error(w, "incident queries busy", 429)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	timedOut := func() bool {
		if ctx.Err() == nil {
			return false
		}
		http.Error(w, "incident query timed out", http.StatusServiceUnavailable)
		return true
	}
	if timedOut() {
		return
	}
	now := time.Now()
	live := map[string]streamIncident{}
	s.mu.Lock()
	for tracker := range s.active {
		for _, v := range []*streamIncident{tracker.stall, tracker.write} {
			if v != nil {
				live[v.ID] = tracker.snapshot(v, now)
			}
		}
	}
	s.mu.Unlock()
	rows := make([]streamIncident, 0, limit+1)
	seen := map[string]bool{}
	incomplete := false
	add := func(v streamIncident) {
		ts := float64(v.TriggeredAt.UnixNano()) / 1e9
		if ts < start || ts >= end || v.TriggeredAt.Before(now.Add(-s.retention)) {
			return
		}
		if cursor.ID != "" && !incidentBefore(streamIncident{ID: cursor.ID, TriggeredAt: cursor.Time}, v) {
			return
		}
		if keyword != "" && !strings.Contains(strings.ToLower(v.QuestionHead), strings.ToLower(keyword)) && !strings.Contains(strings.ToLower(v.QuestionTail), strings.ToLower(keyword)) {
			return
		}
		rows = append(rows, v)
		sort.Slice(rows, func(i, j int) bool { return incidentBefore(rows[i], rows[j]) })
		if len(rows) > limit+1 {
			rows = rows[:limit+1]
		}
	}
	dirs, e := incidentDirs(s.root)
	if e != nil {
		http.Error(w, "incident storage unavailable", 503)
		return
	}
	for _, d := range dirs {
		hour, _ := time.Parse("2006010215", d.Name())
		if timedOut() {
			return
		}
		if float64(hour.Add(time.Hour).Unix()) <= start || float64(hour.Unix()) >= end {
			continue
		}
		entries, e := os.ReadDir(filepath.Join(s.root, d.Name()))
		if e != nil {
			if !os.IsNotExist(e) {
				incomplete = true
			}
			continue
		}
		for _, f := range entries {
			if timedOut() {
				return
			}
			if !f.Type().IsRegular() || !strings.HasSuffix(f.Name(), ".json") {
				continue
			}
			file, e := os.Open(filepath.Join(s.root, d.Name(), f.Name()))
			if e != nil {
				if !os.IsNotExist(e) {
					incomplete = true
				}
				continue
			}
			data, e := io.ReadAll(io.LimitReader(file, 16385))
			file.Close()
			var v streamIncident
			if e != nil || len(data) > 16384 || json.Unmarshal(data, &v) != nil {
				incomplete = true
				continue
			}
			if current, ok := live[v.ID]; ok {
				v = current
				seen[v.ID] = true
			} else if v.State == "ongoing" {
				v.State = "unknown"
				v.EndReason = "end_unobserved"
			}
			add(v)
		}
	}
	for id, v := range live {
		if timedOut() {
			return
		}
		if !seen[id] {
			add(v)
		}
	}
	if timedOut() {
		return
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		v := rows[len(rows)-1]
		next = base64.RawURLEncoding.EncodeToString(encodeJSON(incidentCursor{v.TriggeredAt, v.ID, digest}))
	}
	json.NewEncoder(w).Encode(map[string]any{"records": rows, "next_cursor": next, "retained_since": now.Add(-s.retention), "retention_hours": s.retention.Hours(), "window_partially_expired": start < float64(now.Add(-s.retention).Unix()), "incomplete": incomplete, "dropped_total": s.dropped.Load(), "write_errors_total": s.failures.Load()})
}
