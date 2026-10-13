package aigate

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	streamCaptureMemory    = int64(64 << 20)
	streamCaptureHandles   = 1024
	streamCaptureQueue     = 1024
	streamCaptureDisk      = int64(10 << 30)
	streamCaptureRetention = 72 * time.Hour
	captureEventCharge     = int64(4096) // Includes fixed event and JSON encoding scratch space.
	captureMetaLimit       = 16 << 10
	captureMetaReserve     = int64(2 * captureMetaLimit)
)

// captureEvent contains only positions and bounded diagnostics. Data is reserved
// for a gateway-generated error, never an upstream response or request body.
type captureEvent struct {
	Type                 string    `json:"type"`
	Origin               string    `json:"origin,omitempty"`
	Phase                string    `json:"phase,omitempty"`
	IncidentID           string    `json:"incident_id,omitempty"`
	Reason               string    `json:"reason,omitempty"`
	Error                string    `json:"error,omitempty"`
	At                   time.Time `json:"at"`
	StartedAt            time.Time `json:"started_at,omitempty"`
	WriteStartedAt       time.Time `json:"write_started_at,omitempty"`
	FlushStartedAt       time.Time `json:"flush_started_at,omitempty"`
	FlushReturned        bool      `json:"flush_returned"`
	HasRange             bool      `json:"has_range"`
	Start                int64     `json:"start,omitempty"`
	End                  int64     `json:"end,omitempty"`
	EventIndex           uint64    `json:"event_index,omitempty"`
	IOSequence           uint64    `json:"io_sequence,omitempty"`
	PlannedBytes         int       `json:"planned_bytes,omitempty"`
	WrittenBytes         int       `json:"written_bytes,omitempty"`
	WrittenKnown         bool      `json:"written_known"`
	TotalWritten         int64     `json:"total_written,omitempty"`
	ReadWaitSeconds      float64   `json:"read_wait_seconds,omitempty"`
	LastEventIndex       uint64    `json:"last_event_index,omitempty"`
	LastEventStart       int64     `json:"last_event_start,omitempty"`
	LastEventEnd         int64     `json:"last_event_end,omitempty"`
	LastEventKnown       bool      `json:"last_event_known"`
	LastUsefulEventIndex uint64    `json:"last_useful_event_index,omitempty"`
	LastUsefulStart      int64     `json:"last_useful_start,omitempty"`
	LastUsefulEnd        int64     `json:"last_useful_end,omitempty"`
	LastUsefulAt         time.Time `json:"last_useful_at,omitempty"`
	PositionKnown        bool      `json:"position_known"`
	Data                 []byte    `json:"data,omitempty"`
}

type captureTimelineEvent struct {
	Schema          int     `json:"schema_version"`
	Sequence        uint64  `json:"sequence"`
	RelativeSeconds float64 `json:"relative_seconds"`
	captureEvent
}

// time.Time does not honor omitempty on older supported Go versions. Shadow
// optional timestamps with pointers to omit absent values without changing the
// event API or retaining a second event copy while encoding.
type captureEventWire captureEvent
type captureTimelineWire struct {
	Schema          int     `json:"schema_version"`
	Sequence        uint64  `json:"sequence"`
	RelativeSeconds float64 `json:"relative_seconds"`
	*captureEventWire
	StartedAt      *time.Time `json:"started_at,omitempty"`
	WriteStartedAt *time.Time `json:"write_started_at,omitempty"`
	FlushStartedAt *time.Time `json:"flush_started_at,omitempty"`
	LastUsefulAt   *time.Time `json:"last_useful_at,omitempty"`
}

func captureWire(event *captureTimelineEvent) captureTimelineWire {
	optional := func(at *time.Time) *time.Time {
		if at.IsZero() {
			return nil
		}
		return at
	}
	return captureTimelineWire{Schema: event.Schema, Sequence: event.Sequence, RelativeSeconds: event.RelativeSeconds,
		captureEventWire: (*captureEventWire)(&event.captureEvent), StartedAt: optional(&event.StartedAt),
		WriteStartedAt: optional(&event.WriteStartedAt), FlushStartedAt: optional(&event.FlushStartedAt), LastUsefulAt: optional(&event.LastUsefulAt)}
}

type streamCaptureMeta struct {
	Schema                       int        `json:"schema_version"`
	RequestID                    string     `json:"request_id"`
	ServiceID                    string     `json:"service_id"`
	RouterID                     string     `json:"router_id"`
	BootID                       string     `json:"boot_id"`
	StartedAt                    time.Time  `json:"started_at"`
	EndedAt                      *time.Time `json:"ended_at,omitempty"`
	Retained                     bool       `json:"retained"`
	FirstIncidentID              string     `json:"first_incident_id,omitempty"`
	LastIncidentID               string     `json:"last_incident_id,omitempty"`
	IncidentCount                uint64     `json:"incident_count"`
	TriggeredAt                  *time.Time `json:"triggered_at,omitempty"`
	ResponseStatus               int        `json:"response_status"`
	ContentType                  string     `json:"content_type,omitempty"`
	ContentEncoding              string     `json:"content_encoding,omitempty"`
	DecodeOrigin                 string     `json:"decode_origin,omitempty"`
	Decoded                      bool       `json:"decoded"`
	CaptureState                 string     `json:"capture_state"`
	Outcome                      string     `json:"outcome,omitempty"`
	EOF                          bool       `json:"eof"`
	Done                         bool       `json:"done"`
	RequestExpectedBytes         int64      `json:"request_expected_bytes"`
	RequestSaved                 bool       `json:"request_saved"`
	RequestSavedBytes            int64      `json:"request_saved_bytes"`
	RequestSHA256                string     `json:"request_sha256,omitempty"`
	ResponseReceivedBytes        int64      `json:"response_received_bytes"`
	ResponseSavedBytes           int64      `json:"response_saved_bytes"`
	ResponseSyncedBytes          int64      `json:"response_synced_bytes"`
	ResponseSHA256               string     `json:"response_sha256,omitempty"`
	TimelineSavedBytes           int64      `json:"timeline_saved_bytes"`
	TimelineSyncedBytes          int64      `json:"timeline_synced_bytes"`
	TimelineCompleteBytes        int64      `json:"timeline_complete_bytes"`
	TimelineSHA256               string     `json:"timeline_sha256,omitempty"`
	LastTimelineSequence         uint64     `json:"last_timeline_sequence"`
	FirstMissingResponseOffset   *int64     `json:"first_missing_response_offset,omitempty"`
	FirstMissingTimelineSequence *uint64    `json:"first_missing_timeline_sequence,omitempty"`
	FailureReason                string     `json:"failure_reason,omitempty"`
}

// Only the worker accesses captureDisk, including the directory registry and
// disk counters. The request path never acquires a filesystem-related lock.
type captureDisk struct {
	path                                           string
	response, timeline                             *os.File
	responseHash, timelineHash                     hash.Hash
	responseBytes, timelineBytes, timelineComplete int64
	responseSynced, timelineSynced                 int64
	lastSequence                                   uint64
	requestSaved                                   bool
	requestBytes                                   int64
	requestHash                                    string
	metaBytes                                      int64
	reserved                                       bool
	promoted                                       bool
	failed                                         bool
	final                                          bool
	lastSync                                       time.Time
}

type streamCapture struct {
	store                               *streamCaptureStore
	id                                  string
	started                             time.Time
	raw                                 []byte
	rawCharge                           int64
	requestBytes                        int64
	status                              int
	contentType, encoding, decodeOrigin string
	decoded                             bool
	firstIncident, lastIncident         string
	incidentCount                       uint64
	triggeredAt                         time.Time
	endedAt                             time.Time
	outcome                             string
	eof, done                           bool
	retained, ended, stopped, released  bool
	expired, storageAbandoned           bool
	state, failure                      string
	sequence                            uint64
	received, accepted                  int64
	missingResponse                     *int64
	missingSequence                     *uint64
	pending                             int
	disk                                *captureDisk
}

type captureJob struct {
	capture *streamCapture
	data    []byte
	start   int64
	event   *captureTimelineEvent
	charge  int64
}

type captureStored struct {
	path   string
	at     time.Time
	bytes  int64
	active bool
	safe   bool
}

type streamCaptureStore struct {
	querySlots                      chan struct{}
	mu                              sync.Mutex
	root, serviceID, routerID, boot string
	audit                           *log.Logger
	directory                       *os.File
	active                          map[*streamCapture]bool
	queue                           chan captureJob
	wake                            chan struct{}
	done                            chan struct{}
	once                            sync.Once
	closed                          bool
	usedBytes                       int64
	queued                          int
	maxMemory                       int64
	maxHandles, maxQueue            int
	maxDisk                         int64
	retention                       time.Duration
	stored                          map[string]*captureStored
	diskBytes, diskReserved         int64
	lastCleanup                     time.Time
	dropped, failures               atomic.Uint64
	peakBytes                       atomic.Int64
	// Injected by local fault-isolation tests before starting requests.
	writePayload func(*os.File, []byte) (int, error)
}

func newStreamCaptureStore(root, serviceID, routerID, boot string, audit *log.Logger) (*streamCaptureStore, error) {
	if audit == nil {
		audit = log.New(io.Discard, "", 0)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("invalid stream capture directory")
	}
	directory, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	if err = lockState(directory); err != nil {
		directory.Close()
		return nil, err
	}
	s := &streamCaptureStore{querySlots: make(chan struct{}, 2), root: root, serviceID: serviceID, routerID: routerID, boot: boot, audit: audit, directory: directory,
		active: make(map[*streamCapture]bool), queue: make(chan captureJob, streamCaptureQueue), wake: make(chan struct{}, 1), done: make(chan struct{}),
		maxMemory: streamCaptureMemory, maxHandles: streamCaptureHandles, maxQueue: streamCaptureQueue, maxDisk: streamCaptureDisk,
		retention: streamCaptureRetention, stored: make(map[string]*captureStored), writePayload: func(f *os.File, p []byte) (int, error) { return f.Write(p) }}
	if err = s.recoverStored(time.Now()); err != nil {
		unlockState(directory)
		directory.Close()
		return nil, err
	}
	go s.run()
	return s, nil
}

func captureString(value string, limit int) string {
	if len(value) > limit {
		value = value[:limit]
	}
	return strings.Clone(value)
}

func (s *streamCaptureStore) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *streamCaptureStore) begin(requestID string, raw []byte, started time.Time) *streamCapture {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.active) >= s.maxHandles || !slowRequestID(requestID) {
		s.dropped.Add(1)
		return nil
	}
	c := &streamCapture{store: s, id: requestID, started: started, requestBytes: int64(len(raw)), state: "recording"}
	s.active[c] = true
	charge := int64(cap(raw))
	if charge > s.maxMemory-s.usedBytes {
		c.freezeLocked("request_memory_full", "failed", 0)
	} else {
		c.raw = raw
		c.rawCharge = charge
		s.usedBytes += charge
		s.notePeakLocked()
	}
	return c
}

func (c *streamCapture) setResponse(status int, contentType, encoding, decodeOrigin string, decoded bool) {
	if c == nil {
		return
	}
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	if c.released {
		return
	}
	c.status, c.contentType, c.encoding, c.decodeOrigin, c.decoded = status, captureString(contentType, 256), captureString(encoding, 128), captureString(decodeOrigin, 32), decoded
}

func (c *streamCapture) freezeLocked(reason, state string, sequence uint64) {
	if !c.stopped {
		c.stopped, c.failure, c.state = true, reason, state
		missing := c.accepted
		c.missingResponse = &missing
		c.store.dropped.Add(1)
	}
	if sequence > 0 && c.missingSequence == nil {
		n := sequence
		c.missingSequence = &n
	}
}

func (c *streamCapture) reserveLocked(charge int64, sequence uint64) bool {
	s := c.store
	if c.released || s.closed {
		return false
	}
	if c.stopped {
		if sequence > 0 && c.missingSequence == nil {
			n := sequence
			c.missingSequence = &n
		}
		return false
	}
	if s.queued >= s.maxQueue {
		c.freezeLocked("queue_full", "truncated", sequence)
		return false
	}
	if charge > s.maxMemory-s.usedBytes {
		c.freezeLocked("memory_full", "truncated", sequence)
		return false
	}
	s.queued++
	s.usedBytes += charge
	s.notePeakLocked()
	c.pending++
	return true
}

func (s *streamCaptureStore) notePeakLocked() {
	if s.usedBytes > s.peakBytes.Load() {
		s.peakBytes.Store(s.usedBytes)
	}
}

func (c *streamCapture) enqueueLocked(job captureJob, sequence uint64) bool {
	select {
	case c.store.queue <- job:
		return true
	default:
		c.store.usedBytes -= job.charge
		c.store.queued--
		c.pending--
		c.freezeLocked("queue_full", "truncated", sequence)
		c.store.signal()
		return false
	}
}

func (c *streamCapture) response(at time.Time, start int64, data []byte) {
	if c == nil {
		return
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.released {
		return
	}
	c.received = max(c.received, start+int64(len(data)))
	if c.stopped || len(data) == 0 {
		return
	}
	if start != c.accepted || start < 0 {
		c.freezeLocked("response_offset_gap", "truncated", 0)
		s.signal()
		return
	}
	charge := int64(len(data))
	if !c.reserveLocked(charge, 0) {
		s.signal()
		return
	}
	copyOfData := make([]byte, len(data))
	copy(copyOfData, data)
	// The slot was reserved before allocation. Every producer holds mu and
	// queued includes the writing job, so this send cannot wait for the worker.
	if c.enqueueLocked(captureJob{capture: c, start: start, data: copyOfData, charge: charge}, 0) {
		c.accepted += int64(len(data))
	}
}

func captureEventCost(event captureEvent) int64 {
	// Escaped strings can expand sixfold; Marshal also owns encoder scratch.
	return captureEventCharge + 14*int64(min(len(event.Type), 64)+min(len(event.Origin), 32)+min(len(event.Phase), 64)+min(len(event.IncidentID), 128)+min(len(event.Reason), 256)+min(len(event.Error), 1024)) + 4*int64(len(event.Data))
}

func (c *streamCapture) eventLocked(event captureEvent) {
	c.sequence++
	sequence := c.sequence
	if len(event.Data) > 16<<10 {
		c.freezeLocked("gateway_event_too_large", "truncated", sequence)
		c.store.signal()
		return
	}
	charge := captureEventCost(event)
	if !c.reserveLocked(charge, sequence) {
		c.store.signal()
		return
	}
	event.Type, event.Origin, event.Phase = captureString(event.Type, 64), captureString(event.Origin, 32), captureString(event.Phase, 64)
	event.IncidentID, event.Reason, event.Error = captureString(event.IncidentID, 128), captureString(event.Reason, 256), captureString(event.Error, 1024)
	if len(event.Data) > 0 {
		data := make([]byte, len(event.Data))
		copy(data, event.Data)
		event.Data = data
	}
	relative := max(0, event.At.Sub(c.started).Seconds())
	event.At = event.At.UTC()
	event.StartedAt = event.StartedAt.UTC()
	event.WriteStartedAt = event.WriteStartedAt.UTC()
	event.FlushStartedAt = event.FlushStartedAt.UTC()
	event.LastUsefulAt = event.LastUsefulAt.UTC()
	line := &captureTimelineEvent{Schema: 1, Sequence: sequence, RelativeSeconds: relative, captureEvent: event}
	c.enqueueLocked(captureJob{capture: c, event: line, charge: charge}, sequence)
}

func (c *streamCapture) event(event captureEvent) {
	if c == nil {
		return
	}
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	if c.released || c.ended {
		return
	}
	c.eventLocked(event)
}

func (c *streamCapture) trigger(id, kind string, at time.Time, details ...captureEvent) {
	if c == nil {
		return
	}
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	if c.released || c.ended {
		return
	}
	if !c.retained {
		c.retained, c.firstIncident, c.triggeredAt = true, captureString(id, 128), at
	}
	c.lastIncident, c.incidentCount = captureString(id, 128), c.incidentCount+1
	event := captureEvent{}
	if len(details) > 0 {
		event = details[0]
	}
	event.Type, event.IncidentID, event.Reason, event.At = "incident_trigger", id, kind, at
	c.eventLocked(event)
	c.store.signal()
}

func (c *streamCapture) releaseRawLocked() {
	c.store.usedBytes -= c.rawCharge
	c.raw, c.rawCharge = nil, 0
}

func (c *streamCapture) finish(at time.Time, outcome string, eof, done bool) {
	if c == nil {
		return
	}
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	if c.released || c.ended {
		return
	}
	c.eventLocked(captureEvent{Type: "end", At: at, Reason: outcome})
	c.ended, c.endedAt, c.outcome, c.eof, c.done = true, at, captureString(outcome, 128), eof, done
	if !c.retained {
		c.releaseRawLocked()
	}
	c.store.signal()
}

func (s *streamCaptureStore) close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		for c := range s.active {
			if !c.ended {
				c.ended, c.endedAt, c.state, c.failure = true, time.Now(), "end_unknown", "shutdown_without_stream_end"
			}
		}
		close(s.queue)
		s.mu.Unlock()
		<-s.done
	})
}

func (s *streamCaptureStore) run() {
	ticker := time.NewTicker(time.Second)
	defer func() { ticker.Stop(); unlockState(s.directory); s.directory.Close(); close(s.done) }()
	for {
		select {
		case job, ok := <-s.queue:
			if !ok {
				s.reconcileAll(time.Now(), true)
				return
			}
			s.safeWork(job.capture, func() error { return s.process(job) })
			s.mu.Lock()
			s.usedBytes -= job.charge
			s.queued--
			job.capture.pending--
			s.mu.Unlock()
			s.safeWork(job.capture, func() error { return s.reconcile(job.capture, time.Now(), false) })
		case <-s.wake:
			s.reconcileAll(time.Now(), false)
		case now := <-ticker.C:
			s.reconcileAll(now, false)
		}
	}
}

func (s *streamCaptureStore) safeWork(c *streamCapture, work func() error) {
	var err error
	func() {
		defer func() {
			if recover() != nil {
				err = errors.New("capture_worker_panic")
			}
		}()
		err = work()
	}()
	if err == nil {
		return
	}
	s.failures.Add(1)
	s.mu.Lock()
	reason := "storage_write_failed"
	if err.Error() == "capture_worker_panic" {
		reason = "capture_worker_panic"
	}
	if err.Error() == "capture_disk_full" {
		reason = "disk_budget_exhausted"
	}
	c.freezeLocked(reason, "truncated", 0)
	if c.disk != nil {
		c.disk.failed = true
		missing := c.disk.responseBytes
		c.missingResponse = &missing
	} else {
		c.storageAbandoned = true
	}
	c.releaseRawLocked()
	final := c.ended && c.pending == 0
	s.mu.Unlock()
	if final {
		if c.disk != nil {
			s.closeDisk(c.disk)
			c.disk.final = true
			if stored := s.stored[c.disk.path]; stored != nil {
				stored.active = false
			}
		}
		s.release(c)
	}
	s.audit.Print(string(encodeJSON(map[string]any{"event": "stream_capture_failed", "request_id": c.id, "reason": reason})))
}

func (s *streamCaptureStore) reconcileAll(now time.Time, closing bool) {
	s.mu.Lock()
	active := make([]*streamCapture, 0, len(s.active))
	for c := range s.active {
		active = append(active, c)
	}
	s.mu.Unlock()
	for _, c := range active {
		s.safeWork(c, func() error { return s.reconcile(c, now, closing) })
	}
	if now.Sub(s.lastCleanup) >= time.Minute {
		if err := s.cleanup(now, 0); err != nil {
			s.failures.Add(1)
		}
		s.lastCleanup = now
	}
}

func (s *streamCaptureStore) process(job captureJob) error {
	c := job.capture
	s.mu.Lock()
	discard := c.released || c.expired || c.storageAbandoned || (c.ended && !c.retained)
	s.mu.Unlock()
	if discard {
		return nil
	}
	eventSaved := false
	if job.event != nil {
		defer func() {
			if !eventSaved {
				s.mu.Lock()
				if c.missingSequence == nil || job.event.Sequence < *c.missingSequence {
					seq := job.event.Sequence
					c.missingSequence = &seq
				}
				s.mu.Unlock()
			}
		}()
	}
	if err := s.ensureDisk(c); err != nil {
		return err
	}
	d := c.disk
	if d.failed || d.final {
		return nil
	}
	if job.event != nil {
		data, err := json.Marshal(captureWire(job.event))
		if err != nil {
			return err
		}
		data = append(data, '\n')
		if len(data) > 64<<10 {
			return errors.New("timeline_event_too_large")
		}
		if err := s.capacity(int64(len(data))); err != nil {
			return err
		}
		n, err := s.writePayload(d.timeline, data)
		if n > 0 {
			d.timelineHash.Write(data[:n])
			d.timelineBytes += int64(n)
			s.account(d.path, int64(n))
		}
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		if err != nil {
			s.mu.Lock()
			seq := job.event.Sequence
			if c.missingSequence == nil {
				c.missingSequence = &seq
			}
			s.mu.Unlock()
			return err
		}
		d.timelineComplete, d.lastSequence = d.timelineBytes, job.event.Sequence
		eventSaved = true
		return nil
	}
	if job.start != d.responseBytes {
		return errors.New("response_prefix_gap")
	}
	if err := s.capacity(int64(len(job.data))); err != nil {
		return err
	}
	n, err := s.writePayload(d.response, job.data)
	if n > 0 {
		d.responseHash.Write(job.data[:n])
		d.responseBytes += int64(n)
		s.account(d.path, int64(n))
	}
	if err == nil && n != len(job.data) {
		err = io.ErrShortWrite
	}
	return err
}

func (s *streamCaptureStore) ensureDisk(c *streamCapture) error {
	if c.disk != nil {
		return nil
	}
	if err := s.capacity(captureMetaReserve); err != nil {
		return err
	}
	path := filepath.Join(s.root, ".pending-"+c.id)
	if err := os.Mkdir(path, 0700); err != nil {
		return err
	}
	d := &captureDisk{path: path, responseHash: sha256.New(), timelineHash: sha256.New(), reserved: true}
	c.disk = d
	s.diskReserved += captureMetaReserve
	s.stored[path] = &captureStored{path: path, at: c.started, active: true, safe: true}
	var err error
	d.response, err = os.OpenFile(filepath.Join(path, "response.sse"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	d.timeline, err = os.OpenFile(filepath.Join(path, "timeline.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	// Ordinary temporary streams need no durability checkpoint. A retained
	// request publishes metadata only after its data files have been synced.
	return nil
}

func (s *streamCaptureStore) reconcile(c *streamCapture, now time.Time, closing bool) error {
	s.mu.Lock()
	if c.released {
		s.mu.Unlock()
		return nil
	}
	retained, ended, pending := c.retained, c.ended, c.pending
	if c.storageAbandoned {
		s.mu.Unlock()
		if ended && pending == 0 {
			s.release(c)
		}
		return nil
	}
	at := c.started
	if retained {
		at = c.triggeredAt
	}
	expired := c.expired || (!at.IsZero() && !at.Add(s.retention).After(now))
	if expired {
		c.expired = true
		c.freezeLocked("retention_expired", "truncated", 0)
		c.releaseRawLocked()
	}
	s.mu.Unlock()
	if expired {
		if c.disk != nil && !c.disk.final {
			s.closeDisk(c.disk)
			if err := s.removeRecord(c.disk.path); err != nil {
				return err
			}
			c.disk.final = true
		}
		if ended && pending == 0 {
			s.release(c)
		}
		return nil
	}
	if ended && !retained {
		if pending != 0 {
			return nil
		}
		if c.disk != nil {
			s.closeDisk(c.disk)
			if err := s.removeRecord(c.disk.path); err != nil {
				return err
			}
		}
		s.release(c)
		return nil
	}
	if retained {
		if err := s.ensureDisk(c); err != nil {
			return err
		}
		d := c.disk
		if !d.promoted {
			path := filepath.Join(s.root, "request-"+c.id)
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				return errors.New("capture_record_exists")
			}
			if err := os.Rename(d.path, path); err != nil {
				return err
			}
			stored := s.stored[d.path]
			delete(s.stored, d.path)
			stored.path, stored.at = path, at
			s.stored[path] = stored
			d.path, d.promoted = path, true
			if err := s.directory.Sync(); err != nil {
				return err
			}
		}
		if !d.requestSaved && !d.failed {
			s.mu.Lock()
			raw := c.raw
			s.mu.Unlock()
			if raw != nil {
				if err := s.capacity(int64(len(raw))); err != nil {
					return err
				}
				f, err := os.OpenFile(filepath.Join(d.path, "request.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
				if err != nil {
					return err
				}
				func() {
					defer f.Close()
					var n int
					n, err = s.writePayload(f, raw)
					if n >= 0 && n <= len(raw) {
						h := sha256.Sum256(raw[:n])
						d.requestHash = hex.EncodeToString(h[:])
					}
					if n > 0 {
						d.requestBytes = int64(n)
						s.account(d.path, int64(n))
					}
					if err == nil && n != len(raw) {
						err = io.ErrShortWrite
					}
					if err == nil {
						err = f.Sync()
					}
				}()
				if err != nil {
					return err
				}
				d.requestSaved = true
				s.mu.Lock()
				c.releaseRawLocked()
				s.mu.Unlock()
			}
		}
		final := ended && pending == 0
		if final || d.lastSync.IsZero() || now.Sub(d.lastSync) >= time.Second || closing {
			if err := s.syncRecord(c, final); err != nil {
				if final {
					s.closeDisk(d)
					s.release(c)
				}
				return err
			}
			d.lastSync = now
		}
		if final {
			d.final = true
			s.closeDisk(d)
			s.stored[d.path].active = false
			s.release(c)
		}
	}
	return nil
}

func (s *streamCaptureStore) syncRecord(c *streamCapture, final bool) error {
	d := c.disk
	if d.response != nil {
		if err := d.response.Sync(); err != nil {
			return err
		}
		d.responseSynced = d.responseBytes
	}
	if d.timeline != nil {
		if err := d.timeline.Sync(); err != nil {
			return err
		}
		d.timelineSynced = d.timelineBytes
	}
	return s.publishMeta(c, final)
}

func (s *streamCaptureStore) meta(c *streamCapture, final bool) streamCaptureMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := c.disk
	m := streamCaptureMeta{Schema: 1, RequestID: c.id, ServiceID: s.serviceID, RouterID: s.routerID, BootID: s.boot, StartedAt: c.started.UTC(),
		Retained: c.retained, FirstIncidentID: c.firstIncident, LastIncidentID: c.lastIncident, IncidentCount: c.incidentCount,
		ResponseStatus: c.status, ContentType: c.contentType, ContentEncoding: c.encoding, DecodeOrigin: c.decodeOrigin, Decoded: c.decoded,
		CaptureState: c.state, Outcome: c.outcome, EOF: c.eof, Done: c.done, RequestExpectedBytes: c.requestBytes,
		RequestSaved: d.requestSaved, RequestSavedBytes: d.requestBytes, RequestSHA256: d.requestHash,
		ResponseReceivedBytes: c.received, ResponseSavedBytes: d.responseBytes, ResponseSyncedBytes: d.responseSynced,
		TimelineSavedBytes: d.timelineBytes, TimelineSyncedBytes: d.timelineSynced, TimelineCompleteBytes: d.timelineComplete, LastTimelineSequence: d.lastSequence,
		FirstMissingResponseOffset: c.missingResponse, FirstMissingTimelineSequence: c.missingSequence, FailureReason: c.failure}
	if c.retained {
		t := c.triggeredAt.UTC()
		m.TriggeredAt = &t
	}
	if c.ended {
		t := c.endedAt.UTC()
		m.EndedAt = &t
	}
	if final {
		if !c.stopped && c.state != "end_unknown" {
			if d.requestSaved && d.responseBytes == c.received {
				m.CaptureState = "complete"
			} else {
				m.CaptureState, m.FailureReason = "truncated", "request_or_response_missing"
			}
		}
		m.ResponseSHA256, m.TimelineSHA256 = hex.EncodeToString(d.responseHash.Sum(nil)), hex.EncodeToString(d.timelineHash.Sum(nil))
	}
	return m
}

func (s *streamCaptureStore) publishMeta(c *streamCapture, final bool) error {
	d := c.disk
	data, err := json.Marshal(s.meta(c, final))
	if err != nil {
		return err
	}
	if len(data) > captureMetaLimit {
		return errors.New("capture_metadata_too_large")
	}
	if err := captureAtomicMeta(d.path, data); err != nil {
		return err
	}
	s.account(d.path, int64(len(data))-d.metaBytes)
	d.metaBytes = int64(len(data))
	return nil
}

func captureAtomicMeta(path string, data []byte) error {
	tmp := filepath.Join(path, "meta.json.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { f.Close(); os.Remove(tmp) }()
	if n, err := f.Write(data); err != nil {
		return err
	} else if n != len(data) {
		return io.ErrShortWrite
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(path, "meta.json")); err != nil {
		return err
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (s *streamCaptureStore) closeDisk(d *captureDisk) {
	if d.response != nil {
		d.response.Close()
		d.response = nil
	}
	if d.timeline != nil {
		d.timeline.Close()
		d.timeline = nil
	}
	if d.reserved {
		s.diskReserved -= captureMetaReserve
		d.reserved = false
	}
}

func (s *streamCaptureStore) release(c *streamCapture) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.released {
		return
	}
	c.releaseRawLocked()
	c.released = true
	delete(s.active, c)
	// Completed records are discovered from disk during cleanup; keeping them
	// in this map would let control metadata grow with all 72 hours of traffic.
	if c.disk != nil {
		delete(s.stored, c.disk.path)
	}
}

func (s *streamCaptureStore) account(path string, bytes int64) {
	s.diskBytes += bytes
	if record := s.stored[path]; record != nil {
		record.bytes += bytes
	}
}

func (s *streamCaptureStore) capacity(additional int64) error {
	if s.diskBytes+s.diskReserved+additional <= s.maxDisk {
		return nil
	}
	return s.cleanup(time.Now(), additional)
}

func (s *streamCaptureStore) cleanup(now time.Time, additional int64) error {
	for {
		directory, err := os.Open(s.root)
		if err != nil {
			return err
		}
		// Keep a bounded batch of the oldest safe records, not an index of all
		// retained requests. A second pass is needed only for large evictions.
		records := make([]captureStored, 0, streamCaptureHandles+64)
		for {
			entries, readErr := directory.ReadDir(64)
			for _, entry := range entries {
				if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
					continue
				}
				id := strings.TrimPrefix(entry.Name(), "request-")
				if id == entry.Name() {
					id = strings.TrimPrefix(entry.Name(), ".pending-")
				}
				if !slowRequestID(id) {
					continue
				}
				path := filepath.Join(s.root, entry.Name())
				if live := s.stored[path]; live != nil && live.active {
					continue
				}
				_, size, err := captureRecordFiles(path)
				if err != nil {
					continue
				}
				info, err := entry.Info()
				if err != nil {
					directory.Close()
					return err
				}
				at := info.ModTime()
				data, err := captureReadSmall(filepath.Join(path, "meta.json"), captureMetaLimit)
				var meta streamCaptureMeta
				if err == nil && len(data) <= captureMetaLimit && json.Unmarshal(data, &meta) == nil && meta.Schema == 1 && meta.RequestID == id {
					if !meta.StartedAt.IsZero() {
						at = meta.StartedAt
					}
					if meta.TriggeredAt != nil && !meta.TriggeredAt.IsZero() {
						at = *meta.TriggeredAt
					}
				}
				if !at.Add(s.retention).After(now) {
					if err := s.removeRecord(path); err != nil {
						directory.Close()
						return err
					}
					continue
				}
				records = append(records, captureStored{path: path, at: at, bytes: size, safe: true})
			}
			if len(records) > streamCaptureHandles {
				sort.Slice(records, func(i, j int) bool { return records[i].at.Before(records[j].at) })
				records = records[:streamCaptureHandles]
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				directory.Close()
				return readErr
			}
		}
		directory.Close()
		if s.diskBytes+s.diskReserved+additional <= s.maxDisk {
			return nil
		}
		if len(records) == 0 {
			return errors.New("capture_disk_full")
		}
		sort.Slice(records, func(i, j int) bool { return records[i].at.Before(records[j].at) })
		for _, record := range records {
			if s.diskBytes+s.diskReserved+additional <= s.maxDisk {
				return nil
			}
			if err := s.removeRecord(record.path); err != nil {
				return err
			}
		}
	}
}

func captureRecordFiles(path string) ([]os.DirEntry, int64, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() {
		return nil, 0, errors.New("invalid capture record directory")
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, 0, err
	}
	var size int64
	for _, entry := range entries {
		switch entry.Name() {
		case "request.json", "response.sse", "timeline.jsonl", "meta.json", "meta.json.tmp":
		default:
			return nil, size, errors.New("unknown capture file")
		}
		info, err := entry.Info()
		if err != nil {
			return nil, size, err
		}
		if !info.Mode().IsRegular() {
			return nil, size, errors.New("nonregular capture file")
		}
		size += info.Size()
	}
	return entries, size, nil
}

func (s *streamCaptureStore) removeRecord(path string) error {
	entries, size, err := captureRecordFiles(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err = os.Remove(filepath.Join(path, entry.Name())); err != nil {
			return err
		}
	}
	if err = os.Remove(path); err != nil {
		return err
	}
	s.diskBytes -= size
	delete(s.stored, path)
	return nil
}

func captureSummaryRequests(root string) map[string]bool {
	ids := map[string]bool{}
	dirs, _ := incidentDirs(root)
	for _, dir := range dirs {
		entries, _ := os.ReadDir(filepath.Join(root, dir.Name()))
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			f, err := os.Open(filepath.Join(root, dir.Name(), entry.Name()))
			if err != nil {
				continue
			}
			data, err := io.ReadAll(io.LimitReader(f, 16385))
			f.Close()
			var record struct {
				RequestID string `json:"request_id"`
			}
			if err == nil && len(data) <= 16384 && json.Unmarshal(data, &record) == nil && slowRequestID(record.RequestID) {
				ids[record.RequestID] = true
			}
		}
	}
	return ids
}

func (s *streamCaptureStore) recoverStored(now time.Time) error {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	s.diskBytes, err = captureTreeBytes(s.root)
	if err != nil {
		return err
	}
	summaryIDs := captureSummaryRequests(filepath.Dir(s.root))
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		name := entry.Name()
		id := strings.TrimPrefix(name, "request-")
		pending := false
		if id == name {
			id = strings.TrimPrefix(name, ".pending-")
			pending = id != name
		}
		if !slowRequestID(id) {
			continue
		}
		path := filepath.Join(s.root, name)
		_, size, inspectErr := captureRecordFiles(path)
		if inspectErr != nil {
			continue
		} // Unknown files and links are never followed or removed.
		m := streamCaptureMeta{}
		data, readErr := captureReadSmall(filepath.Join(path, "meta.json"), captureMetaLimit)
		valid := readErr == nil && len(data) <= captureMetaLimit && json.Unmarshal(data, &m) == nil && m.Schema == 1 && m.RequestID == id
		at := now
		if info, err := entry.Info(); err == nil {
			at = info.ModTime()
		}
		if valid {
			at = m.StartedAt
			if m.TriggeredAt != nil {
				at = *m.TriggeredAt
			}
		}
		s.stored[path] = &captureStored{path: path, at: at, bytes: size, safe: true}
		if pending && !m.Retained && !summaryIDs[id] {
			if err = s.removeRecord(path); err != nil {
				return err
			}
			continue
		}
		if pending {
			final := filepath.Join(s.root, "request-"+id)
			if _, err := os.Lstat(final); !os.IsNotExist(err) {
				delete(s.stored, path)
				continue
			}
			if err = os.Rename(path, final); err != nil {
				return err
			}
			record := s.stored[path]
			delete(s.stored, path)
			record.path = final
			s.stored[final] = record
			path = final
		}
		if valid && m.EndedAt != nil && m.CaptureState != "recording" {
			delete(s.stored, path)
			continue
		}
		if !valid {
			m = streamCaptureMeta{Schema: 1, RequestID: id, ServiceID: s.serviceID, RouterID: s.routerID, StartedAt: at}
		}
		m.Retained, m.CaptureState, m.FailureReason = true, "end_unknown", "process_end_unobserved"
		m.ResponseSavedBytes, m.ResponseSHA256, err = captureFileHash(filepath.Join(path, "response.sse"))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		m.ResponseReceivedBytes = max(m.ResponseReceivedBytes, m.ResponseSavedBytes)
		m.ResponseSyncedBytes = 0
		m.TimelineSavedBytes, m.TimelineSHA256, err = captureFileHash(filepath.Join(path, "timeline.jsonl"))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		m.TimelineSyncedBytes = 0
		m.TimelineCompleteBytes, m.LastTimelineSequence = captureTimelineBoundary(filepath.Join(path, "timeline.jsonl"))
		m.RequestSavedBytes, m.RequestSHA256, err = captureFileHash(filepath.Join(path, "request.json"))
		m.RequestSaved = valid && err == nil && m.RequestSavedBytes == m.RequestExpectedBytes
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if m.TimelineCompleteBytes != m.TimelineSavedBytes {
			seq := m.LastTimelineSequence + 1
			m.FirstMissingTimelineSequence = &seq
		}
		if err = os.Remove(filepath.Join(path, "meta.json.tmp")); err != nil && !os.IsNotExist(err) {
			return err
		}
		data, err = json.Marshal(m)
		if err != nil {
			return err
		}
		s.stored[path].active = true
		if err = s.capacity(int64(len(data))); err != nil {
			s.stored[path].active = false
			return err
		}
		if err = captureAtomicMeta(path, data); err != nil {
			return err
		}
		s.stored[path].active = false
		_, revised, err := captureRecordFiles(path)
		if err != nil {
			return err
		}
		s.account(path, revised-s.stored[path].bytes)
		delete(s.stored, path)
	}
	s.lastCleanup = now
	return s.cleanup(now, 0)
}

func captureReadSmall(path string, limit int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, int64(limit)+1))
}

// Include unknown regular files in the hard disk budget without following links
// or acquiring deletion authority over those files.
func captureTreeBytes(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var total int64
	for {
		entries, readErr := f.ReadDir(64)
		for _, entry := range entries {
			if entry.Type()&os.ModeSymlink != 0 {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return total, err
			}
			if info.Mode().IsRegular() {
				total += info.Size()
			}
			if info.IsDir() {
				n, err := captureTreeBytes(filepath.Join(path, entry.Name()))
				if err != nil {
					return total, err
				}
				total += n
			}
		}
		if readErr == io.EOF {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}

func captureFileHash(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return n, hex.EncodeToString(h.Sum(nil)), err
}

func captureTimelineBoundary(path string) (int64, uint64) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 64<<10)
	var size int64
	var sequence uint64
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return size, sequence
		}
		var event captureTimelineEvent
		if json.Unmarshal(line, &event) != nil || event.Schema != 1 || event.Sequence != sequence+1 {
			return size, sequence
		}
		size += int64(len(line))
		sequence = event.Sequence
	}
}
