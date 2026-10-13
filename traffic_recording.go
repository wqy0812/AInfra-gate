package aigate

// Switch traffic recording. The request path only copies bytes into bounded
// memory and enqueues one job per finished request; a single worker owns all
// directories, files, sealing, capacity accounting and eviction. Recording
// never changes admission, bindings, forwarding or response bytes.
import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	recordingSchema          = 1
	recordingSegment         = 30 * time.Minute
	recordingSegmentLayout   = "20060102T1504"
	recordingMemory          = int64(64 << 20)
	recordingHandles         = 1024
	recordingQueue           = 1024
	recordingDefaultMaxBytes = int64(50 << 30)
	recordingTailBytes       = 8 << 10
	recordingTimelineEvents  = 8192
	recordingEventBlock      = 128
	recordingEventCharge     = int64(64)
	recordingRecordCharge    = int64(recordingTailBytes + 8<<10)
	recordingDrain           = 5 * time.Second
	recordingFinishedKept    = 64
	recordingMaxSampleOneIn  = 1_000_000
	recordingSmallFile       = 1 << 20
)

var (
	recordingRequestHeaders  = []string{"Content-Type", "Content-Encoding", "Accept", "User-Agent", "X-Request-ID"}
	recordingResponseHeaders = []string{"Content-Type", "Content-Encoding", "X-Request-ID", "X-Aigate-Backend", headerRoute, "Retry-After"}
)

// recordingParams is the record-start payload. Windows are UTC half hours so
// that sessions, segments, eviction and replay selection share one unit.
type recordingParams struct {
	RecordingID string    `json:"recording_id"`
	ScheduleID  string    `json:"schedule_id,omitempty"`
	StartAt     time.Time `json:"start_at"`
	EndAt       time.Time `json:"end_at"`
	SampleOneIn uint64    `json:"sample_one_in"`
	Response    string    `json:"response"`
	MaxBytes    int64     `json:"max_bytes"`
}

func recordingInstant(t time.Time) bool {
	if t.IsZero() {
		return false
	}
	if _, offset := t.Zone(); offset != 0 {
		return false
	}
	return t.Equal(t.Truncate(recordingSegment))
}

func (p *recordingParams) normalize(limit int64, now time.Time) error {
	if !validOperationID(p.RecordingID) || (p.ScheduleID != "" && !validOperationID(p.ScheduleID)) {
		return fmt.Errorf("invalid_recording_id")
	}
	if !recordingInstant(p.StartAt) || !recordingInstant(p.EndAt) || !p.EndAt.After(p.StartAt) {
		return fmt.Errorf("invalid_window")
	}
	if !now.Before(p.EndAt) {
		return fmt.Errorf("window_ended")
	}
	p.StartAt, p.EndAt = p.StartAt.UTC(), p.EndAt.UTC()
	if p.SampleOneIn == 0 {
		p.SampleOneIn = 1
	}
	if p.SampleOneIn > recordingMaxSampleOneIn {
		return fmt.Errorf("invalid_sample_rate")
	}
	if p.Response == "" {
		p.Response = "summary"
	}
	if p.Response != "summary" && p.Response != "full" {
		return fmt.Errorf("invalid_response_mode")
	}
	if p.MaxBytes == 0 {
		p.MaxBytes = limit
	}
	if p.MaxBytes < 0 || p.MaxBytes > limit {
		return fmt.Errorf("invalid_max_bytes")
	}
	return nil
}

func recordingSegmentName(start time.Time) string { return start.UTC().Format(recordingSegmentLayout) }

func parseRecordingSegment(name string) (time.Time, bool) {
	t, err := time.Parse(recordingSegmentLayout, name)
	if err != nil || recordingSegmentName(t) != name || !recordingInstant(t) {
		return time.Time{}, false
	}
	return t, true
}

func recordingSampled(id string, oneIn uint64) bool {
	if oneIn <= 1 {
		return true
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return h.Sum64()%oneIn == 0
}

type recordingRouter struct {
	RouterID        string `json:"router_id"`
	BindingRevision uint64 `json:"binding_revision"`
	ConfigDigest    string `json:"config_digest,omitempty"`
}

type recordingSegmentState struct {
	start, end                             time.Time
	name                                   string
	seq                                    uint64
	inflight                               int
	requests, recorded, dropped, truncated uint64
	bytes                                  int64
	routers                                map[recordingRouter]bool
	sealed, pinned, evicted                bool
}

type recordingSession struct {
	params                                 recordingParams
	created, readyAt                       time.Time
	ready, stopped, finalized              bool
	stoppedAt                              time.Time
	endReason, persisted                   string
	inflight                               int
	requests, recorded, dropped, truncated uint64
	bytes                                  int64
	segments                               map[time.Time]*recordingSegmentState
}

func (s *recordingSession) activeFrom() time.Time {
	if s.readyAt.After(s.params.StartAt) {
		return s.readyAt
	}
	return s.params.StartAt
}

// stopTime is the end of sampling: the window end, or an earlier stop.
func (s *recordingSession) stopTime() time.Time {
	if s.stopped && s.stoppedAt.Before(s.params.EndAt) {
		return s.stoppedAt
	}
	return s.params.EndAt
}

func (s *recordingSession) stateAt(now time.Time) string {
	switch {
	case s.finalized:
		return "ended"
	case !s.ready:
		return "preparing"
	case s.stopped || !now.Before(s.params.EndAt):
		return "draining"
	case now.Before(s.activeFrom()):
		return "scheduled"
	}
	return "recording"
}

func (s *recordingSession) segmentLocked(start time.Time) *recordingSegmentState {
	seg := s.segments[start]
	if seg == nil {
		seg = &recordingSegmentState{start: start, end: start.Add(recordingSegment), name: recordingSegmentName(start), routers: map[recordingRouter]bool{}}
		s.segments[start] = seg
	}
	return seg
}

func (s *recordingSession) coverage(seg *recordingSegmentState) (time.Time, time.Time) {
	from, to := seg.start, seg.end
	if a := s.activeFrom(); a.After(from) {
		from = a
	}
	if e := s.stopTime(); e.Before(to) {
		to = e
	}
	if to.Before(from) {
		to = from
	}
	return from, to
}

type recordingSegmentSummary struct {
	Segment       string    `json:"segment"`
	State         string    `json:"state"`
	CoverageStart time.Time `json:"coverage_start"`
	CoverageEnd   time.Time `json:"coverage_end"`
	Partial       bool      `json:"partial"`
	Requests      uint64    `json:"requests"`
	Recorded      uint64    `json:"recorded"`
	Dropped       uint64    `json:"dropped"`
	Truncated     uint64    `json:"truncated"`
	Bytes         int64     `json:"bytes"`
	Pinned        bool      `json:"pinned"`
	Evicted       bool      `json:"evicted"`
}

type recordingSummary struct {
	RecordingID string                    `json:"recording_id"`
	ScheduleID  string                    `json:"schedule_id,omitempty"`
	State       string                    `json:"state"`
	EndReason   string                    `json:"end_reason,omitempty"`
	StartAt     time.Time                 `json:"start_at"`
	EndAt       time.Time                 `json:"end_at"`
	StartedAt   *time.Time                `json:"started_at,omitempty"`
	StoppedAt   *time.Time                `json:"stopped_at,omitempty"`
	SampleOneIn uint64                    `json:"sample_one_in"`
	Response    string                    `json:"response"`
	MaxBytes    int64                     `json:"max_bytes"`
	Requests    uint64                    `json:"requests"`
	Recorded    uint64                    `json:"recorded"`
	Dropped     uint64                    `json:"dropped"`
	Truncated   uint64                    `json:"truncated"`
	Bytes       int64                     `json:"bytes"`
	Inflight    int                       `json:"inflight"`
	Segments    []recordingSegmentSummary `json:"segments"`
}

func (s *recordingSession) summaryLocked(now time.Time) recordingSummary {
	out := recordingSummary{RecordingID: s.params.RecordingID, ScheduleID: s.params.ScheduleID, State: s.stateAt(now), EndReason: s.endReason,
		StartAt: s.params.StartAt, EndAt: s.params.EndAt, SampleOneIn: s.params.SampleOneIn, Response: s.params.Response, MaxBytes: s.params.MaxBytes,
		Requests: s.requests, Recorded: s.recorded, Dropped: s.dropped, Truncated: s.truncated, Bytes: s.bytes, Inflight: s.inflight, Segments: []recordingSegmentSummary{}}
	if s.ready && !now.Before(s.activeFrom()) {
		at := s.activeFrom()
		out.StartedAt = &at
	}
	if s.stopped {
		at := s.stoppedAt
		out.StoppedAt = &at
	}
	for _, seg := range s.orderedSegments() {
		from, to := s.coverage(seg)
		state := "open"
		if seg.sealed {
			state = "sealed"
		}
		out.Segments = append(out.Segments, recordingSegmentSummary{Segment: seg.name, State: state, CoverageStart: from, CoverageEnd: to,
			Partial: to.Sub(from) < recordingSegment, Requests: seg.requests, Recorded: seg.recorded, Dropped: seg.dropped, Truncated: seg.truncated,
			Bytes: seg.bytes, Pinned: seg.pinned, Evicted: seg.evicted})
	}
	return out
}

func (s *recordingSession) orderedSegments() []*recordingSegmentState {
	out := make([]*recordingSegmentState, 0, len(s.segments))
	for _, seg := range s.segments {
		out = append(out, seg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].start.Before(out[j].start) })
	return out
}

// Files written by the worker.
type recordingSessionFile struct {
	Schema      int             `json:"schema_version"`
	RecordingID string          `json:"recording_id"`
	GatewayID   string          `json:"gateway_id"`
	BootID      string          `json:"boot_id"`
	ScheduleID  string          `json:"schedule_id,omitempty"`
	Params      recordingParams `json:"params"`
	State       string          `json:"state"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	EndedAt     *time.Time      `json:"ended_at,omitempty"`
	EndReason   string          `json:"end_reason,omitempty"`
	Requests    uint64          `json:"requests"`
	Recorded    uint64          `json:"recorded"`
	Dropped     uint64          `json:"dropped"`
	Truncated   uint64          `json:"truncated"`
	Bytes       int64           `json:"bytes"`
	Segments    []string        `json:"segments"`
}

type recordingSegmentFile struct {
	Schema        int               `json:"schema_version"`
	RecordingID   string            `json:"recording_id"`
	GatewayID     string            `json:"gateway_id"`
	BootID        string            `json:"boot_id"`
	Segment       string            `json:"segment"`
	Start         time.Time         `json:"start"`
	End           time.Time         `json:"end"`
	CoverageStart time.Time         `json:"coverage_start"`
	CoverageEnd   time.Time         `json:"coverage_end"`
	Partial       bool              `json:"partial"`
	State         string            `json:"state"`
	Requests      uint64            `json:"requests"`
	Recorded      uint64            `json:"recorded"`
	Dropped       uint64            `json:"dropped"`
	Truncated     uint64            `json:"truncated"`
	Bytes         int64             `json:"bytes"`
	Routers       []recordingRouter `json:"routers"`
}

type recordingIndexLine struct {
	Sequence        uint64  `json:"sequence"`
	RequestID       string  `json:"request_id"`
	OffsetSeconds   float64 `json:"offset_seconds"`
	Method          string  `json:"method"`
	Path            string  `json:"path"`
	Status          int     `json:"status"`
	Outcome         string  `json:"outcome"`
	RequestState    string  `json:"request_state"`
	CaptureState    string  `json:"capture_state"`
	RequestBytes    int64   `json:"request_bytes"`
	RouterID        string  `json:"router_id,omitempty"`
	BindingRevision uint64  `json:"binding_revision,omitempty"`
	Directory       string  `json:"directory"`
}

type recordingMeta struct {
	Schema            int               `json:"schema_version"`
	RequestID         string            `json:"request_id"`
	RecordingID       string            `json:"recording_id"`
	GatewayID         string            `json:"gateway_id"`
	BootID            string            `json:"boot_id"`
	Segment           string            `json:"segment"`
	Sequence          uint64            `json:"sequence"`
	ArrivedAt         time.Time         `json:"arrived_at"`
	OffsetSeconds     float64           `json:"offset_seconds"`
	Method            string            `json:"method"`
	Path              string            `json:"path"`
	ContentLength     int64             `json:"content_length"`
	Peer              string            `json:"peer,omitempty"`
	Headers           map[string]string `json:"headers"`
	RouterID          string            `json:"router_id,omitempty"`
	BindingRevision   uint64            `json:"binding_revision,omitempty"`
	ConfigDigest      string            `json:"config_digest,omitempty"`
	UpstreamRequestID string            `json:"upstream_request_id,omitempty"`
	Status            int               `json:"status"`
	ResponseHeaders   map[string]string `json:"response_headers"`
	Outcome           string            `json:"outcome"`
	EndedAt           time.Time         `json:"ended_at"`
	DurationSeconds   float64           `json:"duration_seconds"`
	RequestState      string            `json:"request_state"`
	RequestError      string            `json:"request_error,omitempty"`
	RequestBytes      int64             `json:"request_bytes"`
	RequestSavedBytes int64             `json:"request_saved_bytes"`
	RequestSHA256     string            `json:"request_sha256"`
	ResponseMode      string            `json:"response_mode"`
	ResponseFile      string            `json:"response_file"`
	ResponseBytes     int64             `json:"response_bytes"`
	ResponseSaved     int64             `json:"response_saved_bytes"`
	ResponseSHA256    string            `json:"response_sha256"`
	TimelineEvents    int               `json:"timeline_events"`
	TimelineTruncated bool              `json:"timeline_truncated"`
	TimelineSHA256    string            `json:"timeline_sha256"`
	CaptureState      string            `json:"capture_state"`
	FailureReason     string            `json:"failure_reason,omitempty"`
}

type recordingTimelineLine struct {
	Schema   int     `json:"schema_version"`
	Type     string  `json:"type"`
	Seconds  float64 `json:"t"`
	Duration float64 `json:"duration,omitempty"`
	Start    *int64  `json:"start,omitempty"`
	End      *int64  `json:"end,omitempty"`
	Status   int     `json:"status,omitempty"`
	Detail   string  `json:"detail,omitempty"`
}

const (
	recordingEventUpload uint8 = iota + 1
	recordingEventUploadFailed
	recordingEventHeader
	recordingEventWrite
	recordingEventFlush
)

type recordingEvent struct {
	kind       uint8
	at, dur    time.Duration
	start, end int64
	status     int
}

// trafficRecord is one sampled request. Its methods run on the business
// request goroutine and on the upload reader; all are no-ops after finish.
type trafficRecord struct {
	rec      *trafficRecorder
	session  *recordingSession
	segment  *recordingSegmentState
	seq      uint64
	id       string
	arrived  time.Time
	method   string
	path     string
	peer     string
	length   int64
	headers  map[string]string
	mode     string
	mu       sync.Mutex
	finished bool
	charge   int64

	body                []byte
	bodyBytes           int64
	bodyRead, bodyDone  bool
	bodyTruncated       bool
	bodyError           string
	forwarded           bool
	router              recordingRouter
	upstreamID          string
	status              int
	responseHeaders     map[string]string
	tail                []byte
	tailPos             int
	full                []byte
	responseBytes       int64
	responseTruncated   bool
	events              []recordingEvent
	eventsCharged       int
	timelineTruncated   bool
	outcome             string
	truncationReason    string
	responseHeaderKnown bool
}

func (t *trafficRecord) reserveLocked(n int64) bool {
	if t.rec.reserve(n) {
		t.charge += n
		return true
	}
	return false
}

func (t *trafficRecord) eventLocked(e recordingEvent) {
	if t.timelineTruncated {
		return
	}
	if len(t.events) >= recordingTimelineEvents {
		t.timelineTruncated = true
		return
	}
	if len(t.events) >= t.eventsCharged {
		if !t.reserveLocked(recordingEventCharge * recordingEventBlock) {
			t.timelineTruncated = true
			return
		}
		t.eventsCharged += recordingEventBlock
	}
	t.events = append(t.events, e)
}

func (t *trafficRecord) route(router recordingRouter) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.router = router
}

// forwarding marks that the request passed entry checks and reached the proxy.
func (t *trafficRecord) forwarding() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.forwarded = true
}

func (t *trafficRecord) upstream(id string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.upstreamID = id
}

// bodyChunk receives exactly the bytes forwarded to the proxy.
func (t *trafficRecord) bodyChunk(p []byte, done bool, failure error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return
	}
	t.bodyRead = true
	t.bodyBytes += int64(len(p))
	if len(p) > 0 && !t.bodyTruncated {
		if t.reserveLocked(int64(len(p))) {
			t.body = append(t.body, p...)
		} else {
			t.bodyTruncated = true
			t.truncationReason = "memory_full"
		}
	}
	at := time.Since(t.arrived)
	if failure != nil && t.bodyError == "" {
		t.bodyError = recordingBodyError(failure)
		t.eventLocked(recordingEvent{kind: recordingEventUploadFailed, at: at, end: t.bodyBytes})
	} else if done && !t.bodyDone {
		t.bodyDone = true
		t.eventLocked(recordingEvent{kind: recordingEventUpload, at: at, end: t.bodyBytes})
	}
}

func recordingBodyError(err error) string {
	var limit *http.MaxBytesError
	var timeout interface{ Timeout() bool }
	switch {
	case errors.As(err, &limit):
		return "request_body_too_large"
	case errors.As(err, &timeout) && timeout.Timeout():
		return "request_body_timeout"
	}
	return "request_body_read_failed"
}

func (t *trafficRecord) header(status int, h http.Header) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished || t.responseHeaderKnown {
		return
	}
	t.responseHeaderKnown = true
	t.status = status
	t.responseHeaders = map[string]string{}
	for _, k := range recordingResponseHeaders {
		if v := h.Get(k); v != "" {
			t.responseHeaders[k] = captureString(v, 256)
		}
	}
	t.eventLocked(recordingEvent{kind: recordingEventHeader, at: time.Since(t.arrived), status: status})
}

func (t *trafficRecord) write(start, end time.Time, p []byte) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return
	}
	offset := t.responseBytes
	t.responseBytes += int64(len(p))
	t.eventLocked(recordingEvent{kind: recordingEventWrite, at: start.Sub(t.arrived), dur: end.Sub(start), start: offset, end: t.responseBytes})
	if t.mode == "full" {
		if !t.responseTruncated {
			if t.reserveLocked(int64(len(p))) {
				t.full = append(t.full, p...)
			} else {
				t.responseTruncated = true
				t.truncationReason = "memory_full"
			}
		}
		return
	}
	if len(p) >= len(t.tail) {
		copy(t.tail, p[len(p)-len(t.tail):])
		t.tailPos = 0
		return
	}
	n := copy(t.tail[t.tailPos:], p)
	if n < len(p) {
		copy(t.tail, p[n:])
	}
	t.tailPos = (t.tailPos + len(p)) % len(t.tail)
}

func (t *trafficRecord) flush(start, end time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.finished {
		t.eventLocked(recordingEvent{kind: recordingEventFlush, at: start.Sub(t.arrived), dur: end.Sub(start), start: t.responseBytes, end: t.responseBytes})
	}
}

func (t *trafficRecord) setOutcome(outcome string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.outcome == "" {
		t.outcome = outcome
	}
}

func (t *trafficRecord) tailBytes() []byte {
	if t.responseBytes < int64(len(t.tail)) {
		return append([]byte(nil), t.tail[:t.responseBytes]...)
	}
	return append(append([]byte(nil), t.tail[t.tailPos:]...), t.tail[:t.tailPos]...)
}

// recordingSnapshot is immutable once handed to the worker.
type recordingSnapshot struct {
	meta     recordingMeta
	body     []byte
	response []byte
	events   []recordingEvent
	session  *recordingSession
	segment  *recordingSegmentState
	charge   int64
	truncate bool
}

func (t *trafficRecord) finish(clientErr error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		return
	}
	t.finished = true
	ended := time.Now()
	outcome := t.outcome
	switch {
	case outcome != "":
	case !t.forwarded && t.status >= 400:
		outcome = "rejected"
	case clientErr != nil:
		outcome = "client_cancelled"
	case t.status >= 400:
		outcome = "upstream_error"
	default:
		outcome = "completed"
	}
	requestState := "none"
	switch {
	case t.method == http.MethodGet:
	case t.bodyTruncated:
		requestState = "truncated"
	case t.bodyError != "":
		requestState = "failed"
	case t.bodyDone:
		requestState = "complete"
	case !t.bodyRead:
		requestState = "not_read"
	default:
		requestState = "incomplete"
	}
	response, file := t.full, "response.sse"
	if t.mode != "full" {
		response, file = t.tailBytes(), "response.tail"
	}
	capture := "complete"
	if t.bodyTruncated || t.responseTruncated {
		capture = "truncated"
	}
	segStart := t.segment.start
	meta := recordingMeta{Schema: recordingSchema, RequestID: t.id, RecordingID: t.session.params.RecordingID, GatewayID: t.rec.gatewayID, BootID: t.rec.boot,
		Segment: t.segment.name, Sequence: t.seq, ArrivedAt: t.arrived.UTC(), OffsetSeconds: t.arrived.Sub(segStart).Seconds(), Method: t.method, Path: t.path,
		ContentLength: t.length, Peer: t.peer, Headers: t.headers, RouterID: t.router.RouterID, BindingRevision: t.router.BindingRevision, ConfigDigest: t.router.ConfigDigest,
		UpstreamRequestID: t.upstreamID, Status: t.status, ResponseHeaders: t.responseHeaders, Outcome: outcome, EndedAt: ended.UTC(),
		DurationSeconds: ended.Sub(t.arrived).Seconds(), RequestState: requestState, RequestError: t.bodyError, RequestBytes: t.bodyBytes,
		RequestSavedBytes: int64(len(t.body)), ResponseMode: t.mode, ResponseFile: file, ResponseBytes: t.responseBytes, ResponseSaved: int64(len(response)),
		TimelineEvents: len(t.events), TimelineTruncated: t.timelineTruncated, CaptureState: capture, FailureReason: t.truncationReason}
	if meta.ResponseHeaders == nil {
		meta.ResponseHeaders = map[string]string{}
	}
	snapshot := &recordingSnapshot{meta: meta, body: t.body, response: response, events: t.events, session: t.session, segment: t.segment, charge: t.charge, truncate: capture == "truncated"}
	t.body, t.full, t.tail, t.events = nil, nil, nil, nil
	t.mu.Unlock()
	t.rec.submit(snapshot)
}

// recordingWriter observes response status, writes and flushes without
// changing them. Unwrap keeps ResponseController deadlines working.
type recordingWriter struct {
	http.ResponseWriter
	record *trafficRecord
}

func (w *recordingWriter) WriteHeader(status int) {
	if status >= 200 {
		w.record.header(status, w.Header())
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	w.record.header(http.StatusOK, w.Header())
	start := time.Now()
	n, err := w.ResponseWriter.Write(p)
	w.record.write(start, time.Now(), p[:n])
	return n, err
}

func (w *recordingWriter) FlushError() error {
	start := time.Now()
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.record.flush(start, time.Now())
	return err
}

func (w *recordingWriter) Flush() { _ = w.FlushError() }

func (w *recordingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type recordingStoredSegment struct {
	key, recording, name string
	start                time.Time
	bytes                int64
	sealed, pinned       bool
}

type recordingControlJob struct {
	create   *recordingSession
	pin      bool
	pinID    string
	pinName  string
	done     func(error)
	flushAck chan struct{}
}

// trafficRecorder belongs to one switch process and one gateway directory.
type trafficRecorder struct {
	root, gatewayID, boot string
	maxBytes              int64
	markKey               []byte
	audit                 *log.Logger
	directory             *os.File
	now                   func() time.Time

	mu          sync.Mutex
	sessions    []*recordingSession
	finished    []*recordingSession
	used        int64
	active      int
	closing     bool
	exited      bool
	diskBytes   int64
	pinnedBytes int64
	dropped     uint64
	failures    uint64

	queue    chan *recordingSnapshot
	control  chan recordingControlJob
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	// Worker-only state.
	stored    map[string]*recordingStoredSegment
	acks      []chan struct{}
	writeFile func(path string, data []byte) error
}

func newTrafficRecorder(recordRoot, gatewayID, boot string, maxBytes int64, markKey []byte, audit io.Writer) (*trafficRecorder, error) {
	if recordRoot == "" || !validOperationID(gatewayID) || maxBytes <= 0 {
		return nil, fmt.Errorf("record root, gateway ID and a positive capacity are required")
	}
	if audit == nil {
		audit = io.Discard
	}
	root := filepath.Join(recordRoot, gatewayID)
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("record directory %s must be a 0700 directory", root)
	}
	directory, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	if err = lockState(directory); err != nil {
		directory.Close()
		return nil, fmt.Errorf("record directory is in use: %w", err)
	}
	r := &trafficRecorder{root: root, gatewayID: gatewayID, boot: boot, maxBytes: maxBytes, markKey: markKey, audit: log.New(audit, "", 0),
		directory: directory, now: time.Now, queue: make(chan *recordingSnapshot, recordingQueue), control: make(chan recordingControlJob, 64),
		stop: make(chan struct{}), done: make(chan struct{}), stored: map[string]*recordingStoredSegment{}, writeFile: writeRecordingFile}
	if err = r.recover(); err != nil {
		unlockState(directory)
		directory.Close()
		return nil, err
	}
	go r.run()
	return r, nil
}

func writeRecordingFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func recordingAtomicJSON(dir, name string, value any) (int64, error) {
	data := append(encodeJSON(value), '\n')
	tmp := filepath.Join(dir, "."+name+".tmp")
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmp)
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return 0, err
	}
	if err = os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return 0, err
	}
	d, err := os.Open(dir)
	if err != nil {
		return 0, err
	}
	defer d.Close()
	return int64(len(data)), d.Sync()
}

func recordingReadSmall(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, recordingSmallFile+1))
	if err != nil {
		return nil, err
	}
	if len(data) > recordingSmallFile {
		return nil, fmt.Errorf("%s exceeds %d bytes", filepath.Base(path), recordingSmallFile)
	}
	return data, nil
}

func recordingTreeBytes(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// recover closes sessions left by an earlier boot and rebuilds the capacity
// registry. Interrupted sessions and unsealed segments become end_unknown.
func (r *trafficRecorder) recover() error {
	entries, err := os.ReadDir(r.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !validOperationID(entry.Name()) {
			continue
		}
		dir := filepath.Join(r.root, entry.Name())
		var session recordingSessionFile
		if data, err := recordingReadSmall(filepath.Join(dir, "session.json")); err != nil || json.Unmarshal(data, &session) != nil || session.RecordingID != entry.Name() {
			r.audit.Print(string(encodeJSON(map[string]any{"event": "recording_recover_skipped", "recording_id": entry.Name()})))
			continue
		}
		segments, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, seg := range segments {
			start, ok := parseRecordingSegment(seg.Name())
			if !seg.IsDir() || !ok {
				continue
			}
			path := filepath.Join(dir, seg.Name())
			if _, err := os.Stat(filepath.Join(path, "segment.json")); errors.Is(err, os.ErrNotExist) {
				lines := recordingCountIndex(filepath.Join(path, "index.jsonl"))
				file := recordingSegmentFile{Schema: recordingSchema, RecordingID: session.RecordingID, GatewayID: r.gatewayID, BootID: session.BootID,
					Segment: seg.Name(), Start: start, End: start.Add(recordingSegment), CoverageStart: start, CoverageEnd: start.Add(recordingSegment),
					State: "end_unknown", Recorded: lines, Routers: []recordingRouter{}}
				if _, err := recordingAtomicJSON(path, "segment.json", file); err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
			bytes, err := recordingTreeBytes(path)
			if err != nil {
				return err
			}
			_, pinErr := os.Stat(filepath.Join(path, "pinned"))
			key := session.RecordingID + "/" + seg.Name()
			r.stored[key] = &recordingStoredSegment{key: key, recording: session.RecordingID, name: seg.Name(), start: start, bytes: bytes, sealed: true, pinned: pinErr == nil}
			r.diskBytes += bytes
			if pinErr == nil {
				r.pinnedBytes += bytes
			}
		}
		if session.State != "ended" {
			session.State = "ended"
			session.EndReason = "end_unknown"
			if _, err := recordingAtomicJSON(dir, "session.json", session); err != nil {
				return err
			}
		}
		if info, err := os.Stat(filepath.Join(dir, "session.json")); err == nil {
			r.diskBytes += info.Size()
		}
	}
	return nil
}

func recordingCountIndex(path string) uint64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	var n uint64
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		if len(scanner.Bytes()) > 0 {
			n++
		}
	}
	return n
}

func (r *trafficRecorder) reserve(n int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > recordingMemory-r.used {
		return false
	}
	r.used += n
	return true
}

func (r *trafficRecorder) samplingLocked(now time.Time) *recordingSession {
	for _, s := range r.sessions {
		if s.ready && !s.stopped && !s.finalized && !now.Before(s.activeFrom()) && now.Before(s.params.EndAt) {
			return s
		}
	}
	return nil
}

// begin samples a business request before admission so that entry rejections
// are recorded too. A verified replay mark is never sampled.
func (r *trafficRecorder) begin(req *http.Request, id string, arrived time.Time) *trafficRecord {
	// Platform evaluation traffic (test leases) and verified replays are not business traffic.
	if r == nil || req.Header.Get("X-Code-Eval-Token") != "" || verifyReplayMark(r.markKey, req.Header.Get(headerReplay)) {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return nil
	}
	s := r.samplingLocked(arrived)
	if s == nil || !recordingSampled(id, s.params.SampleOneIn) {
		return nil
	}
	seg := s.segmentLocked(arrived.UTC().Truncate(recordingSegment))
	if seg.sealed {
		return nil // Arrived just before the boundary but sampled after sealing.
	}
	s.requests++
	seg.requests++
	if r.active >= recordingHandles || recordingRecordCharge > recordingMemory-r.used {
		s.dropped++
		seg.dropped++
		r.dropped++
		return nil
	}
	r.active++
	r.used += recordingRecordCharge
	s.inflight++
	seg.inflight++
	seg.seq++
	t := &trafficRecord{rec: r, session: s, segment: seg, seq: seg.seq, id: id, arrived: arrived, method: req.Method, path: req.URL.Path,
		peer: req.RemoteAddr, length: req.ContentLength, headers: map[string]string{}, mode: s.params.Response, charge: recordingRecordCharge}
	if t.mode != "full" {
		t.tail = make([]byte, recordingTailBytes)
	}
	for _, k := range recordingRequestHeaders {
		if v := req.Header.Get(k); v != "" {
			t.headers[k] = captureString(v, 256)
		}
	}
	return t
}

func (r *trafficRecorder) releaseLocked(snapshot *recordingSnapshot) {
	r.used -= snapshot.charge
	r.active--
	snapshot.session.inflight--
	snapshot.segment.inflight--
}

func (r *trafficRecorder) submit(snapshot *recordingSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.exited {
		select {
		case r.queue <- snapshot:
			return
		default:
		}
	}
	snapshot.session.dropped++
	snapshot.segment.dropped++
	r.dropped++
	r.releaseLocked(snapshot)
}

// Command-side operations run under the switch control lock and only touch memory.
func (r *trafficRecorder) startLocked(p recordingParams, now time.Time, done func(error)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return fmt.Errorf("recording_closed")
	}
	for _, s := range append(append([]*recordingSession{}, r.sessions...), r.finished...) {
		if s.params.RecordingID == p.RecordingID {
			return fmt.Errorf("recording_exists")
		}
	}
	for _, s := range r.sessions {
		if s.stopped || s.finalized || !now.Before(s.params.EndAt) {
			continue
		}
		// One sampling or pending session at a time; the next window may queue
		// behind a recording one if it starts no earlier than that window ends.
		if now.Before(s.activeFrom()) || !s.ready || p.StartAt.Before(s.params.EndAt) {
			return fmt.Errorf("recording_active")
		}
	}
	if r.pinnedBytes >= r.maxBytes {
		return fmt.Errorf("capacity_reserved")
	}
	s := &recordingSession{params: p, created: now, segments: map[time.Time]*recordingSegmentState{}}
	select {
	case r.control <- recordingControlJob{create: s, done: done}:
	default:
		return fmt.Errorf("recording_busy")
	}
	r.sessions = append(r.sessions, s)
	return nil
}

func (r *trafficRecorder) findLocked(id string) *recordingSession {
	for _, s := range r.sessions {
		if s.params.RecordingID == id {
			return s
		}
	}
	for _, s := range r.finished {
		if s.params.RecordingID == id {
			return s
		}
	}
	return nil
}

func (r *trafficRecorder) stopLocked(id string, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.findLocked(id)
	if s == nil {
		return fmt.Errorf("recording_not_found")
	}
	r.stopSessionLocked(s, now, "stopped")
	r.signalLocked()
	return nil
}

func (r *trafficRecorder) stopSessionLocked(s *recordingSession, now time.Time, reason string) {
	if s.stopped || s.finalized {
		return
	}
	if !now.Before(s.params.EndAt) {
		return // The window already ended naturally; it is draining.
	}
	s.stopped, s.stoppedAt, s.endReason = true, now, reason
}

func (r *trafficRecorder) signalLocked() {
	select {
	case r.control <- recordingControlJob{}:
	default:
	}
}

func (r *trafficRecorder) statusOf(id string, now time.Time) (*recordingSummary, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.findLocked(id)
	if s == nil {
		return nil, fmt.Errorf("recording_not_found")
	}
	summary := s.summaryLocked(now)
	return &summary, nil
}

func (r *trafficRecorder) pinLocked(id, segment string, pin bool, done func(error)) error {
	if !validOperationID(id) {
		return fmt.Errorf("invalid_recording_id")
	}
	if _, ok := parseRecordingSegment(segment); !ok {
		return fmt.Errorf("invalid_segment")
	}
	select {
	case r.control <- recordingControlJob{pin: pin, pinID: id, pinName: segment, done: done}:
		return nil
	default:
		return fmt.Errorf("recording_busy")
	}
}

type recordingStatus struct {
	MaxBytes      int64              `json:"max_bytes"`
	DiskBytes     int64              `json:"disk_bytes"`
	PinnedBytes   int64              `json:"pinned_bytes"`
	MemoryBytes   int64              `json:"memory_bytes"`
	ActiveRecords int                `json:"active_records"`
	Dropped       uint64             `json:"dropped"`
	WriteFailures uint64             `json:"write_failures"`
	Sessions      []recordingSummary `json:"sessions"`
}

func (r *trafficRecorder) status(now time.Time) any {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := recordingStatus{MaxBytes: r.maxBytes, DiskBytes: r.diskBytes, PinnedBytes: r.pinnedBytes, MemoryBytes: r.used, ActiveRecords: r.active,
		Dropped: r.dropped, WriteFailures: r.failures, Sessions: []recordingSummary{}}
	for _, s := range r.sessions {
		out.Sessions = append(out.Sessions, s.summaryLocked(now))
	}
	return out
}

// endAll stops every sampling or pending session, e.g. on retire.
func (r *trafficRecorder) endAll(reason string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for _, s := range r.sessions {
		r.stopSessionLocked(s, now, reason)
	}
	r.signalLocked()
}

// close stops sampling and lets the worker finish queued records for at most
// recordingDrain before marking the remainder end_unknown.
func (r *trafficRecorder) close(reason string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if !r.closing {
		r.closing = true
		now := r.now()
		for _, s := range r.sessions {
			r.stopSessionLocked(s, now, reason)
		}
	}
	r.mu.Unlock()
	r.stopOnce.Do(func() { close(r.stop) })
	<-r.done
}

func (r *trafficRecorder) run() {
	defer close(r.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case snapshot := <-r.queue:
			r.write(snapshot)
		case job := <-r.control:
			r.controlJob(job)
		case <-ticker.C:
		case <-r.stop:
			r.drain()
			return
		}
		r.maintain(false)
		for _, ack := range r.acks {
			close(ack)
		}
		r.acks = nil
	}
}

func (r *trafficRecorder) drain() {
	deadline := time.Now().Add(recordingDrain)
	for time.Now().Before(deadline) {
		select {
		case snapshot := <-r.queue:
			r.write(snapshot)
			continue
		case job := <-r.control:
			r.controlJob(job)
			continue
		default:
		}
		r.mu.Lock()
		busy := r.active > 0
		r.mu.Unlock()
		if !busy {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.mu.Lock()
	r.exited = true
	r.mu.Unlock()
	for {
		select {
		case job := <-r.control:
			if job.done != nil {
				job.done(fmt.Errorf("recording_closed"))
			}
			continue
		default:
		}
		break
	}
	r.maintain(true)
	unlockState(r.directory)
	r.directory.Close()
}

func (r *trafficRecorder) sessionDir(id string) string { return filepath.Join(r.root, id) }

func (r *trafficRecorder) controlJob(job recordingControlJob) {
	switch {
	case job.create != nil:
		s := job.create
		dir := r.sessionDir(s.params.RecordingID)
		err := os.Mkdir(dir, 0700)
		if errors.Is(err, os.ErrExist) {
			err = fmt.Errorf("recording_exists")
		} else if err != nil {
			err = fmt.Errorf("recording_storage_failed")
		}
		var size int64
		if err == nil {
			r.mu.Lock()
			now := r.now()
			s.ready, s.readyAt = true, now
			file := r.sessionFileLocked(s, now)
			r.mu.Unlock()
			if size, err = recordingAtomicJSON(dir, "session.json", file); err != nil {
				_ = os.RemoveAll(dir)
				err = fmt.Errorf("recording_storage_failed")
			}
		}
		r.mu.Lock()
		if err != nil {
			s.ready = false
			r.removeSessionLocked(s)
		} else {
			r.diskBytes += size
			s.persisted = s.stateAt(r.now())
		}
		r.mu.Unlock()
		if job.done != nil {
			job.done(err)
		}
	case job.pinID != "":
		err := r.pin(job.pinID, job.pinName, job.pin)
		if job.done != nil {
			job.done(err)
		}
	case job.flushAck != nil:
		r.acks = append(r.acks, job.flushAck)
	}
}

func (r *trafficRecorder) removeSessionLocked(s *recordingSession) {
	for i, v := range r.sessions {
		if v == s {
			r.sessions = append(r.sessions[:i], r.sessions[i+1:]...)
			return
		}
	}
}

func (r *trafficRecorder) pin(id, name string, pin bool) error {
	key := id + "/" + name
	stored := r.stored[key]
	if stored == nil {
		return fmt.Errorf("segment_not_found")
	}
	if !stored.sealed {
		return fmt.Errorf("segment_not_sealed")
	}
	marker := filepath.Join(r.sessionDir(id), name, "pinned")
	if pin && !stored.pinned {
		if err := os.WriteFile(marker, []byte(r.now().UTC().Format(time.RFC3339)+"\n"), 0600); err != nil {
			return fmt.Errorf("recording_storage_failed")
		}
	} else if !pin && stored.pinned {
		if err := os.Remove(marker); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("recording_storage_failed")
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if stored.pinned != pin {
		if pin {
			r.pinnedBytes += stored.bytes
		} else {
			r.pinnedBytes -= stored.bytes
		}
	}
	stored.pinned = pin
	if s := r.findLocked(id); s != nil {
		for _, seg := range s.segments {
			if seg.name == name {
				seg.pinned = pin
			}
		}
	}
	return nil
}

func (r *trafficRecorder) sessionFileLocked(s *recordingSession, now time.Time) recordingSessionFile {
	file := recordingSessionFile{Schema: recordingSchema, RecordingID: s.params.RecordingID, GatewayID: r.gatewayID, BootID: r.boot, ScheduleID: s.params.ScheduleID,
		Params: s.params, State: s.stateAt(now), CreatedAt: s.created.UTC(), EndReason: s.endReason, Requests: s.requests, Recorded: s.recorded,
		Dropped: s.dropped, Truncated: s.truncated, Bytes: s.bytes, Segments: []string{}}
	if s.ready && !now.Before(s.activeFrom()) {
		at := s.activeFrom().UTC()
		file.StartedAt = &at
	}
	if s.finalized {
		at := s.stopTime().UTC()
		file.EndedAt = &at
	}
	for _, seg := range s.orderedSegments() {
		file.Segments = append(file.Segments, seg.name)
	}
	return file
}

func timelineLines(arrived time.Time, events []recordingEvent, ended time.Time, outcome string) []byte {
	var b strings.Builder
	write := func(line recordingTimelineLine) {
		line.Schema = recordingSchema
		b.Write(encodeJSON(line))
		b.WriteByte('\n')
	}
	write(recordingTimelineLine{Type: "arrived"})
	for _, e := range events {
		start, end := e.start, e.end
		line := recordingTimelineLine{Seconds: e.at.Seconds(), Duration: e.dur.Seconds()}
		switch e.kind {
		case recordingEventUpload:
			line.Type, line.End = "upload_completed", &end
		case recordingEventUploadFailed:
			line.Type, line.End = "upload_failed", &end
		case recordingEventHeader:
			line.Type, line.Status = "response_header", e.status
		case recordingEventWrite:
			line.Type, line.Start, line.End = "write", &start, &end
		case recordingEventFlush:
			line.Type, line.End = "flush", &end
		}
		write(line)
	}
	write(recordingTimelineLine{Type: "ended", Seconds: ended.Sub(arrived).Seconds(), Detail: outcome})
	return []byte(b.String())
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// write persists one record. Any filesystem failure ends sampling for every
// session; forwarding is unaffected.
func (r *trafficRecorder) write(snapshot *recordingSnapshot) {
	meta := &snapshot.meta
	segDir := filepath.Join(r.sessionDir(meta.RecordingID), meta.Segment)
	recordDir := filepath.Join(segDir, "r-"+meta.RequestID)
	timeline := timelineLines(meta.ArrivedAt, snapshot.events, meta.EndedAt, meta.Outcome)
	meta.RequestSHA256, meta.ResponseSHA256, meta.TimelineSHA256 = sha256Hex(snapshot.body), sha256Hex(snapshot.response), sha256Hex(timeline)
	var written int64
	err := os.MkdirAll(segDir, 0700)
	if err == nil {
		err = os.Mkdir(recordDir, 0700)
	}
	for _, f := range []struct {
		name string
		data []byte
	}{{"request.json", snapshot.body}, {meta.ResponseFile, snapshot.response}, {"timeline.jsonl", timeline}} {
		if err == nil {
			err = r.writeFile(filepath.Join(recordDir, f.name), f.data)
			written += int64(len(f.data))
		}
	}
	if err == nil {
		var n int64
		n, err = recordingAtomicJSON(recordDir, "meta.json", meta)
		written += n
	}
	if err == nil {
		line := append(encodeJSON(recordingIndexLine{Sequence: meta.Sequence, RequestID: meta.RequestID, OffsetSeconds: meta.OffsetSeconds, Method: meta.Method,
			Path: meta.Path, Status: meta.Status, Outcome: meta.Outcome, RequestState: meta.RequestState, CaptureState: meta.CaptureState,
			RequestBytes: meta.RequestBytes, RouterID: meta.RouterID, BindingRevision: meta.BindingRevision, Directory: "r-" + meta.RequestID}), '\n')
		var f *os.File
		if f, err = os.OpenFile(filepath.Join(segDir, "index.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600); err == nil {
			_, err = f.Write(line)
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
			written += int64(len(line))
		}
	}
	key := meta.RecordingID + "/" + meta.Segment
	r.mu.Lock()
	r.releaseLocked(snapshot)
	if err != nil {
		r.failures++
		snapshot.session.dropped++
		snapshot.segment.dropped++
		now := r.now()
		for _, s := range r.sessions {
			r.stopSessionLocked(s, now, "disk_failed")
		}
		r.mu.Unlock()
		_ = os.RemoveAll(recordDir)
		r.audit.Print(string(encodeJSON(map[string]any{"event": "recording_write_failed", "recording_id": meta.RecordingID, "request_id": meta.RequestID, "error": err.Error()})))
		return
	}
	s, seg := snapshot.session, snapshot.segment
	s.recorded++
	seg.recorded++
	if snapshot.truncate {
		s.truncated++
		seg.truncated++
	}
	s.bytes += written
	seg.bytes += written
	if meta.RouterID != "" {
		seg.routers[recordingRouter{RouterID: meta.RouterID, BindingRevision: meta.BindingRevision, ConfigDigest: meta.ConfigDigest}] = true
	}
	r.diskBytes += written
	stored := r.stored[key]
	if stored == nil {
		stored = &recordingStoredSegment{key: key, recording: meta.RecordingID, name: meta.Segment, start: seg.start}
		r.stored[key] = stored
	}
	stored.bytes += written
	if s.bytes >= s.params.MaxBytes {
		r.stopSessionLocked(s, r.now(), "max_bytes")
	}
	r.mu.Unlock()
	r.evict()
}

// evict removes whole sealed, unpinned segments, oldest first. The open
// segments of a live session are never removed; if they alone exceed the
// capacity, sampling stops.
func (r *trafficRecorder) evict() {
	r.mu.Lock()
	over := r.diskBytes > r.maxBytes
	r.mu.Unlock()
	if !over {
		return
	}
	candidates := []*recordingStoredSegment{}
	for _, v := range r.stored {
		if v.sealed && !v.pinned {
			candidates = append(candidates, v)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].start.Equal(candidates[j].start) {
			return candidates[i].start.Before(candidates[j].start)
		}
		return candidates[i].key < candidates[j].key
	})
	for _, v := range candidates {
		r.mu.Lock()
		over = r.diskBytes > r.maxBytes
		r.mu.Unlock()
		if !over {
			return
		}
		if err := os.RemoveAll(filepath.Join(r.sessionDir(v.recording), v.name)); err != nil {
			r.audit.Print(string(encodeJSON(map[string]any{"event": "recording_evict_failed", "segment": v.key, "error": err.Error()})))
			continue
		}
		r.mu.Lock()
		r.diskBytes -= v.bytes
		delete(r.stored, v.key)
		if s := r.findLocked(v.recording); s != nil {
			for _, seg := range s.segments {
				if seg.name == v.name {
					seg.evicted = true
				}
			}
		}
		r.mu.Unlock()
		r.audit.Print(string(encodeJSON(map[string]any{"event": "recording_segment_evicted", "segment": v.key, "bytes": v.bytes})))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.diskBytes > r.maxBytes {
		now := r.now()
		for _, s := range r.sessions {
			r.stopSessionLocked(s, now, "capacity")
		}
	}
}

type recordingSealJob struct {
	session *recordingSession
	segment *recordingSegmentState
	file    recordingSegmentFile
}

// maintain seals finished segments, persists session state transitions and
// finalizes drained sessions. On close, open work is marked end_unknown.
func (r *trafficRecorder) maintain(closing bool) {
	now := r.now()
	var seals []recordingSealJob
	var sessions []*recordingSession
	r.mu.Lock()
	for _, s := range r.sessions {
		if !s.ready || s.finalized {
			continue
		}
		state := s.stateAt(now)
		if state == "recording" {
			s.segmentLocked(now.UTC().Truncate(recordingSegment))
		}
		for _, seg := range s.segments {
			from, to := s.coverage(seg)
			if seg.sealed || (now.Before(to) && !closing) || (seg.inflight > 0 && !closing) {
				continue
			}
			routers := []recordingRouter{}
			for v := range seg.routers {
				routers = append(routers, v)
			}
			sort.Slice(routers, func(i, j int) bool {
				if routers[i].RouterID != routers[j].RouterID {
					return routers[i].RouterID < routers[j].RouterID
				}
				return routers[i].BindingRevision < routers[j].BindingRevision
			})
			sealState := "sealed"
			if seg.inflight > 0 {
				sealState = "end_unknown"
			}
			seals = append(seals, recordingSealJob{session: s, segment: seg, file: recordingSegmentFile{Schema: recordingSchema, RecordingID: s.params.RecordingID,
				GatewayID: r.gatewayID, BootID: r.boot, Segment: seg.name, Start: seg.start, End: seg.end, CoverageStart: from, CoverageEnd: to,
				Partial: to.Sub(from) < recordingSegment, State: sealState, Requests: seg.requests, Recorded: seg.recorded, Dropped: seg.dropped,
				Truncated: seg.truncated, Bytes: seg.bytes, Routers: routers}})
		}
		sessions = append(sessions, s)
	}
	r.mu.Unlock()
	for _, job := range seals {
		dir := filepath.Join(r.sessionDir(job.file.RecordingID), job.file.Segment)
		err := os.MkdirAll(dir, 0700)
		if err == nil {
			if f, e := os.Open(filepath.Join(dir, "index.jsonl")); e == nil {
				_ = f.Sync()
				f.Close()
			}
		}
		var size int64
		if err == nil {
			size, err = recordingAtomicJSON(dir, "segment.json", job.file)
		}
		r.mu.Lock()
		if err != nil {
			r.failures++
			for _, s := range r.sessions {
				r.stopSessionLocked(s, now, "disk_failed")
			}
		} else {
			job.segment.sealed = true
			job.segment.bytes += size
			job.session.bytes += size
			r.diskBytes += size
			key := job.file.RecordingID + "/" + job.file.Segment
			stored := r.stored[key]
			if stored == nil {
				stored = &recordingStoredSegment{key: key, recording: job.file.RecordingID, name: job.file.Segment, start: job.segment.start}
				r.stored[key] = stored
			}
			stored.bytes += size
			stored.sealed = true
		}
		r.mu.Unlock()
	}
	for _, s := range sessions {
		r.mu.Lock()
		state := s.stateAt(now)
		finalize := state == "draining" && (s.inflight == 0 || closing)
		if finalize {
			for _, seg := range s.segments {
				finalize = finalize && seg.sealed
			}
		}
		if finalize {
			if s.endReason == "" {
				s.endReason = "completed"
			}
			if closing && s.inflight > 0 {
				s.endReason = "end_unknown"
			}
			s.finalized = true
			state = "ended"
		}
		if state == s.persisted {
			r.mu.Unlock()
			continue
		}
		file := r.sessionFileLocked(s, now)
		r.mu.Unlock()
		_, err := recordingAtomicJSON(r.sessionDir(s.params.RecordingID), "session.json", file)
		r.mu.Lock()
		if err != nil {
			r.failures++
		} else {
			s.persisted = state
		}
		if s.finalized {
			r.removeSessionLocked(s)
			r.finished = append(r.finished, s)
			if len(r.finished) > recordingFinishedKept {
				r.finished = r.finished[len(r.finished)-recordingFinishedKept:]
			}
		}
		r.mu.Unlock()
	}
	r.evict()
}

// flush waits until queued work and maintenance have run once (tests only).
func (r *trafficRecorder) flush() {
	ack := make(chan struct{})
	r.control <- recordingControlJob{flushAck: ack}
	<-ack
}
