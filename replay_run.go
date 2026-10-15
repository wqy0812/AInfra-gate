package aigate

// Replay sending. timed keeps the recorded arrival schedule and never slows
// arrivals when the target slows; resource exhaustion ends new dispatch;
// concurrency sends the planned sequence with a fixed number of workers.
import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type replayProgress struct {
	Planned     int     `json:"planned"`
	Sent        int64   `json:"sent"`
	Completed   int64   `json:"completed"`
	Failed      int64   `json:"failed"`
	Inflight    int64   `json:"inflight"`
	Unsent      int64   `json:"unsent"`
	Unconfirmed int64   `json:"unconfirmed"`
	Elapsed     float64 `json:"elapsed_seconds"`
}

type replayTargetState struct {
	Name         string `json:"name"`
	ServiceID    string `json:"service_id,omitempty"`
	RouterID     string `json:"router_id,omitempty"`
	ConfigDigest string `json:"config_digest,omitempty"`
}

// replayRun is one run. Its state is persisted in the run directory.
type replaySourceProgress struct {
	State        string         `json:"state"`
	LastError    string         `json:"last_error,omitempty"`
	MaxSendDelay float64        `json:"max_send_delay_seconds"`
	GatewayID    string         `json:"gateway_id"`
	RecordingID  string         `json:"recording_id"`
	Progress     replayProgress `json:"progress"`
}

type replayRun struct {
	persistMu     sync.Mutex
	sources       []replaySourceProgress
	svc           *replayService
	req           replayRequest
	digest        string
	dir           string
	cancel        context.CancelFunc
	stopped       atomic.Bool
	done          chan struct{}
	segmentFiles  []*os.File
	leaseUntil    time.Time
	scheduledAt   *time.Time
	startDeadline time.Time
	startSignal   chan struct{}

	mu       sync.Mutex
	state    string
	errText  string
	created  time.Time
	started  *time.Time
	ended    *time.Time
	plan     *replayPlan
	target   replayTargetState
	progress replayProgress
	begin    time.Time

	sent, completed, failed, inflight atomic.Int64
	lastError                         string
	maxSendDelay                      float64
	maxWriteDelay                     *float64
	fatalError                        string
	output                            replayOutputSummary
	resultMu                          sync.Mutex
	results                           replayResultFile
}

type replayRunView struct {
	Sources        []replaySourceProgress `json:"sources,omitempty"`
	ScheduledAt    *time.Time             `json:"scheduled_at,omitempty"`
	Plan           *replayPlan            `json:"plan,omitempty"`
	RunID          string                 `json:"run_id"`
	State          string                 `json:"state"`
	Error          string                 `json:"error,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	StartedAt      *time.Time             `json:"started_at,omitempty"`
	EndedAt        *time.Time             `json:"ended_at,omitempty"`
	Request        *replayRequest         `json:"request,omitempty"`
	Target         replayTargetState      `json:"target"`
	Progress       replayProgress         `json:"progress"`
	LastError      string                 `json:"last_error,omitempty"`
	MaxSendDelay   float64                `json:"max_send_delay_seconds"`
	MaxWriteDelay  *float64               `json:"max_write_delay_seconds"`
	Output         replayOutputSummary    `json:"output_verification"`
	Excluded       map[string]int         `json:"excluded,omitempty"`
	Rewritten      []string               `json:"rewritten"`
	EndReason      string                 `json:"end_reason,omitempty"`
	StartCancelled bool                   `json:"start_cancelled,omitempty"`
}

func (r *replayRun) viewLocked() replayRunView {
	progress := r.progress
	if (r.state == "running" || r.state == "stopping") && !r.begin.IsZero() {
		progress.Sent, progress.Completed, progress.Failed, progress.Inflight = r.sent.Load(), r.completed.Load(), r.failed.Load(), r.inflight.Load()
		progress.Elapsed = time.Since(r.begin).Seconds()
	}
	progress.Unsent = max(0, int64(progress.Planned)-progress.Sent)
	var excluded map[string]int
	if r.plan != nil {
		excluded = r.plan.Excluded
	}
	reason := r.errText
	if reason == "" && r.ended != nil {
		reason = r.state
	}
	sources := append([]replaySourceProgress(nil), r.sources...)
	for i := range sources {
		sources[i].State = r.state
		if !r.begin.IsZero() && sources[i].Progress.Sent == int64(sources[i].Progress.Planned) && sources[i].Progress.Inflight == 0 {
			sources[i].State = "completed"
		}
		sources[i].Progress.Unsent = max(0, int64(sources[i].Progress.Planned)-sources[i].Progress.Sent)
	}
	return replayRunView{Sources: sources, ScheduledAt: r.scheduledAt, Plan: r.plan, EndReason: reason, Excluded: excluded, Rewritten: r.req.rewritten(), Output: r.output, MaxWriteDelay: r.maxWriteDelay, RunID: r.req.RunID, State: r.state, Error: r.errText, CreatedAt: r.created, StartedAt: r.started, EndedAt: r.ended,
		Request: &r.req, Target: r.target, Progress: progress, LastError: r.lastError, MaxSendDelay: r.maxSendDelay}
}

func (r *replayRun) view() replayRunView {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.viewLocked()
}

func (r *replayRun) persist() error {
	r.persistMu.Lock()
	defer r.persistMu.Unlock()
	r.mu.Lock()
	view := r.viewLocked()
	r.mu.Unlock()
	if _, err := r.svc.writeJSON(r.dir, "state.json", view); err != nil {
		r.svc.audit.Print(string(encodeJSON(map[string]any{"event": "replay_state_write_failed", "run_id": r.req.RunID, "error": err.Error()})))
		return err
	}
	return nil
}

func (r *replayRun) finish(state, errText string) {
	// Publish the terminal state and release the slot under the service lock.
	// start/stop use the same service -> run lock order.
	r.svc.mu.Lock()
	defer r.svc.mu.Unlock()
	r.mu.Lock()
	// stop() acknowledges under this same lock. A successful stop cannot be
	// overwritten by an already chosen completed transition.
	if r.fatalError != "" {
		state, errText = "failed", r.fatalError
	}
	if state == "completed" && r.stopped.Load() {
		state = "stopped"
	}
	now := time.Now().UTC()
	r.state, r.errText, r.ended = state, errText, &now
	r.progress.Sent, r.progress.Completed, r.progress.Failed, r.progress.Inflight = r.sent.Load(), r.completed.Load(), r.failed.Load(), 0
	if !r.begin.IsZero() {
		r.progress.Elapsed = time.Since(r.begin).Seconds()
	}
	r.mu.Unlock()
	if err := r.persist(); err != nil {
		r.mu.Lock()
		r.state, r.errText = "failed", "storage_failed"
		r.mu.Unlock()
	}
	for _, f := range r.segmentFiles {
		unlockState(f)
		f.Close()
	}
	r.segmentFiles = nil
	if r.svc.active == r {
		r.svc.active = nil
	}
	close(r.done)
}

// execute plans, checks the target, sends and persists progress.
func (r *replayRun) execute(ctx context.Context) {
	// One descriptor per selected segment (at most 96), using the same
	// nonblocking directory lock as capacity eviction. No source files change.
	for _, ref := range r.req.sources() {
		for _, name := range ref.Segments {
			f, err := os.Open(filepath.Join(r.svc.recordRoot, ref.GatewayID, ref.RecordingID, name))
			if err != nil {
				r.finish("failed", "segment_not_found")
				return
			}
			if err := lockState(f); err != nil {
				f.Close()
				r.finish("failed", "segment_busy")
				return
			}
			r.segmentFiles = append(r.segmentFiles, f)
		}
	}
	plan, err := buildReplayPlan(r.svc.recordRoot, r.req)
	if err != nil {
		r.finish("failed", err.Error())
		return
	}
	r.mu.Lock()
	r.plan = plan
	r.progress.Planned = plan.Planned
	for _, ref := range r.req.sources() {
		source := replaySourceProgress{GatewayID: ref.GatewayID, RecordingID: ref.RecordingID}
		for _, seg := range plan.Segments {
			if seg.GatewayID == ref.GatewayID && seg.RecordingID == ref.RecordingID {
				source.Progress.Planned += seg.Planned
			}
		}
		r.sources = append(r.sources, source)
	}
	r.mu.Unlock()
	if err = writeReplayPlan(r.dir, plan); err != nil {
		r.finish("failed", "storage_failed")
		return
	}
	target, err := r.svc.checkTarget(ctx, r.req.Target)
	if r.stopped.Load() {
		r.finish("stopped", "")
		return
	}
	if err != nil {
		r.finish("failed", err.Error())
		return
	}
	if r.req.PrepareOnly {
		r.mu.Lock()
		if r.stopped.Load() || ctx.Err() != nil {
			r.mu.Unlock()
			r.finish("stopped", "")
			return
		}
		r.target = target
		r.state = "prepared"
		r.mu.Unlock()
		if err := r.persist(); err != nil {
			r.finish("failed", "storage_failed")
			return
		}
		select {
		case <-ctx.Done():
			r.finish("stopped", "")
			return
		case <-r.startSignal:
		}
		r.mu.Lock()
		due := r.startDeadline
		r.mu.Unlock()
		timer := time.NewTimer(time.Until(due))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			r.finish("stopped", "")
			return
		case <-timer.C:
		}
		if time.Since(due) > replayStartTolerance {
			r.finish("failed", "sync_start_late")
			return
		}
	}
	now := time.Now()
	started := now.UTC()
	r.mu.Lock()
	if r.stopped.Load() || ctx.Err() != nil {
		r.mu.Unlock()
		r.finish("stopped", "")
		return
	}
	r.target, r.started, r.begin = target, &started, now
	r.state = "running"
	r.mu.Unlock()
	if err := r.persist(); err != nil {
		r.finish("failed", "storage_failed")
		return
	}
	results, err := r.svc.openResults(filepath.Join(r.dir, "results.jsonl"))
	if err != nil {
		r.mu.Lock()
		r.output.ResultsFailed = true
		r.mu.Unlock()
		r.finish("failed", "replay_results_failed")
		return
	}
	r.results = results
	if r.req.Mode == "timed" {
		r.sendTimed(ctx, plan)
	} else {
		r.sendConcurrent(ctx, plan)
	}
	if err := results.Close(); err != nil {
		r.mu.Lock()
		r.output.ResultsFailed = true
		r.mu.Unlock()
		r.abort("replay_results_failed")
	}
	r.finish("completed", "")
}

func writeReplayPlan(dir string, plan *replayPlan) error {
	if _, err := recordingAtomicJSON(dir, "plan.json", plan); err != nil {
		return err
	}
	var b bytes.Buffer
	for _, item := range plan.items {
		b.Write(encodeJSON(item))
		b.WriteByte('\n')
	}
	return writeRecordingFile(filepath.Join(dir, "plan.jsonl"), b.Bytes())
}

func (r *replayRun) sendTimed(ctx context.Context, plan *replayPlan) {
	var wg sync.WaitGroup
	timer := time.NewTimer(0)
	defer timer.Stop()
	for _, item := range plan.items {
		due := r.begin.Add(time.Duration(item.OffsetSeconds * float64(time.Second)))
		if wait := time.Until(due); wait > 0 {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
			}
		}
		if ctx.Err() != nil {
			break
		}
		charge := replayBodyCharge(item)
		if !r.svc.bodyMemory.tryAcquire(charge) {
			r.abort("replay_resource_exhausted")
			break
		}
		wg.Add(1)
		go func(item replayPlanItem, due time.Time) {
			defer wg.Done()
			r.send(ctx, item, due, charge)
		}(item, due)
	}
	wg.Wait()
}

func (r *replayRun) sendConcurrent(ctx context.Context, plan *replayPlan) {
	next := atomic.Int64{}
	var wg sync.WaitGroup
	for i := 0; i < r.req.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				n := int(next.Add(1)) - 1
				if n >= len(plan.items) {
					return
				}
				charge := replayBodyCharge(plan.items[n])
				if !r.svc.bodyMemory.acquire(ctx, charge) {
					return
				}
				r.send(ctx, plan.items[n], time.Now(), charge)
			}
		}()
	}
	wg.Wait()
}

func replayBodyCharge(item replayPlanItem) int64 { return item.RequestBytes*6 + (1 << 20) }

// The dispatch claim under r.mu is the stop boundary; claimed requests drain
// using the service context. Every started preparation also joins the drain.
func (r *replayRun) send(runCtx context.Context, item replayPlanItem, due time.Time, charge int64) {
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { r.svc.bodyMemory.release(charge) }) }
	defer release()
	result := replayResult{GatewayID: item.GatewayID, RecordingID: item.RecordingID, Segment: item.Segment, Sequence: item.Sequence, OriginalRequestID: item.RequestID, RequestID: randomID(), PlannedOffset: item.OffsetSeconds, Rewritten: []string{}}
	// Preparation failures and stop-before-send remain visible with null offsets.
	defer func() {
		r.saveResult(result)
		r.mu.Lock()
		for i := range r.sources {
			source := &r.sources[i]
			if source.GatewayID != item.GatewayID || source.RecordingID != item.RecordingID {
				continue
			}
			if result.SendOffset != nil {
				source.Progress.Inflight--
			}
			if result.Error != "" && result.Error != "not_sent" {
				source.Progress.Failed++
				source.LastError = result.Error
			} else if result.SendOffset != nil {
				source.Progress.Completed++
			}
		}
		r.mu.Unlock()
	}()
	fail := func(detail string) {
		result.Error = detail
		r.failed.Add(1)
		r.mu.Lock()
		r.lastError = detail
		r.mu.Unlock()
	}
	if runCtx.Err() != nil {
		result.Error = "not_sent"
		return
	}
	body, err := readReplayBody(item)
	if err != nil {
		fail(err.Error())
		r.abort(err.Error())
		return
	}
	rewritten := rewriteReplayBody(body, r.req)
	if !bytes.Equal(body, rewritten) {
		result.Rewritten = r.req.rewritten()
	}
	body = rewritten
	ctx, cancel := context.WithTimeout(r.svc.ctx, time.Duration(r.req.RequestTimeoutSeconds)*time.Second)
	defer cancel()
	var traceMu sync.Mutex
	var written *float64
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			offset := time.Since(r.begin).Seconds()
			traceMu.Lock()
			written = &offset
			traceMu.Unlock()
		}
	}})
	defer func() {
		traceMu.Lock()
		result.WriteOffset = written
		if written != nil {
			delay := *written - due.Sub(r.begin).Seconds()
			result.writeDelay = &delay
		}
		traceMu.Unlock()
	}()
	upload := &replayUpload{reader: bytes.NewReader(body), release: release}
	defer upload.Close()
	length := len(body)
	body = nil
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, r.svc.targets[r.req.Target]+item.Path, upload)
	if err != nil {
		fail("invalid_request")
		return
	}
	request.ContentLength = int64(length)
	for k, v := range item.Headers {
		request.Header.Set(k, v)
	}
	if request.Header.Get("User-Agent") == "" {
		request.Header.Set("User-Agent", "")
	}
	request.Header.Set(headerRouter, r.target.RouterID)
	request.Header.Set(headerExpectedConfig, r.target.ConfigDigest)
	request.Header.Set(headerReplay, replayMarkValue(r.svc.markKey, r.req.RunID, uint64(item.Sequence)))
	request.Header.Set("X-Request-ID", result.RequestID)
	r.mu.Lock()
	if r.stopped.Load() || runCtx.Err() != nil {
		r.mu.Unlock()
		result.Error = "not_sent"
		return
	}
	offset := time.Since(r.begin).Seconds()
	result.SendOffset = &offset
	if delay := time.Since(due).Seconds(); delay > r.maxSendDelay {
		r.maxSendDelay = delay
	}
	r.sent.Add(1)
	for i := range r.sources {
		if r.sources[i].GatewayID == item.GatewayID && r.sources[i].RecordingID == item.RecordingID {
			r.sources[i].Progress.Sent++
			r.sources[i].Progress.Inflight++
			r.sources[i].MaxSendDelay = max(r.sources[i].MaxSendDelay, time.Since(due).Seconds())
		}
	}
	r.inflight.Add(1)
	r.mu.Unlock()
	defer r.inflight.Add(-1)
	response, err := r.svc.client.Do(request)
	if err != nil {
		fail(replayErrorClass(err))
		return
	}
	defer response.Body.Close()
	result.HTTPStatus = response.StatusCode
	observer := newResponseObserver(response.Header)
	var destination io.Writer = observer
	var errorPrefix replayErrorPrefix
	if response.StatusCode < 200 || response.StatusCode > 299 {
		destination = io.MultiWriter(observer, &errorPrefix)
	}
	_, err = io.CopyBuffer(destination, response.Body, make([]byte, 32<<10))
	transport := ""
	if err != nil {
		transport = replayErrorClass(err)
	}
	observed := observer.finish(transport)
	result.Observation, result.ErrorSummary = &observed, observed.ErrorSummary
	if result.ErrorSummary == "" {
		result.ErrorSummary = string(errorPrefix.data[:errorPrefix.used])
	}
	if err != nil {
		fail(replayErrorClass(err))
		return
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		fail("http_" + strconv.Itoa(response.StatusCode))
		return
	}
	if result.Observation != nil && result.Observation.ResponseError {
		fail("response_error")
		return
	}
	r.completed.Add(1)
}

func replayErrorClass(err error) string {
	var netErr net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	}
	return "connection_error"
}
