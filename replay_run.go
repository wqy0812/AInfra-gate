package aigate

// Replay sending. timed keeps the recorded arrival schedule and never slows
// arrivals when the target slows (max_inflight only records send delay);
// concurrency sends the planned sequence with a fixed number of workers.
import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type replayProgress struct {
	Planned   int     `json:"planned"`
	Sent      int64   `json:"sent"`
	Completed int64   `json:"completed"`
	Failed    int64   `json:"failed"`
	Inflight  int64   `json:"inflight"`
	Elapsed   float64 `json:"elapsed_seconds"`
}

type replayTargetState struct {
	Name         string `json:"name"`
	ServiceID    string `json:"service_id,omitempty"`
	RouterID     string `json:"router_id,omitempty"`
	ConfigDigest string `json:"config_digest,omitempty"`
}

// replayRun is one run. Its state is persisted in the run directory.
type replayRun struct {
	svc     *replayService
	req     replayRequest
	digest  string
	dir     string
	cancel  context.CancelFunc
	stopped atomic.Bool
	done    chan struct{}

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
}

type replayRunView struct {
	RunID          string            `json:"run_id"`
	State          string            `json:"state"`
	Error          string            `json:"error,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	StartedAt      *time.Time        `json:"started_at,omitempty"`
	EndedAt        *time.Time        `json:"ended_at,omitempty"`
	Request        *replayRequest    `json:"request,omitempty"`
	Target         replayTargetState `json:"target"`
	Progress       replayProgress    `json:"progress"`
	LastError      string            `json:"last_error,omitempty"`
	MaxSendDelay   float64           `json:"max_send_delay_seconds"`
	StartCancelled bool              `json:"start_cancelled,omitempty"`
}

func (r *replayRun) viewLocked() replayRunView {
	progress := r.progress
	if (r.state == "running" || r.state == "stopping") && !r.begin.IsZero() {
		progress.Sent, progress.Completed, progress.Failed, progress.Inflight = r.sent.Load(), r.completed.Load(), r.failed.Load(), r.inflight.Load()
		progress.Elapsed = time.Since(r.begin).Seconds()
	}
	return replayRunView{RunID: r.req.RunID, State: r.state, Error: r.errText, CreatedAt: r.created, StartedAt: r.started, EndedAt: r.ended,
		Request: &r.req, Target: r.target, Progress: progress, LastError: r.lastError, MaxSendDelay: r.maxSendDelay}
}

func (r *replayRun) view() replayRunView {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.viewLocked()
}

func (r *replayRun) persist() {
	r.mu.Lock()
	view := r.viewLocked()
	r.mu.Unlock()
	if _, err := recordingAtomicJSON(r.dir, "state.json", view); err != nil {
		r.svc.audit.Print(string(encodeJSON(map[string]any{"event": "replay_state_write_failed", "run_id": r.req.RunID, "error": err.Error()})))
	}
}

func (r *replayRun) finish(state, errText string) {
	// Publish the terminal state and release the slot under the service lock.
	// start/stop use the same service -> run lock order.
	r.svc.mu.Lock()
	defer r.svc.mu.Unlock()
	r.mu.Lock()
	// stop() acknowledges under this same lock. A successful stop cannot be
	// overwritten by an already chosen completed transition.
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
	r.persist()
	if r.svc.active == r {
		r.svc.active = nil
	}
	close(r.done)
}

// execute plans, checks the target, sends and persists progress.
func (r *replayRun) execute(ctx context.Context) {
	plan, err := buildReplayPlan(r.svc.recordRoot, r.req)
	if err != nil {
		r.finish("failed", err.Error())
		return
	}
	r.mu.Lock()
	r.plan = plan
	r.progress.Planned = plan.Planned
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
	now := time.Now()
	started := now.UTC()
	r.mu.Lock()
	r.target, r.started, r.begin = target, &started, now
	if !r.stopped.Load() {
		r.state = "running"
	}
	r.mu.Unlock()
	r.persist()
	if r.req.Mode == "timed" {
		r.sendTimed(ctx, plan)
	} else {
		r.sendConcurrent(ctx, plan)
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
	slots := make(chan struct{}, r.req.MaxInflight)
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
		// At max_inflight the send waits and the delay is recorded; the
		// schedule itself never stretches into a fixed concurrency.
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(item replayPlanItem, due time.Time) {
			defer wg.Done()
			defer func() { <-slots }()
			r.send(ctx, item, due)
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
				r.send(ctx, plan.items[n], time.Now())
			}
		}()
	}
	wg.Wait()
}

// send reserves bounded body memory before reading. The dispatch claim under
// r.mu is the stop boundary; claimed requests drain using the service context.
func (r *replayRun) send(runCtx context.Context, item replayPlanItem, due time.Time) {
	fail := func(detail string) {
		r.failed.Add(1)
		r.mu.Lock()
		r.lastError = detail
		r.mu.Unlock()
	}
	path := filepath.Join(item.Directory, "request.json")
	info, err := os.Stat(path)
	if err != nil {
		fail("record_unreadable")
		return
	}
	if !info.Mode().IsRegular() || info.Size() != item.RequestBytes || info.Size() < 0 || info.Size() > recordingMemory {
		fail("record_changed")
		return
	}
	// Charge for the source, rewritten body and JSON temporary allocations.
	charge := info.Size()*6 + (1 << 20)
	if !r.svc.bodyMemory.acquire(runCtx, charge) {
		return
	}
	defer r.svc.bodyMemory.release(charge)
	file, err := os.Open(path)
	if err != nil {
		fail("record_unreadable")
		return
	}
	body, err := io.ReadAll(io.LimitReader(file, info.Size()+1))
	file.Close()
	if err != nil || int64(len(body)) != info.Size() || sha256Hex(body) != item.RequestSHA256 {
		fail("record_changed")
		return
	}
	if body, err = rewriteReplayBody(body, r.req, item); err != nil {
		fail(err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.svc.ctx, time.Duration(r.req.RequestTimeoutSeconds)*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, r.svc.targets[r.req.Target]+item.Path, bytes.NewReader(body))
	if err != nil {
		fail("invalid_request")
		return
	}
	for k, v := range item.Headers {
		request.Header.Set(k, v)
	}
	if request.Header.Get("User-Agent") == "" {
		request.Header.Set("User-Agent", "")
	}
	request.Header.Set(headerRouter, r.target.RouterID)
	request.Header.Set(headerExpectedConfig, r.target.ConfigDigest)
	request.Header.Set(headerReplay, replayMarkValue(r.svc.markKey, r.req.RunID, uint64(item.Sequence)))
	request.Header.Set("X-Request-ID", randomID())
	r.mu.Lock()
	if r.stopped.Load() || runCtx.Err() != nil {
		r.mu.Unlock()
		return
	}
	if delay := time.Since(due).Seconds(); delay > r.maxSendDelay {
		r.maxSendDelay = delay
	}
	r.sent.Add(1)
	r.inflight.Add(1)
	r.mu.Unlock()
	defer r.inflight.Add(-1)
	response, err := r.svc.client.Do(request)
	if err != nil {
		fail(replayErrorClass(err))
		return
	}
	defer response.Body.Close()
	// Consume responses with constant memory; performance analysis belongs to
	// monitoring, so replay does not parse usage, TTFT or full response bodies.
	_, err = io.Copy(io.Discard, response.Body)
	if err != nil {
		fail(replayErrorClass(err))
		return
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		fail("http_" + strconv.Itoa(response.StatusCode))
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
