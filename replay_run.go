package aigate

// Replay sending. timed keeps the recorded arrival schedule and never slows
// arrivals when the target slows (max_inflight only records send delay);
// concurrency sends the planned sequence with a fixed number of workers.
import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

const replayLineLimit = 1 << 20

type replayResult struct {
	Sequence          int               `json:"sequence"`
	Segment           string            `json:"segment"`
	Warmup            bool              `json:"warmup,omitempty"`
	RequestID         string            `json:"recorded_request_id"`
	ScheduledSeconds  float64           `json:"scheduled_seconds"`
	SentSeconds       float64           `json:"sent_seconds"`
	SendDelaySeconds  float64           `json:"send_delay_seconds"`
	Status            int               `json:"status"`
	HeaderSeconds     *float64          `json:"header_seconds,omitempty"`
	FirstByteSeconds  *float64          `json:"first_byte_seconds,omitempty"`
	TTFTSeconds       *float64          `json:"ttft_seconds,omitempty"`
	E2ESeconds        float64           `json:"e2e_seconds"`
	ResponseBytes     int64             `json:"response_bytes"`
	Usage             map[string]uint64 `json:"usage,omitempty"`
	FinishReason      string            `json:"finish_reason,omitempty"`
	Done              bool              `json:"done"`
	Outcome           string            `json:"outcome"`
	Error             string            `json:"error,omitempty"`
	TargetTokens      uint64            `json:"target_tokens,omitempty"`
	InflightAtSend    int               `json:"inflight_at_send"`
	ReplayRequestID   string            `json:"replay_request_id"`
	UpstreamRequestID string            `json:"upstream_request_id,omitempty"`
}

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
	report   *replayReport
	progress replayProgress
	begin    time.Time

	sent, completed, failed, inflight atomic.Int64
	resultsMu                         sync.Mutex
	results                           []replayResult
	resultFile                        *os.File
	resultWriter                      *bufio.Writer
}

type replayRunView struct {
	RunID     string            `json:"run_id"`
	State     string            `json:"state"`
	Error     string            `json:"error,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	StartedAt *time.Time        `json:"started_at,omitempty"`
	EndedAt   *time.Time        `json:"ended_at,omitempty"`
	Request   replayRequest     `json:"request"`
	Target    replayTargetState `json:"target"`
	Progress  replayProgress    `json:"progress"`
	Report    *replayReport     `json:"report,omitempty"`
}

func (r *replayRun) viewLocked() replayRunView {
	progress := r.progress
	if (r.state == "running" || r.state == "stopping") && !r.begin.IsZero() {
		progress.Sent, progress.Completed, progress.Failed, progress.Inflight = r.sent.Load(), r.completed.Load(), r.failed.Load(), r.inflight.Load()
		progress.Elapsed = time.Since(r.begin).Seconds()
	}
	return replayRunView{RunID: r.req.RunID, State: r.state, Error: r.errText, CreatedAt: r.created, StartedAt: r.started, EndedAt: r.ended,
		Request: r.req, Target: r.target, Progress: progress, Report: r.report}
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
	// Release the single-run slot before the end becomes visible, so a caller
	// that observes the end can start the next run immediately.
	r.svc.release(r)
	r.mu.Lock()
	now := time.Now().UTC()
	r.state, r.errText, r.ended = state, errText, &now
	r.progress.Sent, r.progress.Completed, r.progress.Failed, r.progress.Inflight = r.sent.Load(), r.completed.Load(), r.failed.Load(), 0
	if !r.begin.IsZero() {
		r.progress.Elapsed = time.Since(r.begin).Seconds()
	}
	r.mu.Unlock()
	r.persist()
	close(r.done)
}

// execute plans, checks the target, sends and reports.
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
	file, err := os.OpenFile(filepath.Join(r.dir, "results.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		r.finish("failed", "storage_failed")
		return
	}
	r.resultFile, r.resultWriter = file, bufio.NewWriterSize(file, 256<<10)
	now := time.Now()
	started := now.UTC()
	r.mu.Lock()
	r.target, r.state, r.started, r.begin = target, "running", &started, now
	r.mu.Unlock()
	r.persist()
	if r.req.Mode == "timed" {
		r.sendTimed(ctx, plan)
	} else {
		r.sendConcurrent(ctx, plan)
	}
	if err = r.resultWriter.Flush(); err == nil {
		err = r.resultFile.Sync()
	}
	r.resultFile.Close()
	report := buildReplayReport(r.req, plan, target, r.results, started, time.Now().UTC())
	r.mu.Lock()
	r.report = report
	r.mu.Unlock()
	if _, werr := recordingAtomicJSON(r.dir, "report.json", report); werr != nil && err == nil {
		err = werr
	}
	switch {
	case err != nil:
		r.finish("failed", "storage_failed")
	case r.stopped.Load():
		r.finish("stopped", "")
	default:
		r.finish("completed", "")
	}
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
			r.send(item, due)
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
				r.send(plan.items[n], time.Now())
			}
		}()
	}
	wg.Wait()
}

func secondsPtr(d time.Duration) *float64 {
	v := d.Seconds()
	return &v
}

func (r *replayRun) record(result replayResult) {
	r.resultsMu.Lock()
	defer r.resultsMu.Unlock()
	r.results = append(r.results, result)
	r.resultWriter.Write(encodeJSON(result))
	r.resultWriter.WriteByte('\n')
}

// send issues one request. Requests already sent run to completion even when
// the run is stopped; they are bounded by request_timeout_seconds.
func (r *replayRun) send(item replayPlanItem, due time.Time) {
	result := replayResult{Sequence: item.Sequence, Segment: item.Segment, Warmup: item.Warmup, RequestID: item.RequestID, ScheduledSeconds: due.Sub(r.begin).Seconds(),
		TargetTokens: item.TargetTokens, ReplayRequestID: randomID()}
	fail := func(outcome, detail string) {
		result.Outcome, result.Error = outcome, detail
		r.failed.Add(1)
		r.record(result)
	}
	body, err := os.ReadFile(filepath.Join(item.Directory, "request.json"))
	if err != nil || sha256Hex(body) != item.RequestSHA256 {
		fail("not_sent", "record_changed")
		return
	}
	if body, err = rewriteReplayBody(body, r.req, item); err != nil {
		fail("not_sent", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.svc.ctx, time.Duration(r.req.RequestTimeoutSeconds)*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, r.svc.targets[r.req.Target]+item.Path, bytes.NewReader(body))
	if err != nil {
		fail("not_sent", "invalid_request")
		return
	}
	for k, v := range item.Headers {
		request.Header.Set(k, v)
	}
	// The proxy answers 409 if it was reconfigured or replaced after the check.
	request.Header.Set(headerRouter, r.target.RouterID)
	request.Header.Set(headerExpectedConfig, r.target.ConfigDigest)
	request.Header.Set(headerReplay, replayMarkValue(r.svc.markKey, r.req.RunID, uint64(item.Sequence)))
	request.Header.Set("X-Request-ID", result.ReplayRequestID)
	result.InflightAtSend = int(r.inflight.Add(1))
	defer r.inflight.Add(-1)
	sent := time.Now()
	result.SentSeconds = sent.Sub(r.begin).Seconds()
	result.SendDelaySeconds = sent.Sub(due).Seconds()
	if result.SendDelaySeconds < 0 {
		result.SendDelaySeconds = 0
	}
	r.sent.Add(1)
	response, err := r.svc.client.Do(request)
	if err != nil {
		result.E2ESeconds = time.Since(sent).Seconds()
		outcome := "transport_error"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			outcome = "timeout"
		} else if r.svc.ctx.Err() != nil {
			outcome = "cancelled"
		}
		fail(outcome, replayErrorClass(err))
		return
	}
	defer response.Body.Close()
	result.Status = response.StatusCode
	result.HeaderSeconds = secondsPtr(time.Since(sent))
	result.UpstreamRequestID = diagnosticClientID(response.Header.Get("X-Request-ID"))
	parser := &replayResponseParser{sent: sent, stream: isEventStream(response.Header.Get("Content-Type"))}
	readErr := parser.read(response.Body)
	result.E2ESeconds = time.Since(sent).Seconds()
	result.ResponseBytes, result.FirstByteSeconds, result.TTFTSeconds = parser.bytes, parser.firstByte, parser.ttft
	result.Usage, result.FinishReason, result.Done = parser.usage, parser.finish, parser.done
	switch {
	case readErr != nil && errors.Is(ctx.Err(), context.DeadlineExceeded):
		fail("timeout", "response_timeout")
		return
	case readErr != nil:
		fail("stream_interrupted", replayErrorClass(readErr))
		return
	case response.StatusCode < 200 || response.StatusCode > 299:
		fail("http_error", strconv.Itoa(response.StatusCode))
		return
	case parser.stream && !parser.done:
		fail("stream_incomplete", "missing_done")
		return
	}
	result.Outcome = "completed"
	r.completed.Add(1)
	r.record(result)
}

func isEventStream(contentType string) bool {
	return len(contentType) >= 17 && bytes.EqualFold([]byte(contentType[:17]), []byte("text/event-stream"))
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

// replayResponseParser keeps only timings, usage and the finish reason.
type replayResponseParser struct {
	sent             time.Time
	stream           bool
	bytes            int64
	firstByte, ttft  *float64
	usage            map[string]uint64
	finish           string
	done             bool
	pending          []byte
	nonStreamPayload []byte
}

func (p *replayResponseParser) read(body io.Reader) error {
	buf := make([]byte, 32<<10)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if p.firstByte == nil {
				p.firstByte = secondsPtr(time.Since(p.sent))
			}
			p.bytes += int64(n)
			if p.stream {
				p.feed(buf[:n])
			} else if len(p.nonStreamPayload) < 16<<20 {
				p.nonStreamPayload = append(p.nonStreamPayload, buf[:n]...)
			}
		}
		if err == io.EOF {
			if p.stream && len(p.pending) > 0 {
				p.line(p.pending)
			}
			if !p.stream {
				p.nonStream()
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (p *replayResponseParser) feed(data []byte) {
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			if len(p.pending)+len(data) <= replayLineLimit {
				p.pending = append(p.pending, data...)
			} else {
				p.pending = p.pending[:0]
			}
			return
		}
		line := data[:i]
		if len(p.pending) > 0 {
			line = append(p.pending, line...)
			p.pending = p.pending[:0]
		}
		p.line(line)
		data = data[i+1:]
	}
}

func (p *replayResponseParser) line(line []byte) {
	line = bytes.TrimRight(line, "\r")
	if !bytes.HasPrefix(line, []byte("data:")) {
		return
	}
	payload := bytes.TrimSpace(line[5:])
	if bytes.Equal(payload, []byte("[DONE]")) {
		p.done = true
		return
	}
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content          json.RawMessage `json:"content"`
				ReasoningContent json.RawMessage `json:"reasoning_content"`
				Reasoning        json.RawMessage `json:"reasoning"`
				ToolCalls        json.RawMessage `json:"tool_calls"`
			} `json:"delta"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage json.RawMessage `json:"usage"`
	}
	if json.Unmarshal(payload, &chunk) != nil {
		return
	}
	for _, choice := range chunk.Choices {
		d := choice.Delta
		if p.ttft == nil && (usefulRaw(d.Content) || usefulRaw(d.ReasoningContent) || usefulRaw(d.Reasoning) || usefulRaw(d.ToolCalls)) {
			p.ttft = secondsPtr(time.Since(p.sent))
		}
		if choice.FinishReason != nil && *choice.FinishReason != "" {
			p.finish = *choice.FinishReason
		}
	}
	if len(chunk.Usage) > 0 && !isNull(chunk.Usage) {
		if usage := profileUsage(chunk.Usage); len(usage) > 0 {
			p.usage = usage
		}
	}
}

// usefulRaw is true for a non-empty string or a non-empty array.
func usefulRaw(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || isNull(raw) {
		return false
	}
	switch raw[0] {
	case '"':
		var s string
		return json.Unmarshal(raw, &s) == nil && s != ""
	case '[':
		var v []json.RawMessage
		return json.Unmarshal(raw, &v) == nil && len(v) > 0
	}
	return false
}

func (p *replayResponseParser) nonStream() {
	var v struct {
		Choices []struct {
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage json.RawMessage `json:"usage"`
	}
	if json.Unmarshal(p.nonStreamPayload, &v) != nil {
		return
	}
	for _, c := range v.Choices {
		if c.FinishReason != nil {
			p.finish = *c.FinishReason
		}
	}
	if len(v.Usage) > 0 && !isNull(v.Usage) {
		p.usage = profileUsage(v.Usage)
	}
	p.ttft = p.firstByte
}
