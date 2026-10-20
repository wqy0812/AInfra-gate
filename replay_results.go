package aigate

import (
	"bytes"
	"io"
	"sync"
)

type replayOutputSummary struct {
	Matched       uint64 `json:"matched"`
	Short         uint64 `json:"short"`
	Long          uint64 `json:"long"`
	Unverifiable  uint64 `json:"unverifiable"`
	ActualTokens  uint64 `json:"actual_tokens"`
	Unknown       uint64 `json:"unknown"`
	ResultsFailed bool   `json:"results_failed"`
	Incomplete    bool   `json:"incomplete,omitempty"`
}

type replayResult struct {
	TimingVersion     int      `json:"timing_version,omitempty"`
	EndOffset         *float64 `json:"end_offset_seconds,omitempty"`
	FirstOutput       *float64 `json:"first_output_seconds,omitempty"`
	LastOutput        *float64 `json:"last_output_seconds,omitempty"`
	Stream            bool     `json:"stream"`
	RequestStream     *bool    `json:"request_stream,omitempty"`
	MultipleChoices   bool     `json:"multiple_choices,omitempty"`
	GatewayID         string   `json:"gateway_id"`
	RecordingID       string   `json:"recording_id"`
	Segment           string   `json:"segment"`
	Sequence          int      `json:"sequence"`
	OriginalRequestID string   `json:"original_request_id"`
	RequestID         string   `json:"request_id"`
	PlannedOffset     float64  `json:"planned_offset_seconds"`
	SendOffset        *float64 `json:"send_offset_seconds"`
	WriteOffset       *float64 `json:"write_offset_seconds"`
	HTTPStatus        int      `json:"http_status"`
	Error             string   `json:"error,omitempty"`
	ErrorSummary      string   `json:"error_summary,omitempty"`
	// Retained only for reading historical equal-output result files.
	RecordedTokens *uint64              `json:"recorded_tokens,omitempty"`
	TargetTokens   *uint64              `json:"target_tokens,omitempty"`
	ActualTokens   *uint64              `json:"actual_tokens"`
	Observation    *responseObservation `json:"response_result,omitempty"`
	Verification   string               `json:"verification,omitempty"`
	Rewritten      []string             `json:"rewritten"`
	writeDelay     *float64
}

type replayResultFile interface {
	io.WriteCloser
	Sync() error
}

// HTTP error bodies need a useful bounded diagnostic even for plain text or
// HTML responses that the JSON observer cannot parse.
type replayErrorPrefix struct {
	data [responseErrorLimit]byte
	used int
}

func (p *replayErrorPrefix) Write(b []byte) (int, error) {
	p.used += copy(p.data[p.used:], b)
	return len(b), nil
}

func (r *replayRun) abort(reason string) {
	r.mu.Lock()
	if r.ended != nil {
		r.mu.Unlock()
		return
	}
	changed := r.fatalError == ""
	if changed {
		r.fatalError = reason
	}
	// Expose fatal dispatch failure while accepted requests still drain, so
	// the platform can stop the other overlay participant before we finish.
	r.state, r.errText = "stopping", r.fatalError
	r.stopped.Store(true)
	r.mu.Unlock()
	r.cancel()
	if changed {
		// Best effort: a storage failure must not recurse into abort.
		_ = r.persist()
	}
}

func (r *replayRun) saveResult(result replayResult) {
	if result.Observation != nil && result.Observation.UsageKnown {
		a := result.Observation.Usage["completion_tokens"]
		result.ActualTokens = &a
	}
	// Serialize file writes separately from dispatch/state locks. A slow disk
	// cannot throttle timed dispatch by holding the run lock.
	r.resultMu.Lock()
	line := append(encodeJSON(result), '\n')
	n, err := r.results.Write(line)
	if err == nil && n != len(line) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = r.results.Sync()
	}
	r.resultMu.Unlock()
	if err != nil {
		r.mu.Lock()
		r.output.ResultsFailed = true
		r.mu.Unlock()
		r.abort("replay_results_failed")
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if result.writeDelay != nil {
		delay := *result.writeDelay
		if r.maxWriteDelay == nil || delay > *r.maxWriteDelay {
			v := max(0, delay)
			r.maxWriteDelay = &v
		}
	}
	r.output.add(result)
}

func (s *replayOutputSummary) add(result replayResult) {
	switch result.Verification {
	case "matched":
		s.Matched++
	case "short":
		s.Short++
	case "long":
		s.Long++
	case "unverifiable":
		s.Unverifiable++
	}
	if result.ActualTokens == nil {
		s.Unknown++
	} else {
		s.ActualTokens += *result.ActualTokens
	}
}

// Rebuild only from complete result lines. A crash can leave requests either
// unsent or in flight without a result; absence is not proof of non-dispatch.
// Never rewrite the evidence file or restart any request during recovery.
type replayUpload struct {
	mu      sync.Mutex
	reader  *bytes.Reader
	release func()
}

func (b *replayUpload) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reader == nil {
		return 0, io.EOF
	}
	return b.reader.Read(p)
}
func (b *replayUpload) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reader = nil
	if b.release != nil {
		b.release()
		b.release = nil
	}
	return nil
}
