package aigate

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	GatewayID         string               `json:"gateway_id"`
	RecordingID       string               `json:"recording_id"`
	Segment           string               `json:"segment"`
	Sequence          int                  `json:"sequence"`
	OriginalRequestID string               `json:"original_request_id"`
	RequestID         string               `json:"request_id"`
	PlannedOffset     float64              `json:"planned_offset_seconds"`
	SendOffset        *float64             `json:"send_offset_seconds"`
	WriteOffset       *float64             `json:"write_offset_seconds"`
	HTTPStatus        int                  `json:"http_status"`
	Error             string               `json:"error,omitempty"`
	ErrorSummary      string               `json:"error_summary,omitempty"`
	RecordedTokens    *uint64              `json:"recorded_tokens,omitempty"`
	TargetTokens      *uint64              `json:"target_tokens,omitempty"`
	ActualTokens      *uint64              `json:"actual_tokens"`
	Observation       *responseObservation `json:"response_result,omitempty"`
	Verification      string               `json:"verification,omitempty"`
	Rewritten         []string             `json:"rewritten"`
	writeDelay        *float64
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
	if r.fatalError == "" {
		r.fatalError = reason
	}
	// Expose fatal dispatch failure while accepted requests still drain, so
	// the platform can stop the other overlay participant before we finish.
	r.state, r.errText = "stopping", r.fatalError
	r.stopped.Store(true)
	r.mu.Unlock()
	r.cancel()
}

func (r *replayRun) saveResult(result replayResult) {
	if r.req.FixOutput {
		result.Verification = "unverifiable"
		if result.Observation != nil && result.Observation.UsageKnown {
			a := result.Observation.Usage["completion_tokens"]
			result.ActualTokens = &a
			if result.Error == "" && result.Observation.ProtocolComplete {
				switch {
				case a == *result.TargetTokens:
					result.Verification = "matched"
				case a < *result.TargetTokens:
					result.Verification = "short"
				default:
					result.Verification = "long"
				}
			}
		}
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
	if r.req.FixOutput {
		r.output.add(result)
	}
}

func (s *replayOutputSummary) add(result replayResult) {
	switch result.Verification {
	case "matched":
		s.Matched++
	case "short":
		s.Short++
	case "long":
		s.Long++
	default:
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
func recoverReplayResults(dir string, view *replayRunView) (err error) {
	seen := make(map[int]bool)
	sourceSeen := make(map[string]int64)
	for i := range view.Sources {
		view.Sources[i].Progress = replayProgress{Planned: view.Sources[i].Progress.Planned}
	}
	view.Progress.Sent, view.Progress.Completed, view.Progress.Failed = 0, 0, 0
	view.Progress.Inflight, view.Progress.Unsent = 0, 0
	view.Output = replayOutputSummary{ResultsFailed: view.Output.ResultsFailed}
	defer func() {
		view.Progress.Unconfirmed = max(0, int64(view.Progress.Planned)-int64(len(seen)))
		for i := range view.Sources {
			p := &view.Sources[i].Progress
			p.Unconfirmed = max(0, int64(p.Planned)-sourceSeen[view.Sources[i].GatewayID+"/"+view.Sources[i].RecordingID])
		}
		view.Output.Incomplete = view.Progress.Unconfirmed > 0 || err != nil
		view.Output.ResultsFailed = view.Output.ResultsFailed || err != nil
	}()
	file, err := os.Open(filepath.Join(dir, "results.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		// A crash between publishing running and opening the result file is valid.
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, recordingSmallFile)
	for {
		line, readErr := reader.ReadSlice('\n')
		if readErr == io.EOF && len(line) == 0 {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("incomplete_result_line: %w", readErr)
		}
		var result replayResult
		if json.Unmarshal(line, &result) != nil || result.Sequence < 1 || result.Sequence > view.Progress.Planned ||
			result.Sequence > replayMaxPlanItems || seen[result.Sequence] || result.RequestID == "" || result.OriginalRequestID == "" {
			return fmt.Errorf("invalid_result_line")
		}
		if view.Request != nil && view.Request.FixOutput {
			switch result.Verification {
			case "matched", "short", "long", "unverifiable":
			default:
				return fmt.Errorf("invalid_result_verification")
			}
			view.Output.add(result)
		}
		seen[result.Sequence] = true
		sourceSeen[result.GatewayID+"/"+result.RecordingID]++
		for i := range view.Sources {
			source := &view.Sources[i]
			if source.GatewayID != result.GatewayID || source.RecordingID != result.RecordingID {
				continue
			}
			if result.SendOffset != nil {
				source.Progress.Sent++
			} else {
				source.Progress.Unsent++
			}
			if result.Error != "" && result.Error != "not_sent" {
				source.Progress.Failed++
			} else if result.SendOffset != nil {
				source.Progress.Completed++
			}
		}
		if result.SendOffset != nil {
			view.Progress.Sent++
		} else {
			view.Progress.Unsent++
		}
		if result.Error != "" && result.Error != "not_sent" {
			view.Progress.Failed++
			view.LastError = result.Error
		} else if result.SendOffset != nil && result.Error == "" {
			view.Progress.Completed++
		}
	}
}

// Release request storage as soon as the transport closes the upload, rather
// than retaining a body reservation throughout a possibly hours-long answer.
// The conservative 6*size + 1 MiB accounting still covers read/rewrite/upload.
// GetBody is absent: the transport cannot automatically resend this POST.
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
