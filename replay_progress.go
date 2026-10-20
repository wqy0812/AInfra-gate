package aigate

// The task's local, read-only progress endpoint. It exposes no plan or payload.
import (
	"bufio"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const replayProgressSocketEnv = "AIGATE_REPLAY_PROGRESS_SOCKET"

type replayProgressIdentity struct {
	RunID      string `json:"run_id"`
	BootID     string `json:"boot_id"`
	TaskSHA256 string `json:"task_sha256"`
}

type replayProgressQuery struct {
	replayProgressIdentity
	Op string `json:"op"`
}

type replayProgressView struct {
	SampledAt        *time.Time             `json:"-"`
	RunID            string                 `json:"run_id"`
	State            string                 `json:"state"`
	Progress         replayProgress         `json:"progress"`
	Sources          []replaySourceProgress `json:"sources,omitempty"`
	LastError        string                 `json:"last_error,omitempty"`
	Error            string                 `json:"error,omitempty"`
	EndReason        string                 `json:"end_reason,omitempty"`
	EndedAt          *time.Time             `json:"ended_at,omitempty"`
	DispatchComplete bool                   `json:"dispatch_complete"`
	Truncation       *replayTruncation      `json:"truncation,omitempty"`
}

func (r *replayRun) progressView() replayProgressView {
	r.mu.Lock()
	defer r.mu.Unlock()
	return replayCompactProgress(r.viewLocked())
}

func replayCompactProgress(v replayRunView) replayProgressView {
	return replayProgressView{SampledAt: v.SampledAt, RunID: v.RunID, State: v.State, Progress: v.Progress, Sources: v.Sources,
		LastError: v.LastError, Error: v.Error, EndReason: v.EndReason, EndedAt: v.EndedAt,
		DispatchComplete: v.DispatchComplete, Truncation: v.Truncation}
}

type replayProgressResponse struct {
	replayProgressIdentity
	SampledAt time.Time          `json:"sampled_at"`
	RunView   replayProgressView `json:"run_view"`
}

// Closing the listener joins accept before handlers, so no Add can race Wait.
func startReplayProgress(path string, identity replayProgressIdentity, snapshot func() replayProgressView) (func(), error) {
	if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	var handlers sync.WaitGroup
	done := make(chan struct{})
	slots := make(chan struct{}, 4)
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			select {
			case slots <- struct{}{}:
			default:
				conn.Close()
				continue
			}
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer func() { <-slots }()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				line, err := bufio.NewReader(io.LimitReader(conn, 2049)).ReadBytes('\n')
				var query replayProgressQuery
				if err != nil || len(line) > 2048 || strictDecode(line, &query) != nil || query.Op != "status" || query.replayProgressIdentity != identity {
					_, _ = conn.Write([]byte("{\"error\":\"invalid_progress_query\"}\n"))
					return
				}
				view := snapshot()
				sampled := time.Now().UTC()
				if view.SampledAt != nil {
					sampled = *view.SampledAt
				}
				data := append(encodeJSON(replayProgressResponse{identity, sampled, view}), '\n')
				if len(data) > 64*1024 {
					data = []byte("{\"error\":\"progress_response_too_large\"}\n")
				}
				_, _ = conn.Write(data)
			}()
		}
	}()
	return func() { listener.Close(); <-done; handlers.Wait() }, nil
}
