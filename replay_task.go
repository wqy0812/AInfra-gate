package aigate

// One platform-owned task, with a filesystem command mailbox on shared NAS.
// There is no listening socket or independently deployed replay daemon.
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type replayTaskInput struct {
	BootID  string            `json:"boot_id"`
	Targets map[string]string `json:"targets"`
}
type replayTaskCommand struct {
	Method  string          `json:"method"`
	Path    string          `json:"path"`
	Payload json.RawMessage `json:"payload"`
}
type replayTaskResponse struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}
type taskHTTPWriter struct {
	headers http.Header
	code    int
	body    bytes.Buffer
}

func (w *taskHTTPWriter) Header() http.Header  { return w.headers }
func (w *taskHTTPWriter) WriteHeader(code int) { w.code = code }
func (w *taskHTTPWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = 200
	}
	if w.body.Len()+len(b) > 64<<20 {
		return 0, fmt.Errorf("task response too large")
	}
	return w.body.Write(b)
}

func RunReplayTask(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: aigate-replay-task TASK_DIRECTORY")
	}
	root := args[0]
	raw, e := recordingReadSmall(filepath.Join(root, "task.json"))
	if e != nil {
		return e
	}
	var input replayTaskInput
	if e = json.Unmarshal(raw, &input); e != nil {
		return e
	}
	if !validOperationID(input.BootID) {
		return fmt.Errorf("invalid task boot identity")
	}
	key := os.Getenv("AIGATE_REPLAY_MARK_KEY")
	if len(key) < replayMarkMinBytes {
		return fmt.Errorf("task signing key missing")
	}
	inputRoot := os.Getenv("AIGATE_REPLAY_INPUT_ROOT")
	if inputRoot == "" {
		inputRoot = filepath.Join(root, "input")
	}
	service, e := newReplayService(strings.Repeat("t", 32), []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}, inputRoot, filepath.Join(root, "results"), input.Targets, []byte(key), os.Stderr, true)
	if e != nil {
		return e
	}
	defer func() { service.Close(); _ = os.RemoveAll(inputRoot) }()
	service.boot = input.BootID
	for _, dir := range []string{"commands", "responses"} {
		if e = os.MkdirAll(filepath.Join(root, dir), 0700); e != nil {
			return e
		}
	}
	if _, e = recordingAtomicJSON(root, "ready.json", map[string]any{"boot_id": input.BootID, "ready_at": time.Now().UTC()}); e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	type completedCommand struct {
		name   string
		report bool
		err    error
	}
	completed := make(chan completedCommand, 8)
	inflight := map[string]bool{}
	var workers sync.WaitGroup
	defer workers.Wait()
	reportBusy := false
	lastCommand := time.Now()
	finish := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	drain:
		for {
			select {
			case result := <-completed:
				delete(inflight, result.name)
				if result.report {
					reportBusy = false
				}
				if result.err != nil {
					return result.err
				}
			default:
				break drain
			}
		}
		entries, e := os.ReadDir(filepath.Join(root, "commands"))
		if e != nil {
			return e
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".json") || !validOperationID(strings.TrimSuffix(name, ".json")) || inflight[name] {
				continue
			}
			responsePath := filepath.Join(root, "responses", name)
			if _, e = os.Stat(responsePath); e == nil {
				continue
			}
			raw, e := recordingReadSmall(filepath.Join(root, "commands", name))
			if os.IsNotExist(e) {
				continue
			}
			if e != nil {
				return e
			}
			var command replayTaskCommand
			if e = json.Unmarshal(raw, &command); e != nil {
				return e
			}
			if command.Path == "finish" {
				finish = true
				continue
			}
			report := strings.HasSuffix(command.Path, "/report")
			if len(inflight) >= 8 || (report && reportBusy) {
				continue
			}
			lastCommand = time.Now()
			inflight[name] = true
			if report {
				reportBusy = true
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				err := service.executeTaskCommand(ctx, root, name, command)
				completed <- completedCommand{name, report, err}
			}()
		}
		service.mu.Lock()
		idle := len(service.active) == 0
		ids := make([]string, 0, len(service.runs))
		for id := range service.runs {
			ids = append(ids, id)
		}
		service.mu.Unlock()
		if idle && len(inflight) == 0 && (finish || (len(ids) > 0 && time.Since(lastCommand) > time.Minute) || time.Since(lastCommand) > 15*time.Minute) {
			for _, id := range ids {
				service.loadReport(id)
			}
			return nil
		}
	}

}

var _ io.Writer = (*taskHTTPWriter)(nil)

func (s *replayService) executeTaskCommand(ctx context.Context, root, name string, command replayTaskCommand) error {
	req, e := http.NewRequestWithContext(ctx, command.Method, "http://localhost/control/replay/v1/"+command.Path, bytes.NewReader(command.Payload))
	if e != nil {
		return e
	}
	req.RemoteAddr = "127.0.0.1:1"
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("t", 32))
	out := &taskHTTPWriter{headers: http.Header{}}
	s.Handler().ServeHTTP(out, req)
	if out.code == 0 {
		out.code = 200
	}
	if !json.Valid(out.body.Bytes()) {
		return fmt.Errorf("invalid task response")
	}
	_, e = recordingAtomicJSON(filepath.Join(root, "responses"), name, replayTaskResponse{out.code, out.body.Bytes()})
	return e
}
