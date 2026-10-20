package aigate

// One immutable target task. The Python production entry verifies the complete
// NAS manifest before calling this kernel. No command mailbox or HTTP handler.
import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type replayTaskInput struct {
	BootID    string            `json:"boot_id"`
	Targets   map[string]string `json:"targets"`
	Digests   map[string]string `json:"digests"`
	Request   replayRequest     `json:"request"`
	Execution map[string]any    `json:"execution"`
	PlanID    string            `json:"plan_id"`
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
	if !validOperationID(input.BootID) || !validOperationID(input.PlanID) || len(input.Targets) != 1 || len(input.Digests) == 0 {
		return fmt.Errorf("invalid task identity")
	}
	if input.Request.PrepareOnly || input.Request.BatchID != "" || input.Request.CoordinatedTruncation {
		return fmt.Errorf("task cannot use service coordination")
	}
	if e = input.Request.normalize(); e != nil {
		return e
	}
	rawTarget, ok := input.Targets[input.Request.Target]
	if !ok {
		return fmt.Errorf("task target missing")
	}
	base, e := controlBase(rawTarget)
	if e != nil {
		return e
	}
	input.Targets[input.Request.Target] = base
	saved := map[string]bool{}
	for _, ref := range input.Request.sources() {
		for _, segment := range ref.Segments {
			identity := ref.GatewayID + "/" + ref.RecordingID + "/" + segment
			if !validDigest(input.Digests[identity]) {
				return fmt.Errorf("saved task input missing")
			}
			saved[identity] = true
		}
	}
	if len(saved) != len(input.Digests) {
		return fmt.Errorf("unexpected saved task input")
	}
	key := os.Getenv("AIGATE_REPLAY_MARK_KEY")
	if len(key) < replayMarkMinBytes {
		return fmt.Errorf("task signing key missing")
	}
	inputRoot := os.Getenv("AIGATE_REPLAY_INPUT_ROOT")
	if inputRoot == "" {
		inputRoot = filepath.Join(root, "input")
	}
	// The marker survives crashes. An executor never sends a task a second time.
	started, e := os.OpenFile(filepath.Join(root, "execution.started"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return fmt.Errorf("task already attempted: %w", e)
	}
	e = started.Sync()
	started.Close()
	if e != nil {
		return e
	}
	if e = syncDirectory(root); e != nil {
		return e
	}
	service, e := newReplayService(strings.Repeat("t", 32), []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}, inputRoot, filepath.Join(root, "results"), input.Targets, []byte(key), os.Stderr, true)
	if e != nil {
		return e
	}
	defer service.Close()
	service.taskInputs = saved
	service.boot = input.BootID
	var current atomic.Pointer[replayRun]
	var terminal atomic.Pointer[replayProgressView]
	if path := os.Getenv(replayProgressSocketEnv); path != "" {
		identity := replayProgressIdentity{input.Request.RunID, input.BootID, fmt.Sprintf("%x", sha256.Sum256(raw))}
		closeProgress, err := startReplayProgress(path, identity, func() replayProgressView {
			if run := current.Load(); run != nil {
				return run.progressView()
			}
			if view := terminal.Load(); view != nil {
				return *view
			}
			return replayProgressView{RunID: input.Request.RunID, State: "planning"}
		})
		if err != nil {
			return fmt.Errorf("progress socket: %w", err)
		}
		defer closeProgress()
	}
	if _, e = os.Stat(filepath.Join(root, "stop.json")); e == nil {
		view, _, _ := service.stop(input.Request.RunID)
		view.Request = &input.Request
		compact := replayCompactProgress(view)
		terminal.Store(&compact)
		service.writeJSON(service.runDir(input.Request.RunID), "state.json", view)
		service.saveReport(view)
		return publishTaskReport(root, service.runDir(input.Request.RunID), input)
	} else if !os.IsNotExist(e) {
		return e
	}
	if _, status, reason := service.start(input.Request); status != 200 {
		return fmt.Errorf("task start: %s", reason)
	}
	service.mu.Lock()
	run := service.runs[input.Request.RunID]
	service.mu.Unlock()
	if run == nil {
		return fmt.Errorf("task was not created")
	}
	current.Store(run)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-run.done:
			return publishTaskReport(root, run.dir, input)
		case <-ctx.Done():
			service.stop(input.Request.RunID)
			<-run.done
			return publishTaskReport(root, run.dir, input)
		case <-ticker.C:
			if _, e = os.Stat(filepath.Join(root, "stop.json")); e == nil {
				service.stop(input.Request.RunID)
			} else if !os.IsNotExist(e) {
				service.stop(input.Request.RunID)
			}
		}
	}
}

func publishTaskReport(root, dir string, input replayTaskInput) error {
	f, e := os.Open(filepath.Join(dir, "report.json"))
	if e != nil {
		return e
	}
	raw, e := io.ReadAll(io.LimitReader(f, 64<<20+1))
	f.Close()
	if e != nil {
		return e
	}
	if len(raw) > 64<<20 {
		return fmt.Errorf("report_too_large")
	}
	var value map[string]any
	if e = json.Unmarshal(raw, &value); e != nil {
		return e
	}
	execution := input.Execution
	if execution == nil {
		execution = map[string]any{}
	}
	if b, e := recordingReadSmall(filepath.Join(root, "environment.json")); e == nil {
		var environment map[string]any
		if json.Unmarshal(b, &environment) == nil {
			execution["worker_observation"] = environment
		} else {
			execution["worker_observation_unknown"] = true
		}
	} else {
		execution["worker_observation_unknown"] = true
	}
	value["execution"] = execution
	value["plan_id"] = input.PlanID
	value["input_digests"] = input.Digests
	_, e = recordingAtomicJSON(dir, "report.json", value)
	return e
}
