package aigate

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Keep a response open: UDS must expose live progress without NAS writes.
func TestReplayTaskQueriesLiveProgress(t *testing.T) {
	for _, mode := range []string{"timed", "concurrency"} {
		t.Run(mode, func(t *testing.T) {
			input, req := syntheticReplay(t, 2, 0, `{"stream":true,"messages":[]}`)
			req.Mode, req.Concurrency = mode, 0
			if mode == "concurrency" {
				req.Concurrency = 2
			}
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var calls atomic.Int64
			target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 2 {
					<-release
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n"))
			})
			root := t.TempDir()
			t.Setenv("AIGATE_REPLAY_INPUT_ROOT", input)
			t.Setenv("AIGATE_REPLAY_MARK_KEY", string(testMarkKey))
			task := replayTaskInput{BootID: "boot", PlanID: "plan", Targets: map[string]string{"r1": target.URL}, Digests: map[string]string{"g1/synthetic/" + req.Recording.Segments[0]: runtimeDigest("input")}, Request: req}
			if _, err := recordingAtomicJSON(root, "task.json", task); err != nil {
				t.Fatal(err)
			}
			socketDir, err := os.MkdirTemp("/tmp", "replay-uds-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(socketDir)
			socketPath := filepath.Join(socketDir, "progress.sock")
			t.Setenv(replayProgressSocketEnv, socketPath)
			taskRaw, _ := os.ReadFile(filepath.Join(root, "task.json"))
			identity := replayProgressIdentity{req.RunID, "boot", fmt.Sprintf("%x", sha256.Sum256(taskRaw))}
			done := make(chan error, 1)
			go func() { done <- RunReplayTask([]string{root}) }()
			t.Cleanup(func() {
				unblock()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(10 * time.Second):
					t.Error("task did not exit")
				}
			})
			path := filepath.Join(root, "results", "runs", req.RunID, "state.json")
			waitState := func(check func(replayProgressView) bool) replayProgressView {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				var view replayProgressView
				for time.Now().Before(deadline) {
					response, err := queryReplayProgress(socketPath, identity)
					if err == nil {
						view = response.RunView
						if check(view) {
							return view
						}
					}
					time.Sleep(20 * time.Millisecond)
				}
				t.Fatalf("live progress did not advance: %+v", view)
				return view
			}
			view := waitState(func(v replayProgressView) bool {
				return v.State == "running" && v.Progress.Sent == 2 && v.Progress.Completed == 1 && v.Progress.Inflight == 1 && len(v.Sources) == 1 && v.Sources[0].Progress.Completed == 1 && v.Sources[0].Progress.Inflight == 1
			})
			if view.Progress.Unsent != 0 || view.Progress.Elapsed <= 0 || len(view.Sources) != 1 || view.Sources[0].Progress.Sent != 2 || view.Sources[0].Progress.Completed != 1 || view.Sources[0].Progress.Inflight != 1 {
				t.Fatalf("invalid live progress: %+v", view)
			}
			raw, _ := os.ReadFile(path)
			var checkpoint replayRunView
			if json.Unmarshal(raw, &checkpoint) != nil || checkpoint.Progress.Sent != 0 {
				t.Fatalf("unexpected periodic checkpoint: %s", raw)
			}
			before := view.Progress.Elapsed
			if _, err := recordingAtomicJSON(root, "stop.json", map[string]bool{"stop": true}); err != nil {
				t.Fatal(err)
			}
			waitState(func(v replayProgressView) bool {
				return v.State == "stopping" && v.Progress.Inflight == 1 && v.Progress.Elapsed > before+1
			})
			unblock()
			deadline := time.Now().Add(5 * time.Second)
			var final replayRunView
			for time.Now().Before(deadline) {
				raw, _ := os.ReadFile(path)
				if json.Unmarshal(raw, &final) == nil && final.State == "stopped" && final.EndedAt != nil {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			view = replayCompactProgress(final)
			if view.Progress.Sent != 2 || view.Progress.Completed != 2 || view.Progress.Inflight != 0 || calls.Load() != 2 {
				t.Fatalf("invalid final progress: %+v, calls=%d", view, calls.Load())
			}
		})
	}
}

func TestReplayLiveProgressWriteFailureStopsDispatchAndDrains(t *testing.T) {
	input, req := syntheticReplay(t, 3, 2, `{"stream":true,"messages":[]}`)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int64
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: [DONE]\n\n"))
	})
	f := newReplayFixture(t, input, map[string]string{"r1": target.URL})
	ticks := make(chan time.Time, 1)
	f.svc.checkpointTicker = func(d time.Duration) (<-chan time.Time, func()) {
		if d != 30*time.Second {
			t.Errorf("checkpoint period %s", d)
		}
		return ticks, func() {}
	}
	f.svc.writeJSON = func(dir, name string, value any) (int64, error) {
		if v, ok := value.(replayRunView); ok && v.State == "running" && v.Progress.Sent > 0 {
			return 0, os.ErrPermission
		}
		return recordingAtomicJSON(dir, name, value)
	}
	if _, status, reason := f.svc.start(req); status != 200 {
		t.Fatal(status, reason)
	}
	f.svc.mu.Lock()
	run := f.svc.runs[req.RunID]
	f.svc.mu.Unlock()
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	ticks <- time.Now()
	deadline := time.Now().Add(5 * time.Second)
	for run.view().State != "stopping" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	view := run.view()
	if view.State != "stopping" || view.Error != "storage_failed" || view.Progress.Sent != 1 || view.Progress.Inflight != 1 || view.EndedAt != nil {
		t.Fatalf("write failure did not stop dispatch while draining: %+v", view)
	}
	unblock()
	select {
	case <-run.done:
	case <-time.After(5 * time.Second):
		t.Fatal("failed run did not finish draining")
	}
	raw, err := os.ReadFile(filepath.Join(run.dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	if view.State != "failed" || view.Error != "storage_failed" || view.Progress.Sent != 1 || view.Progress.Completed != 1 || view.Progress.Inflight != 0 || calls.Load() != 1 {
		t.Fatalf("invalid failure snapshot: %+v, calls=%d", view, calls.Load())
	}
}

func TestReplayTaskExecutesOnceAndExits(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "task-input", 3)
	segment := summary.Segments[0].Segment
	os.Remove(filepath.Join(rec.root, "g1", "task-input", segment, "pinned"))
	root := t.TempDir()
	os.Symlink(rec.root, filepath.Join(root, "input"))
	task := replayTaskInput{BootID: "boot", PlanID: "plan", Targets: map[string]string{"r1": rec.proxy.URL + "/v1/chat/completions"}, Digests: map[string]string{"g1/task-input/" + segment: runtimeDigest("input")}, Request: replayRequest{RunID: "task-run", Target: "r1", Mode: "concurrency", Concurrency: 2, Recording: replayRecordingRef{GatewayID: "g1", RecordingID: "task-input", Segments: []string{segment}}}, Execution: map[string]any{"host": "synthetic", "image": "fixed"}}
	recordingAtomicJSON(root, "task.json", task)
	t.Setenv("AIGATE_REPLAY_MARK_KEY", string(testMarkKey))
	before := rec.router.posts.Load()
	done := make(chan error, 1)
	go func() { done <- RunReplayTask([]string{root}) }()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("task did not finish")
	}
	if rec.router.posts.Load()-before != 3 {
		t.Fatal("wrong request count")
	}
	b, e := os.ReadFile(filepath.Join(root, "results", "runs", "task-run", "report.json"))
	if e != nil {
		t.Fatal(e)
	}
	var report map[string]any
	json.Unmarshal(b, &report)
	if report["plan_id"] != "plan" || report["execution"].(map[string]any)["host"] != "synthetic" {
		t.Fatal(report)
	}
	if e = RunReplayTask([]string{root}); e == nil {
		t.Fatal("task reran")
	}
	if rec.router.posts.Load()-before != 3 {
		t.Fatal("duplicate requests")
	}
	for _, name := range []string{"commands", "responses", "input-locks"} {
		if _, e = os.Stat(filepath.Join(root, name)); !os.IsNotExist(e) {
			t.Fatal("legacy mailbox", name)
		}
	}
	if _, e = os.Stat(filepath.Join(rec.root, "g1", "task-input", segment, ".replay.lock")); !os.IsNotExist(e) {
		t.Fatal("input changed")
	}
}
