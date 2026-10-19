package aigate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReplayTaskMailboxUsesProductionKernel(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "task-input", 3)
	os.Remove(filepath.Join(rec.root, "g1", "task-input", summary.Segments[0].Segment, "pinned"))
	root := t.TempDir()
	if e := os.Symlink(rec.root, filepath.Join(root, "input")); e != nil {
		t.Fatal(e)
	}
	recordingAtomicJSON(root, "task.json", replayTaskInput{BootID: "taskboot", Targets: map[string]string{"r1": rec.proxy.URL + "/v1/chat/completions"}, Digests: map[string]string{"g1/task-input/" + summary.Segments[0].Segment: runtimeDigest("original input")}})
	t.Setenv("AIGATE_REPLAY_MARK_KEY", string(testMarkKey))
	done := make(chan error, 1)
	go func() { done <- RunReplayTask([]string{root}) }()
	wait := func(path string) []byte {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if b, e := os.ReadFile(path); e == nil {
				return b
			}
			select {
			case e := <-done:
				t.Fatalf("task exited early: %v", e)
			default:
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("mailbox timeout", path)
		return nil
	}
	wait(filepath.Join(root, "ready.json"))
	call := func(id, method, path string, payload any) map[string]any {
		t.Helper()
		raw, _ := json.Marshal(payload)
		_, e := recordingAtomicJSON(filepath.Join(root, "commands"), id+".json", replayTaskCommand{Method: method, Path: path, Payload: raw})
		if e != nil {
			t.Fatal(e)
		}
		b := wait(filepath.Join(root, "responses", id+".json"))
		var out struct {
			Status int            `json:"status"`
			Body   map[string]any `json:"body"`
		}
		if e = json.Unmarshal(b, &out); e != nil || out.Status != 200 {
			t.Fatal(string(b), e)
		}
		os.Remove(filepath.Join(root, "commands", id+".json"))
		os.Remove(filepath.Join(root, "responses", id+".json"))
		return out.Body
	}
	before := rec.router.posts.Load()
	request := map[string]any{"run_id": "task-run", "recording": map[string]any{"gateway_id": "g1", "recording_id": "task-input", "segments": []string{summary.Segments[0].Segment}}, "filter": map[string]any{"status": "all"}, "target": "r1", "mode": "concurrency", "concurrency": 2}
	call("create", "POST", "runs", request)
	deadline := time.Now().Add(10 * time.Second)
	for {
		view := call(randomID(), "GET", "runs/task-run", nil)
		if view["state"] == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(view)
		}
	}
	if got := rec.router.posts.Load() - before; got != 3 {
		t.Fatalf("sent %d requests", got)
	}
	report := call("report", "GET", "runs/task-run/report", nil)
	if report["version"] != float64(2) {
		t.Fatal(report)
	}
	recordingAtomicJSON(filepath.Join(root, "commands"), "finish.json", replayTaskCommand{Method: "POST", Path: "finish"})
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("task did not exit")
	}
	if _, e := os.Stat(filepath.Join(root, "results", "runs", "task-run", "results.jsonl")); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(root, "input")); e != nil {
		t.Fatal("NAS input removed", e)
	}
	segment := filepath.Join(rec.root, "g1", "task-input", summary.Segments[0].Segment)
	if _, e := os.Stat(filepath.Join(segment, ".replay.lock")); !os.IsNotExist(e) {
		t.Fatal("task wrote input lock")
	}
}
