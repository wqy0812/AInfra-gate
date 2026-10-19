package aigate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
