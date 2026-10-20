package aigate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReplaySendsAvailableRecordsDespiteSourceDrops(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-drop", 1)
	name := summary.Segments[0].Segment
	dir := filepath.Join(rec.root, "g1", "rec-drop", name)
	var segment recordingSegmentFile
	readJSONFile(t, filepath.Join(dir, "segment.json"), &segment)
	segment.Dropped = 2
	if _, err := recordingAtomicJSON(dir, "segment.json", segment); err != nil {
		t.Fatal(err)
	}
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	req := normalizedReplayRequest(t, "dropped", "rec-drop", name, "r1")
	f.svc.start(req)
	view := f.wait(t, req.RunID)
	if view.State != "completed" || view.Progress.Sent != 1 {
		t.Fatalf("%s", encodeJSON(view))
	}
}

func TestReplayStopCancelsBodyBudgetWaitBeforeDispatch(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-budget", 1)
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	if !f.svc.bodyMemory.acquire(context.Background(), replayBodyBudget) {
		t.Fatal("budget unavailable")
	}
	defer f.svc.bodyMemory.release(replayBodyBudget)
	before := rec.router.posts.Load()
	req := normalizedReplayRequest(t, "budget", "rec-budget", summary.Segments[0].Segment, "r1")
	f.svc.start(req)
	deadline := time.Now().Add(3 * time.Second)
	for {
		view, _ := f.svc.loadRun(req.RunID)
		if view.State == "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never started", view)
		}
		time.Sleep(time.Millisecond)
	}
	f.svc.stop(req.RunID)
	view := f.wait(t, req.RunID)
	if view.State != "stopped" || view.Progress.Sent != 0 || rec.router.posts.Load() != before {
		t.Fatalf("sent after stop while waiting for body memory: %+v", view)
	}
}

func TestReplayStateWriteFailuresDoNotPublishSuccess(t *testing.T) {
	for _, phase := range []string{"planning", "running", "completed"} {
		t.Run(phase, func(t *testing.T) {
			rec := newRecordingFixture(t, recordingDefaultMaxBytes)
			summary := recordSample(t, rec, "rec-state", 1)
			f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
			f.svc.writeJSON = func(dir, name string, v any) (int64, error) {
				if view, ok := v.(replayRunView); ok && view.State == phase {
					return 0, fmt.Errorf("disk full")
				}
				return recordingAtomicJSON(dir, name, v)
			}
			before := rec.router.posts.Load()
			req := normalizedReplayRequest(t, "state", "rec-state", summary.Segments[0].Segment, "r1")
			_, code, detail := f.svc.start(req)
			if phase == "planning" {
				if code != 500 || detail != "storage_failed" || len(f.svc.active) != 0 {
					t.Fatal(code, detail)
				}
				if _, err := os.Stat(f.svc.runDir(req.RunID)); !os.IsNotExist(err) {
					t.Fatal("failed start left a conflicting directory", err)
				}
			} else {
				view := f.wait(t, req.RunID)
				if view.State != "failed" || view.Error != "storage_failed" {
					t.Fatalf("%+v", view)
				}
			}
			if phase != "completed" && rec.router.posts.Load() != before {
				t.Fatal("sent without durable start state")
			}
		})
	}
}

func TestReplayIncludesUnknownUsage(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-usage", 1)
	name := summary.Segments[0].Segment
	dir := filepath.Join(rec.root, "g1", "rec-usage", name)
	index := readIndex(t, filepath.Join(dir, "index.jsonl"))
	recordDir := filepath.Join(dir, index[0].Directory)
	var meta recordingMeta
	readJSONFile(t, filepath.Join(recordDir, "meta.json"), &meta)
	meta.ResponseResult = responseObservation{ParseStatus: "ok"}
	if _, err := recordingAtomicJSON(recordDir, "meta.json", meta); err != nil {
		t.Fatal(err)
	}
	before := rec.router.posts.Load()
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	req := normalizedReplayRequest(t, "unknown-usage", "rec-usage", name, "r1")
	f.svc.start(req)
	view := f.wait(t, req.RunID)
	if view.State != "completed" || rec.router.posts.Load() != before+1 {
		t.Fatalf("%s", encodeJSON(view))
	}
}

func TestReplayPreservesAbsentAndPresentUserAgent(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-agent", 1)
	name := summary.Segments[0].Segment
	dir := filepath.Join(rec.root, "g1", "rec-agent", name)
	index := readIndex(t, filepath.Join(dir, "index.jsonl"))
	recordDir := filepath.Join(dir, index[0].Directory)
	var meta recordingMeta
	readJSONFile(t, filepath.Join(recordDir, "meta.json"), &meta)
	agents := make(chan string, 2)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/runtime/v1/status" {
			json.NewEncoder(w).Encode(map[string]any{"service": "aigate-proxy", "router_id": "r1", "config_digest": strings.Repeat("a", 64)})
			return
		}
		agents <- r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	defer target.Close()
	f := newReplayFixture(t, rec.root, map[string]string{"r1": target.URL})
	for i, agent := range []string{"", "business-client/1"} {
		delete(meta.Headers, "User-Agent")
		if agent != "" {
			meta.Headers["User-Agent"] = agent
		}
		if _, err := recordingAtomicJSON(recordDir, "meta.json", meta); err != nil {
			t.Fatal(err)
		}
		req := normalizedReplayRequest(t, fmt.Sprintf("agent-%d", i), "rec-agent", name, "r1")
		f.svc.start(req)
		if view := f.wait(t, req.RunID); view.State != "completed" {
			t.Fatalf("%+v", view)
		}
		if got := <-agents; got != agent {
			t.Fatalf("User-Agent %q, want %q", got, agent)
		}
	}
}

func TestReplayAcknowledgedStopWinsChosenCompletion(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	dir := f.svc.runDir("finishing")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &replayRun{svc: f.svc, req: replayRequest{RunID: "finishing"}, dir: dir, cancel: cancel, done: make(chan struct{}), state: "running", created: time.Now().UTC()}
	f.svc.runs[r.req.RunID], f.svc.active[r.req.RunID] = r, r
	view, status, _ := f.svc.stop(r.req.RunID)
	if status != 200 || view.State != "stopping" {
		t.Fatal(status, view)
	}
	// execute may have chosen completed before stop acquired the lock.
	r.finish("completed", "")
	if got := r.view(); got.State != "stopped" || got.EndedAt == nil {
		t.Fatalf("%+v", got)
	}
}
