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

func TestReplayInvalidatesDroppedMeasuredSource(t *testing.T) {
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
	if view.Report == nil || view.Report.Valid || !strings.Contains(fmt.Sprint(view.Report.Invalid), "source_requests_dropped") || view.Report.Overall.Sent != 1 {
		t.Fatalf("%s", encodeJSON(view))
	}
}

func TestReplayFixOutputRejectsUnknownUsageBeforeSending(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-usage", 1)
	name := summary.Segments[0].Segment
	dir := filepath.Join(rec.root, "g1", "rec-usage", name)
	index := readIndex(t, filepath.Join(dir, "index.jsonl"))
	recordDir := filepath.Join(dir, index[0].Directory)
	var meta recordingMeta
	readJSONFile(t, filepath.Join(recordDir, "meta.json"), &meta)
	if err := os.WriteFile(filepath.Join(recordDir, meta.ResponseFile), []byte("data: [DONE]\n\n"), 0600); err != nil {
		t.Fatal(err)
	}
	before := rec.router.posts.Load()
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	req := normalizedReplayRequest(t, "unknown-usage", "rec-usage", name, "r1")
	req.FixOutput = true
	f.svc.start(req)
	view := f.wait(t, req.RunID)
	if view.State != "failed" || view.Error != "fix_output_usage_unknown" || view.Report != nil || rec.router.posts.Load() != before {
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

func TestReplayStopBeforeStartSurvivesRestart(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-stop", 1)
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	code, stopped := f.call(t, "POST", "/control/replay/v1/runs/late/stop", nil)
	if code != 200 || stopped["state"] != "stopped" || stopped["start_cancelled"] != true {
		t.Fatal(code, stopped)
	}
	// Disk-only recovery, as with a fresh process (no active run).
	if err := f.svc.recover(); err != nil {
		t.Fatal(err)
	}
	before := rec.router.posts.Load()
	req := normalizedReplayRequest(t, "late", "rec-stop", summary.Segments[0].Segment, "r1")
	for i := 0; i < 2; i++ {
		code, view := f.call(t, "POST", "/control/replay/v1/runs", req)
		if code != 200 || view["state"] != "stopped" || view["start_cancelled"] != true {
			t.Fatal(code, view)
		}
	}
	if rec.router.posts.Load() != before || f.svc.active != nil {
		t.Fatal("cancelled start sent traffic")
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
	f.svc.runs[r.req.RunID], f.svc.active = r, r
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
