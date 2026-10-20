package aigate

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

type replayFixture struct{ svc *replayExecutor }

func savedTestInputs(root string) map[string]bool {
	saved := map[string]bool{}
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "segment.json" {
			rel, _ := filepath.Rel(root, filepath.Dir(path))
			saved[filepath.ToSlash(rel)] = true
		}
		return err
	})
	return saved
}
func newReplayFixture(t *testing.T, recordRoot string, targets map[string]string) *replayFixture {
	t.Helper()
	normalized := map[string]string{}
	for k, v := range targets {
		base, err := controlBase(v)
		if err != nil {
			t.Fatal(err)
		}
		normalized[k] = base
	}
	svc, err := newReplayExecutor(recordRoot, t.TempDir(), normalized, testMarkKey, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	svc.taskInputs = savedTestInputs(recordRoot)
	t.Cleanup(svc.Close)
	return &replayFixture{svc: svc}
}
func (f *replayFixture) call(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var value any
	code := 200
	reason := ""
	rest := strings.TrimPrefix(path, "/executor/runs")
	id, action, _ := strings.Cut(strings.TrimPrefix(rest, "/"), "/")
	if method == "POST" && rest == "" {
		var req replayRequest
		if err := strictDecode(encodeJSON(body), &req); err != nil {
			return 400, map[string]any{"error": "invalid_request"}
		}
		if err := req.normalize(); err != nil {
			return 400, map[string]any{"error": err.Error()}
		}
		value, code, reason = f.svc.start(req)
	} else if method == "POST" && action == "stop" {
		value, code, reason = f.svc.stop(id)
	} else if method == "GET" && action == "report" {
		var ok bool
		value, ok = f.svc.loadReport(id)
		if !ok {
			code = 404
		}
	} else if method == "GET" && action == "" {
		var ok bool
		value, ok = f.svc.loadRun(id)
		if !ok {
			code = 404
		}
	} else {
		code = 404
	}
	if reason != "" {
		return code, map[string]any{"error": reason}
	}
	var out map[string]any
	_ = json.Unmarshal(encodeJSON(value), &out)
	return code, out
}

func (f *replayFixture) wait(t *testing.T, id string) replayRunView {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		view, ok := f.svc.loadRun(id)
		if ok && view.EndedAt != nil {
			return view
		}
		time.Sleep(20 * time.Millisecond)
	}
	view, _ := f.svc.loadRun(id)
	t.Fatalf("run did not end: %+v", view)
	return replayRunView{}
}

// recordSample records n streaming chat requests.
func recordSample(t *testing.T, f *recordingFixture, id string, n int) recordingSummary {
	t.Helper()
	now := time.Now()
	startRecording(t, f.c, id, halfHour(now), halfHour(now).Add(time.Hour), nil)
	for i := 0; i < n; i++ {
		body := `{"model":"token-model","messages":[{"role":"user","content":"question ` + strings.Repeat("q", i) + `"}],"stream":true,"max_tokens":` + string(rune('2'+i%5)) + `}`
		if code, out := postChat(t, f.switch_.URL, body, nil); code != 200 {
			t.Fatal(code, out)
		}
		time.Sleep(20 * time.Millisecond)
	}
	recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": id})
	summary := waitRecordingEnded(t, f.c, id)
	for _, segment := range summary.Segments {
		recordingCommand(t, f.c, "record-pin", map[string]any{"recording_id": id, "segment": segment.Segment})
	}
	f.c.recorder.flush()
	return summary
}

func routerBodies(r *tokenRouter, from int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []string{}
	for _, b := range r.bodies[from:] {
		out = append(out, string(b))
	}
	sort.Strings(out)
	return out
}

func TestReplayTimedReproducesRecordedRequests(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-a", 5)
	recordedBodies := []string{}
	for _, b := range routerBodies(rec.router, 0) {
		if !strings.Contains(b, "fails") {
			recordedBodies = append(recordedBodies, b)
		}
	}
	before := len(rec.router.bodies)
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	segment := summary.Segments[0].Segment
	request := map[string]any{"run_id": "run-1", "recording": map[string]any{"gateway_id": "g1", "recording_id": "rec-a", "segments": []string{segment}}, "filter": map[string]any{"status": "all"}, "target": "r1", "mode": "timed"}
	code, out := f.call(t, "POST", "/executor/runs", request)
	if code != 200 {
		t.Fatal(code, out)
	}
	view := f.wait(t, "run-1")
	if view.State != "completed" || view.Progress.Planned != 5 || view.Progress.Completed != 5 ||
		view.Target.RouterID != "r1" || view.Target.ConfigDigest != rec.p.digest || view.MaxSendDelay > 0.5 {
		t.Fatalf("view %s", encodeJSON(view))
	}
	// The target received exactly the recorded requests, with replay marks.
	replayed := routerBodies(rec.router, before)
	if strings.Join(replayed, "\n") != strings.Join(recordedBodies, "\n") {
		t.Fatalf("replayed bodies differ:\n%v\n%v", replayed, recordedBodies)
	}
	rec.router.mu.Lock()
	marked := verifyReplayMark([][]byte{testMarkKey}, rec.router.headers[before].Get(headerReplay))
	rec.router.mu.Unlock()
	if marked {
		t.Fatal("proxy forwarded the replay mark upstream")
	}
	for _, name := range []string{"request.json", "state.json", "plan.json", "plan.jsonl"} {
		if info, err := os.Stat(filepath.Join(f.svc.runDir("run-1"), name)); err != nil || info.Mode().Perm() != 0600 {
			t.Fatal(name, err)
		}
	}
}

func TestReplayConcurrencyPreservesOutputParameters(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-b", 4)
	before := len(rec.router.bodies)
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	request := map[string]any{"run_id": "run-c", "recording": map[string]any{"gateway_id": "g1", "recording_id": "rec-b", "segments": []string{summary.Segments[0].Segment}},
		"filter": map[string]any{"status": "all"}, "target": "r1", "mode": "concurrency", "concurrency": 2, "set_model": "token-model"}
	if code, out := f.call(t, "POST", "/executor/runs", request); code != 200 {
		t.Fatal(code, out)
	}
	view := f.wait(t, "run-c")
	if view.State != "completed" || view.Progress.Completed != 4 || view.Progress.Failed != 0 {
		t.Fatalf("view %s", encodeJSON(view))
	}
	rec.router.mu.Lock()
	defer rec.router.mu.Unlock()
	for _, body := range rec.router.bodies[before:] {
		var fields map[string]json.RawMessage
		json.Unmarshal(body, &fields)
		if len(fields["ignore_eos"]) != 0 || len(fields["min_tokens"]) != 0 || string(fields["stream"]) != "true" {
			t.Fatalf("output parameters changed: %s", body)
		}
	}
}
