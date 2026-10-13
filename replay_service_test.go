package aigate

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

type replayFixture struct {
	svc    *replayService
	server *httptest.Server
	key    string
}

func newReplayFixture(t *testing.T, recordRoot string, targets map[string]string) *replayFixture {
	t.Helper()
	key := strings.Repeat("r", 32)
	normalized := map[string]string{}
	for k, v := range targets {
		base, err := controlBase(v)
		if err != nil {
			t.Fatal(err)
		}
		normalized[k] = base
	}
	svc, err := newReplayService(key, []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}, recordRoot, t.TempDir(), normalized, testMarkKey, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(svc.Handler())
	t.Cleanup(func() { server.Close(); svc.Close() })
	return &replayFixture{svc: svc, server: server, key: key}
}

func (f *replayFixture) call(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(encodeJSON(body))
	}
	req, _ := http.NewRequest(method, f.server.URL+path, reader)
	req.Header.Set("Authorization", "Bearer "+f.key)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var out map[string]any
	data, _ := io.ReadAll(response.Body)
	_ = json.Unmarshal(data, &out)
	return response.StatusCode, out
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

// recordSample records n streaming chat requests and one non-2xx answer.
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
	f.router.failNext.Store(1)
	postChat(t, f.switch_.URL, `{"messages":[{"role":"user","content":"fails"}],"stream":true}`, nil)
	recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": id})
	return waitRecordingEnded(t, f.c, id)
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
	f := newReplayFixture(t, rec.root, map[string]string{"candidate": rec.proxy.URL})
	segment := summary.Segments[0].Segment
	request := map[string]any{"run_id": "run-1", "recording": map[string]any{"gateway_id": "g1", "recording_id": "rec-a", "segments": []string{segment}}, "target": "candidate", "mode": "timed"}
	code, out := f.call(t, "POST", "/control/replay/v1/runs", request)
	if code != 200 {
		t.Fatal(code, out)
	}
	view := f.wait(t, "run-1")
	if view.State != "completed" || view.Report == nil {
		t.Fatalf("view %+v", view)
	}
	report := view.Report
	if report.Overall.Planned != 5 || report.Overall.Completed != 5 || report.Excluded["status"] != 1 || !report.Valid || !report.Original ||
		report.Target.RouterID != "r1" || report.Target.ConfigDigest != rec.p.digest || report.Overall.TTFT.Count != 5 || report.Baseline.Requests != 5 {
		t.Fatalf("report %s", encodeJSON(report))
	}
	if p99 := report.Overall.SendDelay.P99; p99 == nil || *p99 > 0.5 {
		t.Fatalf("send delay %+v", report.Overall.SendDelay)
	}
	if report.Overall.CachedRatio == nil || report.Overall.CompletionTokens == nil || *report.Overall.CompletionTokens != *report.Baseline.CompletionTokens {
		t.Fatalf("usage %s", encodeJSON(report.Overall))
	}
	// The target received exactly the recorded requests, with replay marks.
	replayed := routerBodies(rec.router, before)
	if strings.Join(replayed, "\n") != strings.Join(recordedBodies, "\n") {
		t.Fatalf("replayed bodies differ:\n%v\n%v", replayed, recordedBodies)
	}
	rec.router.mu.Lock()
	marked := verifyReplayMark(testMarkKey, rec.router.headers[before].Get(headerReplay))
	rec.router.mu.Unlock()
	if marked {
		t.Fatal("proxy forwarded the replay mark upstream")
	}
	for _, name := range []string{"request.json", "state.json", "plan.json", "plan.jsonl", "results.jsonl", "report.json"} {
		if info, err := os.Stat(filepath.Join(f.svc.runDir("run-1"), name)); err != nil || info.Mode().Perm() != 0600 {
			t.Fatal(name, err)
		}
	}
}

func TestReplayConcurrencyWithEqualWorkRewrite(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-b", 4)
	before := len(rec.router.bodies)
	f := newReplayFixture(t, rec.root, map[string]string{"candidate": rec.proxy.URL})
	request := map[string]any{"run_id": "run-c", "recording": map[string]any{"gateway_id": "g1", "recording_id": "rec-b", "segments": []string{summary.Segments[0].Segment}},
		"target": "candidate", "mode": "concurrency", "concurrency": 2, "fix_output": true, "set_model": "token-model"}
	if code, out := f.call(t, "POST", "/control/replay/v1/runs", request); code != 200 {
		t.Fatal(code, out)
	}
	view := f.wait(t, "run-c")
	report := view.Report
	if view.State != "completed" || report.Original || len(report.Rewritten) != 2 || report.Overall.Completed != 4 || report.Overall.FixOutputDeviations != 0 {
		t.Fatalf("report %s", encodeJSON(view))
	}
	rec.router.mu.Lock()
	defer rec.router.mu.Unlock()
	for _, body := range rec.router.bodies[before:] {
		var fields map[string]json.RawMessage
		json.Unmarshal(body, &fields)
		if string(fields["ignore_eos"]) != "true" || len(fields["min_tokens"]) == 0 || string(fields["max_tokens"]) != string(fields["min_tokens"]) {
			t.Fatalf("rewrite not forwarded: %s", body)
		}
	}
}

func TestReplayControlValidationAndLifecycle(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-v", 3)
	segment := summary.Segments[0].Segment
	f := newReplayFixture(t, rec.root, map[string]string{"candidate": rec.proxy.URL, "entry": rec.switch_.URL})
	req, _ := http.NewRequest("GET", f.server.URL+"/control/replay/v1/status", nil)
	if response, _ := http.DefaultClient.Do(req); response.StatusCode != 403 {
		t.Fatal("unauthenticated status allowed")
	}
	if code, out := f.call(t, "GET", "/control/replay/v1/status", nil); code != 200 || out["service"] != "aigate-replay" {
		t.Fatal(code, out)
	}
	if code, out := f.call(t, "GET", "/control/replay/v1/recordings?gateway_id=g1", nil); code != 200 || !strings.Contains(string(encodeJSON(out)), segment) {
		t.Fatal(code, out)
	}
	base := func(id string) map[string]any {
		return map[string]any{"run_id": id, "recording": map[string]any{"gateway_id": "g1", "recording_id": "rec-v", "segments": []string{segment}}, "target": "candidate", "mode": "timed"}
	}
	for name, mutate := range map[string]func(map[string]any){
		"unknown_target":  func(m map[string]any) { m["target"] = "http://127.0.0.1:1" },
		"invalid_mode":    func(m map[string]any) { m["mode"] = "speed" },
		"invalid_filter":  func(m map[string]any) { m["filter"] = map[string]any{"path": "/v1/models"} },
		"invalid_rewrite": func(m map[string]any) { m["fix_output"] = true; m["set_max_tokens"] = 5 },
	} {
		m := base("bad")
		mutate(m)
		if code, out := f.call(t, "POST", "/control/replay/v1/runs", m); code != 400 {
			t.Fatal(name, code, out)
		}
	}
	// A switch business entry is not a proxy target.
	m := base("to-entry")
	m["target"] = "entry"
	f.call(t, "POST", "/control/replay/v1/runs", m)
	if view := f.wait(t, "to-entry"); view.State != "failed" || view.Error != "target_not_proxy" {
		t.Fatal(view)
	}
	// Missing or unsealed segments fail during planning.
	m = base("missing")
	m["recording"].(map[string]any)["segments"] = []string{"20200101T0000"}
	f.call(t, "POST", "/control/replay/v1/runs", m)
	if view := f.wait(t, "missing"); view.State != "failed" || view.Error != "segment_not_found" {
		t.Fatal(view)
	}
	// A long run is stopped; the same run_id is idempotent; a different body conflicts.
	rec.router.delay = 300 * time.Millisecond
	m = base("long")
	m["mode"], m["concurrency"] = "concurrency", 1
	if code, out := f.call(t, "POST", "/control/replay/v1/runs", m); code != 200 {
		t.Fatal(code, out)
	}
	if code, out := f.call(t, "POST", "/control/replay/v1/runs", m); code != 200 || out["run_id"] != "long" {
		t.Fatal("not idempotent", code, out)
	}
	if code, out := f.call(t, "POST", "/control/replay/v1/runs", base("other")); code != 409 || !strings.Contains(string(encodeJSON(out)), "run_active") {
		t.Fatal(code, out)
	}
	changed := base("long")
	if code, _ := f.call(t, "POST", "/control/replay/v1/runs", changed); code != 409 {
		t.Fatal("conflicting run accepted", code)
	}
	time.Sleep(100 * time.Millisecond)
	if code, out := f.call(t, "POST", "/control/replay/v1/runs/long/stop", nil); code != 200 {
		t.Fatal(code, out)
	}
	if view := f.wait(t, "long"); view.State != "stopped" || view.Report == nil || view.Report.Overall.Sent >= 3 {
		t.Fatalf("stop %s", encodeJSON(view))
	}
	if code, _ := f.call(t, "GET", "/control/replay/v1/runs/nope", nil); code != 404 {
		t.Fatal("unknown run found")
	}
}

func TestReplayRecoveryMarksInterruptedRuns(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "runs", "old")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	recordingAtomicJSON(dir, "state.json", replayRunView{RunID: "old", State: "running", CreatedAt: time.Now()})
	svc, err := newReplayService(strings.Repeat("r", 32), []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}, t.TempDir(), data, map[string]string{"t": "http://127.0.0.1:1"}, testMarkKey, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	view, ok := svc.loadRun("old")
	if !ok || view.State != "end_unknown" || view.Error != "service_restarted" {
		t.Fatal(view)
	}
	if _, err := newReplayService(strings.Repeat("r", 32), []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}, t.TempDir(), data, map[string]string{"t": "http://127.0.0.1:1"}, testMarkKey, io.Discard); err == nil {
		t.Fatal("data root shared")
	}
}

func TestReplayResponseParserTTFT(t *testing.T) {
	p := &replayResponseParser{sent: time.Now(), stream: true}
	stream := "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n: keepalive\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n"
	if err := p.read(strings.NewReader(stream)); err != nil {
		t.Fatal(err)
	}
	if p.ttft == nil || !p.done || p.finish != "length" || p.usage["completion_tokens"] != 1 {
		t.Fatalf("%+v", p)
	}
	p = &replayResponseParser{sent: time.Now(), stream: true}
	p.read(strings.NewReader("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"))
	if p.ttft != nil || p.done {
		t.Fatal("role chunk counted as output")
	}
}
