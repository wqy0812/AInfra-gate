package aigate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func syntheticReplay(t *testing.T, n int, interval float64, body string) (string, replayRequest) {
	t.Helper()
	root := t.TempDir()
	start := time.Date(2026, 10, 14, 2, 0, 0, 0, time.UTC)
	segment := recordingSegmentName(start)
	dir := filepath.Join(root, "g1", "synthetic", segment)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	session := recordingSessionFile{Schema: recordingSchema, GatewayID: "g1", RecordingID: "synthetic"}
	if _, err := recordingAtomicJSON(filepath.Dir(dir), "session.json", session); err != nil {
		t.Fatal(err)
	}
	seg := recordingSegmentFile{Schema: recordingSchema, Segment: segment, State: "sealed", Start: start, End: start.Add(recordingSegment), CoverageStart: start, CoverageEnd: start.Add(recordingSegment), Recorded: uint64(n)}
	if _, err := recordingAtomicJSON(dir, "segment.json", seg); err != nil {
		t.Fatal(err)
	}
	var index bytes.Buffer
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("request-%d", i)
		name := fmt.Sprintf("r-%06d", i)
		record := filepath.Join(dir, name)
		if err := os.Mkdir(record, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(record, "request.json"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		meta := recordingMeta{Schema: recordingSchema, RequestID: id, Method: "POST", Path: "/v1/chat/completions", RequestBytes: int64(len(body)), RequestSavedBytes: int64(len(body)), RequestSHA256: sha256Hex([]byte(body)), RequestState: "complete", CaptureState: "complete", Headers: map[string]string{"Content-Type": "application/json"}, ResponseResult: responseObservation{UsageKnown: true, Usage: map[string]uint64{"completion_tokens": 800}, ParseStatus: "ok", ProtocolComplete: true}}
		if _, err := recordingAtomicJSON(record, "meta.json", meta); err != nil {
			t.Fatal(err)
		}
		line := recordingIndexLine{RequestID: id, Directory: name, OffsetSeconds: float64(i) * interval, Method: "POST", Path: meta.Path, Status: 200, RequestState: "complete", CaptureState: "complete"}
		index.Write(encodeJSON(line))
		index.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "index.jsonl"), index.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	req := replayRequest{RunID: "synthetic-run", Recording: replayRecordingRef{GatewayID: "g1", RecordingID: "synthetic", Segments: []string{segment}}, Target: "r1", Mode: "timed"}
	if err := req.normalize(); err != nil {
		t.Fatal(err)
	}
	return root, req
}

func replayTarget(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/runtime/v1/status" {
			json.NewEncoder(w).Encode(map[string]any{"service": "aigate-proxy", "router_id": "r1", "config_digest": strings.Repeat("a", 64)})
			return
		}
		handler(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func readReplayResults(t *testing.T, f *replayFixture, id string) []replayResult {
	t.Helper()
	file, err := os.Open(filepath.Join(f.svc.runDir(id), "results.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var out []replayResult
	d := json.NewDecoder(file)
	for {
		var result replayResult
		err := d.Decode(&result)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, result)
	}
	return out
}

func TestReplayTimedExceeds512WhileResponsesBlocked(t *testing.T) {
	root, req := syntheticReplay(t, 530, .004, `{"messages":[],"max_tokens":9}`)
	gate := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(gate) })
	arrived := make(chan struct{}, 530)
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		arrived <- struct{}{}
		<-gate
		io.WriteString(w, `{}`)
	})
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	// Cleanup release must precede both servers' cleanup on a test failure.
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	f.svc.start(req)
	deadline := time.After(10 * time.Second)
	for i := 0; i < 530; i++ {
		select {
		case <-arrived:
		case <-deadline:
			view, _ := f.svc.loadRun(req.RunID)
			t.Fatalf("only %d arrived: %+v", i, view)
		}
	}
	view, _ := f.svc.loadRun(req.RunID)
	if view.Progress.Inflight != 530 || view.Progress.Completed != 0 {
		t.Fatalf("%+v", view)
	}
	f.svc.stop(req.RunID)
	if _, status, code := f.svc.start(replayRequest{RunID: "another"}); status != 409 || code != "run_active" {
		t.Fatal("released before drain", status, code)
	}
	once.Do(func() { close(gate) })
	view = f.wait(t, req.RunID)
	if view.State != "stopped" || view.Progress.Completed != 530 || view.Progress.Unsent != 0 {
		t.Fatalf("%+v", view)
	}
	if len(readReplayResults(t, f, req.RunID)) != 530 || f.svc.bodyMemory.used != 0 {
		t.Fatal("missing results or leaked budget")
	}
}

func TestReplayTimedKeepsSparseOffsetsAndFilterGaps(t *testing.T) {
	root, req := syntheticReplay(t, 3, .3, `{}`)
	dir := filepath.Join(root, "g1", "synthetic", req.Recording.Segments[0])
	lines := readIndex(t, filepath.Join(dir, "index.jsonl"))
	lines[0].Status = 400
	lines[0].Outcome = "rejected"
	lines[1].Status = 502
	lines[1].CaptureState = "truncated"
	var index bytes.Buffer
	for _, line := range lines {
		index.Write(encodeJSON(line))
		index.WriteByte('\n')
	}
	os.WriteFile(filepath.Join(dir, "index.jsonl"), index.Bytes(), 0600)
	req.Filter.Status = "all"
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); io.WriteString(w, `{}`) })
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	f.svc.start(req)
	view := f.wait(t, req.RunID)
	results := readReplayResults(t, f, req.RunID)
	if view.Progress.Sent != 3 || view.Excluded["status"] != 0 {
		t.Fatalf("%+v", view)
	}
	for _, result := range results {
		want := float64(result.Sequence-1) * .3
		if result.PlannedOffset != want || result.SendOffset == nil || *result.SendOffset < want-.02 || result.WriteOffset == nil {
			t.Fatalf("offsets: %+v", result)
		}
	}
}

func TestReplayConcurrencyUsesExactlyNAndStopDrains(t *testing.T) {
	root, req := syntheticReplay(t, 12, 10, `{}`)
	req.Mode, req.Concurrency = "concurrency", 3
	arrived, release, drain := make(chan struct{}, 12), make(chan struct{}, 12), make(chan struct{})
	var once sync.Once
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		arrived <- struct{}{}
		select {
		case <-release:
		case <-drain:
		}
		io.WriteString(w, `{}`)
	})
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	t.Cleanup(func() { once.Do(func() { close(drain) }) })
	f.svc.start(req)
	for i := 0; i < 3; i++ {
		select {
		case <-arrived:
		case <-time.After(3 * time.Second):
			t.Fatal("did not fill concurrency")
		}
	}
	select {
	case <-arrived:
		t.Fatal("exceeded concurrency")
	case <-time.After(100 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("waited for source interval")
	}
	f.svc.stop(req.RunID)
	view, _ := f.svc.loadRun(req.RunID)
	if view.EndedAt != nil {
		t.Fatal("did not drain")
	}
	once.Do(func() { close(drain) })
	view = f.wait(t, req.RunID)
	if view.State != "stopped" || view.Progress.Sent != 4 || view.Progress.Completed != 4 || view.Progress.Unsent != 8 {
		t.Fatalf("%+v", view)
	}
}

func TestReplayTimedResourceExhaustionNeverWaits(t *testing.T) {
	root, req := syntheticReplay(t, 2, .1, `{}`)
	var requests atomic.Int64
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1) })
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	f.svc.bodyMemory.acquire(context.Background(), replayBodyBudget)
	defer f.svc.bodyMemory.release(replayBodyBudget)
	f.svc.start(req)
	view := f.wait(t, req.RunID)
	if view.State != "failed" || view.Error != "replay_resource_exhausted" || view.Progress.Unsent != 2 || requests.Load() != 0 {
		t.Fatalf("%+v", view)
	}
}

type failedReplayResults struct{}

func (failedReplayResults) Write([]byte) (int, error) { return 0, errors.New("disk full") }
func (failedReplayResults) Sync() error               { return nil }
func (failedReplayResults) Close() error              { return nil }

func TestReplayResultFailureStopsNewSends(t *testing.T) {
	root, req := syntheticReplay(t, 4, .3, `{}`)
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"usage":{"completion_tokens":800}}`)
	})
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	f.svc.openResults = func(string) (replayResultFile, error) { return failedReplayResults{}, nil }
	f.svc.start(req)
	view := f.wait(t, req.RunID)
	if view.State != "failed" || view.Error != "replay_results_failed" || view.Progress.Sent != 1 || !view.Output.ResultsFailed || view.Output.Matched != 0 {
		t.Fatalf("%+v", view)
	}
	if f.svc.bodyMemory.used != 0 {
		t.Fatal("leaked budget")
	}
}

func TestReplayObservesResponsesWithoutRewritesOrRetries(t *testing.T) {
	root, req := syntheticReplay(t, 10, 0, `{"messages":[],"stream":true}`)
	req.Mode, req.Concurrency = "concurrency", 1
	var calls atomic.Int64
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"messages":[],"stream":true}` {
			t.Error("request was rewritten")
		}
		switch calls.Add(1) {
		case 1:
			io.WriteString(w, `{"usage":{"completion_tokens":800}}`)
		case 2:
			io.WriteString(w, `{"usage":{"completion_tokens":100}}`)
		case 3:
			io.WriteString(w, `{"usage":{"completion_tokens":801}}`)
		case 4:
			io.WriteString(w, `{"choices":[]}`)
		case 5:
			w.WriteHeader(400)
			io.WriteString(w, "context exceeded or unsupported parameter: "+strings.Repeat("x", 8000))
		case 6:
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":800}}\n\n")
		case 7:
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":2}}\n\ndata: {\"usage\":{\"completion_tokens\":800}}\n\ndata: [DONE]\n\n")
		case 8:
			io.WriteString(w, `{"usage":{"completion_tokens":800},"broken":`)
		case 9:
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":800}}\n\ndata: {\"error\":{\"message\":\"generation failed\"}}\n\ndata: [DONE]\n\n")
		case 10:
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"usage\":{\"completion_tokens\":800}}\n\n")
			w.(http.Flusher).Flush()
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
		default:
			t.Error("unexpected retry")
		}
	})
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	f.svc.start(req)
	view := f.wait(t, req.RunID)
	if view.State != "completed" || view.Progress.Sent != 10 || view.Progress.Failed != 3 || view.Output.Matched != 0 || view.Output.Short != 0 || view.Output.Long != 0 || view.Output.Unverifiable != 0 || view.Output.Unknown != 2 || view.Output.ActualTokens != 5701 {
		t.Fatalf("%+v", view)
	}
	results := readReplayResults(t, f, req.RunID)
	if len(results) != 10 || results[4].ErrorSummary == "" || results[5].Observation.ProtocolComplete || results[9].Observation.TransportError != "unexpected_eof" || !results[9].Observation.UsageKnown {
		t.Fatal("lost diagnostics")
	}
	if len(results[4].ErrorSummary) > responseErrorLimit {
		t.Fatal("unbounded error summary")
	}
}

func TestReplayModelRewritePreservesEveryOtherField(t *testing.T) {
	for _, stream := range []string{"", `,"stream":false`, `,"stream":true`} {
		body := []byte(` {"model":"old","messages":[{"role":"user","content":"unchanged"}],"n":2,"max_tokens":9,"max_completion_tokens":12,"stop":["x"],"stop_token_ids":[3],"min_tokens":1,"ignore_eos":false,"temperature":0.6,"stream_options":{"other":7}` + stream + `} `)
		got := rewriteReplayBody(body, replayRequest{})
		if !bytes.Equal(body, got) {
			t.Fatal("original bytes changed")
		}
		got = rewriteReplayBody(body, replayRequest{SetModel: "new"})
		var before, after map[string]json.RawMessage
		json.Unmarshal(body, &before)
		json.Unmarshal(got, &after)
		if string(after["model"]) != `"new"` {
			t.Fatal("model not filled")
		}
		delete(before, "model")
		delete(after, "model")
		if !bytes.Equal(encodeJSON(before), encodeJSON(after)) {
			t.Fatalf("parameters changed: %s", got)
		}
	}
}

type replayRoundTrip func(*http.Request) (*http.Response, error)

func (fn replayRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

func TestReplayWriteTimingUnknownAndDelayed(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(fmt.Sprint(success), func(t *testing.T) {
			root, req := syntheticReplay(t, 1, 0, `{}`)
			f := newReplayFixture(t, root, map[string]string{"r1": "http://synthetic.invalid"})
			f.svc.client.Transport = replayRoundTrip(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/runtime/v1/status" {
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"service":"aigate-proxy","router_id":"r1","config_digest":"` + strings.Repeat("a", 64) + `"}`))}, nil
				}
				defer r.Body.Close()
				io.Copy(io.Discard, r.Body)
				trace := httptrace.ContextClientTrace(r.Context())
				if !success {
					trace.WroteRequest(httptrace.WroteRequestInfo{Err: errors.New("write failed")})
					return nil, errors.New("write failed")
				}
				time.Sleep(80 * time.Millisecond)
				trace.WroteRequest(httptrace.WroteRequestInfo{})
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			})
			f.svc.start(req)
			view := f.wait(t, req.RunID)
			result := readReplayResults(t, f, req.RunID)[0]
			if success {
				if result.WriteOffset == nil || *result.WriteOffset-*result.SendOffset < .05 || view.MaxWriteDelay == nil {
					t.Fatalf("%+v", result)
				}
			} else if result.WriteOffset != nil || view.MaxWriteDelay != nil {
				t.Fatal("invented successful write")
			}
		})
	}
}

func TestReplayPlanRequiresSchemaAndRequestIntegrity(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*recordingMeta)
		body   string
		want   string
	}{
		{"old_schema", func(m *recordingMeta) { m.Schema = 1 }, `{}`, "recording_schema_unsupported_rerecord"},
		{"corrupt", func(m *recordingMeta) { m.RequestSHA256 = "bad" }, `{}`, "record_changed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, req := syntheticReplay(t, 1, 0, test.body)
			dir := filepath.Join(root, "g1", "synthetic", req.Recording.Segments[0], "r-000000")
			var meta recordingMeta
			readJSONFile(t, filepath.Join(dir, "meta.json"), &meta)
			test.mutate(&meta)
			recordingAtomicJSON(dir, "meta.json", meta)
			// The absent response.tail must never be consulted.
			if _, err := buildReplayPlan(root, req); err == nil || err.Error() != test.want {
				t.Fatal(err)
			}
		})
	}
}

func TestReplayInvalidStructuredUsageDoesNotReadTail(t *testing.T) {
	root, req := syntheticReplay(t, 1, 0, `{}`)
	dir := filepath.Join(root, "g1", "synthetic", req.Recording.Segments[0], "r-000000")
	raw, _ := os.ReadFile(filepath.Join(dir, "meta.json"))
	raw = bytes.Replace(raw, []byte(`"completion_tokens":800`), []byte(`"completion_tokens":-1`), 1)
	os.WriteFile(filepath.Join(dir, "meta.json"), raw, 0600)
	os.WriteFile(filepath.Join(dir, "response.tail"), []byte(`{"usage":{"completion_tokens":800}}`), 0600)
	if _, err := buildReplayPlan(root, req); err != nil {
		t.Fatal(err)
	}
}

func TestReplayMalformedRequestsAreSentUnchanged(t *testing.T) {
	for _, body := range []string{`{"broken":`, `null`, `[1,2]`} {
		t.Run(body, func(t *testing.T) {
			root, req := syntheticReplay(t, 1, 0, body)
			req.SetModel = "automatic-model"
			var calls atomic.Int64
			target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
				got, _ := io.ReadAll(r.Body)
				if string(got) != body {
					t.Errorf("failed request changed: %s", got)
				}
				calls.Add(1)
				w.WriteHeader(400)
				io.WriteString(w, `{"error":{"message":"invalid request"}}`)
			})
			f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
			f.svc.start(req)
			view := f.wait(t, req.RunID)
			result := readReplayResults(t, f, req.RunID)[0]
			if calls.Load() != 1 || view.Progress.Sent != 1 || view.Progress.Failed != 1 || result.Error != "http_400" || len(result.Rewritten) != 0 {
				t.Fatalf("failure not replayed faithfully: %+v %+v", view, result)
			}
		})
	}
}
