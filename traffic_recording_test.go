package aigate

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tokenRouter is a minimal OpenAI-compatible Router used by recording and
// replay tests. Streaming responses emit one content chunk per output token
// (max_tokens, default 3) and a final usage chunk.
type tokenRouter struct {
	server   *httptest.Server
	posts    atomic.Int64
	gate     chan struct{}
	mu       sync.Mutex
	bodies   [][]byte
	headers  []http.Header
	delay    time.Duration
	failNext atomic.Int64
}

func newTokenRouter(t *testing.T) *tokenRouter {
	t.Helper()
	r := &tokenRouter{}
	r.server = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.server.Close)
	return r
}

func (r *tokenRouter) serve(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		io.WriteString(w, `{"data":[{"id":"token-model"}]}`)
		return
	}
	body, _ := io.ReadAll(req.Body)
	r.posts.Add(1)
	r.mu.Lock()
	r.bodies = append(r.bodies, body)
	r.headers = append(r.headers, req.Header.Clone())
	r.mu.Unlock()
	if r.failNext.Load() > 0 {
		r.failNext.Add(-1)
		w.WriteHeader(500)
		io.WriteString(w, `{"error":"boom"}`)
		return
	}
	var p struct {
		Stream    bool `json:"stream"`
		MaxTokens int  `json:"max_tokens"`
	}
	_ = json.Unmarshal(body, &p)
	n := p.MaxTokens
	if n <= 0 {
		n = 3
	}
	usage := map[string]any{"prompt_tokens": len(body), "completion_tokens": n, "total_tokens": len(body) + n, "prompt_tokens_details": map[string]any{"cached_tokens": len(body) / 2}}
	if !p.Stream {
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": strings.Repeat("x", n)}, "finish_reason": "stop"}}, "usage": usage})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
	w.(http.Flusher).Flush()
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	if r.gate != nil {
		select {
		case <-r.gate:
		case <-req.Context().Done():
			return
		}
	}
	for i := 0; i < n; i++ {
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		w.(http.Flusher).Flush()
	}
	fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: %s\n\ndata: [DONE]\n\n", encodeJSON(map[string]any{"choices": []any{}, "usage": usage}))
}

type recordingFixture struct {
	c       *ControlledGateway
	p       *ProxyGateway
	switch_ *httptest.Server
	proxy   *httptest.Server
	router  *tokenRouter
	root    string
}

var testMarkKey = []byte(strings.Repeat("m", 32))

func newRecordingFixture(t *testing.T, maxBytes int64, extraMarkKeys ...[]byte) *recordingFixture {
	t.Helper()
	f := &recordingFixture{router: newTokenRouter(t), root: t.TempDir()}
	f.p, f.proxy = splitProxyFixture(t, "r1", f.router.server.URL, `{}`)
	markKeys := append([][]byte{testMarkKey}, extraMarkKeys...)
	f.p.markKeys = markKeys
	c, err := newSwitchGateway("g1", strings.Repeat("k", 32), []string{"127.0.0.1/32"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.enableRecording(f.root, maxBytes, markKeys); err != nil {
		t.Fatal(err)
	}
	f.c = c
	f.switch_ = httptest.NewServer(c)
	t.Cleanup(func() { f.switch_.Close(); c.Close() })
	splitActivate(t, c, f.p, f.proxy.URL, "op-1", 1)
	return f
}

func recordingCommand(t *testing.T, c *ControlledGateway, action string, payload any) (RuntimeResult, int) {
	t.Helper()
	c.mu.Lock()
	cmd := RuntimeCommand{ProtocolVersion: 3, OperationID: "rec-op", GatewayID: c.id, ExpectedBootID: c.boot, Sequence: c.sequence + 1, ExpectedRevision: c.revisionLocked(), Action: action, Payload: encodeJSON(payload)}
	c.mu.Unlock()
	r := httptest.NewRequest("POST", "/control/v3/commands", bytes.NewReader(encodeJSON(cmd)))
	r.RemoteAddr = "127.0.0.1:321"
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("k", 32))
	w := httptest.NewRecorder()
	c.ControlHandler().ServeHTTP(w, r)
	var result RuntimeResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("%s: %d %s", action, w.Code, w.Body.String())
	}
	return result, w.Code
}

func waitCommand(t *testing.T, c *ControlledGateway, seq uint64) RuntimeResult {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		result := *c.results[seq]
		c.mu.Unlock()
		if result.Status != "running" {
			return result
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("command did not finish")
	return RuntimeResult{}
}

func startRecording(t *testing.T, c *ControlledGateway, id string, start, end time.Time, extra map[string]any) RuntimeResult {
	t.Helper()
	payload := map[string]any{"recording_id": id, "start_at": start, "end_at": end}
	for k, v := range extra {
		payload[k] = v
	}
	result, code := recordingCommand(t, c, "record-start", payload)
	if code != 200 || result.Status == "failed" {
		t.Fatalf("record-start: %d %+v", code, result)
	}
	return waitCommand(t, c, result.Sequence)
}

func waitRecordingEnded(t *testing.T, c *ControlledGateway, id string) recordingSummary {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		summary, err := c.recorder.statusOf(id, time.Now())
		if err == nil && summary.State == "ended" {
			return *summary
		}
		time.Sleep(20 * time.Millisecond)
	}
	summary, _ := c.recorder.statusOf(id, time.Now())
	t.Fatalf("recording did not end: %+v", summary)
	return recordingSummary{}
}

func halfHour(now time.Time) time.Time { return now.UTC().Truncate(recordingSegment) }

func readJSONFile(t *testing.T, path string, v any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, v); err != nil {
		t.Fatal(path, err)
	}
}

func readIndex(t *testing.T, path string) []recordingIndexLine {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []recordingIndexLine
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var line recordingIndexLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatal(err)
		}
		out = append(out, line)
	}
	return out
}

func postChat(t *testing.T, url, body string, header map[string]string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", url+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(data)
}

func TestRecordingSwitchProxyEndToEnd(t *testing.T) {
	secondKey := []byte(strings.Repeat("n", 32))
	f := newRecordingFixture(t, recordingDefaultMaxBytes, secondKey)
	now := time.Now()
	startRecording(t, f.c, "rec-1", halfHour(now), halfHour(now).Add(time.Hour), map[string]any{"schedule_id": "rule-1"})
	streamBody := `{"model":"token-model","messages":[{"role":"user","content":"hello recording"}],"stream":true,"max_tokens":4}`
	code, streamed := postChat(t, f.switch_.URL, streamBody, map[string]string{"X-Request-ID": "client-1", "Authorization": "Bearer secret"})
	if code != 200 || !strings.Contains(streamed, "[DONE]") {
		t.Fatal(code, streamed)
	}
	plainBody := `{"model":"token-model","messages":[{"role":"user","content":"plain"}]}`
	if code, body := postChat(t, f.switch_.URL, plainBody, nil); code != 200 {
		t.Fatal(code, body)
	}
	// A verified replay mark is never recorded; a forged one is ordinary traffic.
	if code, body := postChat(t, f.switch_.URL, `{"messages":[{"role":"user","content":"replayed"}],"stream":true}`, map[string]string{headerReplay: replayMarkValue(testMarkKey, "run-1", 1)}); code != 200 {
		t.Fatal(code, body)
	}
	if code, body := postChat(t, f.switch_.URL, plainBody, map[string]string{headerReplay: replayMarkValue(secondKey, "run-2", 1)}); code != 200 {
		t.Fatal(code, body)
	}
	if code, body := postChat(t, f.switch_.URL, `{"messages":[{"role":"user","content":"forged"}],"stream":true}`, map[string]string{headerReplay: "run-1/1/" + strings.Repeat("0", 64)}); code != 200 {
		t.Fatal(code, body)
	}
	if response, err := http.Get(f.switch_.URL + "/v1/models"); err != nil || response.StatusCode != 200 {
		t.Fatal(err)
	} else {
		response.Body.Close()
	}
	// The proxy labels verified replays and leaves the rest unchanged.
	if result, code := recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": "rec-1"}); code != 200 || result.Status != "completed" {
		t.Fatal(code, result)
	}
	summary := waitRecordingEnded(t, f.c, "rec-1")
	if summary.EndReason != "stopped" || summary.Requests != 4 || summary.Recorded != 4 || summary.Dropped != 0 || len(summary.Segments) != 1 || summary.Segments[0].State != "sealed" {
		t.Fatalf("summary %+v", summary)
	}
	dir := filepath.Join(f.root, "g1", "rec-1")
	var session recordingSessionFile
	readJSONFile(t, filepath.Join(dir, "session.json"), &session)
	if session.State != "ended" || session.EndReason != "stopped" || session.ScheduleID != "rule-1" || session.GatewayID != "g1" || session.Recorded != 4 || len(session.Segments) != 1 {
		t.Fatalf("session %+v", session)
	}
	segDir := filepath.Join(dir, session.Segments[0])
	var segment recordingSegmentFile
	readJSONFile(t, filepath.Join(segDir, "segment.json"), &segment)
	if segment.State != "sealed" || segment.Recorded != 4 || !segment.Partial || len(segment.Routers) != 1 || segment.Routers[0].RouterID != "r1" || segment.Routers[0].BindingRevision != 1 {
		t.Fatalf("segment %+v", segment)
	}
	index := readIndex(t, filepath.Join(segDir, "index.jsonl"))
	if len(index) != 4 {
		t.Fatalf("index %+v", index)
	}
	var stream recordingIndexLine
	for _, line := range index {
		if line.Path == "/v1/chat/completions" && line.RequestBytes == int64(len(streamBody)) {
			stream = line
		}
		if info, err := os.Stat(filepath.Join(segDir, line.Directory)); err != nil || info.Mode().Perm() != 0700 {
			t.Fatal("record directory permissions", err)
		}
	}
	if stream.Status != 200 || stream.Outcome != "completed" || stream.RequestState != "complete" || stream.CaptureState != "complete" {
		t.Fatalf("stream index %+v", stream)
	}
	recordDir := filepath.Join(segDir, stream.Directory)
	body, _ := os.ReadFile(filepath.Join(recordDir, "request.json"))
	if string(body) != streamBody {
		t.Fatalf("request bytes %q", body)
	}
	tail, _ := os.ReadFile(filepath.Join(recordDir, "response.body"))
	if streamed != string(tail) || !strings.Contains(string(tail), "completion_tokens") {
		t.Fatalf("tail %q", tail)
	}
	var meta recordingMeta
	readJSONFile(t, filepath.Join(recordDir, "meta.json"), &meta)
	if meta.Schema != 2 || !meta.ResponseResult.UsageKnown || !meta.ResponseResult.ProtocolComplete || meta.ResponseResult.Usage["completion_tokens"] == 0 {
		t.Fatalf("missing structured response: %+v", meta.ResponseResult)
	}
	if meta.Headers["X-Request-ID"] != "client-1" || meta.Headers["Authorization"] != "" || meta.RouterID != "r1" || meta.BindingRevision != 1 || meta.ConfigDigest != f.p.digest ||
		meta.UpstreamRequestID == "" || !strings.HasPrefix(meta.ResponseHeaders["Content-Type"], "text/event-stream") || meta.RequestSHA256 != sha256Hex([]byte(streamBody)) || meta.Segment != session.Segments[0] {
		t.Fatalf("meta %+v", meta)
	}
	timeline, _ := os.ReadFile(filepath.Join(recordDir, "timeline.jsonl"))
	for _, want := range []string{`"type":"arrived"`, `"type":"upload_completed"`, `"type":"response_header"`, `"type":"write"`, `"type":"flush"`, `"type":"ended"`} {
		if !strings.Contains(string(timeline), want) {
			t.Fatalf("timeline lacks %s: %s", want, timeline)
		}
	}
	if info, _ := os.Stat(filepath.Join(recordDir, "request.json")); info.Mode().Perm() != 0600 {
		t.Fatal("record file permissions", info.Mode())
	}
}

func TestRecordingLongResponseWritesFullBodyAndStructuredUsage(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			f := newRecordingFixture(t, recordingDefaultMaxBytes)
			now := time.Now()
			startRecording(t, f.c, "long-json", halfHour(now), halfHour(now).Add(time.Hour), nil)
			body := `{"model":"token-model","messages":[{"role":"user","content":"` + strings.Repeat("input", 4000) + `"}],"max_tokens":20000,"stream":` + fmt.Sprint(stream) + `}`
			code, response := postChat(t, f.switch_.URL, body, nil)
			if code != 200 || len(response) < 20000 {
				t.Fatal("forwarding changed", code, response[:min(160, len(response))])
			}
			recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": "long-json"})
			summary := waitRecordingEnded(t, f.c, "long-json")
			dir := filepath.Join(f.root, "g1", "long-json", summary.Segments[0].Segment)
			index := readIndex(t, filepath.Join(dir, "index.jsonl"))
			record := filepath.Join(dir, index[0].Directory)
			savedRequest, _ := os.ReadFile(filepath.Join(record, "request.json"))
			if string(savedRequest) != body {
				t.Fatal("request was truncated")
			}
			tail, err := os.ReadFile(filepath.Join(record, "response.body"))
			if err != nil || string(tail) != response {
				t.Fatal("full response differs", err)
			}
			var meta recordingMeta
			readJSONFile(t, filepath.Join(record, "meta.json"), &meta)
			if meta.ResponseResult.Usage["completion_tokens"] != 20000 || !meta.ResponseResult.ProtocolComplete || meta.ResponseBytes != int64(len(response)) || meta.ResponseSaved != int64(len(response)) || meta.ResponseSHA256 != sha256Hex(tail) {
				t.Fatalf("%+v", meta)
			}
			entries, _ := os.ReadDir(record)
			if len(entries) != 4 {
				t.Fatalf("unexpected files: %v", entries)
			}
			raw, _ := os.ReadFile(filepath.Join(record, "meta.json"))
			if strings.Contains(string(raw), "response_mode") {
				t.Fatal("retired mode persisted")
			}
		})
	}
}

func TestRecordingCapturesRejectionsAndCancellation(t *testing.T) {
	f := newRecordingFixture(t, recordingDefaultMaxBytes)
	f.router.gate = make(chan struct{})
	now := time.Now()
	startRecording(t, f.c, "rec-r", halfHour(now), halfHour(now).Add(time.Hour), nil)
	// Admission rejection: the entry is quiesced.
	splitControlCommand(t, f.c, "op-q", "quiesce", map[string]any{}, 3)
	if code, _ := postChat(t, f.switch_.URL, `{"messages":[]}`, nil); code != 503 {
		t.Fatal("expected rejection", code)
	}
	splitControlCommand(t, f.c, "op-q", "resume", map[string]any{}, 3)
	// Client cancellation while the stream waits on the Router.
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", f.switch_.URL+"/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"cancel"}],"stream":true}`))
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(response.Body)
	if _, err = reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	cancel()
	response.Body.Close()
	close(f.router.gate)
	recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": "rec-r"})
	summary := waitRecordingEnded(t, f.c, "rec-r")
	if summary.Recorded != 2 {
		t.Fatalf("summary %+v", summary)
	}
	index := readIndex(t, filepath.Join(f.root, "g1", "rec-r", summary.Segments[0].Segment, "index.jsonl"))
	outcomes := map[string]recordingIndexLine{}
	for _, line := range index {
		outcomes[line.Outcome] = line
	}
	if rejected := outcomes["rejected"]; rejected.Status != 503 || rejected.RequestState != "not_read" {
		t.Fatalf("rejected %+v", index)
	}
	if cancelled := outcomes["client_cancelled"]; cancelled.Status != 200 || cancelled.RequestState != "complete" {
		t.Fatalf("cancelled %+v", index)
	}
}

func TestRecordingBindingSwitchKeepsRevisionPerRecord(t *testing.T) {
	f := newRecordingFixture(t, recordingDefaultMaxBytes)
	p2, proxy2 := splitProxyFixture(t, "r2", f.router.server.URL, `{}`)
	now := time.Now()
	startRecording(t, f.c, "rec-s", halfHour(now), halfHour(now).Add(time.Hour), nil)
	postChat(t, f.switch_.URL, `{"messages":[{"role":"user","content":"before"}],"stream":true}`, nil)
	splitActivate(t, f.c, p2, proxy2.URL, "op-2", 2)
	postChat(t, f.switch_.URL, `{"messages":[{"role":"user","content":"after"}],"stream":true}`, nil)
	recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": "rec-s"})
	summary := waitRecordingEnded(t, f.c, "rec-s")
	var segment recordingSegmentFile
	readJSONFile(t, filepath.Join(f.root, "g1", "rec-s", summary.Segments[0].Segment, "segment.json"), &segment)
	if len(segment.Routers) != 2 || segment.Routers[0].RouterID != "r1" || segment.Routers[1].RouterID != "r2" || segment.Routers[1].BindingRevision != 2 {
		t.Fatalf("routers %+v", segment.Routers)
	}
}

func TestRecordingCommandValidationAndScheduling(t *testing.T) {
	c, err := newSwitchGateway("g2", strings.Repeat("k", 32), []string{"127.0.0.1/32"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now()
	if result, _ := recordingCommand(t, c, "record-start", map[string]any{"recording_id": "a", "start_at": halfHour(now), "end_at": halfHour(now).Add(time.Hour)}); result.Error != "recording_disabled" {
		t.Fatal(result)
	}
	root := t.TempDir()
	if err := c.enableRecording(root, 1<<30, nil); err != nil {
		t.Fatal(err)
	}
	future := halfHour(now).Add(2 * time.Hour)
	shanghai := time.FixedZone("CST", 8*3600)
	for _, bad := range []map[string]any{
		{"recording_id": "a", "start_at": future.Add(time.Minute), "end_at": future.Add(time.Hour)},
		{"recording_id": "a", "start_at": future.In(shanghai), "end_at": future.Add(time.Hour)},
		{"recording_id": "a", "start_at": future, "end_at": future},
		{"recording_id": "a", "start_at": halfHour(now).Add(-time.Hour), "end_at": halfHour(now)},
		{"recording_id": "a", "start_at": future, "end_at": future.Add(time.Hour), "response": "bodies"},
		{"recording_id": "a", "start_at": future, "end_at": future.Add(time.Hour), "max_bytes": 2 << 30},
		{"recording_id": "bad id", "start_at": future, "end_at": future.Add(time.Hour)},
	} {
		if result, _ := recordingCommand(t, c, "record-start", bad); result.Status != "failed" {
			t.Fatal("accepted", bad, result)
		}
	}
	first := startRecording(t, c, "w1", future, future.Add(time.Hour), nil)
	if first.Status != "completed" {
		t.Fatal(first)
	}
	summary, _ := c.recorder.statusOf("w1", time.Now())
	if summary.State != "scheduled" {
		t.Fatal(summary)
	}
	// A second pending window conflicts; an existing ID is rejected.
	if result, _ := recordingCommand(t, c, "record-start", map[string]any{"recording_id": "w2", "start_at": future.Add(time.Hour), "end_at": future.Add(2 * time.Hour)}); result.Error != "recording_active" {
		t.Fatal(result)
	}
	// Stopping a pending window cancels it without sampling anything.
	recordingCommand(t, c, "record-stop", map[string]any{"recording_id": "w1"})
	ended := waitRecordingEnded(t, c, "w1")
	if ended.EndReason != "stopped" || ended.Requests != 0 {
		t.Fatal(ended)
	}
	if result, _ := recordingCommand(t, c, "record-start", map[string]any{"recording_id": "w1", "start_at": future, "end_at": future.Add(time.Hour)}); result.Error != "recording_exists" {
		t.Fatal(result)
	}
	// A recording window may be followed by one adjacent pending window.
	startRecording(t, c, "now", halfHour(now), halfHour(now).Add(time.Hour), nil)
	if result := startRecording(t, c, "next", halfHour(now).Add(time.Hour), halfHour(now).Add(2*time.Hour), nil); result.Status != "completed" {
		t.Fatal(result)
	}
	if result, _ := recordingCommand(t, c, "record-start", map[string]any{"recording_id": "third", "start_at": halfHour(now).Add(2 * time.Hour), "end_at": halfHour(now).Add(3 * time.Hour)}); result.Error != "recording_active" {
		t.Fatal(result)
	}
	status := c.recorder.status(time.Now()).(recordingStatus)
	if len(status.Sessions) != 2 || status.Sessions[0].State != "recording" || status.Sessions[1].State != "scheduled" {
		t.Fatalf("status %+v", status)
	}
	// Retire ends every session.
	recordingCommand(t, c, "retire", map[string]any{})
	for _, id := range []string{"now", "next"} {
		if summary := waitRecordingEnded(t, c, id); summary.EndReason != "retired" {
			t.Fatal(summary)
		}
	}
}

func TestRecordingDiskFailureLeavesResponsesUnchanged(t *testing.T) {
	f := newRecordingFixture(t, recordingDefaultMaxBytes)
	body := `{"messages":[{"role":"user","content":"same bytes"}],"stream":true}`
	_, before := postChat(t, f.switch_.URL, body, nil)
	f.c.recorder.writeFile = func(string, []byte) error { return fmt.Errorf("disk full") }
	now := time.Now()
	startRecording(t, f.c, "rec-d", halfHour(now), halfHour(now).Add(time.Hour), nil)
	_, during := postChat(t, f.switch_.URL, body, nil)
	if stripVolatileSSE(before) != stripVolatileSSE(during) {
		t.Fatalf("response changed:\n%s\n%s", before, during)
	}
	summary := waitRecordingEnded(t, f.c, "rec-d")
	if summary.EndReason != "disk_failed" || summary.Dropped != 1 {
		t.Fatalf("summary %+v", summary)
	}
	// Later business requests are not sampled once the session ended.
	if code, _ := postChat(t, f.switch_.URL, body, nil); code != 200 {
		t.Fatal(code)
	}
}

func stripVolatileSSE(s string) string { return s }

func TestRecordingMemoryBudgetTruncatesWithoutChangingForwarding(t *testing.T) {
	f := newRecordingFixture(t, recordingDefaultMaxBytes)
	now := time.Now()
	startRecording(t, f.c, "rec-m", halfHour(now), halfHour(now).Add(time.Hour), nil)
	// Exhaust the recording memory budget except for the per-record charge.
	f.c.recorder.mu.Lock()
	f.c.recorder.used = recordingMemory - recordingRecordCharge
	f.c.recorder.mu.Unlock()
	body := `{"messages":[{"role":"user","content":"` + strings.Repeat("z", 4096) + `"}],"stream":true}`
	if code, out := postChat(t, f.switch_.URL, body, nil); code != 200 || !strings.Contains(out, "[DONE]") {
		t.Fatal(code, out)
	}
	f.c.recorder.mu.Lock()
	f.c.recorder.used = 0
	f.c.recorder.mu.Unlock()
	f.router.mu.Lock()
	forwarded := string(f.router.bodies[len(f.router.bodies)-1])
	f.router.mu.Unlock()
	if !strings.Contains(forwarded, strings.Repeat("z", 4096)) {
		t.Fatal("forwarded body changed")
	}
	recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": "rec-m"})
	summary := waitRecordingEnded(t, f.c, "rec-m")
	index := readIndex(t, filepath.Join(f.root, "g1", "rec-m", summary.Segments[0].Segment, "index.jsonl"))
	if summary.Truncated != 1 || len(index) != 1 || index[0].RequestState != "truncated" || index[0].CaptureState != "truncated" {
		t.Fatalf("summary %+v index %+v", summary, index)
	}
}

func writeStoredSegment(t *testing.T, root, recording, name string, size int, pinned bool) {
	t.Helper()
	dir := filepath.Join(root, recording, name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	start, _ := parseRecordingSegment(name)
	if _, err := recordingAtomicJSON(dir, "segment.json", recordingSegmentFile{Schema: 1, RecordingID: recording, Segment: name, Start: start, State: "sealed"}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.jsonl"), bytes.Repeat([]byte("x"), size), 0600); err != nil {
		t.Fatal(err)
	}
	if pinned {
		os.WriteFile(filepath.Join(dir, "pinned"), []byte("1\n"), 0600)
	}
}

func writeStoredSession(t *testing.T, root, recording, state string) {
	t.Helper()
	dir := filepath.Join(root, recording)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := recordingAtomicJSON(dir, "session.json", recordingSessionFile{Schema: 1, RecordingID: recording, GatewayID: "g1", State: state}); err != nil {
		t.Fatal(err)
	}
}

func TestRecordingRecoveryAndCapacityEviction(t *testing.T) {
	recordRoot := t.TempDir()
	root := filepath.Join(recordRoot, "g1")
	writeStoredSession(t, root, "old", "ended")
	writeStoredSegment(t, root, "old", "20990101T0000", 4000, false)
	writeStoredSegment(t, root, "old", "20990101T0030", 4000, true)
	writeStoredSegment(t, root, "old", "20990101T0100", 4000, false)
	writeStoredSession(t, root, "crashed", "recording")
	if err := os.MkdirAll(filepath.Join(root, "crashed", "20990102T0000"), 0700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(root, "crashed", "20990102T0000", "index.jsonl"), []byte("{}\n{}\n"), 0600)
	r, err := newTrafficRecorder(recordRoot, "g1", "boot", 9000, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var session recordingSessionFile
	readJSONFile(t, filepath.Join(root, "crashed", "session.json"), &session)
	var segment recordingSegmentFile
	readJSONFile(t, filepath.Join(root, "crashed", "20990102T0000", "segment.json"), &segment)
	if session.State != "ended" || session.EndReason != "end_unknown" || segment.State != "end_unknown" || segment.Recorded != 2 {
		t.Fatalf("recovery %+v %+v", session, segment)
	}
	// Startup does not catch up. An explicit scheduled maintenance pass evicts the oldest
	// unpinned sealed segments and keeps the pinned one.
	r.flush()
	if _, err := os.Stat(filepath.Join(root, "old", "20990101T0000")); err != nil {
		t.Fatal("startup evicted before daily maintenance", err)
	}
	r.maintainStorage()
	if _, err := os.Stat(filepath.Join(root, "old", "20990101T0000")); !os.IsNotExist(err) {
		t.Fatal("oldest segment kept", err)
	}
	if _, err := os.Stat(filepath.Join(root, "old", "20990101T0030")); err != nil {
		t.Fatal("pinned segment evicted", err)
	}
	r.mu.Lock()
	disk, pinned := r.diskBytes, r.pinnedBytes
	r.mu.Unlock()
	if disk > 9000 || pinned == 0 {
		t.Fatal(disk, pinned)
	}
	// Unpin and pin through the worker.
	done := make(chan error, 1)
	if err := r.pinLocked("old", "20990101T0030", false, func(e error) { done <- e }); err != nil || <-done != nil {
		t.Fatal("unpin", err)
	}
	if err := r.pinLocked("old", "20991231T2330", true, func(e error) { done <- e }); err != nil {
		t.Fatal(err)
	} else if e := <-done; e == nil || e.Error() != "segment_not_found" {
		t.Fatal(e)
	}
	// Unpinning a segment that no longer exists has nothing left to release.
	if err := r.pinLocked("old", "20991231T2330", false, func(e error) { done <- e }); err != nil || <-done != nil {
		t.Fatal("unpin missing segment", err)
	}
	// A storage error is not proof that the pin marker is gone.
	if err := os.WriteFile(filepath.Join(r.root, "blocked"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.pinLocked("blocked", "20990101T0030", false, func(e error) { done <- e }); err != nil {
		t.Fatal(err)
	} else if e := <-done; e == nil || e.Error() != "recording_storage_failed" {
		t.Fatal("unpin storage error", e)
	}
	if err := r.pinLocked("old", "20990101T0030", true, func(e error) { done <- e }); err != nil || <-done != nil {
		t.Fatal("pin before restart", err)
	}
	r.close("process_exit")
	// Pinned exports may temporarily exceed the soft capacity.
	r2, err := newTrafficRecorder(recordRoot, "g1", "boot2", 10, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.close("process_exit")
	if err := r2.pinLocked("old", "20990101T0030", true, func(e error) { done <- e }); err != nil || <-done != nil {
		t.Fatal("pin", err)
	}
	r2.flush()
	if _, err := os.Stat(filepath.Join(root, "old", "20990101T0100")); !os.IsNotExist(err) {
		t.Fatal("unpinned segment kept over capacity", err)
	}
	now := time.Now()
	p := recordingParams{RecordingID: "blocked", StartAt: halfHour(now), EndAt: halfHour(now).Add(time.Hour)}
	if err := p.normalize(10, now); err != nil {
		t.Fatal(err)
	}
	if err := r2.startLocked(p, now, func(error) {}); err != nil {
		t.Fatal(err)
	}
}

func TestRecordingDirectoryIsExclusive(t *testing.T) {
	root := t.TempDir()
	r, err := newTrafficRecorder(root, "g1", "b", 1<<20, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close("process_exit")
	if _, err := newTrafficRecorder(root, "g1", "b2", 1<<20, nil, io.Discard); err == nil {
		t.Fatal("second recorder shared the directory")
	}
}

func TestReplayMarkVerification(t *testing.T) {
	value := replayMarkValue(testMarkKey, "run-1", 7)
	if !verifyReplayMark([][]byte{testMarkKey}, value) {
		t.Fatal("valid mark rejected")
	}
	for _, bad := range []string{"", "run-1/7", "run-1/8/" + strings.Split(value, "/")[2], "run 1/7/" + strings.Split(value, "/")[2], value + "0"} {
		if verifyReplayMark([][]byte{testMarkKey}, bad) {
			t.Fatal("accepted", bad)
		}
	}
	if verifyReplayMark(nil, value) || verifyReplayMark([][]byte{[]byte(strings.Repeat("n", 32))}, value) {
		t.Fatal("accepted with wrong key")
	}
}

func TestProxyLabelsOnlyVerifiedReplayTraffic(t *testing.T) {
	router := newTokenRouter(t)
	p, s := splitProxyFixture(t, "r9", router.server.URL, `{"profile":{"enabled":true}}`)
	secondKey := []byte(strings.Repeat("n", 32))
	p.markKeys = [][]byte{testMarkKey, secondKey}
	body := `{"messages":[{"role":"user","content":"hi"}],"stream":true}`
	postChat(t, s.URL, body, map[string]string{headerReplay: replayMarkValue(testMarkKey, "run-9", 1)})
	postChat(t, s.URL, body, map[string]string{headerReplay: replayMarkValue(secondKey, "run-9", 3)})
	postChat(t, s.URL, body, map[string]string{headerReplay: replayMarkValue([]byte(strings.Repeat("u", 32)), "run-9", 4)})
	postChat(t, s.URL, body, map[string]string{headerReplay: "run-9/2/" + strings.Repeat("a", 64)})
	postChat(t, s.URL, body, nil)
	response, err := http.Get(s.URL + "/metrics")
	metrics := splitRead(t, response, err)
	var replay, business string
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, "aigate_requests_routed_total{") {
			if strings.Contains(line, `traffic="replay"`) {
				replay = line
			} else {
				business = line
			}
		}
	}
	if !strings.HasSuffix(replay, "} 2") || !strings.HasSuffix(business, "} 3") {
		t.Fatalf("replay=%q business=%q\n%s", replay, business, metrics)
	}
	if router.posts.Load() != 5 {
		t.Fatal("replay label changed forwarding")
	}
	// Every event of the verified replay, including start, carries the label.
	profileDrain(t, p.g.profile)
	files, _ := filepath.Glob(filepath.Join(p.g.config.Profile.Directory, "events-v2-*.jsonl"))
	labelled := map[string]int{}
	for _, file := range files {
		data, _ := os.ReadFile(file)
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			var e profileEvent
			if json.Unmarshal([]byte(line), &e) == nil && e.Traffic == "replay" {
				labelled[e.Kind]++
			}
		}
	}
	if labelled["start"] != 2 || labelled["input"] != 2 || labelled["end"] != 2 {
		t.Fatalf("replay events %v", labelled)
	}
	p.quiesce()
	for _, key := range p.markKeys {
		if code, _ := postChat(t, s.URL, body, map[string]string{headerReplay: replayMarkValue(key, "run-9", 5)}); code != http.StatusServiceUnavailable {
			t.Fatalf("replay mark bypassed admission: %d", code)
		}
	}
	if router.posts.Load() != 5 {
		t.Fatal("request forwarded after admission closed")
	}
}

func TestRecordingExcludesInvalidValidAndExpiredTestMarkers(t *testing.T) {
	f := newRecordingFixture(t, recordingDefaultMaxBytes)
	now := time.Now()
	startRecording(t, f.c, "rec-markers", halfHour(now), halfHour(now).Add(time.Hour), nil)
	body := `{"model":"token-model","messages":[{"role":"user","content":"test"}]}`
	if status, _ := postChat(t, f.switch_.URL, body, map[string]string{"X-Code-Eval-Token": "invalid"}); status != 409 {
		t.Fatal(status)
	}
	token := strings.Repeat("t", 32)
	payload := map[string]any{"task_id": "task", "token": token, "model": "token-model", "router_id": "r1", "binding_revision": 1}
	commandV3(t, f.c, "test", "test-acquire", payload)
	if status, _ := postChat(t, f.switch_.URL, body, map[string]string{"X-Code-Eval-Token": token}); status != 200 {
		t.Fatal(status)
	}
	commandV3(t, f.c, "test", "test-release", payload)
	if status, _ := postChat(t, f.switch_.URL, body, map[string]string{"X-Code-Eval-Token": token}); status != 409 {
		t.Fatal(status)
	}
	if status, _ := postChat(t, f.switch_.URL, body, nil); status != 200 {
		t.Fatal(status)
	}
	recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": "rec-markers"})
	if summary := waitRecordingEnded(t, f.c, "rec-markers"); summary.Recorded != 1 {
		t.Fatalf("%+v", summary)
	}
}

func TestRecordingCancellationBeforeResponseHeaders(t *testing.T) {
	f := newRecordingFixture(t, recordingDefaultMaxBytes)
	entered := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}))
	defer target.Close()
	// The live switch forwarding path, with a target that has not sent headers.
	f.c.mu.Lock()
	f.c.active.forward.target, _ = url.Parse(target.URL)
	f.c.mu.Unlock()
	now := time.Now()
	startRecording(t, f.c, "rec-cancel-early", halfHour(now), halfHour(now).Add(time.Hour), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", f.switch_.URL+"/v1/chat/completions", strings.NewReader(`{"messages":[]}`))
	done := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(req)
		if response != nil {
			response.Body.Close()
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("target did not receive request")
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("client request was not cancelled")
	}
	recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": "rec-cancel-early"})
	summary := waitRecordingEnded(t, f.c, "rec-cancel-early")
	rows := readIndex(t, filepath.Join(f.root, "g1", "rec-cancel-early", summary.Segments[0].Segment, "index.jsonl"))
	if len(rows) != 1 || rows[0].Outcome != "client_cancelled" {
		t.Fatalf("%+v", rows)
	}
}

func TestRecordingResponseBudgetPreservesRequestAndReportsTruncation(t *testing.T) {
	now := time.Now()
	rec := &trafficRecorder{used: recordingRecordCharge, active: 1, queue: make(chan *recordingSnapshot, 1)}
	session := &recordingSession{params: recordingParams{RecordingID: "budget"}, inflight: 1}
	seg := &recordingSegmentState{start: halfHour(now), name: recordingSegmentName(halfHour(now)), inflight: 1}
	record := &trafficRecord{rec: rec, session: session, segment: seg, arrived: now, method: "POST", charge: recordingRecordCharge}
	body := []byte(`{"messages":[]}`)
	record.bodyChunk(body, true, nil)
	record.forwarding()
	record.header(200, http.Header{"Content-Type": {"application/json"}})
	prefix := []byte(`{"answer":"` + strings.Repeat("x", 10000))
	record.write(now, now, prefix)
	reserved := recordingMemory - rec.used
	if !rec.reserve(reserved) {
		t.Fatal("could not reserve budget")
	}
	suffix := []byte(strings.Repeat("y", 10000) + `","usage":{"completion_tokens":99}}`)
	record.write(now, now, suffix)
	record.finish(nil)
	snapshot := <-rec.queue
	if snapshot.meta.CaptureState != "truncated" || snapshot.meta.FailureReason != "memory_full" || snapshot.meta.RequestState != "complete" || !bytes.Equal(snapshot.body, body) {
		t.Fatalf("lost request or truncation status: %+v", snapshot.meta)
	}
	if !bytes.Equal(snapshot.response, prefix) || snapshot.meta.ResponseBytes != int64(len(prefix)+len(suffix)) || snapshot.meta.ResponseSaved != int64(len(prefix)) || snapshot.meta.ResponseResult.Usage["completion_tokens"] != 99 {
		t.Fatalf("lost prefix, byte counts or observation: %+v", snapshot.meta)
	}
	if rec.used != recordingMemory {
		t.Fatal("budget exceeded")
	}
	rec.releaseLocked(snapshot)
	if rec.used != reserved || rec.active != 0 {
		t.Fatal("capture budget leaked")
	}
}
