package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func controlledFixture(t *testing.T) *ControlledGateway {
	t.Helper()
	c, e := newControlledGateway("g1", strings.Repeat("k", 32), []string{"127.0.0.1/32"}, io.Discard)
	if e != nil {
		t.Fatal(e)
	}
	c.dataRoot = t.TempDir()
	t.Cleanup(c.Close)
	return c
}
func controlRequestV2(t *testing.T, c *ControlledGateway, cmd RuntimeCommand) (int, RuntimeResult) {
	t.Helper()
	r := httptest.NewRequest("POST", "/control/v2/commands", bytes.NewReader(encodeJSON(cmd)))
	r.RemoteAddr = "127.0.0.1:12345"
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("k", 32))
	w := httptest.NewRecorder()
	c.ControlHandler().ServeHTTP(w, r)
	var result RuntimeResult
	_ = json.Unmarshal(w.Body.Bytes(), &result)
	return w.Code, result
}
func commandV2(t *testing.T, c *ControlledGateway, owner, action string, payload any) RuntimeResult {
	t.Helper()
	c.mu.Lock()
	cmd := RuntimeCommand{ProtocolVersion: 2, OperationID: owner, GatewayID: c.id, ExpectedBootID: c.boot, Sequence: c.sequence + 1, ExpectedRevision: c.revisionLocked(), Action: action, Payload: encodeJSON(payload)}
	c.mu.Unlock()
	status, result := controlRequestV2(t, c, cmd)
	if status != 200 || result.Status == "failed" {
		t.Fatalf("%s: status=%d %+v", action, status, result)
	}
	return result
}
func snapshotV2(router, url string, revision uint64) RuntimeSnapshot {
	return RuntimeSnapshot{RouterID: router, RouterVersion: 1, GatewayConfigVersion: 1, BindingRevision: revision,
		Backend: &Backend{Adapter: "openai", URL: url + "/v1/chat/completions", ModelsURL: url + "/v1/models"}, Settings: json.RawMessage(`{"public_model_name":"public"}`)}
}
func activateV2(t *testing.T, c *ControlledGateway, owner string, s RuntimeSnapshot) {
	t.Helper()
	commandV2(t, c, owner, "prepare", s)
	commandV2(t, c, owner, "quiesce", map[string]any{})
	commandV2(t, c, owner, "activate", map[string]any{"admission": true})
}

func TestV2UnconfiguredAndRetiredFailClosed(t *testing.T) {
	c := controlledFixture(t)
	for _, path := range []string{"/v1/models", "/v1/chat/completions"} {
		w := httptest.NewRecorder()
		c.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 503 {
			t.Fatal(w.Code)
		}
	}
	commandV2(t, c, "retire", "retire", map[string]any{})
	status, _ := controlRequestV2(t, c, RuntimeCommand{ProtocolVersion: 2, OperationID: "late", GatewayID: c.id, ExpectedBootID: c.boot, Sequence: 2, Action: "quiesce", Payload: json.RawMessage(`{}`)})
	if status != 409 {
		t.Fatal("retired instance accepted a mutation")
	}
	if e := run([]string{"switch", "single", "r1"}, io.Discard, io.Discard); e == nil {
		t.Fatal("legacy CLI remains enabled")
	}
}

func TestV2CandidateNoDiscoveryAndPassiveFailure(t *testing.T) {
	c := controlledFixture(t)
	p := newProtocolBackend(t)
	activateV2(t, c, "bind", snapshotV2("r1", p.server.URL, 1))
	commandV2(t, c, "swap", "prepare", snapshotV2("r2", "http://127.0.0.1:1", 2))
	if p.gets.Load() != 0 {
		t.Fatal("prepare performed network I/O")
	}
	c.mu.Lock()
	if c.active.snapshot.RouterID != "r1" {
		t.Fatal("prepare changed route")
	}
	c.mu.Unlock()
	commandV2(t, c, "swap", "quiesce", map[string]any{})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(basicBody)))
	if w.Code != 503 {
		t.Fatal("quiesce admitted business")
	}
	commandV2(t, c, "swap", "activate", map[string]any{"admission": true})
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.admission || c.active.snapshot.RouterID != "r2" {
		t.Fatal("unhealthy passive target prevented activation")
	}
}

func TestV2SSEOldSnapshotSurvivesSwap(t *testing.T) {
	c := controlledFixture(t)
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	one.streamGate = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(one.streamGate) }) }
	defer release()
	activateV2(t, c, "bind", snapshotV2("r1", one.server.URL, 1))
	server := httptest.NewServer(c)
	defer server.Close()
	r, e := http.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user","content":"old"}],"stream":true}`))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Body.Close()
	reader := bufio.NewReader(r.Body)
	if _, e = reader.ReadString('\n'); e != nil {
		t.Fatal(e)
	}
	activateV2(t, c, "swap", snapshotV2("r2", two.server.URL, 2))
	w := httptest.NewRecorder()
	c.ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(basicBody)))
	if w.Code != 200 || w.Header().Get("X-Aigate-Backend") != "r2" || strings.Contains(w.Body.String(), `"model":`) {
		t.Fatal(w.Code, w.Body.String())
	}
	c.mu.Lock()
	found := false
	for _, g := range c.generations {
		if g.snapshot.RouterID == "r1" && g.counts.Streams == 1 {
			found = true
		}
	}
	c.mu.Unlock()
	if !found {
		t.Fatal("old SSE runtime disappeared")
	}
	release()
	tail, e := io.ReadAll(reader)
	if e != nil || !strings.Contains(string(tail), "[DONE]") {
		t.Fatal("old SSE did not complete", e, string(tail))
	}
}

func TestV2CommandIdentityAndEpoch(t *testing.T) {
	c := controlledFixture(t)
	cmd := RuntimeCommand{ProtocolVersion: 2, OperationID: "pause", GatewayID: c.id, ExpectedBootID: c.boot, Sequence: 10, Action: "quiesce", Payload: json.RawMessage(`{}`)}
	for i := 0; i < 2; i++ {
		status, result := controlRequestV2(t, c, cmd)
		if status != 200 || result.Status != "completed" {
			t.Fatal(status, result)
		}
	}
	cmd.Action = "retire"
	status, _ := controlRequestV2(t, c, cmd)
	if status != 409 {
		t.Fatal("sequence reused with different payload")
	}
	cmd.Sequence = 9
	status, _ = controlRequestV2(t, c, cmd)
	if status != 409 {
		t.Fatal("late command accepted")
	}
	cmd.Sequence = 11
	cmd.ExpectedBootID = "old-boot"
	status, _ = controlRequestV2(t, c, cmd)
	if status != 409 {
		t.Fatal("wrong boot accepted")
	}
}

func TestV2ValidateUsesRealProtocolAndCannotChangeRoute(t *testing.T) {
	c := controlledFixture(t)
	p := newProtocolBackend(t)
	commandV2(t, c, "verify", "prepare", snapshotV2("r1", p.server.URL, 1))
	result := commandV2(t, c, "verify", "validate", map[string]any{"rounds": 1, "step_seconds": 2, "timeout_seconds": 10, "max_tokens": 1024})
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		r := c.results[result.Sequence]
		status := r.Status
		passed := r.Report != nil && r.Report.Passed
		active := c.active
		c.mu.Unlock()
		if status != "running" {
			if status != "completed" || !passed || active != nil {
				t.Fatal(status, passed)
			}
			return
		}
		time.Sleep(time.Millisecond * 5)
	}
	t.Fatal("validation did not finish")
}

func TestV2TestReleaseTombstone(t *testing.T) {
	c := controlledFixture(t)
	p := newProtocolBackend(t)
	activateV2(t, c, "bind", snapshotV2("r1", p.server.URL, 1))
	token := strings.Repeat("t", 32)
	commandV2(t, c, "test", "test-release", map[string]any{"task_id": "task", "token": token})
	c.mu.Lock()
	seq := c.sequence + 1
	c.mu.Unlock()
	status, result := controlRequestV2(t, c, RuntimeCommand{ProtocolVersion: 2, OperationID: "test", GatewayID: c.id, ExpectedBootID: c.boot, Sequence: seq, ExpectedRevision: 1, Action: "test-acquire", Payload: encodeJSON(map[string]any{"task_id": "task", "token": token, "model": "m", "router_id": "r1", "binding_revision": 1})})
	if status != 200 || result.Error != "test_already_released" {
		t.Fatal(status, result)
	}
}

func TestV2HistoricalAliasCannotChangeBackendResponse(t *testing.T) {
	for _, tc := range []struct {
		name, contentType, body string
		status                  int
		stream                  bool
	}{
		{"json", "application/json", `{ "model":"后端/Real-Model", "choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}] }`, 200, false},
		{"missing", "application/json", `{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`, 200, false},
		{"error", "application/json", `{ "error":"unavailable", "model":"backend-error" }`, 503, false},
		{"stream", "text/event-stream", "data: {\"model\":\"backend-stream\",\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"total_tokens\":8}}\n\ndata: [DONE]\n\n", 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := upstreamServer(t, "discovered-model", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			c := controlledFixture(t)
			s := snapshotV2("r1", u.URL, 1) // Contains a historical public_model_name.
			original := string(s.Settings)
			activateV2(t, c, "restore", s)
			if string(s.Settings) != original || strings.Contains(string(c.active.snapshot.Settings), "public_model_name") {
				t.Fatal("legacy alias must be ignored without mutating the source snapshot")
			}
			w := httptest.NewRecorder()
			c.ServeHTTP(w, httptest.NewRequest("GET", "/v1/models", nil))
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"id":"discovered-model"`) {
				t.Fatalf("model discovery changed: %s", w.Body.String())
			}
			w = httptest.NewRecorder()
			body := encodeJSON(map[string]any{"messages": []any{map[string]string{"role": "user", "content": "hello"}}, "stream": tc.stream})
			c.ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(body)))
			if w.Code != tc.status || w.Body.String() != tc.body {
				t.Fatalf("backend response changed: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

// A drained generation's observers close outside c.mu: business requests keep
// running while its writer finishes, and a prepare that reopens the same
// directory waits for the writer instead of blocking admission.
func TestV2ObserverCloseAndReopenDoNotBlockBusiness(t *testing.T) {
	p := newProtocolBackend(t)
	c := controlledFixture(t)
	slow := func(threshold string, revision uint64) RuntimeSnapshot {
		s := snapshotV2("r1", p.server.URL, revision)
		s.Settings = json.RawMessage(`{"slow_requests":{"enabled":true,"threshold_seconds":` + threshold + `}}`)
		return s
	}
	send := func(owner, action string, payload any) (RuntimeCommand, chan RuntimeResult) {
		c.mu.Lock()
		cmd := RuntimeCommand{ProtocolVersion: 2, OperationID: owner, GatewayID: c.id, ExpectedBootID: c.boot, Sequence: c.sequence + 1, ExpectedRevision: c.revisionLocked(), Action: action, Payload: encodeJSON(payload)}
		c.mu.Unlock()
		done := make(chan RuntimeResult, 1)
		go func() { _, result := controlRequestV2(t, c, cmd); done <- result }()
		return cmd, done
	}
	await := func(done chan RuntimeResult) RuntimeResult {
		select {
		case result := <-done:
			return result
		case <-time.After(5 * time.Second):
			t.Fatal("command blocked")
			return RuntimeResult{}
		}
	}
	business := func() {
		w := httptest.NewRecorder()
		finished := make(chan struct{})
		go func() { c.ServeHTTP(w, httptest.NewRequest("GET", "/v1/models", nil)); close(finished) }()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatal("business request blocked by observer I/O")
		}
		if w.Code != 200 {
			t.Fatalf("business request: %d", w.Code)
		}
	}
	activateV2(t, c, "first", slow("300", 1))
	old := c.active.g.slow
	writing, release := make(chan struct{}), make(chan struct{})
	old.write = func(slowRequestJob) error { close(writing); <-release; return nil }
	old.enqueue(slowRequestJob{meta: slowRequestMeta{RequestID: strings.Repeat("a", 32)}, body: []byte("{}")})
	<-writing
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	// A policy change retires the first writer while it is still writing.
	for _, action := range []string{"prepare", "quiesce", "activate"} {
		payload := any(map[string]any{})
		if action == "prepare" {
			payload = slow("600", 2)
		} else if action == "activate" {
			payload = map[string]any{"admission": true}
		}
		if _, done := send("second", action, payload); await(done).Status != "completed" {
			t.Fatalf("%s failed", action)
		}
	}
	business()
	cmd, prepared := send("third", "prepare", slow("300", 3))
	for deadline := time.Now().Add(5 * time.Second); ; {
		c.mu.Lock()
		result := c.results[cmd.Sequence]
		running := result != nil && result.Status == "running"
		c.mu.Unlock()
		if running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("prepare did not start")
		}
		time.Sleep(time.Millisecond)
	}
	business()
	select {
	case result := <-prepared:
		t.Fatalf("prepare reopened a directory still held by its writer: %+v", result)
	default:
	}
	close(release)
	if result := await(prepared); result.Status != "completed" {
		t.Fatalf("prepare after release: %+v", result)
	}
	c.mu.Lock()
	reopened := c.candidate.g.slow
	c.mu.Unlock()
	if reopened == old || reopened.c.Directory != old.c.Directory {
		t.Fatal("prepare did not reopen the released directory")
	}
}
