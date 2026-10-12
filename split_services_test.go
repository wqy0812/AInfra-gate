package aigate

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func splitProxyFixture(t *testing.T, id, upstream, settings string) (*ProxyGateway, *httptest.Server) {
	t.Helper()
	p, err := newProxyGateway(ProxyConfig{ServiceID: "proxy-" + id, RouterID: id, Backend: &Backend{Adapter: "openai", URL: upstream + "/v1/chat/completions", ModelsURL: upstream + "/v1/models"}, Settings: json.RawMessage(settings)}, t.TempDir(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(p)
	t.Cleanup(func() { s.Close(); p.Close() })
	return p, s
}
func splitSwitchFixture(t *testing.T, id string) (*ControlledGateway, *httptest.Server) {
	t.Helper()
	c, err := newSwitchGateway(id, strings.Repeat("k", 32), []string{"127.0.0.1/32"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(c)
	t.Cleanup(func() { s.Close(); c.Close() })
	return c, s
}
func splitControlCommand(t *testing.T, c *ControlledGateway, owner, action string, payload any, version int) RuntimeResult {
	t.Helper()
	c.mu.Lock()
	cmd := RuntimeCommand{ProtocolVersion: version, OperationID: owner, GatewayID: c.id, ExpectedBootID: c.boot, Sequence: c.sequence + 1, ExpectedRevision: c.revisionLocked(), Action: action, Payload: encodeJSON(payload)}
	c.mu.Unlock()
	r := httptest.NewRequest("POST", fmt.Sprintf("/control/v%d/commands", version), bytes.NewReader(encodeJSON(cmd)))
	r.RemoteAddr = "127.0.0.1:321"
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("k", 32))
	w := httptest.NewRecorder()
	c.ControlHandler().ServeHTTP(w, r)
	var result RuntimeResult
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Status == "failed" {
		t.Fatalf("%s: %d %s", action, w.Code, w.Body.String())
	}
	return result
}
func splitActivate(t *testing.T, c *ControlledGateway, p *ProxyGateway, target, owner string, revision uint64) {
	t.Helper()
	splitControlCommand(t, c, owner, "prepare", SwitchSnapshot{RouterID: p.router, RouterVersion: 1, GatewayConfigVersion: 1, BindingRevision: revision, TargetURL: target, ConfigDigest: p.digest}, 3)
	splitControlCommand(t, c, owner, "quiesce", map[string]any{}, 3)
	splitControlCommand(t, c, owner, "activate", map[string]any{"admission": true}, 3)
}
func splitRead(t *testing.T, response *http.Response, err error) string {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, e := io.ReadAll(response.Body)
	if e != nil {
		t.Fatal(e)
	}
	return string(data)
}

func TestSplitStandaloneUnauthenticatedConfigurationAndStatus(t *testing.T) {
	u := newProtocolBackend(t)
	p, s := splitProxyFixture(t, "r1", u.server.URL, `{"profile":{"enabled":true}}`)
	for _, path := range []string{"/health", "/v1/models", "/runtime/v1/status", "/metrics", "/profile/v1/status"} {
		response, err := http.Get(s.URL + path)
		body := splitRead(t, response, err)
		if response.StatusCode != 200 {
			t.Fatalf("%s: %d %s", path, response.StatusCode, body)
		}
	}
	response, err := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user","content":"hi"}],"stream":true}`))
	if body := splitRead(t, response, err); !strings.Contains(body, "[DONE]") {
		t.Fatal(body)
	}
	response, err = http.Get(s.URL + "/control/v3/status")
	body := splitRead(t, response, err)
	if response.StatusCode != 404 {
		t.Fatal(body)
	}
	p.mu.Lock()
	n := p.counts.Business
	p.mu.Unlock()
	if n != 0 {
		t.Fatal("leaked business count")
	}
}

func TestSplitCheckConfigNoIOAndRedactsHeadersAndQuery(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "proxy.json")
	root := filepath.Join(dir, "no-data")
	cfg := ProxyConfig{ServiceID: "p", RouterID: "r", Backend: &Backend{Adapter: "openai", URL: "http://unreachable.invalid/v1/chat/completions?api_key=SECRET_QUERY", ModelsURL: "http://unreachable.invalid/v1/models?token=SECRET_MODELS", Headers: map[string]string{"Authorization": "Bearer SECRET_HEADER"}}, Settings: json.RawMessage(`{"profile":{"enabled":true}}`)}
	if err := os.WriteFile(config, encodeJSON(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := RunProxy([]string{"--check-config", "--config", config, "--data-root", root}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "SECRET") || strings.Contains(out.String(), "api_key") {
		t.Fatal("status leaked secrets", out.String())
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("check-config created storage", err)
	}
	var checked map[string]any
	if json.Unmarshal(out.Bytes(), &checked) != nil {
		t.Fatal(out.String())
	}
	p, err := newProxyGateway(cfg, root, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if checked["config_digest"] != p.digest {
		t.Fatal("check and runtime digests differ")
	}
	for _, settings := range []string{`{"profile":{"api_key":""}}`, `{"profile":{"api_key_env":""}}`, `{"profile":{"listen":""}}`, `{"slow_requests":{"directory":""}}`, `{"timeout_seconds":-1}`, `{"unknown":true}`} {
		cfg.Settings = json.RawMessage(settings)
		if _, err := prepareProxyGateway(cfg, root, io.Discard, true); err == nil {
			t.Fatal("invalid config accepted", settings)
		}
	}
}

func TestSplitSharedTargetsAdaptExactlyOnceAndObserveOnce(t *testing.T) {
	var posts atomic.Int32
	u := upstreamServer(t, "actual-model", func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if json.NewDecoder(r.Body).Decode(&payload) != nil {
			t.Error("invalid payload")
		}
		if payload["model"] != "actual-model" {
			t.Error(payload)
		}
		metadata := payload["metadata"].(map[string]any)
		if metadata["workspace"] != `{"name":"work"}` {
			t.Error("double adaptation", metadata)
		}
		posts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, goodResponse)
	})
	cfg := ProxyConfig{ServiceID: "p", RouterID: "r1", Backend: &Backend{Adapter: "openai", URL: u.URL + "/v1/chat/completions", ModelsURL: u.URL + "/v1/models", Compatibility: Compatibility{MetadataValues: "workspace_object_json_string"}}, Settings: json.RawMessage(`{"profile":{"enabled":true}}`)}
	p, err := newProxyGateway(cfg, t.TempDir(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ps := httptest.NewServer(p)
	defer ps.Close()
	c1, s1 := splitSwitchFixture(t, "g1")
	c2, s2 := splitSwitchFixture(t, "g2")
	c3, s3 := splitSwitchFixture(t, "g3")
	for i, c := range []*ControlledGateway{c1, c2, c3} {
		splitActivate(t, c, p, ps.URL, fmt.Sprint("bind", i), 1)
	}
	for _, base := range []string{s1.URL, s2.URL, s3.URL, ps.URL} {
		response, err := http.Post(base+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"alias","messages":[{"role":"user"}],"metadata":{"workspace":{"name":"work"}}}`))
		body := splitRead(t, response, err)
		if response.StatusCode != 200 {
			t.Fatal(response.StatusCode, body)
		}
	}
	if posts.Load() != 4 {
		t.Fatal("request replayed", posts.Load())
	}
	response, err := http.Get(ps.URL + "/metrics")
	body := splitRead(t, response, err)
	total := 0.0
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "aigate_generation_requests_ended_total{") {
			fields := strings.Fields(line)
			var value float64
			fmt.Sscan(fields[len(fields)-1], &value)
			total += value
		}
	}
	if total != 4 {
		t.Fatalf("observations duplicated/missing: %v\n%s", total, body)
	}
	splitControlCommand(t, c1, "maintenance", "quiesce", map[string]any{}, 3)
	response, err = http.Get(s1.URL + "/v1/models")
	splitRead(t, response, err)
	if response.StatusCode != 503 {
		t.Fatal("paused entry admitted")
	}
	response, err = http.Get(ps.URL + "/v1/models")
	splitRead(t, response, err)
	if response.StatusCode != 200 {
		t.Fatal("entry maintenance stopped direct proxy")
	}
}

func TestSplitUploadAndSSEStayOnAdmittedSnapshot(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	p1, s1 := splitProxyFixture(t, "r1", one.server.URL, `{}`)
	p2, s2 := splitProxyFixture(t, "r2", two.server.URL, `{}`)
	c, s := splitSwitchFixture(t, "g1")
	splitActivate(t, c, p1, s1.URL, "bind", 1)
	reader, writer := io.Pipe()
	req, _ := http.NewRequest("POST", s.URL+"/v1/chat/completions", reader)
	req.Header.Set("Content-Type", "application/json")
	done := make(chan *http.Response, 1)
	fail := make(chan error, 1)
	go func() {
		response, err := s.Client().Do(req)
		if err != nil {
			fail <- err
		} else {
			done <- response
		}
	}()
	if _, err := writer.Write([]byte(`{"messages":[{"role":"user","content":"`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		c.mu.Lock()
		n := c.active.counts.Business
		c.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("upload never admitted")
		}
		time.Sleep(time.Millisecond)
	}
	splitActivate(t, c, p2, s2.URL, "switch", 2)
	io.WriteString(writer, `old"}]}`)
	writer.Close()
	select {
	case response := <-done:
		body := splitRead(t, response, nil)
		if response.StatusCode != 200 || response.Header.Get("X-Aigate-Backend") != "r1" {
			t.Fatal(response.StatusCode, body)
		}
	case err := <-fail:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("old upload stuck")
	}
	if one.posts.Load() != 1 || two.posts.Load() != 0 {
		t.Fatal("upload switched target")
	}
	two.streamGate = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(two.streamGate) }) }
	defer release()
	response, err := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user"}],"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	br := bufio.NewReader(response.Body)
	if _, err = br.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	splitActivate(t, c, p1, s1.URL, "switch-back", 3)
	release()
	tail, err := io.ReadAll(br)
	if err != nil || !strings.Contains(string(tail), "[DONE]") {
		t.Fatal("SSE truncated", err, string(tail))
	}
}

func TestSplitControlAuthenticationLegacyAndConfigFences(t *testing.T) {
	u := newProtocolBackend(t)
	p, ps := splitProxyFixture(t, "r1", u.server.URL, `{}`)
	c, s := splitSwitchFixture(t, "g1")
	splitActivate(t, c, p, ps.URL, "bind", 1)
	for _, version := range []int{2, 3} {
		r := httptest.NewRequest("GET", fmt.Sprintf("/control/v%d/status", version), nil)
		r.RemoteAddr = "127.0.0.1:1"
		w := httptest.NewRecorder()
		c.ControlHandler().ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatal("missing key accepted")
		}
		r.Header.Set("Authorization", "Bearer "+strings.Repeat("k", 32))
		r.RemoteAddr = "192.0.2.1:1"
		w = httptest.NewRecorder()
		c.ControlHandler().ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatal("foreign source accepted")
		}
	}
	r := httptest.NewRequest("POST", "/control/v2/commands", strings.NewReader(`{"protocol_version":2,"action":"prepare"}`))
	r.RemoteAddr = "127.0.0.1:1"
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("k", 32))
	w := httptest.NewRecorder()
	c.ControlHandler().ServeHTTP(w, r)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "legacy_command_removed") {
		t.Fatal(w.Code, w.Body.String())
	}
	token := strings.Repeat("t", 32)
	splitControlCommand(t, c, "test", "test-acquire", map[string]any{"task_id": "task", "token": token, "router_id": "r1", "binding_revision": 1, "model": "wrong-model"}, 2)
	req, _ := http.NewRequest("POST", s.URL+"/v1/chat/completions", strings.NewReader(basicBody))
	req.Header.Set("X-Code-Eval-Token", token)
	response, err := s.Client().Do(req)
	body := splitRead(t, response, err)
	if response.StatusCode != 409 || !strings.Contains(body, "test_model_changed") || u.posts.Load() != 0 {
		t.Fatal("expected model fence failed", response.StatusCode, body)
	}
	splitControlCommand(t, c, "test", "test-release", map[string]any{"task_id": "task", "token": token}, 2)
	req, _ = http.NewRequest("POST", s.URL+"/v1/chat/completions", strings.NewReader(basicBody))
	req.Header.Set(headerExpectedConfig, strings.Repeat("0", 64))
	req.Header.Set(headerExpectedModel, "bad")
	response, err = s.Client().Do(req)
	body = splitRead(t, response, err)
	if response.StatusCode != 200 {
		t.Fatal("untrusted metadata was not overwritten", body)
	}
	req, _ = http.NewRequest("POST", ps.URL+"/v1/chat/completions", strings.NewReader(basicBody))
	req.Header.Set(headerExpectedConfig, strings.Repeat("f", 64))
	response, err = ps.Client().Do(req)
	body = splitRead(t, response, err)
	if response.StatusCode != 409 {
		t.Fatal("config mismatch forwarded", body)
	}
}

func TestSplitCancellationAndForwardDeadline(t *testing.T) {
	cancelled := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	u := upstreamServer(t, "m", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
			cancelled <- struct{}{}
		case <-release:
		}
	})
	p, ps := splitProxyFixture(t, "r1", u.URL, `{}`)
	c, s := splitSwitchFixture(t, "g1")
	c.forwardTimeout = 150 * time.Millisecond
	splitActivate(t, c, p, ps.URL, "bind", 1)
	started := time.Now()
	response, err := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(basicBody))
	if err == nil {
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("forwarding was unbounded")
	}
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not reach Router")
	}
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		n := c.active.refs
		c.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("request remained in flight")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSplitNonReadingClientReleasesLeaseAtDeadline(t *testing.T) {
	u := upstreamServer(t, "m", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		chunk := []byte("data: " + strings.Repeat("x", 32*1024) + "\n\n")
		for {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if r.Context().Err() != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})
	p, ps := splitProxyFixture(t, "r1", u.URL, `{}`)
	c, s := splitSwitchFixture(t, "g1")
	c.forwardTimeout = 250 * time.Millisecond
	splitActivate(t, c, p, ps.URL, "bind", 1)
	splitControlCommand(t, c, "test", "test-acquire", map[string]any{"task_id": "task", "token": strings.Repeat("t", 32), "router_id": "r1", "binding_revision": 1, "model": "m"}, 3)
	conn, err := net.Dial("tcp", strings.TrimPrefix(s.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetReadBuffer(1024)
	}
	body := `{"messages":[{"role":"user"}],"stream":true}`
	fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: fixture\r\nContent-Type: application/json\r\nX-Code-Eval-Token: %s\r\nContent-Length: %d\r\n\r\n%s", strings.Repeat("t", 32), len(body), body)
	deadline := time.Now().Add(3 * time.Second)
	seen := false
	for {
		c.mu.Lock()
		n := c.active.refs
		inflight := c.test.Inflight
		c.mu.Unlock()
		if n > 0 {
			seen = true
		}
		if seen && n == 0 && inflight == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("non-reading client pinned lease", seen, n, inflight)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSplitValidationTraversesProxyAndChecksIdentity(t *testing.T) {
	u := newProtocolBackend(t)
	p, ps := splitProxyFixture(t, "r1", u.server.URL, `{}`)
	c, _ := splitSwitchFixture(t, "g1")
	splitControlCommand(t, c, "validate", "prepare", SwitchSnapshot{RouterID: "r1", RouterVersion: 1, GatewayConfigVersion: 1, BindingRevision: 1, TargetURL: ps.URL, ConfigDigest: p.digest}, 3)
	result := splitControlCommand(t, c, "validate", "validate", map[string]any{"rounds": 1, "step_seconds": 2, "timeout_seconds": 10}, 3)
	deadline := time.Now().Add(12 * time.Second)
	for {
		c.mu.Lock()
		r := c.results[result.Sequence]
		state := r.Status
		pass := r.Report != nil && r.Report.Passed
		c.mu.Unlock()
		if state != "running" {
			if state != "completed" || !pass {
				t.Fatal("validation failed", state)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("validation stuck")
		}
		time.Sleep(time.Millisecond)
	}
	if u.posts.Load() != 4 {
		t.Fatal("protocol probes did not reach Router", u.posts.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.mu.Lock()
	g := c.candidate
	clone := *g
	clone.snapshot.ConfigDigest = strings.Repeat("0", 64)
	c.mu.Unlock()
	if _, err := validateSwitchTarget(ctx, &clone, WarmupPolicy{TimeoutSeconds: 1}); err == nil {
		t.Fatal("wrong config validated")
	}
}
