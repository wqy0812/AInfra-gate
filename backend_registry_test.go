package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func backendRequest(m *Management, target string, values map[string]any) BackendWriteRequest {
	fields := map[string]json.RawMessage{}
	for name, value := range values {
		fields[name] = encodeJSON(value)
	}
	return BackendWriteRequest{OperationID: randomID(), ExpectedRevision: m.status().Revision, Target: target, Backend: fields}
}

func writeBackendOK(t *testing.T, m *Management, create bool, req BackendWriteRequest) *Operation {
	t.Helper()
	op, err := m.writeBackend(create, req)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func TestOnlineBackendsAPIAndNewRoute(t *testing.T) {
	one, two, three := newProtocolBackend(t), newProtocolBackend(t), newProtocolBackend(t)
	g, business, logs := serveTest(t, managedConfig(t, one, two))
	admin := httptest.NewServer(g.adminHandler())
	defer admin.Close()
	m := g.management
	req := backendRequest(m, "api3", map[string]any{"url": three.server.URL + "/infer", "models_url": three.server.URL + "/v1/models"})
	secret := "Bearer online-private-secret"
	req.Credentials = map[string]HeaderChange{"Authorization": {Action: "replace", Value: &secret}}
	call := func(method, path string, body any) (int, []byte) {
		t.Helper()
		text := ""
		if body != nil {
			text = string(encodeJSON(body))
		}
		r, data := request(t, admin, method, path, text, "Bearer admin-secret")
		return r.StatusCode, data
	}
	if status, data := call("POST", "/admin/v1/backends", req); status != 201 {
		t.Fatalf("create: %d %s", status, data)
	}
	if one.posts.Load()+two.posts.Load()+three.posts.Load() != 0 {
		t.Fatal("saving sent generation traffic")
	}
	if m.status().ActiveBackend != "api1" {
		t.Fatal("save changed route")
	}
	for _, path := range []string{"/admin/v1/backends", "/admin/v1/status", "/admin/v1/operations/" + req.OperationID} {
		status, data := call("GET", path, nil)
		if status != 200 || bytes.Contains(data, []byte(secret)) || bytes.Contains(data, []byte("fingerprint")) || bytes.Contains(data, []byte("registry")) {
			t.Fatalf("unsafe response: %s %d", path, status)
		}
	}
	if status, _ := call("POST", "/admin/v1/backends", req); status != 201 {
		t.Fatal("retry failed")
	}
	req.Backend["preferred_model"] = encodeJSON("different")
	if status, _ := call("POST", "/admin/v1/backends", req); status != 409 {
		t.Fatal("idempotency conflict not rejected")
	}
	op := waitOp(t, m, submitOp(t, m, "switch", "api3").ID)
	if op.Outcome != "completed" {
		t.Fatalf("new backend switch: %s", encodeJSON(op))
	}
	before := three.posts.Load()
	r, _ := request(t, business, "POST", "/v1/chat/completions", basicBody, "")
	if r.StatusCode != 200 || three.posts.Load() != before+1 {
		t.Fatal("new backend did not serve business")
	}
	if strings.Contains(logs.String(), secret) {
		t.Fatal("secret logged")
	}
	// The same authentication, CSRF and strict body guards apply to PATCH.
	patch := backendRequest(m, "api2", map[string]any{"preferred_model": "local-protocol-model"})
	if status, _ := call("PATCH", "/admin/v1/backends/api2", patch); status != 200 {
		t.Fatal("patch failed")
	}
	r, _ = request(t, admin, "GET", "/admin/v1/backends", "", "")
	if r.StatusCode != 401 {
		t.Fatal("anonymous registry access")
	}
	if status, _ := call("PATCH", "/admin/v1/backends/api1", patch); status != 400 {
		t.Fatal("path mismatch accepted")
	}
	if status, _ := call("POST", "/admin/v1/backends", map[string]any{"unknown": 1}); status != 400 {
		t.Fatal("unknown envelope accepted")
	}
	r, _ = request(t, admin, "GET", "/backends.js", "", "")
	if r.StatusCode != 200 {
		t.Fatal("editor asset missing")
	}
}

func TestOnlineBackendEditUsesNewCredentialsAndPayload(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	target := &protocolBackend{}
	var verified atomic.Int64
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer new-key" || r.Header.Get("X-Env") != "Token env-value" {
			w.WriteHeader(401)
			return
		}
		if r.Method == "POST" {
			data, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(data))
			var b map[string]any
			_ = json.Unmarshal(data, &b)
			if b["temperature"] != 0.25 {
				w.WriteHeader(400)
				return
			}
			verified.Add(1)
		}
		target.serve(w, r)
	}))
	defer downstream.Close()
	t.Setenv("AIGATE_ONLINE_HEADER", "env-value")
	g, business, _ := serveTest(t, managedConfig(t, one, two))
	m := g.management
	// Populate a cache that must not survive an endpoint edit.
	if _, err := g.pool.discover(context.Background(), "api2"); err != nil {
		t.Fatal(err)
	}
	oldRuntime := g.pool.snapshot("api2")
	m.mu.Lock()
	m.state.Backends["api2"].LastWarmup = &WarmupReport{Passed: true, Fresh: true}
	m.state.RollbackCandidate = "api2"
	m.state.RollbackArmed = true
	m.mu.Unlock()
	req := backendRequest(m, "api2", map[string]any{"url": downstream.URL + "/infer", "models_url": downstream.URL + "/v1/models", "overrides": map[string]any{"temperature": 0.25}})
	secret := "Bearer new-key"
	req.Credentials = map[string]HeaderChange{"Authorization": {Action: "replace", Value: &secret}, "X-Env": {Action: "replace", Env: "AIGATE_ONLINE_HEADER", Prefix: "Token "}}
	writeBackendOK(t, m, false, req)
	status := m.status()
	if status.Backends["api2"].LastWarmup != nil || status.RollbackArmed || status.RollbackCandidate != "" || g.pool.snapshot("api2") == oldRuntime {
		t.Fatal("stale validation or runtime retained")
	}
	req = backendRequest(m, "api2", map[string]any{"preferred_model": "local-protocol-model"})
	writeBackendOK(t, m, false, req) // Omitted credentials preserve the actual secret.
	if g.pool.snapshot("api2").backend.headers.Get("Authorization") != secret {
		t.Fatal("omitted secret erased")
	}
	if waitOp(t, m, submitOp(t, m, "switch", "api2").ID).Outcome != "completed" {
		t.Fatal("edited backend failed warmup")
	}
	before := verified.Load()
	r, _ := request(t, business, "POST", "/v1/chat/completions", basicBody, "")
	if r.StatusCode != 200 || verified.Load() != before+1 {
		t.Fatal("new credentials/overrides not used")
	}
	if two.posts.Load() != 0 {
		t.Fatal("old endpoint used after update")
	}
}

func TestOnlineBackendDrainAndLongStream(t *testing.T) {
	one, two, three := newProtocolBackend(t), newProtocolBackend(t), newProtocolBackend(t)
	one.streamGate = make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(one.streamGate) })
	g, business, _ := serveTest(t, managedConfig(t, one, two))
	m := g.management
	response, err := http.Post(business.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	if _, err = reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	_, problem := m.writeBackend(false, backendRequest(m, "api1", map[string]any{}))
	if problem == nil || problem.Code != "backend_not_drained" {
		t.Fatal("active edit allowed")
	}
	writeBackendOK(t, m, true, backendRequest(m, "api3", map[string]any{"url": three.server.URL + "/infer", "models_url": three.server.URL + "/v1/models"}))
	writeBackendOK(t, m, false, backendRequest(m, "api2", map[string]any{"preferred_model": "local-protocol-model"}))
	op := waitOp(t, m, submitOp(t, m, "switch", "api3").ID)
	if op.Outcome != "drain_timeout" {
		t.Fatalf("old stream not retained: %s", op.Outcome)
	}
	_, problem = m.writeBackend(false, backendRequest(m, "api1", map[string]any{}))
	if problem == nil || problem.Code != "backend_not_drained" {
		t.Fatal("draining backend edit allowed")
	}
	once.Do(func() { close(one.streamGate) })
	rest, err := io.ReadAll(reader)
	if err != nil || !bytes.Contains(rest, []byte("[DONE]")) {
		t.Fatal("old stream interrupted")
	}
	waitState(t, m, func(s ManagementStatus) bool { return s.Counts["api1"].Business == 0 })
	writeBackendOK(t, m, false, backendRequest(m, "api1", map[string]any{"preferred_model": "local-protocol-model"}))
	if g.config.Backends["api3"] != nil {
		t.Fatal("bootstrap config map mutated")
	}
}

func TestOnlineBackendConflictsAndDiskFailure(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	g, _, _ := serveTest(t, managedConfig(t, one, two))
	m := g.management
	for _, reason := range []string{"busy", "unknown", "probe", "business"} {
		m.mu.Lock()
		switch reason {
		case "busy":
			m.busy = "other-op"
		case "unknown":
			m.state.Backends["api2"].DrainUnknown = true
		case "probe":
			m.counts["api2"].Probes = 1
		case "business":
			m.counts["api2"].Business = 1
		}
		m.mu.Unlock()
		_, err := m.writeBackend(false, backendRequest(m, "api2", nil))
		if err == nil || err.Status != 409 {
			t.Fatalf("%s edit accepted", reason)
		}
		m.mu.Lock()
		m.busy = ""
		m.state.Backends["api2"].DrainUnknown = false
		m.counts["api2"].Probes = 0
		m.counts["api2"].Business = 0
		m.mu.Unlock()
	}
	req := backendRequest(m, "api2", map[string]any{"preferred_model": "new-model"})
	before := g.pool.snapshot("api2")
	rev := m.status().Revision
	m.mu.Lock()
	persist := m.persist
	m.persist = func(ManagedState) error { return fmt.Errorf("disk full") }
	m.mu.Unlock()
	_, problem := m.writeBackend(false, req)
	if problem == nil || problem.Code != "state_write_failed" || g.pool.snapshot("api2") != before || m.status().Revision != rev {
		t.Fatal("failed write published configuration")
	}
	m.mu.Lock()
	m.persist = persist
	m.mu.Unlock()
	writeBackendOK(t, m, false, req)
	stale := backendRequest(m, "api2", nil)
	stale.ExpectedRevision = rev
	if _, e := m.writeBackend(false, stale); e == nil || e.Code != "stale_revision" {
		t.Fatal("stale update accepted")
	}
	// Maintenance is retained while a drained backend is edited.
	submitOp(t, m, "maintenance", "api2")
	writeBackendOK(t, m, false, backendRequest(m, "api2", nil))
	if !m.status().Backends["api2"].Maintenance {
		t.Fatal("edit unlocked maintenance")
	}
}

func TestOnlineBackendInvalidCandidates(t *testing.T) {
	g, _, _ := serveTest(t, managedConfig(t, newProtocolBackend(t), newProtocolBackend(t)))
	m := g.management
	tests := []map[string]any{{"url": "file:///tmp/private"}, {"url": "http://user:secret@host/infer"}, {"models_url": ""}, {"adapter": "template"}, {"headers": map[string]string{"Authorization": "secret"}}, {"overrides": []int{1}}, {"compatibility": map[string]any{"drop_fields": []string{"model"}}}, {"compatibility": map[string]any{"unexpected": true}}}
	for _, fields := range tests {
		if _, e := m.writeBackend(false, backendRequest(m, "api2", fields)); e == nil {
			t.Fatalf("invalid candidate accepted: %v", fields)
		}
	}
	req := backendRequest(m, "api2", nil)
	req.Credentials = map[string]HeaderChange{"Authorization": {Action: "replace", Env: "AIGATE_MISSING_ENV_FOR_TEST"}}
	t.Setenv("AIGATE_MISSING_ENV_FOR_TEST", "")
	if _, e := m.writeBackend(false, req); e == nil {
		t.Fatal("missing env accepted")
	}
	secret := "bad\r\nheader"
	req.OperationID = randomID()
	req.Credentials = map[string]HeaderChange{"Authorization": {Action: "replace", Value: &secret}}
	if _, e := m.writeBackend(false, req); e == nil {
		t.Fatal("header injection accepted")
	}
}

func TestOnlineBackendMigrationAndRestart(t *testing.T) {
	one, two, three := newProtocolBackend(t), newProtocolBackend(t), newProtocolBackend(t)
	c := managedConfig(t, one, two)
	legacy, err := readManagedState(c)
	if err != nil {
		t.Fatal(err)
	}
	legacy.Backends["api2"].Maintenance = true
	original := encodeJSON(legacy)
	if err = os.WriteFile(c.Management.StateFile, original, 0644); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.json")
	os.WriteFile(configPath, encodeJSON(c), 0600)
	if err = run([]string{"check", "--config", configPath}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(c.Management.StateFile)
	if !bytes.Equal(after, original) {
		t.Fatal("check migrated state")
	}
	g, err := newGateway(c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	backups, _ := filepath.Glob(c.Management.StateFile + ".v1-backup-*")
	if len(backups) != 1 {
		g.Close()
		t.Fatal("missing migration backup")
	}
	backup, _ := os.ReadFile(backups[0])
	info, _ := os.Stat(backups[0])
	if !bytes.Equal(backup, original) || info.Mode().Perm() != 0600 {
		g.Close()
		t.Fatal("unsafe backup")
	}
	req := backendRequest(g.management, "api3", map[string]any{"url": three.server.URL + "/infer", "models_url": three.server.URL + "/v1/models"})
	secret := "persisted-secret"
	req.Credentials = map[string]HeaderChange{"X-Secret": {Action: "replace", Value: &secret}}
	writeBackendOK(t, g.management, true, req)
	if waitOp(t, g.management, submitOp(t, g.management, "switch", "api3").ID).Outcome != "completed" {
		g.Close()
		t.Fatal("switch failed")
	}
	g.Close()
	// Stale bootstrap credentials and route must not override the online registry.
	c.Backends["api1"].HeadersEnv = map[string]string{"Authorization": "AIGATE_REMOVED_BOOTSTRAP_SECRET"}
	t.Setenv("AIGATE_REMOVED_BOOTSTRAP_SECRET", "")
	os.WriteFile(configPath, encodeJSON(c), 0600)
	if err = run([]string{"check", "--config", configPath}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	g, err = newGateway(c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if g.management.status().ActiveBackend != "api3" || !g.management.status().Backends["api2"].Maintenance || g.pool.snapshot("api3").backend.headers.Get("X-Secret") != secret {
		t.Fatal("online config not restored")
	}
	if _, e := g.management.writeBackend(true, req); e != nil {
		t.Fatal("retry did not survive restart", e)
	}
	info, _ = os.Stat(c.Management.StateFile)
	if info.Mode().Perm() != 0600 {
		t.Fatal("state not private")
	}
}

func TestOnlineBackendConcurrentRegistryAndSwitch(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	g, business, _ := serveTest(t, managedConfig(t, one, two))
	m := g.management
	var workers sync.WaitGroup
	failures := make(chan error, 16)
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 12; j++ {
				_ = m.backendList()
				_ = m.status()
				r, e := http.Post(business.URL+"/v1/chat/completions", "application/json", strings.NewReader(basicBody))
				if e != nil {
					failures <- e
					return
				}
				io.Copy(io.Discard, r.Body)
				r.Body.Close()
				if r.StatusCode != 200 {
					failures <- fmt.Errorf("request %d", r.StatusCode)
					return
				}
			}
		}()
	}
	for i := 0; i < 12; i++ {
		writeBackendOK(t, m, true, backendRequest(m, fmt.Sprintf("extra%d", i), map[string]any{"url": two.server.URL + "/infer", "models_url": two.server.URL + "/v1/models"}))
		writeBackendOK(t, m, false, backendRequest(m, "api2", map[string]any{"preferred_model": "local-protocol-model"}))
	}
	if waitOp(t, m, submitOp(t, m, "switch", "extra11").ID).Outcome != "completed" {
		t.Fatal("concurrent switch failed")
	}
	workers.Wait()
	close(failures)
	for e := range failures {
		t.Error(e)
	}
}

func TestOnlineBackendCredentialActions(t *testing.T) {
	secret := "Bearer private"
	old := &Backend{Adapter: "openai", Headers: map[string]string{"authorization": secret, "X-Delete": "remove"}, HeadersEnv: map[string]string{"X-Env": "OLD_ENV"}, HeaderPrefixes: map[string]string{"X-Env": "Token "}}
	req := BackendWriteRequest{Backend: map[string]json.RawMessage{}, Credentials: map[string]HeaderChange{"Authorization": {Action: "keep"}, "x-delete": {Action: "delete"}, "X-Env": {Action: "replace", Value: &secret}}}
	b, err := editedBackend(old, req)
	if err != nil {
		t.Fatal(err)
	}
	if b.Headers["Authorization"] != secret || b.Headers["X-Env"] != secret || b.HeadersEnv["X-Env"] != "" || b.HeaderPrefixes["X-Env"] != "" {
		t.Fatal("credential source replacement failed")
	}
	if _, ok := b.Headers["X-Delete"]; ok {
		t.Fatal("delete retained secret")
	}
	if old.Headers["X-Delete"] != "remove" || old.HeadersEnv["X-Env"] != "OLD_ENV" {
		t.Fatal("old config mutated")
	}
}

func TestOnlineBackendSameRevisionOnlyOneWriter(t *testing.T) {
	g, _, _ := serveTest(t, managedConfig(t, newProtocolBackend(t), newProtocolBackend(t)))
	m := g.management
	reqs := []BackendWriteRequest{backendRequest(m, "api2", map[string]any{"preferred_model": "first"}), backendRequest(m, "api2", map[string]any{"preferred_model": "second"})}
	results := make(chan *ManagementError, 2)
	for _, req := range reqs {
		go func(req BackendWriteRequest) { _, err := m.writeBackend(false, req); results <- err }(req)
	}
	successes, conflicts := 0, 0
	for range reqs {
		err := <-results
		if err == nil {
			successes++
		} else if err.Code == "stale_revision" {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatal("concurrent writes overwrote each other")
	}
}
