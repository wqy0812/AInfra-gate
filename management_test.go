package main

import (
	"bufio"
	"bytes"
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
	"time"
)

// Protocol-capable local inference endpoint. Probe requests really traverse HTTP,
// the selected adapter and SSE parser; tests never contact DCU or A3.
type protocolBackend struct {
	server         *httptest.Server
	businessStatus atomic.Int64
	probeBad       atomic.Bool
	posts          atomic.Int64
	gets           atomic.Int64
	probeGate      chan struct{}
	probeStarted   chan struct{}
	streamGate     chan struct{}
	discoveryGate  chan struct{}
	mu             sync.Mutex
	models         []string
}

func newProtocolBackend(t *testing.T) *protocolBackend {
	p := &protocolBackend{}
	p.server = httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.server.Close)
	return p
}

func (p *protocolBackend) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		p.gets.Add(1)
		if p.discoveryGate != nil {
			select {
			case <-p.discoveryGate:
			case <-r.Context().Done():
				return
			}
		}
		io.WriteString(w, `{"data":[{"id":"local-protocol-model"}]}`)
		return
	}
	p.posts.Add(1)
	var b map[string]json.RawMessage
	if json.NewDecoder(r.Body).Decode(&b) != nil {
		w.WriteHeader(400)
		return
	}
	model, _ := rawString(b["model"])
	p.mu.Lock()
	p.models = append(p.models, model)
	p.mu.Unlock()
	var metadata map[string]any
	_ = json.Unmarshal(b["metadata"], &metadata)
	probe := metadata["mode"] == "gateway_warmup"
	if probe && p.probeGate != nil {
		if p.probeStarted != nil {
			select {
			case p.probeStarted <- struct{}{}:
			default:
			}
		}
		select {
		case <-p.probeGate:
		case <-r.Context().Done():
			return
		}
	}
	if probe && p.probeBad.Load() {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
		return
	}
	if !probe && p.businessStatus.Load() != 0 {
		w.WriteHeader(int(p.businessStatus.Load()))
		io.WriteString(w, "private backend failure")
		return
	}
	var messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	_ = json.Unmarshal(b["messages"], &messages)
	content := "hello"
	for _, msg := range messages {
		if msg.Role == "tool" {
			content = msg.Content
		}
	}
	if string(b["stream"]) != "true" {
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": content}, "finish_reason": "stop"}}, "usage": map[string]any{"total_tokens": 8}})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	if len(b["tools"]) > 0 {
		text := messages[0].Content
		start := strings.Index(text, "aigate-echo-")
		marker := strings.SplitN(text[start:], ".", 2)[0]
		args := string(encodeJSON(map[string]any{"value": marker}))
		for i, part := range []string{args[:len(args)/2], args[len(args)/2:]} {
			fn := map[string]any{"arguments": part}
			tool := map[string]any{"index": 0, "function": fn}
			if i == 0 {
				tool["id"] = "call-echo"
				tool["type"] = "function"
				fn["name"] = "aigate_echo"
			}
			fmt.Fprintf(w, "data: %s\n\n", encodeJSON(map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": []any{tool}}}}}))
			w.(http.Flusher).Flush()
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n")
	} else {
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
		w.(http.Flusher).Flush()
		if !probe && p.streamGate != nil {
			select {
			case <-p.streamGate:
			case <-r.Context().Done():
				return
			}
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
	}
	io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"total_tokens\":8}}\n\ndata: [DONE]\n\n")
}

func managedConfig(t *testing.T, backends ...*protocolBackend) *Config {
	t.Helper()
	urls := []string{}
	for _, p := range backends {
		urls = append(urls, p.server.URL)
	}
	c := fixtureConfig(t, urls...)
	c.Routing = Policy{Mode: "single", Backend: "api1"}
	c.Management = ManagementConfig{Enabled: true, Socket: filepath.Join(controlTempDir(t), "control.sock"), StateFile: filepath.Join(t.TempDir(), "state.json"), WarmupRounds: 1, WarmupStepSeconds: 1, WarmupTimeoutSeconds: 5, DrainTimeoutSeconds: .12, FailureWindowSeconds: 5, RollbackRetrySeconds: .03}
	c.Management.defaults(c, filepath.Join(t.TempDir(), "config.json"))
	return c
}

func waitState(t *testing.T, m *Management, predicate func(ManagementStatus) bool) ManagementStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s := m.status()
		if predicate(s) {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	s := m.status()
	t.Fatalf("state condition timed out: active=%s busy=%s alert=%s operations=%s", s.ActiveBackend, s.Busy, s.Alert, encodeJSON(s.Operations))
	return s
}

func submitOp(t *testing.T, m *Management, kind, target string) *Operation {
	t.Helper()
	s := m.status()
	op, err := m.submit(kind, OperationRequest{randomID(), target, s.Revision})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func waitOp(t *testing.T, m *Management, id string) *Operation {
	t.Helper()
	s := waitState(t, m, func(s ManagementStatus) bool {
		op := operationByID(&s.ManagedState, id)
		return op != nil && op.Outcome != "running"
	})
	return operationByID(&s.ManagedState, id)
}

func TestManagedWarmupSwitchDrainMaintenanceRestore(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	one.streamGate = make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(one.streamGate) })
	c := managedConfig(t, one, two)
	g, s, _ := serveTest(t, c)
	m := g.management
	resp, err := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	if _, err = reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	status := m.status()
	if status.Counts["api1"].Business != 1 || status.Counts["api1"].Streams != 1 {
		t.Fatal("stream not counted")
	}
	op := submitOp(t, m, "switch", "api2")
	result := waitOp(t, m, op.ID)
	if result.Outcome != "drain_timeout" || !result.RouteCommitted || result.Report == nil || !result.Report.Passed || len(result.Report.Steps) != 5 {
		t.Fatalf("bad switch result: %s", encodeJSON(result))
	}
	status = m.status()
	if status.ActiveBackend != "api2" || !status.RollbackArmed {
		t.Fatal("route did not commit")
	}
	if _, err := m.submit("maintenance", OperationRequest{randomID(), "api1", status.Revision}); err == nil || err.Code != "backend_not_drained" {
		t.Fatal("maintenance allowed before drain")
	}
	before := one.posts.Load()
	r, _ := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
	if r.StatusCode != 200 || one.posts.Load() != before {
		t.Fatal("new request reached old backend")
	}
	release.Do(func() { close(one.streamGate) })
	rest, err := io.ReadAll(reader)
	if err != nil || !bytes.Contains(rest, []byte("[DONE]")) {
		t.Fatal("switch interrupted old stream")
	}
	waitState(t, m, func(s ManagementStatus) bool {
		return s.Counts["api1"].Business == 0 && operationByID(&s.ManagedState, op.ID).Outcome == "completed"
	})
	maint := submitOp(t, m, "maintenance", "api1")
	if maint.Outcome != "completed" || m.status().RollbackArmed {
		t.Fatal("maintenance did not revoke rollback")
	}
	if _, err := m.acquire("api1", true, false, false); err == nil {
		t.Fatal("probe bypassed maintenance")
	}
	status = m.status()
	if _, err := m.submit("switch", OperationRequest{randomID(), "api1", status.Revision}); err == nil {
		t.Fatal("switched to maintenance backend")
	}
	restored := waitOp(t, m, submitOp(t, m, "restore", "api1").ID)
	if restored.Outcome != "completed" || m.status().Backends["api1"].Maintenance || m.status().ActiveBackend != "api2" {
		t.Fatal("restore changed routing or failed")
	}
	if result := waitOp(t, m, submitOp(t, m, "switch", "api1").ID); result.Outcome != "completed" {
		t.Fatal(result.Error)
	}
}

func TestManagedWarmupFailureDoesNotSwitch(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	two.probeBad.Store(true)
	g, _, _ := serveTest(t, managedConfig(t, one, two))
	op := waitOp(t, g.management, submitOp(t, g.management, "switch", "api2").ID)
	if op.Outcome != "failed" || op.RouteCommitted || g.management.status().ActiveBackend != "api1" {
		t.Fatal("failed protocol warmup changed routing")
	}
	if two.gets.Load() != 1 || two.posts.Load() == 0 {
		t.Fatal("did not verify real generation")
	}
}

func TestManagedDiscoveryReservationAcrossSwitch(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	one.discoveryGate = make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(one.discoveryGate) })
	g, s, _ := serveTest(t, managedConfig(t, one, two))
	done := make(chan int, 1)
	go func() {
		r, err := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(basicBody))
		if err != nil {
			done <- 0
			return
		}
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		done <- r.StatusCode
	}()
	waitState(t, g.management, func(s ManagementStatus) bool { return s.Counts["api1"].Business == 1 })
	result := waitOp(t, g.management, submitOp(t, g.management, "switch", "api2").ID)
	if result.Outcome != "drain_timeout" {
		t.Fatal("discovery waiter was not counted for drain")
	}
	once.Do(func() { close(one.discoveryGate) })
	if <-done != 200 {
		t.Fatal("reserved old request did not complete")
	}
	waitState(t, g.management, func(s ManagementStatus) bool { return s.Counts["api1"].Business == 0 })
}

func TestManagedAutomaticRollbackAndLock(t *testing.T) {
	for _, lock := range []bool{false, true} {
		t.Run(fmt.Sprint(lock), func(t *testing.T) {
			one, two := newProtocolBackend(t), newProtocolBackend(t)
			g, s, _ := serveTest(t, managedConfig(t, one, two))
			m := g.management
			if waitOp(t, m, submitOp(t, m, "switch", "api2").ID).Outcome != "completed" {
				t.Fatal("initial switch failed")
			}
			if lock {
				submitOp(t, m, "maintenance", "api1")
			}
			two.businessStatus.Store(503)
			for i := 0; i < 3; i++ {
				r, _ := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
				if r.StatusCode != 503 {
					t.Fatal(r.StatusCode)
				}
			}
			if lock {
				status := m.status()
				if status.ActiveBackend != "api2" || status.Busy != "" || one.posts.Load() != 0 {
					t.Fatal("maintenance backend was used for rollback")
				}
				return
			}
			status := waitState(t, m, func(s ManagementStatus) bool { return s.ActiveBackend == "api1" && s.Busy == "" })
			if status.RollbackArmed || status.Operations[len(status.Operations)-1].Kind != "rollback" || one.posts.Load() != 4 {
				t.Fatal("rollback did not validate candidate or remained armed")
			}
			if two.posts.Load() != 7 {
				t.Fatalf("business requests replayed: target posts %d", two.posts.Load())
			}
		})
	}
}

func TestManagedFailureWindowAndOldRevision(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	g, _, _ := serveTest(t, managedConfig(t, one, two))
	m := g.management
	old, _ := m.acquire("", false, false, false)
	op := submitOp(t, m, "switch", "api2")
	waitOp(t, m, op.ID)
	old.release("backend_failure")
	if m.status().Failures != 0 {
		t.Fatal("old route failure affected new route")
	}
	a, _ := m.acquire("", false, false, false)
	a.release("backend_failure")
	b, _ := m.acquire("", false, false, false)
	b.release("success")
	if m.status().Failures != 0 {
		t.Fatal("success did not reset failures")
	}
	c, _ := m.acquire("", false, false, false)
	c.release("client_cancelled")
	if m.status().Failures != 0 {
		t.Fatal("client cancellation counted as backend fault")
	}
}

func TestManagedPersistenceFailureAndDeduplication(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	g, _, _ := serveTest(t, managedConfig(t, one, two))
	m := g.management
	before := m.status()
	req := OperationRequest{randomID(), "api2", before.Revision}
	op, err := m.submit("warmup", req)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := m.submit("warmup", req); err != nil || duplicate.ID != op.ID {
		t.Fatal("operation not idempotent")
	}
	if _, err := m.submit("switch", req); err == nil || err.Code != "operation_id_conflict" {
		t.Fatal("conflicting operation ID accepted")
	}
	waitOp(t, m, op.ID)
	if two.posts.Load() != 4 {
		t.Fatal("duplicate warmed twice")
	}
	if _, err := m.submit("warmup", OperationRequest{randomID(), "api2", before.Revision}); err == nil || err.Code != "stale_revision" {
		t.Fatal("stale version accepted")
	}
	m.mu.Lock()
	persist := m.persist
	m.persist = func(s ManagedState) error {
		if s.ActiveBackend == "api2" {
			return fmt.Errorf("disk full")
		}
		return persist(s)
	}
	m.mu.Unlock()
	op = waitOp(t, m, submitOp(t, m, "switch", "api2").ID)
	if op.Outcome != "failed" || op.RouteCommitted || m.status().ActiveBackend != "api1" {
		t.Fatal("failed route persistence changed state")
	}
	data, _ := os.ReadFile(g.config.Management.StateFile)
	var disk ManagedState
	_ = decodeJSON(data, &disk)
	if disk.ActiveBackend != "api1" {
		t.Fatal("disk route diverged")
	}
	m.mu.Lock()
	m.persist = persist
	m.mu.Unlock()
}

func TestManagedStateLockRestartAndReadOnlyCheck(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	c := managedConfig(t, one, two)
	configPath := filepath.Join(t.TempDir(), "config.json")
	os.WriteFile(configPath, encodeJSON(c), 0600)
	if err := legacyRun([]string{"check", "--config", configPath}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(c.Management.StateFile); !os.IsNotExist(err) {
		t.Fatal("check created state")
	}
	g, err := newGateway(c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := newGateway(c, io.Discard); err == nil {
		other.Close()
		g.Close()
		t.Fatal("second instance obtained state lock")
	}
	submitOp(t, g.management, "maintenance", "api2")
	g.Close()
	g, err = newGateway(c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !g.management.status().Backends["api2"].Maintenance {
		g.Close()
		t.Fatal("maintenance lock lost")
	}
	g.Close()
	disk, err := readManagedState(c)
	if err != nil {
		t.Fatal(err)
	}
	disk.CleanShutdown = false
	disk.Backends["api2"].Maintenance = false
	disk.Operations = append(disk.Operations, &Operation{ID: "interrupted-test", Kind: "switch", Source: "api1", Target: "api2", Outcome: "running"})
	writeManagedState(c.Management.StateFile, disk)
	g, err = newGateway(c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	s := g.management.status()
	if !s.Backends["api2"].DrainUnknown || operationByID(&s.ManagedState, "interrupted-test").Outcome != "interrupted" || s.RollbackArmed {
		t.Fatal("unsafe restart recovery")
	}
	if _, err := g.management.submit("maintenance", OperationRequest{randomID(), "api2", s.Revision}); err == nil {
		t.Fatal("unconfirmed drain accepted")
	}
	submitOp(t, g.management, "confirm-drain", "api2")
	submitOp(t, g.management, "maintenance", "api2")
}

func TestManagedLocalCLI(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	c := managedConfig(t, one, two)
	g, business, logs := serveTest(t, c)
	startTestControl(t, g)
	for _, path := range []string{"/", "/index.html", "/app.js", "/backends.js", "/style.css", "/docs", "/admin/v1/status", "/admin/v1/backends"} {
		r, _ := request(t, business, "GET", path, "", "")
		if r.StatusCode != 404 {
			t.Fatalf("removed endpoint still served: %s", path)
		}
	}
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(cfgPath, encodeJSON(c), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	cli := func(args ...string) {
		t.Helper()
		out.Reset()
		if err := legacyRun(append(args, "--config", cfgPath, "--json"), &out, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	cli("warmup", "api2")
	cli("switch", "single", "api2")
	var op Operation
	if decodeJSON(out.Bytes(), &op) != nil || op.Outcome != "completed" || g.management.status().ActiveBackend != "api2" {
		t.Fatal("CLI did not switch through local socket")
	}
	cli("maintenance", "api1")
	if !g.management.status().Backends["api1"].Maintenance {
		t.Fatal("CLI did not lock maintenance")
	}
	cli("restore", "api1")
	if g.management.status().Backends["api1"].Maintenance {
		t.Fatal("CLI did not restore")
	}
	cli("status")
	if strings.Contains(logs.String(), "aigate-echo-") || strings.Contains(logs.String(), "private") {
		t.Fatal("management exposed generated output")
	}
	if err := legacyRun([]string{"status", "--config", cfgPath, "--admin-url", "http://127.0.0.1:8081"}, io.Discard, io.Discard); err == nil {
		t.Fatal("removed HTTP option accepted")
	}
}

func TestSSEObserverProtocolBoundaries(t *testing.T) {
	good := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\r\n\r\ndata: [DONE]\r\n\r\n"
	for _, test := range []struct {
		name, data     string
		valid, unknown bool
	}{
		{"valid", good, true, false}, {"truncated", strings.Split(good, "data: [DONE]")[0], false, false}, {"error", good + "data: {\"error\":{\"message\":\"private\"}}\n\n", false, false}, {"oversize", "data: " + strings.Repeat("x", maxSSEEvent+1), false, true}, {"invalid", "data: nope\n\n" + good, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := newSSEObserver(false)
			for i := 0; i < len(test.data); i += 7 {
				end := i + 7
				if end > len(test.data) {
					end = len(test.data)
				}
				o.Feed([]byte(test.data[i:end]))
			}
			if o.valid() != test.valid || o.Unknown != test.unknown {
				t.Fatal("wrong classification")
			}
			if o.Content != "" || len(o.Tools) != 0 {
				t.Fatal("business observer retained output")
			}
		})
	}
}

func TestManagedQueuedConnectionAndCancellation(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	one.streamGate = make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(one.streamGate) })
	c := managedConfig(t, one, two)
	c.HTTPPool.MaxConnsPerHost = 1
	g, s, _ := serveTest(t, c)
	response, err := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(response.Body)
	if _, err = reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() {
		r, err := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(basicBody))
		if err != nil {
			done <- 0
			return
		}
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		done <- r.StatusCode
	}()
	waitState(t, g.management, func(s ManagementStatus) bool { return s.Counts["api1"].Business == 2 })
	result := waitOp(t, g.management, submitOp(t, g.management, "switch", "api2").ID)
	if result.Outcome != "drain_timeout" {
		t.Fatal("connection waiter was omitted from drain")
	}
	response.Body.Close()
	if <-done != 200 {
		t.Fatal("queued request failed after old stream cancellation")
	}
	waitState(t, g.management, func(s ManagementStatus) bool { return s.Counts["api1"].Business == 0 })
	if g.management.status().Failures != 0 {
		t.Fatal("cancelled old stream affected new route")
	}
}

func TestManagedRollbackCandidateFailureAndOperationConflict(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	one.probeGate = make(chan struct{})
	one.probeStarted = make(chan struct{}, 1)
	var once sync.Once
	defer once.Do(func() { close(one.probeGate) })
	g, s, _ := serveTest(t, managedConfig(t, one, two))
	m := g.management
	if waitOp(t, m, submitOp(t, m, "switch", "api2").ID).Outcome != "completed" {
		t.Fatal("switch failed")
	}
	one.probeBad.Store(true)
	two.businessStatus.Store(503)
	for i := 0; i < 3; i++ {
		request(t, s, "POST", "/v1/chat/completions", basicBody, "")
	}
	// Discovery also counts as a probe, and publishing its result advances the
	// revision. Wait until generation is actually blocked before taking a snapshot.
	select {
	case <-one.probeStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("rollback generation probe did not start")
	}
	status := m.status()
	if _, err := m.writeBackend(false, backendRequest(m, "api1", nil)); err == nil || err.Code != "operation_in_progress" {
		t.Fatalf("backend edit raced with rollback: %v", err)
	}
	if _, err := m.submit("maintenance", OperationRequest{randomID(), "api1", status.Revision}); err == nil || err.Code != "operation_in_progress" {
		t.Fatalf("maintenance raced with rollback probe: %v", err)
	}
	once.Do(func() { close(one.probeGate) })
	op := waitOp(t, m, status.Busy)
	if op.Outcome != "failed" || op.RouteCommitted || m.status().ActiveBackend != "api2" {
		t.Fatal("failed rollback candidate was activated")
	}
}

func TestManagedConcurrentCutover(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	g, s, _ := serveTest(t, managedConfig(t, one, two))
	m := g.management
	var workers sync.WaitGroup
	failures := make(chan error, 48)
	for i := 0; i < 48; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			r, err := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(basicBody))
			if err != nil {
				failures <- err
				return
			}
			defer r.Body.Close()
			_, err = io.Copy(io.Discard, r.Body)
			if err != nil || r.StatusCode != 200 {
				failures <- fmt.Errorf("request did not complete: %v, %d", err, r.StatusCode)
			}
		}()
	}
	op := submitOp(t, m, "switch", "api2")
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	waitOp(t, m, op.ID)
	waitState(t, m, func(s ManagementStatus) bool {
		return s.Busy == "" && s.Counts["api1"].Business == 0 && s.Counts["api2"].Business == 0
	})
	submitOp(t, m, "maintenance", "api1")
	before := one.posts.Load()
	for i := 0; i < 8; i++ {
		r, _ := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
		if r.StatusCode != 200 {
			t.Fatal(r.StatusCode)
		}
	}
	if one.posts.Load() != before {
		t.Fatal("request reached maintenance backend after acknowledged lock")
	}
}

func TestManagedConfigAndMaintenanceWriteFailure(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	c := managedConfig(t, one, two)
	c.RoutingFile = "routing.json"
	if c.validate() == nil {
		t.Fatal("dual routing sources accepted")
	}
	c.RoutingFile = ""
	c.Routing.Mode = "round_robin"
	if c.validate() == nil {
		t.Fatal("managed round robin accepted")
	}
	c.Routing.Mode = "single"
	g, _, _ := serveTest(t, c)
	m := g.management
	m.mu.Lock()
	persist := m.persist
	m.persist = func(s ManagedState) error { return fmt.Errorf("disk full") }
	m.mu.Unlock()
	status := m.status()
	if _, err := m.submit("maintenance", OperationRequest{randomID(), "api2", status.Revision}); err == nil {
		t.Fatal("failed persistence reported success")
	}
	if m.status().Backends["api2"].Maintenance {
		t.Fatal("failed maintenance write changed runtime lock")
	}
	m.mu.Lock()
	m.persist = persist
	m.mu.Unlock()
}

func TestManagedRestartDrainConfirmationCompletesWaitingSwitch(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	g, _, _ := serveTest(t, managedConfig(t, one, two))
	m := g.management
	m.mu.Lock()
	s := m.cloneLocked()
	s.Backends["api1"].DrainUnknown = true
	if err := m.commitLocked(s); err != nil {
		t.Fatal(err)
	}
	m.mu.Unlock()
	op := waitOp(t, m, submitOp(t, m, "switch", "api2").ID)
	if op.Outcome != "drain_timeout" || !op.RouteCommitted {
		t.Fatal("uncertain old drain was declared complete")
	}
	submitOp(t, m, "confirm-drain", "api1")
	status := m.status()
	if operationByID(&status.ManagedState, op.ID).Outcome != "completed" {
		t.Fatal("drain confirmation did not finish waiting switch")
	}
}

func TestWarmupUsageOnlyKeepsNumericCounters(t *testing.T) {
	got := string(usageCounters(json.RawMessage(`{"total_tokens":8,"prompt_tokens":null,"private":"secret","completion_tokens_details":{"reasoning_tokens":2,"secret":"private"}}`)))
	if strings.Contains(got, "secret") || strings.Contains(got, "private") || strings.Contains(got, "prompt_tokens") || !strings.Contains(got, "reasoning_tokens") {
		t.Fatal("usage did not preserve only numeric counters")
	}
}
