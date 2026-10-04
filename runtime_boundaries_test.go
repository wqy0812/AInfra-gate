package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
)

func runtimeHTTP(t *testing.T, c *ControlledGateway, method, path, body, remote, key string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.RemoteAddr = remote
	r.Header.Set("Authorization", "Bearer "+key)
	w := httptest.NewRecorder()
	c.ControlHandler().ServeHTTP(w, r)
	return w
}

func TestRuntimeControlHTTPBoundaries(t *testing.T) {
	c := controlledFixture(t)
	key := strings.Repeat("k", 32)
	for _, tc := range []struct {
		name, method, path, body, remote, key, code string
		status                                      int
	}{
		{"foreign source", "GET", "/control/v2/status", "", "192.0.2.1:1", key, "unauthorized", 403},
		{"bad remote", "GET", "/control/v2/status", "", "bad", key, "unauthorized", 403},
		{"wrong key", "GET", "/control/v2/status", "", "127.0.0.1:1", "wrong", "unauthorized", 403},
		{"unknown path", "GET", "/control/v2/missing", "", "127.0.0.1:1", key, "not_found", 404},
		{"wrong method", "DELETE", "/control/v2/commands", "", "127.0.0.1:1", key, "not_found", 404},
		{"invalid seq", "GET", "/control/v2/commands/not-number", "", "127.0.0.1:1", key, "invalid_sequence", 400},
		{"missing result", "GET", "/control/v2/commands/1", "", "127.0.0.1:1", key, "result_unavailable", 404},
		{"bad json", "POST", "/control/v2/commands", "{", "127.0.0.1:1", key, "invalid_request", 400},
		{"unknown field", "POST", "/control/v2/commands", `{"unknown":1}`, "127.0.0.1:1", key, "invalid_request", 400},
		{"oversized", "POST", "/control/v2/commands", strings.Repeat(" ", 1<<20+1), "127.0.0.1:1", key, "invalid_request", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runtimeHTTP(t, c, tc.method, tc.path, tc.body, tc.remote, tc.key)
			if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.code) {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
		})
	}
	w := runtimeHTTP(t, c, "GET", "/control/v2/status", "", "[::ffff:127.0.0.1]:1", key)
	var status map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || status["current"] != nil || status["candidate"] != nil || status["protocol_version"] != float64(2) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestRuntimeCommandFencingAndLookup(t *testing.T) {
	c := controlledFixture(t)
	base := RuntimeCommand{ProtocolVersion: 2, OperationID: "owner", GatewayID: c.id, ExpectedBootID: c.boot, Sequence: 1, Action: "quiesce", Payload: json.RawMessage(`{}`)}
	for _, tc := range []struct {
		name   string
		change func(*RuntimeCommand)
		status int
	}{
		{"version", func(v *RuntimeCommand) { v.ProtocolVersion = 1 }, 400},
		{"gateway", func(v *RuntimeCommand) { v.GatewayID = "other" }, 400},
		{"operation", func(v *RuntimeCommand) { v.OperationID = "bad/identity" }, 400},
		{"sequence", func(v *RuntimeCommand) { v.Sequence = 0 }, 400},
		{"boot", func(v *RuntimeCommand) { v.ExpectedBootID = "old" }, 409},
		{"binding", func(v *RuntimeCommand) { v.ExpectedRevision = 1 }, 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := base
			tc.change(&cmd)
			code, _ := controlRequestV2(t, c, cmd)
			if code != tc.status {
				t.Fatal(code)
			}
		})
	}
	code, result := controlRequestV2(t, c, base)
	if code != 200 || result.Status != "completed" || result.FinishedAt == nil {
		t.Fatalf("%d %+v", code, result)
	}
	code, replay := controlRequestV2(t, c, base)
	if code != 200 || replay.Sequence != result.Sequence {
		t.Fatal("idempotent command was not replayed")
	}
	conflict := base
	conflict.Action = "retire"
	if code, _ := controlRequestV2(t, c, conflict); code != 409 {
		t.Fatal(code)
	}
	w := runtimeHTTP(t, c, "GET", "/control/v2/commands/1", "", "127.0.0.1:1", strings.Repeat("k", 32))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"operation_id":"owner"`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	c.mu.Lock()
	delete(c.results, 1)
	c.mu.Unlock()
	if code, _ := controlRequestV2(t, c, base); code != 409 {
		t.Fatal("stale command accepted", code)
	}
}

func TestRuntimeSnapshotRejectsInvalidConfigurationBeforeDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		change     func(*RuntimeSnapshot)
	}{
		{"missing backend", "invalid_snapshot", func(s *RuntimeSnapshot) { s.Backend = nil }},
		{"bad router id", "invalid_snapshot", func(s *RuntimeSnapshot) { s.RouterID = "bad/router" }},
		{"old revision", "invalid_snapshot", func(s *RuntimeSnapshot) { s.BindingRevision = 0 }},
		{"missing router version", "invalid_snapshot", func(s *RuntimeSnapshot) { s.RouterVersion = 0 }},
		{"wrong adapter", "invalid_backend", func(s *RuntimeSnapshot) { s.Backend.Adapter = "template" }},
		{"environment headers", "invalid_backend", func(s *RuntimeSnapshot) { s.Backend.HeadersEnv = map[string]string{"X-Key": "SECRET"} }},
		{"header prefixes", "invalid_backend", func(s *RuntimeSnapshot) { s.Backend.HeaderPrefixes = map[string]string{"X-Key": "Bearer "} }},
		{"gateway loop", "gateway_target_forbidden", func(s *RuntimeSnapshot) { s.GatewayAddresses = []string{"http://127.0.0.1:1/v1"} }},
		{"null settings", "invalid_settings", func(s *RuntimeSnapshot) { s.Settings = json.RawMessage(`null`) }},
		{"unknown setting", "unknown_setting", func(s *RuntimeSnapshot) { s.Settings = json.RawMessage(`{"unexpected":1}`) }},
		{"wrong setting type", "invalid_settings", func(s *RuntimeSnapshot) { s.Settings = json.RawMessage(`{"timeout_seconds":"bad"}`) }},
		{"bootstrap profile directory", "bootstrap_setting_forbidden", func(s *RuntimeSnapshot) { s.Settings = json.RawMessage(`{"profile":{"directory":"/tmp/forbidden"}}`) }},
		{"bootstrap slow directory", "bootstrap_setting_forbidden", func(s *RuntimeSnapshot) {
			s.Settings = json.RawMessage(`{"slow_requests":{"directory":"/tmp/forbidden"}}`)
		}},
		{"profile key missing", "profile_key_required", func(s *RuntimeSnapshot) { s.Settings = json.RawMessage(`{"profile":{"enabled":true}}`) }},
		{"invalid timeout", "runtime_prepare_failed", func(s *RuntimeSnapshot) { s.Settings = json.RawMessage(`{"timeout_seconds":-1}`) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := controlledFixture(t)
			s := snapshotV2("router", "http://127.0.0.1:1", 1)
			tc.change(&s)
			cmd := RuntimeCommand{OperationID: "owner", Payload: encodeJSON(s)}
			c.mu.Lock()
			e := c.prepareLocked(cmd)
			candidate := c.candidate
			c.mu.Unlock()
			if e == nil || e.Error() != tc.code || candidate != nil {
				t.Fatalf("error=%v candidate=%v", e, candidate)
			}
		})
	}
}

func TestRuntimeActionFailuresDoNotChangeRoute(t *testing.T) {
	c := controlledFixture(t)
	for _, tc := range []struct{ action, payload, code string }{
		{"quiesce", `{"unexpected":true}`, "invalid_payload"},
		{"activate", `{"admission":"yes"}`, "invalid_activation"},
		{"activate", `{}`, "candidate_missing"},
		{"resume", `{}`, "resume_not_owned"},
		{"validate", `{}`, "candidate_missing"},
		{"unknown", `{}`, "unsupported_command"},
		{"test-acquire", `{}`, "invalid_test_identity"},
	} {
		t.Run(tc.action+tc.code, func(t *testing.T) {
			c.mu.Lock()
			e := c.executeLocked(RuntimeCommand{Action: tc.action, OperationID: "owner", Payload: json.RawMessage(tc.payload)}, &RuntimeResult{})
			c.mu.Unlock()
			if e == nil || e.Error() != tc.code {
				t.Fatal(e)
			}
		})
	}
	s := snapshotV2("r", "http://127.0.0.1:1", 1)
	commandV2(t, c, "owner", "prepare", s)
	w := runtimeHTTP(t, c, "GET", "/control/v2/status", "", "127.0.0.1:1", strings.Repeat("k", 32))
	if !strings.Contains(w.Body.String(), `"router_id":"r"`) || !strings.Contains(w.Body.String(), `"candidate":{`) {
		t.Fatal(w.Body)
	}
	for _, payload := range []string{`{"rounds":-1}`, `{"rounds":21}`, `{"step_seconds":-1}`, `{"timeout_seconds":-1}`, `{"max_tokens":-1}`, `{"unknown":1}`} {
		c.mu.Lock()
		e := c.executeLocked(RuntimeCommand{Action: "validate", OperationID: "owner", Payload: json.RawMessage(payload)}, &RuntimeResult{})
		c.mu.Unlock()
		if e == nil || e.Error() != "invalid_validation_policy" {
			t.Fatal(payload, e)
		}
	}
}

func TestRuntimeTestLeaseReleaseAndIdentity(t *testing.T) {
	c := controlledFixture(t)
	activateV2(t, c, "bind", snapshotV2("r", "http://127.0.0.1:1", 1))
	token := strings.Repeat("t", 32)
	payload := map[string]any{"task_id": "task", "token": token, "model": "m", "router_id": "r", "binding_revision": 1}
	call := func(action string, p any) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.testCommandLocked(RuntimeCommand{Action: action, Payload: encodeJSON(p)}, &RuntimeResult{})
	}
	if e := call("test-acquire", payload); e != nil {
		t.Fatal(e)
	}
	if e := call("test-acquire", payload); e != nil {
		t.Fatal("lease replay", e)
	}
	if e := call("test-status", map[string]any{"task_id": "other", "token": strings.Repeat("x", 32)}); e == nil || e.Error() != "test_occupied" {
		t.Fatal(e)
	}
	c.mu.Lock()
	c.test.Inflight = 1
	c.mu.Unlock()
	if e := call("test-release", payload); e != nil {
		t.Fatal(e)
	}
	c.mu.Lock()
	releasing := c.test != nil && c.test.Releasing
	c.test.Inflight = 0
	c.mu.Unlock()
	if !releasing {
		t.Fatal("inflight lease was removed")
	}
	if e := call("test-release", payload); e != nil {
		t.Fatal(e)
	}
	if e := call("test-acquire", payload); e == nil || e.Error() != "test_already_released" {
		t.Fatal(e)
	}
	if e := call("test-status", payload); e != nil {
		t.Fatal(e)
	}
	payload["token"] = strings.Repeat("n", 32)
	payload["binding_revision"] = 2
	if e := call("test-acquire", payload); e == nil || e.Error() != "test_target_changed" {
		t.Fatal(e)
	}
}

func TestRuntimeCLIFailsBeforeServingAndClosesPartialListeners(t *testing.T) {
	t.Setenv("AIGATE_ID", "gateway")
	t.Setenv("AIGATE_CONTROL_KEY", strings.Repeat("k", 32))
	for _, args := range [][]string{{"--help"}, {"version"}} {
		var out bytes.Buffer
		if e := run(args, &out, io.Discard); e != nil {
			t.Fatal(e)
		}
	}
	for _, args := range [][]string{{"--unknown"}, {"--shutdown-timeout", "0s"}, {"--allowed-cidrs", "bad"}, {"serve", "--listen", "bad-address"}} {
		if e := run(args, io.Discard, io.Discard); e == nil {
			t.Fatal("invalid startup accepted", args)
		}
	}
	occupied, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer occupied.Close()
	if e := run([]string{"serve", "--listen", "127.0.0.1:0", "--control-listen", occupied.Addr().String(), "--profile-listen", ""}, io.Discard, io.Discard); e == nil {
		t.Fatal("occupied control listener accepted")
	}
}
