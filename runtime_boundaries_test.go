package aigate

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
		{"foreign source", "GET", "/control/v3/status", "", "192.0.2.1:1", key, "unauthorized", 403},
		{"bad remote", "GET", "/control/v3/status", "", "bad", key, "unauthorized", 403},
		{"wrong key", "GET", "/control/v3/status", "", "127.0.0.1:1", "wrong", "unauthorized", 403},
		{"unknown path", "GET", "/control/v3/missing", "", "127.0.0.1:1", key, "not_found", 404},
		{"wrong method", "DELETE", "/control/v3/commands", "", "127.0.0.1:1", key, "not_found", 404},
		{"invalid seq", "GET", "/control/v3/commands/not-number", "", "127.0.0.1:1", key, "invalid_sequence", 400},
		{"missing result", "GET", "/control/v3/commands/1", "", "127.0.0.1:1", key, "result_unavailable", 404},
		{"bad json", "POST", "/control/v3/commands", "{", "127.0.0.1:1", key, "invalid_request", 400},
		{"unknown field", "POST", "/control/v3/commands", `{"unknown":1}`, "127.0.0.1:1", key, "invalid_request", 400},
		{"oversized", "POST", "/control/v3/commands", strings.Repeat(" ", 1<<20+1), "127.0.0.1:1", key, "invalid_request", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runtimeHTTP(t, c, tc.method, tc.path, tc.body, tc.remote, tc.key)
			if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.code) {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
		})
	}
	w := runtimeHTTP(t, c, "GET", "/control/v3/status", "", "[::ffff:127.0.0.1]:1", key)
	var status map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || status["current"] != nil || status["candidate"] != nil || status["protocol_version"] != float64(3) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestRuntimeCommandFencingAndLookup(t *testing.T) {
	c := controlledFixture(t)
	base := RuntimeCommand{ProtocolVersion: 3, OperationID: "owner", GatewayID: c.id, ExpectedBootID: c.boot, Sequence: 1, Action: "quiesce", Payload: json.RawMessage(`{}`)}
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
			code, _ := controlRequest(t, c, cmd)
			if code != tc.status {
				t.Fatal(code)
			}
		})
	}
	code, result := controlRequest(t, c, base)
	if code != 200 || result.Status != "completed" || result.FinishedAt == nil {
		t.Fatalf("%d %+v", code, result)
	}
	code, replay := controlRequest(t, c, base)
	if code != 200 || replay.Sequence != result.Sequence {
		t.Fatal("idempotent command was not replayed")
	}
	conflict := base
	conflict.Action = "retire"
	if code, _ := controlRequest(t, c, conflict); code != 409 {
		t.Fatal(code)
	}
	w := runtimeHTTP(t, c, "GET", "/control/v3/commands/1", "", "127.0.0.1:1", strings.Repeat("k", 32))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"operation_id":"owner"`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	c.mu.Lock()
	delete(c.results, 1)
	c.mu.Unlock()
	if code, _ := controlRequest(t, c, base); code != 409 {
		t.Fatal("stale command accepted", code)
	}
}

func TestRuntimeSnapshotRejectsInvalidConfigurationBeforeDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		change     func(*RuntimeSnapshot)
	}{
		{"missing target", "invalid_target", func(s *RuntimeSnapshot) { s.TargetURL = "" }},
		{"bad router id", "invalid_snapshot", func(s *RuntimeSnapshot) { s.RouterID = "bad/router" }},
		{"old revision", "invalid_snapshot", func(s *RuntimeSnapshot) { s.BindingRevision = 0 }},
		{"missing router version", "invalid_snapshot", func(s *RuntimeSnapshot) { s.RouterVersion = 0 }},
		{"missing config version", "invalid_snapshot", func(s *RuntimeSnapshot) { s.GatewayConfigVersion = 0 }},
		{"missing digest", "invalid_snapshot", func(s *RuntimeSnapshot) { s.ConfigDigest = "" }},
		{"target credentials", "invalid_target", func(s *RuntimeSnapshot) { s.TargetURL = "http://u:secret@proxy.test" }},
		{"target query", "invalid_target", func(s *RuntimeSnapshot) { s.TargetURL = "http://proxy.test?key=secret" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := controlledFixture(t)
			s := RuntimeSnapshot{RouterID: "router", RouterVersion: 1, GatewayConfigVersion: 1, BindingRevision: 1, TargetURL: "http://127.0.0.1:1", ConfigDigest: strings.Repeat("a", 64)}
			tc.change(&s)
			c.mu.Lock()
			err := c.prepareSwitchLocked(RuntimeCommand{OperationID: "owner", Payload: encodeJSON(s)})
			candidate := c.candidate
			c.mu.Unlock()
			if err == nil || err.Error() != tc.code || candidate != nil {
				t.Fatalf("err=%v candidate=%v", err, candidate)
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
	s := switchSnapshot(t, "r", "http://127.0.0.1:1", 1)
	commandV3(t, c, "owner", "prepare", s)
	w := runtimeHTTP(t, c, "GET", "/control/v3/status", "", "127.0.0.1:1", strings.Repeat("k", 32))
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
	activateV3(t, c, "bind", switchSnapshot(t, "r", "http://127.0.0.1:1", 1))
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
		if e := RunSwitch(args, &out, io.Discard); e != nil {
			t.Fatal(e)
		}
	}
	for _, args := range [][]string{{"--unknown"}, {"--shutdown-timeout", "0s"}, {"--allowed-cidrs", "bad"}, {"serve", "--listen", "bad-address"}} {
		if e := RunSwitch(args, io.Discard, io.Discard); e == nil {
			t.Fatal("invalid startup accepted", args)
		}
	}
	occupied, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer occupied.Close()
	if e := RunSwitch([]string{"serve", "--listen", "127.0.0.1:0", "--control-listen", occupied.Addr().String()}, io.Discard, io.Discard); e == nil {
		t.Fatal("occupied control listener accepted")
	}
}

// Fixed execution images query v2 to find old requests on another switch;
// current traffic on a shared Router is deliberately outside that view.
func TestV2StatusPreservesRetiringCountsAndTestOccupancy(t *testing.T) {
	c := controlledFixture(t)
	activateV3(t, c, "first", switchSnapshot(t, "r1", "http://127.0.0.1:1", 1))
	c.mu.Lock()
	old := c.active
	old.refs = 1
	old.counts.Business, old.counts.Streams = 1, 1
	c.mu.Unlock()
	activateV3(t, c, "second", switchSnapshot(t, "r2", "http://127.0.0.1:1", 2))
	c.mu.Lock()
	c.active.refs = 1
	c.active.counts.Business = 1
	c.test = &runtimeTest{TaskID: "fixed-runtime", RouterID: "r2", Inflight: 1}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		old.refs, c.active.refs = 0, 0
		c.test = nil
		c.cleanupLocked()
		c.mu.Unlock()
	}()
	for _, version := range []string{"2", "3"} {
		w := runtimeHTTP(t, c, "GET", "/control/v"+version+"/status", "", "127.0.0.1:1", c.key)
		var status struct {
			Counts []RuntimeCounts `json:"counts"`
			Scope  string          `json:"counts_scope"`
			Test   *runtimeTest    `json:"test_occupancy"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil {
			t.Fatal(w.Code, w.Body.String())
		}
		if status.Test == nil || status.Test.TaskID != "fixed-runtime" || status.Test.Inflight != 1 {
			t.Fatal("compatibility status dropped test occupancy")
		}
		if version == "2" {
			if status.Scope != "retiring_snapshots" || len(status.Counts) != 1 || status.Counts[0].RouterID != "r1" || status.Counts[0].Streams != 1 {
				t.Fatal("v2 must report the old snapshot, not current shared traffic", w.Body.String())
			}
		} else if len(status.Counts) != 2 {
			t.Fatal("v3 omitted current or retiring counts", w.Body.String())
		}
	}
}

func TestV2TestCommandsShareV3SequenceAndRejectLegacyPrepare(t *testing.T) {
	c := controlledFixture(t)
	snapshot := switchSnapshot(t, "r1", "http://127.0.0.1:1", 1)
	activateV3(t, c, "bind", snapshot)
	payload := map[string]any{"task_id": "fixed-runtime", "token": strings.Repeat("t", 32), "model": "m", "router_id": "r1", "binding_revision": 1}
	var acquired RuntimeCommand
	for _, action := range []string{"test-acquire", "test-status", "test-release"} {
		cmd := RuntimeCommand{ProtocolVersion: 2, OperationID: "legacy", GatewayID: c.id, ExpectedBootID: c.boot,
			Sequence: c.sequence + 1, ExpectedRevision: 1, Action: action, Payload: encodeJSON(payload)}
		code, result := controlRequest(t, c, cmd)
		if code != 200 || result.Status != "completed" || c.sequence != cmd.Sequence {
			t.Fatal("v2 command did not use the shared sequence", code, result)
		}
		if action == "test-acquire" {
			acquired = cmd
			if c.test.ConfigDigest != snapshot.ConfigDigest {
				t.Fatal("fixed runtime did not inherit the bound config assertion")
			}
		}
	}
	if code, result := controlRequest(t, c, acquired); code != 200 || result.Status != "completed" || c.test != nil {
		t.Fatal("old acquire replay resurrected occupancy", code, result)
	}
	before := c.sequence
	cmd := RuntimeCommand{ProtocolVersion: 2, OperationID: "legacy-prepare", GatewayID: c.id, ExpectedBootID: c.boot,
		Sequence: before + 1, ExpectedRevision: 1, Action: "prepare", Payload: encodeJSON(snapshot)}
	w := runtimeHTTP(t, c, "POST", "/control/v2/commands", string(encodeJSON(cmd)), "127.0.0.1:1", c.key)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "legacy_command_removed") || c.sequence != before {
		t.Fatal("legacy prepare changed control state", w.Code, w.Body.String())
	}
	commandV3(t, c, "next", "quiesce", map[string]any{})
	if c.sequence != before+1 {
		t.Fatal("v3 did not continue after legacy test commands")
	}
}
