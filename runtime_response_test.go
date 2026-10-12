package aigate

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

type controlBlockingResponse struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
}

func (w *controlBlockingResponse) Write(data []byte) (int, error) {
	close(w.entered)
	<-w.release
	return w.ResponseRecorder.Write(data)
}

func TestControlResponseBackpressureDoesNotBlockRuntime(t *testing.T) {
	for _, kind := range []string{"status", "result", "new_command", "replay", "conflict", "missing_result"} {
		t.Run(kind, func(t *testing.T) {
			c := controlledFixture(t)
			backend := newProtocolBackend(t)
			activateV3(t, c, "bind", switchSnapshot(t, "r1", backend.server.URL, 1))
			cmd := RuntimeCommand{ProtocolVersion: 3, OperationID: "probe", GatewayID: c.id, ExpectedBootID: c.boot,
				Sequence: c.sequence + 1, ExpectedRevision: 1, Action: "test-status",
				Payload: encodeJSON(map[string]string{"task_id": "task", "token": strings.Repeat("t", 32)})}
			if kind != "new_command" {
				if code, result := controlRequest(t, c, cmd); code != 200 || result.Status != "completed" {
					t.Fatal(code, result)
				}
			}
			method, path, body := "GET", "/control/v3/status", []byte(nil)
			switch kind {
			case "result":
				path = "/control/v3/commands/" + strconv.FormatUint(cmd.Sequence, 10)
			case "missing_result":
				path = "/control/v3/commands/999"
			case "new_command", "replay", "conflict":
				method, path = "POST", "/control/v3/commands"
				if kind == "conflict" {
					cmd.OperationID = "conflicting"
				}
				body = encodeJSON(cmd)
			}
			r := httptest.NewRequest(method, path, bytes.NewReader(body))
			r.RemoteAddr = "127.0.0.1:12345"
			r.Header.Set("Authorization", "Bearer "+c.key)
			w := &controlBlockingResponse{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
			done := make(chan struct{})
			go func() { c.ControlHandler().ServeHTTP(w, r); close(done) }()
			defer func() { close(w.release); <-done }()
			select {
			case <-w.entered:
			case <-time.After(time.Second):
				t.Fatal("control response did not start")
			}
			business := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				w := httptest.NewRecorder()
				c.ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`)))
				business <- w
			}()
			select {
			case result := <-business:
				if result.Code != 200 || !strings.Contains(result.Body.String(), "[DONE]") {
					t.Fatal(result.Code, result.Body.String())
				}
			case <-time.After(time.Second):
				t.Fatal("control response blocks stream admission or completion")
			}
			commandDone := make(chan struct{})
			go func() {
				commandV3(t, c, "independent", "test-status", map[string]string{"task_id": "task", "token": strings.Repeat("t", 32)})
				close(commandDone)
			}()
			select {
			case <-commandDone:
			case <-time.After(time.Second):
				t.Fatal("control response blocks the next command")
			}
		})
	}
}

func TestControlStatusResponseIsDetachedFromTestOccupancy(t *testing.T) {
	c := controlledFixture(t)
	c.mu.Lock()
	c.test = &runtimeTest{TaskID: "before", Inflight: 1}
	response := newRuntimeResponse(200, c.statusLocked())
	c.test.TaskID, c.test.Inflight = "after", 2
	c.mu.Unlock()
	w := httptest.NewRecorder()
	response.write(w)
	var status struct {
		Test *runtimeTest `json:"test_occupancy"`
	}
	if json.Unmarshal(w.Body.Bytes(), &status) != nil || status.Test.TaskID != "before" || status.Test.Inflight != 1 {
		t.Fatal(w.Body.String())
	}
}
