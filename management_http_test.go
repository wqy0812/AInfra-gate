package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestManagementHTTPAuthTopologyAndIdempotency(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	c := managedConfig(t, one, two)
	t.Setenv("TEST_CONTROL_KEY", strings.Repeat("k", 32))
	c.Management.HTTP = ManagementHTTPConfig{Listen: "127.0.0.1:0", KeyEnv: "TEST_CONTROL_KEY", AllowedCIDRs: []string{"127.0.0.1/32"}, GatewayURLs: []string{"http://business.test:80/v1"}}
	g, _, _ := serveTest(t, c)
	call := func(remote, key, command string, payload any) *httptest.ResponseRecorder {
		req := controlRequest{Command: command}
		if payload != nil {
			req.Payload = encodeJSON(payload)
		}
		r := httptest.NewRequest("POST", "http://admin/control/v1", bytes.NewReader(encodeJSON(req)))
		r.RemoteAddr = remote
		r.Header.Set("Authorization", key)
		w := httptest.NewRecorder()
		g.management.httpHandler().ServeHTTP(w, r)
		return w
	}
	auth := "Bearer " + strings.Repeat("k", 32)
	for _, pair := range [][2]string{{"127.0.0.1:2", "Bearer wrong"}, {"192.0.2.1:2", auth}} {
		if w := call(pair[0], pair[1], "status", nil); w.Code != 403 {
			t.Fatal(w.Code)
		}
	}
	if w := call("127.0.0.1:2", auth, "maintenance", nil); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w := call("127.0.0.1:2", auth, "status", nil); w.Code != 200 || strings.Contains(w.Body.String(), "registry") {
		t.Fatal(w.Code, w.Body.String())
	}
	bad := backendRequest(g.management, "router_bad", map[string]any{"url": "http://BUSINESS.test/v1/chat/completions", "models_url": two.server.URL + "/v1/models"})
	if w := call("127.0.0.1:2", auth, "backend-create", bad); w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
	req := backendRequest(g.management, "router_new", map[string]any{"url": two.server.URL + "/v1/chat/completions", "models_url": two.server.URL + "/v1/models"})
	secret := "Bearer private-router-key"
	req.Credentials = map[string]HeaderChange{"Authorization": {Action: "replace", Value: &secret}}
	first := call("127.0.0.1:2", auth, "backend-create", req)
	if first.Code != 200 {
		t.Fatal(first.Body.String())
	}
	second := call("127.0.0.1:2", auth, "backend-create", req)
	if second.Code != 200 || first.Body.String() != second.Body.String() {
		t.Fatal(second.Body.String())
	}
	view := call("127.0.0.1:2", auth, "backends-list", nil)
	if strings.Contains(view.Body.String(), secret) {
		t.Fatal("credential leaked")
	}
	var decoded map[string]json.RawMessage
	if json.Unmarshal(view.Body.Bytes(), &decoded) != nil {
		t.Fatal("invalid JSON")
	}
}

func TestManagementHTTPRequiresExplicitAccessPolicy(t *testing.T) {
	t.Setenv("TEST_CONTROL_KEY", strings.Repeat("k", 32))
	c := ManagementHTTPConfig{Listen: "127.0.0.1:0", KeyEnv: "TEST_CONTROL_KEY"}
	if c.validate(true) == nil {
		t.Fatal("missing network policy accepted")
	}
	c.AllowedCIDRs = []string{"127.0.0.1/32"}
	c.GatewayURLs = []string{"http://gateway.test/v1"}
	if c.validate(false) == nil || c.validate(true) != nil {
		t.Fatal("enablement validation")
	}
	if c.checkRouter(&Backend{URL: "http://gateway.test/v1/chat/completions", ModelsURL: "http://router.test/v1/models"}) == nil {
		t.Fatal("gateway chaining accepted")
	}
	_ = http.MethodPost
}

func TestControlAddressNormalization(t *testing.T) {
	c := ManagementHTTPConfig{Listen: "enabled", GatewayURLs: []string{"http://gateway.test/v1"}}
	for _, u := range []string{"http://GATEWAY.test:80/%76%31/chat/completions", "http://gateway.test./v1/models", "http://gateway.test/other/../v1/chat/completions"} {
		if c.checkRouter(&Backend{URL: u, ModelsURL: "http://router.test/v1/models"}) == nil {
			t.Fatalf("gateway alias accepted: %s", u)
		}
	}
}
