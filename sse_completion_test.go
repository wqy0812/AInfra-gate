package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func completionEvent(index any, delta map[string]any, finish any) string {
	choice := map[string]any{"delta": delta, "finish_reason": finish}
	if index != nil {
		choice["index"] = index
	}
	return "data: " + string(encodeJSON(map[string]any{"choices": []any{choice}})) + "\n\n"
}

func TestSSECompletionBoundaries(t *testing.T) {
	const done = "data: [DONE]\n\n"
	empty := completionEvent(nil, map[string]any{"content": ""}, "stop")
	one := completionEvent(0, map[string]any{"content": "one"}, "stop")
	two := completionEvent(1, map[string]any{"content": "two"}, "stop")
	for _, tc := range []struct{ name, payload, data, class string }{
		{"empty", `{}`, empty + done, ""},
		{"refusal", `{}`, completionEvent(nil, map[string]any{"refusal": "declined"}, "stop") + done, ""},
		{"filter", `{}`, completionEvent(nil, map[string]any{}, "content_filter") + done, ""},
		{"tool", `{}`, completionEvent(nil, map[string]any{"tool_calls": []any{map[string]any{"index": 0, "id": "call", "function": map[string]any{"name": "test", "arguments": "{}"}}}}, "tool_calls") + done, ""},
		{"all", `{"n":2}`, one + two + done, ""},
		{"interleaved", `{"n":2}`, completionEvent(1, map[string]any{"content": "start"}, nil) + one + two + done, ""},
		{"partial", `{"n":2}`, one + completionEvent(1, map[string]any{"content": "partial"}, nil) + done, "stream_incomplete"},
		{"missing_choice", `{"n":2}`, one + done, "stream_incomplete"},
		{"missing_done", `{}`, one, "stream_incomplete"},
		{"done_only", `{}`, done, "stream_incomplete"},
		{"pending_data", `{}`, one + done + "data: {}\n", "stream_incomplete"},
		{"pending_line", `{}`, one + done + "data: {}", "stream_incomplete"},
		{"usage", `{}`, one + "data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\n" + done, ""},
		{"comments_after_done", `{}`, one + done + ": heartbeat\r\n\r\n\n", ""},
		{"done_before_output", `{}`, done + one, "protocol_error"},
		{"duplicate_done", `{}`, one + done + done, "protocol_error"},
		{"usage_after_done", `{}`, one + done + "data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\n", "protocol_error"},
		{"output_after_done", `{}`, one + done + completionEvent(0, map[string]any{"content": "late"}, nil), "protocol_error"},
		{"output_after_finish", `{}`, one + completionEvent(0, map[string]any{"content": "late"}, nil) + done, "protocol_error"},
		{"duplicate_finish", `{}`, one + completionEvent(0, map[string]any{}, "stop") + done, "protocol_error"},
		{"reasoning_after_finish", `{}`, one + completionEvent(0, map[string]any{"reasoning_content": "late"}, nil) + done, "protocol_error"},
		{"refusal_after_finish", `{}`, one + completionEvent(0, map[string]any{"refusal": "late"}, nil) + done, "protocol_error"},
		{"tools_after_finish", `{}`, one + completionEvent(0, map[string]any{"tool_calls": []any{map[string]any{"index": 0}}}, nil) + done, "protocol_error"},
		{"missing_index", `{"n":2}`, empty + two + done, "protocol_error"},
		{"negative_index", `{}`, completionEvent(-1, map[string]any{}, "stop") + done, "protocol_error"},
		{"large_index", `{}`, completionEvent(1, map[string]any{}, "stop") + done, "protocol_error"},
		{"fraction_index", `{}`, completionEvent(0.5, map[string]any{}, "stop") + done, "protocol_error"},
		{"null_index", `{}`, "data: {\"choices\":[{\"index\":null,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" + done, "protocol_error"},
		{"over_limit", `{"n":1025}`, one + done, "unknown"},
		{"bad_n", `{"n":"two"}`, one + done, "unknown"},
		{"error_priority", `{}`, "data: {\"error\":\"failed\"}\n\n" + strings.Repeat("x", maxSSEEvent+1), "stream_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := newSSEObserver(false)
			o.expectPayload([]byte(tc.payload))
			for start := 0; start < len(tc.data); start += 7 {
				o.Feed([]byte(tc.data[start:min(start+7, len(tc.data))]))
			}
			if got := o.completionClass(); got != tc.class {
				t.Fatalf("class=%s want=%s", got, tc.class)
			}
			if o.Content != "" || len(o.Tools) != 0 {
				t.Fatal("business observer retained output")
			}
			if tc.name == "empty" && o.FirstMS != nil {
				t.Fatal("empty completion invented first token time")
			}
		})
	}
	o := newSSEObserver(false)
	o.expectPayload([]byte(`{"n":1024}`))
	for i := 0; i < maxSSEChoices; i++ {
		o.Feed([]byte(completionEvent(i, map[string]any{}, "stop")))
	}
	o.Feed([]byte(done))
	if !o.valid() || len(o.finishedChoices) != maxSSEChoices {
		t.Fatal("choice limit boundary failed")
	}
	for _, data := range []string{empty + done, completionEvent(nil, map[string]any{"refusal": "declined"}, "stop") + done} {
		probe := newSSEObserver(true)
		probe.Feed([]byte(data))
		if !probe.valid() || probe.validProbe() || probe.errorMessage() == "" {
			t.Fatal("probe accepted empty/refusal output")
		}
	}
}

func TestSSETerminalOrderBusinessAndProbe(t *testing.T) {
	const done = "data: [DONE]\n\n"
	first := completionEvent(0, map[string]any{"content": "first"}, "stop")
	for _, wire := range []string{done + first, first + done + first,
		first + completionEvent(0, map[string]any{"content": "late"}, nil) + done, first + done + done} {
		g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
			return generationResponse(200, "text/event-stream", wire), nil
		})
		response := generationRequest(g, true)
		if response.Code != 200 || response.Body.String() != wire {
			t.Fatal("terminal validation changed forwarded bytes")
		}
		requireGenerationCount(t, g.profile, true, "error", "protocol_error", "200", 1)
		probe := newSSEObserver(true)
		for i := range wire {
			probe.Feed([]byte{wire[i]})
		}
		if probe.validProbe() || probe.errorMessage() == "" || strings.Contains(probe.Content, "late") {
			t.Fatal("probe accepted terminal-order violation or retained late output")
		}
	}
}

func TestSSETerminalOrderWarmup(t *testing.T) {
	c := fixtureConfig(t, "http://backend.invalid")
	c.Management = ManagementConfig{Enabled: true, StateFile: filepath.Join(t.TempDir(), "state.json")}
	c.Management.defaults(c, "/tmp/config.json")
	g, err := newGateway(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	first := completionEvent(0, map[string]any{"content": "first"}, "stop")
	const done = "data: [DONE]\n\n"
	wire := ""
	g.client.Transport = generationTransport(func(r *http.Request) (*http.Response, error) {
		return generationResponse(200, "text/event-stream", wire), nil
	})
	payload := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "probe"}}, "stream": true}
	for _, data := range []string{done + first, first + done + first,
		first + completionEvent(0, map[string]any{"content": "late"}, nil) + done, first + done + done} {
		wire = data
		if _, _, err := g.management.probe(context.Background(), "api1", "real", false, payload); err == nil {
			t.Fatal("warmup accepted terminal-order violation")
		}
	}
	wire = first + "data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\n" + done + ": heartbeat\n\n"
	if _, _, err := g.management.probe(context.Background(), "api1", "real", false, payload); err != nil {
		t.Fatal(err)
	}
}

func TestSSECompletionGatewayModes(t *testing.T) {
	for _, profiled := range []bool{false, true} {
		for _, managed := range []bool{false, true} {
			for _, legacyAlias := range []bool{false, true} {
				t.Run(fmt.Sprintf("profile=%t/managed=%t/legacyAlias=%t", profiled, managed, legacyAlias), func(t *testing.T) {
					t.Setenv("AIGATE_PROFILE_KEY", "fixture-key")
					c := fixtureConfig(t, "http://backend.invalid")
					if legacyAlias {
						c = withLegacyModelAlias(t, c)
					}
					if profiled {
						c.Profile.Enabled = true
						c.Profile.Directory = t.TempDir()
					}
					if managed {
						c.Management = ManagementConfig{Enabled: true, StateFile: filepath.Join(t.TempDir(), "state.json")}
						c.Management.defaults(c, "/tmp/config.json")
					}
					var logs bytes.Buffer
					g, err := newGateway(c, &logs)
					if err != nil {
						t.Fatal(err)
					}
					defer g.Close()
					upstream := ""
					expectedN := 1
					g.client.Transport = generationTransport(func(r *http.Request) (*http.Response, error) {
						if r.Method == "GET" {
							return generationResponse(200, "application/json", `{"data":[{"id":"real"}]}`), nil
						}
						var body map[string]any
						if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
							t.Fatal(err)
						}
						if body["n"] != float64(expectedN) {
							t.Fatalf("effective n=%v want=%d", body["n"], expectedN)
						}
						return generationResponse(200, "text/event-stream", upstream), nil
					})
					send := func(n int, data, outcome string) {
						t.Helper()
						upstream = data
						expectedN = n
						logs.Reset()
						r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(fmt.Sprintf(`{"messages":[{"role":"user","content":"test"}],"stream":true,"n":%d}`, n)))
						w := httptest.NewRecorder()
						g.ServeHTTP(w, r)
						if w.Code != 200 {
							t.Fatalf("status=%d", w.Code)
						}
						if w.Body.String() != data {
							t.Fatal("forwarding changed")
						}
						if !strings.Contains(logs.String(), `"outcome":"`+outcome+`"`) {
							t.Fatalf("wrong audit: %s", logs.String())
						}
					}
					empty := completionEvent(nil, map[string]any{"content": ""}, "stop") + "data: [DONE]\n\n"
					for i := 0; i < 3; i++ {
						send(1, empty, "completed")
					}
					if managed && g.management.status().Failures != 0 {
						t.Fatal("empty completions counted toward rollback")
					}
					if profiled {
						requireGenerationCount(t, g.profile, true, "completed", "", "200", 3)
						if strings.Contains(generationMetrics(g.profile), "aigate_first_increment_seconds_count{") {
							t.Fatal("invented first token")
						}
						if !strings.Contains(generationMetrics(g.profile), `outcome="completed",request_scope="all"} 3`) {
							t.Fatal("profile outcome disagrees")
						}
					}
					partial := completionEvent(0, map[string]any{"content": "one"}, "stop") + completionEvent(1, map[string]any{"content": "partial"}, nil) + "data: [DONE]\n\n"
					send(2, partial, "upstream_protocol_error")
					if managed && g.management.status().Failures != 1 {
						t.Fatal("incomplete stream not counted toward rollback")
					}
					if profiled {
						requireGenerationCount(t, g.profile, true, "error", "stream_incomplete", "200", 1)
					}
				})
			}
		}
	}
}

func TestSSEEffectiveOverridesAndProbe(t *testing.T) {
	c := fixtureConfig(t, "http://backend.invalid")
	c.Management = ManagementConfig{Enabled: true, StateFile: filepath.Join(t.TempDir(), "state.json")}
	c.Management.defaults(c, "/tmp/config.json")
	c.Backends["api1"].Overrides = map[string]json.RawMessage{"n": json.RawMessage("2")}
	var logs bytes.Buffer
	g, err := newGateway(c, &logs)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	data := completionEvent(0, map[string]any{"content": "one"}, "stop") + "data: [DONE]\n\n"
	g.client.Transport = generationTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "GET" {
			return generationResponse(200, "application/json", `{"data":[{"id":"real"}]}`), nil
		}
		raw, _ := io.ReadAll(r.Body)
		var sent rawObject
		_ = json.Unmarshal(raw, &sent)
		if string(sent["n"]) != "2" {
			t.Fatalf("override not sent: %s", raw)
		}
		return generationResponse(200, "text/event-stream", data), nil
	})
	generationRequest(g, true)
	if !strings.Contains(logs.String(), `"outcome":"upstream_protocol_error"`) {
		t.Fatal("business observer ignored override")
	}
	payload := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "probe"}}, "stream": true}
	if _, _, err := g.management.probe(context.Background(), "api1", "real", false, payload); err == nil {
		t.Fatal("probe ignored override")
	}
	data = completionEvent(0, map[string]any{"content": ""}, "stop") + completionEvent(1, map[string]any{"content": ""}, "stop") + "data: [DONE]\n\n"
	if _, observer, err := g.management.probe(context.Background(), "api1", "real", false, payload); err == nil || !observer.valid() {
		t.Fatal("probe accepted complete empty output or rejected protocol completeness")
	}
	data = completionEvent(0, map[string]any{"content": "one"}, "stop") + completionEvent(1, map[string]any{"content": "two"}, "stop") + "data: [DONE]\n\n"
	if _, _, err := g.management.probe(context.Background(), "api1", "real", false, payload); err != nil {
		t.Fatal(err)
	}
}
