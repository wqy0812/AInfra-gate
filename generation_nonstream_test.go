package aigate

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNonstreamObservationPreservesResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body, encoding, result, class string
	}{
		{"text", goodResponse, "", "completed", ""},
		{"empty_text", `{"choices":[{"message":{"role":"assistant","content":""},"finish_reason":"stop"}]}`, "", "completed", ""},
		{"tools", `{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call1"}]},"finish_reason":"tool_calls"}]}`, "", "completed", ""},
		{"reasoning", `{"choices":[{"message":{"role":"assistant","reasoning_content":"thinking"},"finish_reason":"length"}]}`, "", "completed", ""},
		{"refusal", `{"choices":[{"message":{"role":"assistant","refusal":"cannot answer"},"finish_reason":"stop"}]}`, "", "completed", ""},
		{"filtered", `{"choices":[{"message":{"role":"assistant","content":null},"finish_reason":"content_filter"}]}`, "", "completed", ""},
		{"null_error", `{"error":null,"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`, "", "completed", ""},
		{"error", `{"error":{"message":"private-error"},"model":"real"}`, "", "error", "response_error"},
		{"error_over_choices", `{"error":"private-error","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`, "", "error", "response_error"},
		{"invalid_json", `{`, "", "error", "protocol_error"},
		{"html", `<html>error</html>`, "", "error", "protocol_error"},
		{"empty", ``, "", "error", "protocol_error"},
		{"null", `null`, "", "unknown", ""},
		{"extension", `{"answer":"ok"}`, "", "unknown", ""},
		{"empty_choices", `{"choices":[]}`, "", "unknown", ""},
		{"wrong_choices", `{"choices":[1]}`, "", "unknown", ""},
		{"null_message", `{"choices":[{"message":null,"finish_reason":"stop"}]}`, "", "unknown", ""},
		{"no_finish", `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`, "", "unknown", ""},
		{"partial_candidates", `{"choices":[{"message":{"role":"assistant"},"finish_reason":"stop"},{"message":{"role":"assistant"},"finish_reason":null}]}`, "", "unknown", ""},
		{"oversized", strings.Repeat(" ", maxNonstreamObservationBytes+1), "", "unknown", ""},
		{"encoded", "opaque compressed bytes", "custom", "unknown", ""},
		{"identity", goodResponse, "identity", "completed", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := generationGateway(t, func(*http.Request) (*http.Response, error) {
				r := generationResponse(201, "application/json", tc.body)
				if tc.encoding != "" {
					r.Header.Set("Content-Encoding", tc.encoding)
				}
				return r, nil
			})
			w := generationRequest(g, false)
			if w.Code != 201 || w.Body.String() != tc.body || w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Content-Encoding") != tc.encoding {
				t.Fatal("observation changed the forwarded response")
			}
			requireGenerationCount(t, g.profile, false, tc.result, tc.class, "201", 1)
		})
	}
}

func TestSwitchNonstreamObservationAndArbitraryRequestModel(t *testing.T) {
	const upstreamBody = `{"error":{"message":"private-response-marker"},"model":"real"}`
	u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
		var body rawObject
		if json.NewDecoder(r.Body).Decode(&body) != nil || string(body["model"]) != `"real"` {
			t.Error("arbitrary client model was not replaced by the discovered model")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, upstreamBody)
	})
	c := controlledFixture(t)
	s, proxy := proxySnapshot(t, "api1", u.URL, 1, json.RawMessage(`{"profile":{"enabled":true}}`), nil)
	activateV3(t, c, "bind", s)
	server := httptest.NewServer(c)
	defer server.Close()
	r, body := request(t, server, "POST", "/v1/chat/completions", `{"model":"任意未注册模型","messages":[{"role":"user","content":"hello"}]}`, "")
	if r.StatusCode != 200 || string(body) != upstreamBody {
		t.Fatal("two-hop observation changed upstream status/body")
	}
	g := proxy.g
	requireGenerationCount(t, g.profile, false, "error", "response_error", "200", 1)
	profileDrain(t, g.profile)
	files, err := filepath.Glob(filepath.Join(g.config.Profile.Directory, "events-v2-*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "private-response-marker") {
			t.Fatal("observation persisted the upstream error body")
		}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			var e profileEvent
			if json.Unmarshal([]byte(line), &e) != nil {
				t.Fatal("invalid profile event")
			}
			if e.Kind == "end" {
				found = true
				if e.GenerationResult != "error" || e.ErrorClass != "response_error" || e.UpstreamStatus != 200 || e.Outcome != "completed" {
					t.Fatalf("incorrect observation: %+v", e)
				}
			}
		}
	}
	if !found {
		t.Fatal("missing observation end event")
	}
}
