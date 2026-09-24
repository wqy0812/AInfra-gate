package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBackendModelConfig(t *testing.T) {
	backend := `{"adapter":"openai","url":"http://localhost/infer","models_url":"http://localhost/models"}`
	for _, tc := range []struct {
		name, fields, backends string
		valid                  bool
	}{
		{"sole default", "", `"a":` + backend, true},
		{"multiple require target", "", `"a":` + backend + `,"b":` + backend, false},
		{"file supplies target", `"routing_file":"routing.json",`, `"a":` + backend + `,"b":` + backend, true},
		{"explicit empty target", `"routing":{"mode":"single"},`, `"a":` + backend, false},
		{"legacy", `"routing":{"mode":"round_robin"},`, `"a":` + backend, false},
		{"legacy with file", `"routing_file":"routing.json","routing":{"mode":"round_robin"},`, `"a":` + backend, false},
		{"alias", `"public_model_name":"统一模型",`, `"a":` + backend, true},
		{"disabled", `"public_model_name":"",`, `"a":` + backend, true},
		{"space", `"public_model_name":"  ",`, `"a":` + backend, true},
		{"trim", `"public_model_name":"alias ",`, `"a":` + backend, true},
		{"null", `"public_model_name":null,`, `"a":` + backend, true},
		{"number", `"public_model_name":42,`, `"a":` + backend, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := parseConfig([]byte(`{`+tc.fields+`"backends":{`+tc.backends+`}}`), "config.json")
			if err == nil {
				err = c.validate()
			}
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
		})
	}
}

func TestBackendModelABSwitchAndAttribution(t *testing.T) {
	makeBackend := func(model string) string {
		return upstreamServer(t, model, func(w http.ResponseWriter, r *http.Request) {
			var body rawObject
			_ = json.NewDecoder(r.Body).Decode(&body)
			if string(body["model"]) != string(encodeJSON(model)) {
				t.Error("alias sent downstream")
			}
			io.WriteString(w, strings.ReplaceAll(goodResponse, `"ignored"`, string(encodeJSON(model))))
		}).URL
	}
	c := fixtureConfig(t, makeBackend("real-a"), makeBackend("real-b"))
	c = withLegacyModelAlias(t, c)
	c.RoutingFile = filepath.Join(t.TempDir(), "routing.json")
	if err := publishPolicy(c, Policy{Mode: "single", Backend: "api1"}); err != nil {
		t.Fatal(err)
	}
	_, server, logs := serveTest(t, c)
	for _, backend := range []string{"api1", "api2", "api1"} {
		if err := publishPolicy(c, Policy{Mode: "single", Backend: backend}); err != nil {
			t.Fatal(err)
		}
		r, models := request(t, server, "GET", "/v1/models", "", "")
		actual := map[string]string{"api1": "real-a", "api2": "real-b"}[backend]
		if r.StatusCode != 200 || !bytes.Contains(models, []byte(`"id":`+string(encodeJSON(actual)))) {
			t.Fatalf("models: %s", models)
		}
		r, data := request(t, server, "POST", "/v1/chat/completions", `{"model":"ignored","messages":[{"role":"user","content":"hi"}]}`, "")
		if r.StatusCode != 200 || !bytes.Contains(data, []byte(`"model":`+string(encodeJSON(actual)))) {
			t.Fatalf("chat: %s", data)
		}
	}
	if !strings.Contains(logs.String(), `"model":"real-a"`) || !strings.Contains(logs.String(), `"model":"real-b"`) {
		t.Fatal("lost real model attribution")
	}
}

func TestBackendModelLeavesUnrecognizedResponsesUntouched(t *testing.T) {
	for _, body := range []string{
		`{"error":"downstream"}`,
		`{ "error":{"message":"downstream"}, "model":"real" }`,
		`{"error":null,"model":"real","choices":[{"message":{"role":"assistant","content":"ok"}}]}`,
		`{"error":"downstream","model":"real","choices":[{"message":{"role":"assistant","content":"ok"}}]}`,
		`{"model":"real"}`,
		`{"choices":[]}`,
		`{"model":"real","choices":[]}`,
		`{"choices":[1]}`,
		`{"choices":[null]}`,
		`{"choices":[{"message":null}]}`,
		`{"choices":[{"message":{"role":"user","content":"input"}}]}`,
		`{"choices":[{"message":{"role":"assistant","content":"ok"}},1]}`,
		`not-json`,
	} {
		t.Run(body, func(t *testing.T) {
			u := upstreamServer(t, "real", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, body)
			})
			c := fixtureConfig(t, u.URL)
			c = withLegacyModelAlias(t, c)
			_, s, _ := serveTest(t, c)
			r, data := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
			if r.StatusCode != 200 || string(data) != body {
				t.Fatalf("upstream response changed: status=%d", r.StatusCode)
			}
		})
	}
}

func TestBackendModelDoesNotAddMissingModel(t *testing.T) {
	for _, message := range []string{
		`{"role":"assistant","content":"answer"}`,
		`{"role":"assistant","content":""}`,
		`{"role":"assistant","content":null,"tool_calls":[{"id":"call1","function":{"name":"tool","arguments":"{}"}}]}`,
		`{"role":"assistant","reasoning_content":"thinking"}`,
		`{"role":"assistant","refusal":"cannot answer"}`,
		`{"role":"assistant","content":null}`,
		`{"role":"assistant","content":[{"type":"text","text":"answer"}]}`,
	} {
		t.Run(message, func(t *testing.T) {
			// Missing model must stay missing, regardless of message content.
			body := `{"choices":[{"message":` + message + `}],"usage":{"large":9007199254740993},"metadata":{"model":"nested-real"}}`
			u := upstreamServer(t, "real", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			})
			c := fixtureConfig(t, u.URL)
			c = withLegacyModelAlias(t, c)
			_, s, _ := serveTest(t, c)
			r, data := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
			if r.StatusCode != 200 {
				t.Fatal(r.StatusCode)
			}
			if string(data) != body {
				t.Fatalf("response changed: %s", data)
			}
		})
	}
}

func TestBackendModelManagedSwitch(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	c := managedConfig(t, one, two)
	c = withLegacyModelAlias(t, c)
	g, server, _ := serveTest(t, c)
	for _, target := range []string{"api2", "api1"} {
		op := waitOp(t, g.management, submitOp(t, g.management, "switch", target).ID)
		if op.Outcome != "completed" {
			t.Fatalf("switch: %+v", op)
		}
		r, data := request(t, server, "POST", "/v1/chat/completions", `{"messages":[{"role":"user"}],"stream":true}`, "")
		if r.StatusCode != 200 || bytes.Contains(data, []byte(`"model":`)) || !bytes.Contains(data, []byte("[DONE]")) {
			t.Fatalf("stream: %s", data)
		}
	}
	for _, p := range []*protocolBackend{one, two} {
		p.mu.Lock()
		for _, model := range p.models {
			if model != "local-protocol-model" {
				t.Error("alias changed warmup or business model")
			}
		}
		p.mu.Unlock()
	}
}

func TestBackendModelTemplateAndDiscoveryFailure(t *testing.T) {
	t.Run("template", func(t *testing.T) {
		u := upstreamServer(t, "actual", func(w http.ResponseWriter, r *http.Request) {
			var body rawObject
			_ = json.NewDecoder(r.Body).Decode(&body)
			if string(body["engine"]) != `"actual"` {
				t.Error("template received alias")
			}
			io.WriteString(w, `{"answer":"hello"}`)
		})
		c := fixtureConfig(t, u.URL)
		c = withLegacyModelAlias(t, c)
		b := c.Backends["api1"]
		b.Adapter = "template"
		b.RequestTemplate = json.RawMessage(`{"engine":{"$ref":"model"}}`)
		b.ResponseContentPath = "answer"
		_, server, _ := serveTest(t, c)
		r, data := request(t, server, "POST", "/v1/chat/completions", basicBody, "")
		if r.StatusCode != 200 || !bytes.Contains(data, []byte(`"model":"actual"`)) {
			t.Fatalf("response: %s", data)
		}
	})
	t.Run("discovery required", func(t *testing.T) {
		u := upstreamServer(t, "actual", nil)
		c := fixtureConfig(t, u.URL)
		c = withLegacyModelAlias(t, c)
		c.Backends["api1"].PreferredModel = new(string)
		_, server, _ := serveTest(t, c)
		r, data := request(t, server, "GET", "/v1/models", "", "")
		requireError(t, r, data, 503, "model_discovery_failed")
	})
}

// Force upstream Read boundaries independently of HTTP buffering.
type modelChunkReader struct {
	io.Reader
	chunk int
}

func (r modelChunkReader) Read(p []byte) (int, error) {
	if len(p) > r.chunk {
		p = p[:r.chunk]
	}
	return r.Reader.Read(p)
}

func TestBackendModelStreamFramingAndFields(t *testing.T) {
	input := ": heartbeat\r\n\r\nevent: message\r\nid: 7\r\nretry: 1000\r\ndata: {\"model\":\"real\",\r\ndata: \"choices\":[{\"delta\":{\"content\":\"real\"},\"finish_reason\":\"stop\"}],\"large\":9007199254740993}\r\n\r\ndata: {\"choices\":[],\"usage\":{\"total_tokens\":8}}\r\rdata: [DONE]\n\n"
	for _, chunk := range []int{1, 2, 17, len(input)} {
		t.Run(fmt.Sprint(chunk), func(t *testing.T) {
			c := withLegacyModelAlias(t, fixtureConfig(t, "http://backend.invalid"))
			g, err := newGateway(c, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			g.client.Transport = generationTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method == "GET" {
					return generationResponse(200, "application/json", `{"data":[{"id":"discovered"}]}`), nil
				}
				response := generationResponse(200, "text/event-stream", input)
				response.Body = io.NopCloser(modelChunkReader{strings.NewReader(input), chunk})
				return response, nil
			})
			w := httptest.NewRecorder()
			g.ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user"}],"stream":true}`)))
			if w.Code != 200 || w.Body.String() != input {
				t.Fatalf("stream changed: %s", w.Body.String())
			}
		})
	}
}

func TestBackendModelStreamFailures(t *testing.T) {
	for _, tc := range []struct {
		name, stream string
	}{
		{"invalid JSON", "data: {broken}\n\n"},
		{"oversize", "data: " + strings.Repeat("x", maxSSEEvent)},
		{"partial", "data: {\"model\":\"real\"}"},
		{"missing done", "data: {\"choices\":[]}\n\n"},
		{"upstream error", "event: error\ndata: {\"error\":{\"message\":\"failure\"}}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, tc.stream)
			})
			c := fixtureConfig(t, u.URL)
			c = withLegacyModelAlias(t, c)
			_, server, _ := serveTest(t, c)
			r, data := request(t, server, "POST", "/v1/chat/completions", `{"messages":[{"role":"user"}],"stream":true}`, "")
			if r.StatusCode != 200 {
				t.Fatal(r.StatusCode)
			}
			if string(data) != tc.stream {
				t.Fatalf("stream changed: %.200s", data)
			}
		})
	}
}

func TestBackendModelLiveStreamSwitchAndCancel(t *testing.T) {
	for _, cancelStream := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancelStream), func(t *testing.T) {
			release := make(chan struct{})
			cancelled := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			one := upstreamServer(t, "real-a", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"model\":\"real-a\",\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
				w.(http.Flusher).Flush()
				select {
				case <-release:
					io.WriteString(w, "data: [DONE]\n\n")
				case <-r.Context().Done():
					close(cancelled)
				}
			})
			two := upstreamServer(t, "real-b", func(w http.ResponseWriter, _ *http.Request) {
				io.WriteString(w, strings.ReplaceAll(goodResponse, `"ignored"`, `"real-b"`))
			})
			c := fixtureConfig(t, one.URL, two.URL)
			c = withLegacyModelAlias(t, c)
			c.RoutingFile = filepath.Join(t.TempDir(), "routing.json")
			if err := publishPolicy(c, Policy{Mode: "single", Backend: "api1"}); err != nil {
				t.Fatal(err)
			}
			_, server, _ := serveTest(t, c)
			client := &http.Client{Timeout: 3 * time.Second}
			resp, err := client.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user"}],"stream":true}`))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			reader := bufio.NewReader(resp.Body)
			line, err := reader.ReadString('\n')
			if err != nil || !strings.Contains(line, `"model":"real-a"`) {
				t.Fatalf("first event delayed or wrong: %s %v", line, err)
			}
			if err := publishPolicy(c, Policy{Mode: "single", Backend: "api2"}); err != nil {
				t.Fatal(err)
			}
			r, data := request(t, server, "POST", "/v1/chat/completions", basicBody, "")
			if r.StatusCode != 200 || !bytes.Contains(data, []byte(`"model":"real-b"`)) {
				t.Fatal("new route failed")
			}
			select {
			case <-cancelled:
				t.Fatal("switch cancelled old stream")
			default:
			}
			if !cancelStream {
				once.Do(func() { close(release) })
				rest, err := io.ReadAll(reader)
				if err != nil || !bytes.Contains(rest, []byte("data: [DONE]")) || bytes.Contains(rest, []byte("invalid_upstream_stream")) {
					t.Fatalf("old stream did not complete after switching: %s %v", rest, err)
				}
				return
			}

			resp.Body.Close()
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("client cancellation did not reach upstream")
			}
		})
	}
}

// Exercise the actual legacy config parser instead of setting a retired field.
func withLegacyModelAlias(t *testing.T, c *Config) *Config {
	t.Helper()
	var fields rawObject
	if err := json.Unmarshal(encodeJSON(c), &fields); err != nil {
		t.Fatal(err)
	}
	fields["public_model_name"] = encodeJSON("retired-alias")
	next, err := parseConfig(encodeJSON(fields), "config.json")
	if err != nil {
		t.Fatal(err)
	}
	return next
}
