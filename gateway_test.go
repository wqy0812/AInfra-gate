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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *lockedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.String() }

const basicBody = `{"messages":[{"role":"user","content":"一二三四五六七八九十秘密尾部"}]}`
const goodResponse = `{"id":"upstream-id","model":"ignored","choices":[{"index":0,"message":{"role":"assistant","content":"回答"},"finish_reason":"stop"}],"usage":{"total_tokens":20},"extra":9007199254740993}`

func fixtureConfig(t *testing.T, urls ...string) *Config {
	t.Helper()
	backends := map[string]any{}
	for i, url := range urls {
		backends[fmt.Sprintf("api%d", i+1)] = map[string]any{"adapter": "openai", "url": url + "/infer", "models_url": url + "/v1/models"}
	}
	c, err := parseConfig(encodeJSON(map[string]any{"timeout_seconds": 2, "discovery_timeout_seconds": 1, "routing": Policy{Mode: "single", Backend: "api1"}, "backends": backends}), filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func upstreamServer(t *testing.T, model string, post http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"data":[{"id":%q}]}`, model)
			return
		}
		if post == nil {
			_, _ = io.WriteString(w, goodResponse)
			return
		}
		post(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func serveTest(t *testing.T, c *Config) (*Gateway, *httptest.Server, *lockedBuffer) {
	t.Helper()
	logs := &lockedBuffer{}
	g, err := newGateway(c, logs)
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(g)
	t.Cleanup(func() { s.Close(); g.Close() })
	return g, s, logs
}

func request(t *testing.T, server *httptest.Server, method, path, body, auth string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, data
}

func requireError(t *testing.T, response *http.Response, data []byte, status int, code string) {
	t.Helper()
	if response.StatusCode != status {
		t.Fatalf("status = %d, want %d", response.StatusCode, status)
	}
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &envelope) != nil || envelope.Error.Code != code || envelope.Error.RequestID == "" || envelope.Error.RequestID != response.Header.Get("X-Request-ID") {
		t.Fatalf("invalid error envelope; wanted %s", code)
	}
	if strings.Contains(string(data), "private") || strings.Contains(string(data), "secret") {
		t.Fatal("error exposed downstream data")
	}
}

func equivalentJSON(t *testing.T, a, b []byte) {
	t.Helper()
	var av, bv any
	if decodeJSON(a, &av) != nil || decodeJSON(b, &bv) != nil || !reflect.DeepEqual(av, bv) {
		t.Fatal("JSON values differ (payload omitted to protect request contents)")
	}
}

func TestForwardFieldsAuthAndPrivateAudit(t *testing.T) {
	t.Setenv("UPSTREAM_KEY", "downstream-secret")
	var sent []byte
	u := upstreamServer(t, "real-model", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Key") != "downstream-secret" || r.Header.Get("Authorization") != "" {
			t.Error("downstream credentials incorrect")
		}
		sent, _ = io.ReadAll(r.Body)
		_, _ = io.WriteString(w, goodResponse)
	})
	c := fixtureConfig(t, u.URL)
	c.Backends["api1"].HeadersEnv = map[string]string{"X-Key": "UPSTREAM_KEY"}
	_, s, logs := serveTest(t, c)
	body := `{"model":null,"messages":[{"role":"system","content":"system-private"},{"role":"user","content":"一二三四五六七八九十秘密尾部"},{"role":"assistant","content":null,"tool_calls":[{"id":"call1"}]}],"temperature":0.3,"max_tokens":77,"seed":9007199254740993,"patches":{},"result_format":"message"}`
	response, data := request(t, s, "POST", "/v1/chat/completions", body, "Bearer client-secret")
	if response.StatusCode != 200 {
		t.Fatalf("status %d", response.StatusCode)
	}
	var expected rawObject
	_ = json.Unmarshal([]byte(body), &expected)
	expected["model"] = encodeJSON("real-model")
	equivalentJSON(t, sent, encodeJSON(expected))
	var got rawObject
	_ = json.Unmarshal(data, &got)
	if string(got["extra"]) != "9007199254740993" || string(got["model"]) != `"real-model"` {
		t.Fatal("response fields changed")
	}
	var entry auditEntry
	if json.Unmarshal([]byte(strings.TrimSpace(logs.String())), &entry) != nil {
		t.Fatal("invalid audit")
	}
	if entry.QuestionPrefix != "一二三四五六七八九十" || entry.RequestID != response.Header.Get("X-Request-ID") || entry.Outcome != "completed" {
		t.Fatal("incorrect audit")
	}
	for _, word := range []string{"秘密尾部", "private", "secret"} {
		if strings.Contains(logs.String(), word) {
			t.Fatal("audit exposed private contents")
		}
	}
}

func TestValidationAndAuthentication(t *testing.T) {
	var hits atomic.Int64
	u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) { hits.Add(1); _, _ = io.WriteString(w, goodResponse) })
	t.Setenv("GATEWAY_KEY", "expected")
	c := fixtureConfig(t, u.URL)
	c.GatewayAPIKeyEnv = "GATEWAY_KEY"
	_, s, logs := serveTest(t, c)
	for _, path := range []string{"/v1/models", "/v1/chat/completions"} {
		method := "GET"
		if strings.Contains(path, "chat") {
			method = "POST"
		}
		r, b := request(t, s, method, path, basicBody, "")
		requireError(t, r, b, 401, "unauthorized")
	}
	cases := []string{`null`, `[]`, `{}`, `{} {}`, `{"messages":"private"}`, `{"messages":[]}`, `{"messages":[null]}`, `{"messages":[{"role":"bad"}]}`, `{"messages":[{"role":"user","content":42}]}`, `{"messages":[{"role":"user","content":[null]}]}`,
		`{"messages":[{"role":"user"}],"stream":null}`, `{"messages":[{"role":"user"}],"stream":"true"}`, `{"messages":[{"role":"user"}],"max_tokens":true}`, `{"messages":[{"role":"user"}],"max_tokens":1.5}`, `{"messages":[{"role":"user"}],"max_tokens":0}`, `{"messages":[{"role":"user"}],"temperature":3}`, `{"messages":[{"role":"user"}],"temperature":"1"}`, `{"messages":[{"role":"user"}],"model":2}`}
	for _, body := range cases {
		r, b := request(t, s, "POST", "/v1/chat/completions", body, "Bearer expected")
		requireError(t, r, b, 400, "invalid_request")
	}
	if hits.Load() != 0 {
		t.Fatal("invalid requests reached upstream")
	}
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var e auditEntry
		_ = json.Unmarshal([]byte(line), &e)
		if e.QuestionPrefix != "" {
			t.Fatal("rejected input logged")
		}
	}
	r, _ := request(t, s, "POST", "/v1/chat/completions", basicBody, "Bearer expected")
	if r.StatusCode != 200 || hits.Load() != 1 {
		t.Fatal("valid auth failed")
	}
	r, _ = request(t, s, "GET", "/v1/models", "", "Bearer expected")
	if r.StatusCode != 200 {
		t.Fatal("models auth failed")
	}
}

func TestCompatibilityAndOverrides(t *testing.T) {
	original := rawObject{"metadata": json.RawMessage(`{"string":"unchanged","already":"{\"x\":1}","number":12,"flag":false,"null":null,"list":["中",1],"object":{"n":9007199254740993}}`), "patches": json.RawMessage(`{}`)}
	rules := Compatibility{MetadataValues: "json_string"}
	converted, err := applyCompatibility(original, rules)
	if err != nil {
		t.Fatal(err)
	}
	var before, after rawObject
	_ = json.Unmarshal(original["metadata"], &before)
	_ = json.Unmarshal(converted["metadata"], &after)
	for key, raw := range before {
		str, ok := rawString(after[key])
		if !ok {
			t.Fatal("metadata value is not a string")
		}
		if value, ok := rawString(raw); ok {
			if value != str {
				t.Fatal("string double encoded")
			}
		} else {
			equivalentJSON(t, raw, []byte(str))
		}
	}
	again, err := applyCompatibility(converted, rules)
	if err != nil {
		t.Fatal(err)
	}
	equivalentJSON(t, encodeJSON(converted), encodeJSON(again))
	kept, _ := applyCompatibility(original, Compatibility{})
	equivalentJSON(t, encodeJSON(original), encodeJSON(kept))
	dropped, _ := applyCompatibility(original, Compatibility{DropFields: []string{"patches"}})
	if _, ok := dropped["patches"]; ok {
		t.Fatal("explicit drop ignored")
	}
	for _, raw := range []string{`null`, `[]`, `"bad"`} {
		_, err := applyCompatibility(rawObject{"metadata": json.RawMessage(raw)}, rules)
		if err == nil || err.Code != "invalid_metadata" {
			t.Fatal("invalid metadata accepted")
		}
	}
	body, _ := parseChat([]byte(basicBody))
	b := &Backend{Adapter: "openai", Overrides: rawObject{"model": json.RawMessage(`"wrong"`), "stream": json.RawMessage(`true`), "temperature": json.RawMessage(`0.2`)}}
	payload, e := makePayload(body, b, "real")
	if e != nil {
		t.Fatal(e)
	}
	var fields rawObject
	_ = json.Unmarshal(payload, &fields)
	if string(fields["model"]) != `"real"` || string(fields["stream"]) != "false" {
		t.Fatal("overrides changed model/stream contract")
	}
}

func TestTemplate(t *testing.T) {
	var sent []byte
	u := upstreamServer(t, "real-model", func(w http.ResponseWriter, r *http.Request) {
		sent, _ = io.ReadAll(r.Body)
		_, _ = io.WriteString(w, `{"data":[{"answer":"你好"}]}`)
	})
	c := fixtureConfig(t, u.URL)
	b := c.Backends["api1"]
	b.Adapter = "template"
	b.ResponseContentPath = "data.0.answer"
	b.RequestTemplate = json.RawMessage(`{"engine":{"$ref":"model"},"history":{"$ref":"request.messages"},"question":{"$ref":"question"},"options":{"limit":{"$ref":"request.max_tokens","$default":50}},"fixed":[1,true]}`)
	_, s, _ := serveTest(t, c)
	for _, body := range []string{basicBody, `{"messages":[{"role":"user","content":"一二三四五六七八九十秘密尾部"}],"max_tokens":null}`} {
		r, data := request(t, s, "POST", "/v1/chat/completions", body, "")
		if r.StatusCode != 200 {
			t.Fatal("template failed")
		}
		var fields rawObject
		_ = json.Unmarshal(sent, &fields)
		if len(fields) != 5 {
			t.Fatal("template forwarded undeclared fields")
		}
		equivalentJSON(t, fields["history"], []byte(`[{"role":"user","content":"一二三四五六七八九十秘密尾部"}]`))
		if string(fields["engine"]) != `"real-model"` {
			t.Fatal("template model incorrect")
		}
		equivalentJSON(t, fields["options"], []byte(`{"limit":50}`))
		var result map[string]any
		_ = decodeJSON(data, &result)
		answer, ok := lookup(result, "choices.0.message.content")
		if !ok || answer != "你好" || result["usage"] != nil {
			t.Fatal("template response incorrect")
		}
	}
	r, data := request(t, s, "POST", "/v1/chat/completions", `{"messages":[{"role":"user"}],"stream":true}`, "")
	requireError(t, r, data, 400, "stream_not_supported")
}

func TestTemplateMissingFields(t *testing.T) {
	var posts atomic.Int64
	u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) { posts.Add(1); _, _ = io.WriteString(w, `{"data":{}}`) })
	c := fixtureConfig(t, u.URL)
	b := c.Backends["api1"]
	b.Adapter = "template"
	b.RequestTemplate = json.RawMessage(`{"x":{"$ref":"request.required"}}`)
	b.ResponseContentPath = "data.answer"
	_, s, _ := serveTest(t, c)
	r, data := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
	requireError(t, r, data, 400, "missing_template_field")
	r, data = request(t, s, "POST", "/v1/chat/completions", `{"messages":[{"role":"user"}],"required":"ok"}`, "")
	requireError(t, r, data, 502, "invalid_upstream_response")
	if posts.Load() != 1 {
		t.Fatal("unexpected template forwarding")
	}
}

func TestUpstreamErrors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   int
		code   string
	}{
		{"not_json", 200, "private-secret", 502, "invalid_upstream_json"},
		{"invalid_response", 200, `{"error":"private-secret"}`, 502, "invalid_upstream_response"},
		{"null", 200, `null`, 502, "invalid_upstream_response"},
		{"bad_choice", 200, `{"choices":[{"message":{"role":"assistant","content":null}}]}`, 502, "invalid_upstream_response"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "/must-not-follow")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			_, s, _ := serveTest(t, fixtureConfig(t, u.URL))
			r, data := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
			requireError(t, r, data, tc.want, tc.code)
		})
	}
}

func TestGenerationTimeoutAndConnectionFailure(t *testing.T) {
	for _, mode := range []string{"headers", "body", "connection"} {
		t.Run(mode, func(t *testing.T) {
			u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if mode == "body" {
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			})
			c := fixtureConfig(t, u.URL)
			c.TimeoutSeconds = 0.08
			g, s, _ := serveTest(t, c)
			if mode == "connection" {
				if _, err := g.pool.discover(context.Background(), "api1"); err != nil {
					t.Fatal(err)
				}
				u.Close()
			}
			r, data := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
			status, code := 504, "upstream_timeout"
			if mode == "connection" {
				status, code = 502, "upstream_unavailable"
			}
			requireError(t, r, data, status, code)
		})
	}
}

func TestUnicodeMultimodalQuestion(t *testing.T) {
	body, err := parseChat([]byte(`{"messages":[{"role":"user","content":"old"},{"role":"user","content":[{"type":"image_url","image_url":{"url":"private"}},{"type":"text","text":"你🙂\n"},{"type":"text","text":"好"},{"type":"text","text":12}]}]}`))
	if err != nil || body.question != "你🙂\n好" {
		t.Fatal("multimodal extraction failed")
	}
	u := upstreamServer(t, "real", nil)
	_, s, logs := serveTest(t, fixtureConfig(t, u.URL))
	r, _ := request(t, s, "POST", "/v1/chat/completions", string(encodeJSON(body.fields)), "")
	if r.StatusCode != 200 {
		t.Fatal("request failed")
	}
	if len(strings.Split(strings.TrimSpace(logs.String()), "\n")) != 1 {
		t.Fatal("log injection")
	}
}

func TestRealLocalFixture(t *testing.T) {
	for _, mode := range []string{"json_string", "workspace_object_json_string"} {
		t.Run(mode, func(t *testing.T) { testRealLocalFixture(t, mode) })
	}
}

func testRealLocalFixture(t *testing.T, mode string) {
	data, err := os.ReadFile("body-agent-ds4.json")
	if os.IsNotExist(err) {
		t.Skip("local private fixture is not distributed")
	}
	if err != nil {
		t.Fatal(err)
	}
	var sent []byte
	u := upstreamServer(t, "real-model", func(w http.ResponseWriter, r *http.Request) {
		sent, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
	c := fixtureConfig(t, u.URL)
	c.Backends["api1"].Compatibility.MetadataValues = mode
	_, s, _ := serveTest(t, c)
	r, result := request(t, s, "POST", "/v1/chat/completions", string(data), "")
	if r.StatusCode != 200 || string(result) != "data: [DONE]\n\n" {
		t.Fatal("fixture failed")
	}
	var original, actual rawObject
	_ = json.Unmarshal(data, &original)
	_ = json.Unmarshal(sent, &actual)
	if len(original)+boolInt(original["model"] == nil) != len(actual) {
		t.Fatal("fixture fields lost")
	}
	for key, raw := range original {
		if key != "model" && key != "metadata" {
			equivalentJSON(t, raw, actual[key])
		}
	}
	if string(actual["model"]) != `"real-model"` {
		t.Fatal("fixture model incorrect")
	}
	var before, after rawObject
	_ = json.Unmarshal(original["metadata"], &before)
	_ = json.Unmarshal(actual["metadata"], &after)
	if len(before) != len(after) {
		t.Fatal("metadata fields lost")
	}
	for key, raw := range before {
		if mode == "workspace_object_json_string" && (key != "workspace" || !bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{"))) {
			equivalentJSON(t, raw, after[key])
			continue
		}
		str, ok := rawString(after[key])
		if !ok {
			t.Fatal("metadata not converted")
		}
		if original, ok := rawString(raw); ok {
			if original != str {
				t.Fatal("metadata string changed")
			}
		} else {
			equivalentJSON(t, raw, []byte(str))
		}
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestSSEFlushSwitchAndPassThrough(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	first := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{}\"}}]}}]}\n\n"
	tail := "data: {\"choices\":[],\"usage\":{\"total_tokens\":20}}\n\ndata: [DONE]\n\n"
	var observedMu sync.Mutex
	observed := []string{}
	post := func(wait bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var fields rawObject
			_ = json.NewDecoder(r.Body).Decode(&fields)
			model, _ := rawString(fields["model"])
			observedMu.Lock()
			observed = append(observed, model)
			observedMu.Unlock()
			if string(fields["stream_options"]) != `{"include_usage":true}` {
				t.Error("stream options changed")
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, first)
			w.(http.Flusher).Flush()
			if wait {
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			_, _ = io.WriteString(w, tail)
		}
	}
	u1 := upstreamServer(t, "model1", post(true))
	u2 := upstreamServer(t, "model2", post(false))
	c := fixtureConfig(t, u1.URL, u2.URL)
	c.RoutingFile = filepath.Join(t.TempDir(), "routing.json")
	if err := publishPolicy(c, Policy{Mode: "single", Backend: "api1"}); err != nil {
		t.Fatal(err)
	}
	_, s, logs := serveTest(t, c)
	body := `{"stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"call1"}]}]}`
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != strings.Split(first, "\n")[0]+"\n" {
		t.Fatal("first event not immediately forwarded")
	}
	if err := publishPolicy(c, Policy{Mode: "single", Backend: "api2"}); err != nil {
		t.Fatal(err)
	}
	r, data := request(t, s, "POST", "/v1/chat/completions", body, "")
	if r.StatusCode != 200 || string(data) != first+tail {
		t.Fatal("switched stream changed")
	}
	once.Do(func() { close(release) })
	rest, err := io.ReadAll(reader)
	if err != nil || line+string(rest) != first+tail {
		t.Fatal("inflight stream was interrupted or rewritten")
	}
	observedMu.Lock()
	defer observedMu.Unlock()
	if !reflect.DeepEqual(observed, []string{"model1", "model2"}) {
		t.Fatal("switch did not select expected models")
	}
	if strings.Contains(logs.String(), "upstream_stream_error") {
		t.Fatal("successful stream logged as failed")
	}
}

func TestSSEErrorsAndTimeout(t *testing.T) {
	for _, mode := range []string{"wrong_media", "broken", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if mode == "wrong_media" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, goodResponse)
					return
				}
				if mode == "broken" {
					conn, buffer, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					_, _ = buffer.WriteString("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 999\r\n\r\ndata: {}\n\n")
					_ = buffer.Flush()
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {}\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			})
			c := fixtureConfig(t, u.URL)
			c.TimeoutSeconds = 0.1
			_, s, logs := serveTest(t, c)
			r, data := request(t, s, "POST", "/v1/chat/completions", `{"messages":[{"role":"user"}],"stream":true}`, "")
			if mode == "wrong_media" {
				requireError(t, r, data, 502, "invalid_upstream_stream")
				return
			}
			code := "upstream_stream_error"
			if mode == "timeout" {
				code = "upstream_timeout"
			}
			if r.StatusCode != 200 || !strings.Contains(string(data), code) || strings.Contains(string(data), "[DONE]") {
				t.Fatalf("stream error missing: %s", code)
			}
			if !strings.Contains(logs.String(), `"outcome":"`+code+`"`) {
				t.Fatal("stream outcome incorrect")
			}
		})
	}
}

func TestClientDisconnectCancelsUpstream(t *testing.T) {
	cancelled := make(chan struct{})
	u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(cancelled)
	})
	_, s, _ := serveTest(t, fixtureConfig(t, u.URL))
	client := &http.Client{Timeout: 5 * time.Second}
	r, err := client.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user"}],"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(r.Body).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	_ = r.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("disconnect did not cancel upstream")
	}
}

func TestDocumentationAndHealth(t *testing.T) {
	u := upstreamServer(t, "real", nil)
	_, s, _ := serveTest(t, fixtureConfig(t, u.URL))
	for _, path := range []string{"/health", "/openapi.json"} {
		r, data := request(t, s, "GET", path, "", "")
		if r.StatusCode != 200 || len(data) == 0 {
			t.Fatal("missing endpoint")
		}
		if path == "/openapi.json" && !json.Valid(data) {
			t.Fatal("invalid OpenAPI")
		}
	}
	r, data := request(t, s, "GET", "/docs", "", "")
	requireError(t, r, data, 404, "not_found")
	r, data = request(t, s, "GET", "/missing", "", "")
	requireError(t, r, data, 404, "not_found")
	r, data = request(t, s, "GET", "/v1/chat/completions", "", "")
	requireError(t, r, data, 405, "method_not_allowed")
}
