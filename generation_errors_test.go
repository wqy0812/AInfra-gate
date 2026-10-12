package aigate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type generationTransport func(*http.Request) (*http.Response, error)

func (f generationTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func generationGateway(t *testing.T, f generationTransport) *Gateway {
	t.Helper()
	t.Setenv("AIGATE_PROFILE_KEY", "fixture-key")
	c := fixtureConfig(t, "http://backend.invalid")
	c.Profile.Enabled, c.Profile.Directory = true, t.TempDir()
	g, err := newGateway(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	g.client.Transport = generationTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "GET" {
			return generationResponse(200, "application/json", `{"data":[{"id":"real"}]}`), nil
		}
		return f(r)
	})
	return g
}

func generationResponse(status int, contentType, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(body))}
}

func generationRequest(g *Gateway, stream bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(fmt.Sprintf(`{"messages":[{"role":"user","content":"hello"}],"stream":%t}`, stream)))
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	return w
}

func generationMetrics(p *requestProfiler) string {
	var b bytes.Buffer
	p.metrics(&b)
	return b.String()
}

func requireGenerationCount(t *testing.T, p *requestProfiler, stream bool, result, class, status string, count int) {
	t.Helper()
	m := generationMetrics(p)
	tags := fmt.Sprintf(`{backend="api1",model="real",stream="%t"`, stream)
	wants := []string{fmt.Sprintf(`aigate_generation_requests_ended_total%s,result="%s",request_scope="all"} %d`, tags, result, count)}
	if class != "" {
		wants = append(wants, fmt.Sprintf(`aigate_upstream_errors_total%s,error_class="%s",upstream_status="%s",request_scope="all"} %d`, tags, class, status, count))
	}
	for _, want := range wants {
		if !strings.Contains(m, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
	if class == "" && strings.Contains(m, "aigate_upstream_errors_total{") {
		t.Error("unexpected backend error")
	}
}

func TestGenerationHTTPPassthrough(t *testing.T) {
	for _, tc := range []struct {
		status int
		class  string
	}{
		{400, "request_rejected"}, {401, "auth"}, {403, "auth"}, {404, "not_found"}, {413, "request_rejected"}, {422, "request_rejected"}, {429, "rate_limited"},
		{500, "server_error"}, {502, "server_error"}, {503, "server_error"}, {504, "server_error"}, {302, "unexpected_redirect"}, {409, "http_other"},
	} {
		for _, content := range []struct{ kind, body string }{{"application/json", `{"error":{"code":"private-original","message":"原始原因"}}`}, {"text/plain", "private-text\n"}, {"text/html", "<p>private-error</p>"}, {"", ""}} {
			t.Run(fmt.Sprintf("%d/%s", tc.status, content.kind), func(t *testing.T) {
				calls := 0
				g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
					calls++
					res := generationResponse(tc.status, content.kind, content.body)
					res.Header.Set("Retry-After", "7")
					res.Header.Set("WWW-Authenticate", `Bearer realm="backend"`)
					res.Header.Set("Location", "/no-follow")
					res.Header.Set("Set-Cookie", "secret=cookie")
					res.Header.Set("X-Request-ID", "upstream-id")
					res.Header.Set("Connection", "Retry-After, X-Private")
					res.Header.Set("X-Private", "secret")
					return res, nil
				})
				for _, stream := range []bool{false, true} {
					w := generationRequest(g, stream)
					if w.Code != tc.status || w.Body.String() != content.body {
						t.Fatalf("status/body changed: %d %q", w.Code, w.Body.String())
					}
					if w.Header().Get("Content-Type") != content.kind || w.Header().Get("Location") != "/no-follow" || w.Header().Get("WWW-Authenticate") == "" {
						t.Fatal("lost representation headers")
					}
					for _, key := range []string{"Connection", "Retry-After", "Set-Cookie", "X-Private"} {
						if w.Header().Get(key) != "" {
							t.Errorf("forwarded %s", key)
						}
					}
					if id := w.Header().Get("X-Request-ID"); id == "" || id == "upstream-id" {
						t.Fatal("lost gateway request ID")
					}
					requireGenerationCount(t, g.profile, stream, "error", tc.class, strconv.Itoa(tc.status), 1)
				}
				if calls != 2 {
					t.Fatalf("unexpected retry/redirect: %d", calls)
				}
				g.profile.close()
				files, _ := filepath.Glob(filepath.Join(g.config.Profile.Directory, "events-v2-*.jsonl"))
				for _, file := range files {
					b, err := os.ReadFile(file)
					if err != nil {
						t.Fatal(err)
					}
					if bytes.Contains(b, []byte("private-")) {
						t.Fatal("persisted error body")
					}
				}
			})
		}
	}
}

func TestGenerationRetryAfterAndEncoding(t *testing.T) {
	g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
		res := generationResponse(429, "application/octet-stream", "\x00\xffraw")
		res.Header.Set("Retry-After", "Wed, 21 Oct 2030 07:28:00 GMT")
		res.Header.Set("Content-Encoding", "custom")
		return res, nil
	})
	w := generationRequest(g, false)
	if w.Header().Get("Retry-After") != "Wed, 21 Oct 2030 07:28:00 GMT" || w.Header().Get("Content-Encoding") != "custom" || w.Body.String() != "\x00\xffraw" {
		t.Fatal("error representation changed")
	}
}

func TestGenerationProtocolAndTransportCounts(t *testing.T) {
	good := "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
	for _, tc := range []struct {
		name, contentType, body, class, result string
		stream                                 bool
	}{
		{"json", "application/json", goodResponse, "", "completed", false},
		{"invalid_json", "application/json", "{", "protocol_error", "error", false},
		{"invalid_response", "application/json", `{"error":"secret"}`, "response_error", "error", false},
		{"wrong_sse_type", "application/json", goodResponse, "protocol_error", "error", true},
		{"sse", "text/event-stream", good, "", "completed", true},
		{"sse_error", "text/event-stream", "data: {\"error\":{\"message\":\"secret\"}}\n\n", "stream_error", "error", true},
		{"sse_incomplete", "text/event-stream", "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n", "stream_incomplete", "error", true},
		{"sse_invalid", "text/event-stream", "data: {\n\n", "protocol_error", "error", true},
		{"sse_unknown", "text/event-stream", "data: " + strings.Repeat("x", maxSSEEvent+1), "", "unknown", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
				return generationResponse(200, tc.contentType, tc.body), nil
			})
			w := generationRequest(g, tc.stream)
			if !tc.stream && (w.Code != 200 || w.Body.String() != tc.body || w.Header().Get("Content-Type") != tc.contentType) {
				t.Fatal("non-streaming upstream response changed")
			}
			if tc.name == "sse_error" && (w.Code != 200 || w.Body.String() != tc.body) {
				t.Fatal("SSE error changed")
			}
			requireGenerationCount(t, g.profile, tc.stream, tc.result, tc.class, "200", 1)
		})
	}
	for _, tc := range []struct {
		name, class string
		err         error
	}{{"connection", "connection_error", errors.New("connection refused")}, {"timeout", "timeout", context.DeadlineExceeded}} {
		t.Run(tc.name, func(t *testing.T) {
			g := generationGateway(t, func(r *http.Request) (*http.Response, error) { return nil, tc.err })
			generationRequest(g, false)
			requireGenerationCount(t, g.profile, false, "error", tc.class, "none", 1)
		})
	}
}

type generationBrokenBody struct {
	data string
	err  error
}

func (b *generationBrokenBody) Read(p []byte) (int, error) {
	if len(b.data) > 0 {
		n := copy(p, b.data)
		b.data = b.data[n:]
		return n, nil
	}
	return 0, b.err
}
func (b *generationBrokenBody) Close() error { return nil }

func TestGenerationReadFailureAndErrorPriority(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      int
		stream      bool
		body, class string
		err         error
	}{
		{"http", 503, false, "original", "server_error", io.ErrUnexpectedEOF},
		{"json", 200, false, "{", "response_read_error", io.ErrUnexpectedEOF},
		{"json_timeout", 200, false, "{", "timeout", context.DeadlineExceeded},
		{"sse", 200, true, "data: {}\n\n", "response_read_error", io.ErrUnexpectedEOF},
		{"sse_error", 200, true, "data: {\"error\":\"original\"}\n\n", "stream_error", io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
				res := generationResponse(tc.status, "text/event-stream", "")
				res.Body = &generationBrokenBody{tc.body, tc.err}
				return res, nil
			})
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(fmt.Sprintf(`{"messages":[{"role":"user","content":"x"}],"stream":%t}`, tc.stream)))
			var recovered any
			func() { defer func() { recovered = recover() }(); g.ServeHTTP(w, r) }()
			if tc.status == 503 {
				if recovered != http.ErrAbortHandler || w.Body.String() != tc.body {
					t.Fatal("truncated raw error was not aborted unchanged")
				}
			} else if recovered != nil {
				t.Fatal(recovered)
			}
			requireGenerationCount(t, g.profile, tc.stream, "error", tc.class, strconv.Itoa(tc.status), 1)
		})
	}
}

func TestGenerationExcludesDiscoveryAndProbes(t *testing.T) {
	g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
		return generationResponse(503, "text/plain", "private"), nil
	})
	for _, path := range []string{"/v1/models", "/health"} {
		g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}
	g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader("{")))
	body, _ := parseChat([]byte(basicBody))
	_, err := g.generate(context.Background(), body, g.config.Backend, "real")
	if err == nil || err.Status != 502 || err.UpstreamStatus != 503 {
		t.Fatal("probe behavior changed")
	}
	g.client.Transport = generationTransport(func(r *http.Request) (*http.Response, error) {
		return generationResponse(401, "text/plain", "private"), nil
	})
	cache := g.pool.runtime.cache
	cache.mu.Lock()
	cache.model = ""
	cache.mu.Unlock()
	w := generationRequest(g, false)
	if w.Code != 503 {
		t.Fatalf("discovery changed: %d", w.Code)
	}
	if strings.Contains(generationMetrics(g.profile), "aigate_generation_requests_ended_total{") {
		t.Fatal("counted non-generation requests")
	}
}

func TestGenerationConcurrentCountersAndRestart(t *testing.T) {
	g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
		return generationResponse(429, "text/plain", "original"), nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); generationRequest(g, true) }()
	}
	wg.Wait()
	requireGenerationCount(t, g.profile, true, "error", "rate_limited", "429", 40)
	start := g.profile.values["aigate_error_metrics_start_time_seconds"]
	g.profile.close()
	p, err := newRequestProfiler(g.config.Profile)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	requireGenerationCount(t, p, true, "error", "rate_limited", "429", 40)
	if p.values["aigate_error_metrics_start_time_seconds"] != start {
		t.Fatal("error counter origin changed")
	}
}

type generationFailWriter struct{ *httptest.ResponseRecorder }

func (w generationFailWriter) Write(p []byte) (int, error) { return 0, io.ErrClosedPipe }

func TestGenerationClientCancellationAndWriteFailure(t *testing.T) {
	for _, backendError := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel/%t", backendError), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
				cancel()
				if backendError {
					return generationResponse(429, "text/plain", "original"), nil
				}
				return nil, context.Canceled
			})
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(basicBody)).WithContext(ctx)
			g.ServeHTTP(httptest.NewRecorder(), r)
			if backendError {
				requireGenerationCount(t, g.profile, false, "error", "rate_limited", "429", 1)
			} else {
				requireGenerationCount(t, g.profile, false, "client_cancelled", "", "none", 1)
			}
		})
	}
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("write/%t", stream), func(t *testing.T) {
			g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
				if stream {
					return generationResponse(200, "text/event-stream", "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"), nil
				}
				return generationResponse(200, "application/json", goodResponse), nil
			})
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(fmt.Sprintf(`{"messages":[{"role":"user","content":"x"}],"stream":%t}`, stream)))
			g.ServeHTTP(generationFailWriter{httptest.NewRecorder()}, r)
			requireGenerationCount(t, g.profile, stream, "client_disconnected", "", "200", 1)
		})
	}
}

func TestGenerationMetricsUpgradeAndGroupLimit(t *testing.T) {
	p, c := profileFixture(t)
	oldStart := p.counterStarted
	// Produce the previous checkpoint shape: existing counters, no error origin.
	p.mu.Lock()
	delete(p.values, "aigate_error_metrics_start_time_seconds")
	delete(p.types, "aigate_error_metrics_start_time_seconds")
	p.mu.Unlock()
	p.close()
	next, err := newRequestProfiler(c.Profile)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	if next.counterStarted != oldStart || next.values["aigate_error_metrics_start_time_seconds"] < oldStart {
		t.Fatal("upgrade discarded counters or backdated error coverage")
	}
	for i := 0; i < 70; i++ {
		next.mu.Lock()
		tags := next.tags(fmt.Sprintf("backend-%d", i), "real")
		next.generationEnd(&generationResult{stream: true, started: true, status: 503, errorClass: "server_error", result: "error"}, tags)
		next.mu.Unlock()
	}
	m := generationMetrics(next)
	if len(next.groups) != 65 || !strings.Contains(m, `aigate_upstream_errors_total{backend="overflow",model="overflow",stream="true",error_class="server_error",upstream_status="503",request_scope="all"} 6`) {
		t.Fatal("unbounded error groups")
	}
	for _, auth := range []string{"", "Bearer fixture-key"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/metrics", nil)
		r.Header.Set("Authorization", auth)
		next.handler().ServeHTTP(w, r)
		if auth == "" {
			if w.Code != 401 {
				t.Fatal("unauthenticated metrics")
			}
		} else if w.Code != 200 || !strings.Contains(w.Body.String(), "aigate_upstream_errors_total") {
			t.Fatal("missing authenticated error metrics")
		}
	}
}

func TestGenerationRealHTTPTruncationAndDeadline(t *testing.T) {
	for _, mode := range []string{"truncated", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("AIGATE_PROFILE_KEY", "fixture-key")
			u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
				if mode == "deadline" {
					w.Header().Set("Content-Type", "text/plain")
					w.WriteHeader(503)
					io.WriteString(w, "original")
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					return
				}
				conn, b, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				b.WriteString("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: 999\r\n\r\noriginal")
				b.Flush()
			})
			c := fixtureConfig(t, u.URL)
			c.Profile.Enabled = true
			c.Profile.Directory = t.TempDir()
			c.TimeoutSeconds = .1
			g, s, _ := serveTest(t, c)
			res, err := s.Client().Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(basicBody))
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			b, readErr := io.ReadAll(res.Body)
			if res.StatusCode != 503 || readErr == nil || strings.Contains(string(b), "upstream_") || !strings.HasPrefix("original", string(b)) {
				t.Fatalf("truncation status=%d body=%q err=%v", res.StatusCode, b, readErr)
			}
			deadline := time.Now().Add(time.Second)
			for !strings.Contains(generationMetrics(g.profile), "aigate_upstream_errors_total{") && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			requireGenerationCount(t, g.profile, false, "error", "server_error", "503", 1)
		})
	}
}
