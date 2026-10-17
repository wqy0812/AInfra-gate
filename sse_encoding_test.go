package aigate

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/iotest"
	"time"
)

func gzipSSEFixture(t *testing.T, data string) []byte {
	t.Helper()
	var encoded bytes.Buffer
	w := gzip.NewWriter(&encoded)
	if _, err := io.WriteString(w, data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestSSEEncodingStreaming(t *testing.T) {
	first := completionEvent(0, map[string]any{"content": "hello"}, nil)
	last := completionEvent(0, map[string]any{}, "stop") + "data: [DONE]\n\n"
	for _, tc := range []struct{ name, accept, encoding string }{
		{"explicit_gzip", "gzip", "gzip"},
		{"transport_gzip", "", "gzip"},
		{"identity", "identity", "identity"},
		{"plain", "identity", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(gate) }) }
			defer release()
			u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				if tc.encoding != "" {
					w.Header().Set("Content-Encoding", tc.encoding)
				}
				var out io.Writer = w
				var gz *gzip.Writer
				if tc.encoding == "gzip" {
					gz = gzip.NewWriter(w)
					out = gz
					defer gz.Close()
				}
				io.WriteString(out, first)
				if gz != nil {
					gz.Flush()
				}
				http.NewResponseController(w).Flush()
				select {
				case <-gate:
					io.WriteString(out, last)
				case <-r.Context().Done():
				}
			})
			cfg := fixtureConfig(t, u.URL)
			cfg.Backend.Headers = map[string]string{"Accept-Encoding": tc.accept}
			cfg.Profile.Enabled, cfg.Profile.APIKey, cfg.Profile.Directory = true, "fixture-key", t.TempDir()
			g, server, logs := serveTest(t, cfg)
			captureRoot := filepath.Join(t.TempDir(), "requests")
			captureStore, err := newStreamCaptureStore(captureRoot, "gzip-proxy", "gzip-router", "gzip-boot", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer captureStore.close()
			g.captures = captureStore
			client := &http.Client{Timeout: 3 * time.Second}
			response, err := client.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			reader := bufio.NewReader(response.Body)
			line, err := reader.ReadString('\n')
			if err != nil || line != strings.SplitAfter(first, "\n")[0] {
				t.Fatalf("first SSE event was delayed or encoded: line=%q err=%v", line, err)
			}
			captureStore.mu.Lock()
			var capturing *streamCapture
			for c := range captureStore.active {
				capturing = c
				break
			}
			captureStore.mu.Unlock()
			if capturing == nil {
				t.Fatal("missing capture handle")
			}
			capturing.trigger("gzip-fixture", "output_stall", time.Now())
			release()
			rest, err := io.ReadAll(reader)
			if err != nil || line+string(rest) != first+last || response.Header.Get("Content-Encoding") != "" {
				t.Fatalf("changed decoded stream: err=%v encoding=%q", err, response.Header.Get("Content-Encoding"))
			}
			e := diagnosticAudit(t, logs)
			if e.GenerationResult != "completed" || e.ErrorClass != "" {
				t.Fatalf("decoded stream misclassified: %+v", e)
			}
			requireGenerationCount(t, g.profile, true, "completed", "", "", 1)
			captureStore.close()
			data, err := os.ReadFile(filepath.Join(captureRoot, "request-"+capturing.id, "meta.json"))
			if err != nil {
				t.Fatal(err)
			}
			var meta streamCaptureMeta
			if err = json.Unmarshal(data, &meta); err != nil {
				t.Fatal(err)
			}
			origin := ""
			if tc.encoding == "gzip" {
				origin = "proxy"
				if tc.accept == "" {
					origin = "transport"
				}
			}
			if meta.Decoded != (tc.encoding == "gzip") || meta.DecodeOrigin != origin || meta.ContentEncoding != tc.encoding || meta.CaptureState != "complete" {
				t.Fatalf("encoding metadata: %+v", meta)
			}
		})
	}
}

func TestSSEEncodingFailures(t *testing.T) {
	plain := completionEvent(0, map[string]any{"content": "hello"}, "stop") + "data: [DONE]\n\n"
	encoded := gzipSSEFixture(t, plain)
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-8] ^= 1
	for _, tc := range []struct {
		name, coding, class string
		body                []byte
		status, readBytes   int
	}{
		{"empty", "gzip", "response_read_error", nil, 200, 0},
		{"bad_header", "gzip", "response_read_error", []byte("invalid gzip header"), 200, 0},
		{"truncated", "gzip", "response_read_error", encoded[:len(encoded)-4], 200, len(plain)},
		{"checksum", "gzip", "response_read_error", corrupt, 200, len(plain)},
		{"unknown", "br", "protocol_error", []byte("opaque upstream body"), 502, 0},
		{"stacked", "gzip, br", "protocol_error", encoded, 502, 0},
		{"repeated_gzip", "gzip, gzip", "protocol_error", encoded, 502, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bodyClosed := false
			g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
				res := generationResponse(200, "text/event-stream", "")
				res.Header.Set("Content-Encoding", tc.coding)
				res.Body = &encodingTestBody{Reader: bytes.NewReader(tc.body), onClose: func() { bodyClosed = true }}
				return res, nil
			})
			logs := &lockedBuffer{}
			g.audit = log.New(logs, "", 0)
			w := generationRequest(g, true)
			// After a forwarded [DONE] the failure is classified but no event follows.
			delivered := tc.readBytes == len(plain)
			if w.Code != tc.status || strings.Contains(w.Body.String(), `"error"`) == delivered || (delivered && w.Body.String() != plain) || !bodyClosed {
				t.Fatalf("decode failure lost: status=%d body=%q closed=%t", w.Code, w.Body.String(), bodyClosed)
			}
			e := diagnosticAudit(t, logs)
			operation := "upstream_body_read"
			if tc.status == 502 {
				operation = "upstream_content_decode"
			}
			if e.Transport == nil || e.Transport.ReadBytes != int64(tc.readBytes) || len(e.Transport.Failures) == 0 || e.Transport.Failures[0].Operation != operation {
				t.Fatalf("decode diagnostics lost: %+v", e.Transport)
			}
			requireGenerationCount(t, g.profile, true, "error", tc.class, "200", 1)
		})
	}
}

func TestSSEReadErrorAfterDoneIsRecordedWithoutEvent(t *testing.T) {
	plain := completionEvent(0, map[string]any{"content": "hello"}, "stop") + "data: [DONE]\n\n"
	g := generationGateway(t, func(*http.Request) (*http.Response, error) {
		res := generationResponse(200, "text/event-stream", "")
		res.Body = io.NopCloser(io.MultiReader(strings.NewReader(plain), iotest.ErrReader(syscall.ECONNRESET)))
		return res, nil
	})
	logs := &lockedBuffer{}
	g.audit = log.New(logs, "", 0)
	w := generationRequest(g, true)
	if w.Code != 200 || w.Body.String() != plain {
		t.Fatalf("event written after [DONE]: %q", w.Body.String())
	}
	if e := diagnosticAudit(t, logs); e.Outcome != "upstream_stream_error" || e.GenerationResult != "error" || e.ErrorClass != "response_read_error" {
		t.Fatalf("read failure after [DONE] not recorded: %+v", e)
	}
	requireGenerationCount(t, g.profile, true, "error", "response_read_error", "200", 1)
}

// Admission validation may reject these responses, but business forwarding
// must preserve the upstream bytes, including malformed tool arguments.
func TestSSEAdmissionFailuresStillPassThrough(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"unterminated_done", completionEvent(0, map[string]any{"content": "hello"}, "stop") + "data: [DONE]"},
		{"non_json_tool_arguments", completionEvent(0, map[string]any{"tool_calls": []any{
			map[string]any{"index": 0, "id": "call-1", "type": "function", "function": map[string]any{
				"name": "lookup", "arguments": `{"value":NaN}`,
			}},
		}}, "tool_calls") + "data: [DONE]\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := generationGateway(t, func(*http.Request) (*http.Response, error) {
				return generationResponse(http.StatusOK, "text/event-stream", tc.body), nil
			})
			w := generationRequest(g, true)
			if w.Code != http.StatusOK || w.Body.String() != tc.body {
				t.Fatalf("business forwarding changed upstream response: status=%d body=%q", w.Code, w.Body.String())
			}
		})
	}
}

type encodingTestBody struct {
	io.Reader
	onClose func()
}

func (b *encodingTestBody) Close() error { b.onClose(); return nil }

type encodingSignalWriter struct {
	*httptest.ResponseRecorder
	flushed chan struct{}
	once    sync.Once
}

func (w *encodingSignalWriter) Flush() {
	w.ResponseRecorder.Flush()
	w.once.Do(func() { close(w.flushed) })
}

func TestSSEEncodingCancelAndTimeout(t *testing.T) {
	for _, cancelClient := range []bool{true, false} {
		name := "timeout"
		if cancelClient {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			started, upstreamDone := make(chan struct{}), make(chan struct{})
			u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
				defer close(upstreamDone)
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Content-Encoding", "gzip")
				w.WriteHeader(200)
				http.NewResponseController(w).Flush()
				close(started)
				<-r.Context().Done() // block before even sending the gzip header
			})
			cfg := fixtureConfig(t, u.URL)
			cfg.Backend.Headers = map[string]string{"Accept-Encoding": "gzip"}
			cfg.Profile.Enabled, cfg.Profile.APIKey, cfg.Profile.Directory = true, "fixture-key", t.TempDir()
			if !cancelClient {
				cfg.TimeoutSeconds = 0.08
			}
			g, _, logs := serveTest(t, cfg)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`)).WithContext(ctx)
			done := make(chan struct{})
			w := &encodingSignalWriter{ResponseRecorder: httptest.NewRecorder(), flushed: make(chan struct{})}
			go func() { defer close(done); g.ServeHTTP(w, r) }()
			for _, signal := range []chan struct{}{started, w.flushed} {
				select {
				case <-signal:
				case <-time.After(3 * time.Second):
					t.Fatal("SSE decoding did not start")
				}
			}
			if cancelClient {
				cancel()
			}
			for _, signal := range []chan struct{}{done, upstreamDone} {
				select {
				case <-signal:
				case <-time.After(3 * time.Second):
					t.Fatal("blocked gzip header read was not cancelled")
				}
			}
			e := diagnosticAudit(t, logs)
			if e.Transport == nil || e.Transport.ReadBytes != 0 || len(e.Transport.Failures) == 0 || e.Transport.Failures[0].Operation != "upstream_body_read" {
				t.Fatalf("invalid decode byte accounting: %+v", e.Transport)
			}
			if cancelClient {
				requireGenerationCount(t, g.profile, true, "client_cancelled", "", "", 1)
			} else {
				requireGenerationCount(t, g.profile, true, "error", "timeout", "200", 1)
			}
		})
	}
}
