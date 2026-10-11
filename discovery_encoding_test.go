package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiscoveryResponseEncoding(t *testing.T) {
	models := `{"data":[{"id":"discovered-model"}]}`
	encoded := gzipSSEFixture(t, models)
	stream := completionEvent(0, map[string]any{"content": "hello"}, "stop") + "data: [DONE]\n\n"
	for _, tc := range []struct {
		name, accept, coding string
		body                 []byte
	}{
		{"plain", "identity", "", []byte(models)},
		{"identity", "identity", "identity", []byte(models)},
		{"transport_gzip", "", "gzip", encoded},
		{"explicit_gzip", "gzip", "gzip", encoded},
		{"mixed_case_gzip", "gzip", "GZip", encoded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var discoveries, generations atomic.Int64
			u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					discoveries.Add(1)
					wantAccept := tc.accept
					if wantAccept == "" {
						wantAccept = "gzip"
					}
					if got := r.Header.Get("Accept-Encoding"); got != wantAccept {
						t.Errorf("discovery Accept-Encoding=%q, want %q", got, wantAccept)
					}
					w.Header().Set("Content-Type", "application/json")
					if tc.coding != "" {
						w.Header().Set("Content-Encoding", tc.coding)
					}
					_, _ = w.Write(tc.body)
					return
				}
				generations.Add(1)
				body, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(body), `"model":"discovered-model"`) {
					t.Errorf("generation did not use discovered model: %s", body)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, stream)
			}))
			defer u.Close()
			cfg := fixtureConfig(t, u.URL)
			if tc.accept != "" {
				cfg.Backend.Headers = map[string]string{"Accept-Encoding": tc.accept}
			}
			_, server, _ := serveTest(t, cfg)
			response, data := request(t, server, "GET", "/v1/models", "", "")
			if response.StatusCode != 200 || !strings.Contains(string(data), `"id":"discovered-model"`) {
				t.Fatalf("encoded model discovery failed: status=%d body=%s", response.StatusCode, data)
			}
			response, data = request(t, server, "POST", "/v1/chat/completions", `{"messages":[{"role":"user","content":"hello"}],"stream":true}`, "")
			if response.StatusCode != 200 || string(data) != stream {
				t.Fatalf("discovery blocked or changed SSE: status=%d body=%s", response.StatusCode, data)
			}
			if discoveries.Load() != 1 || generations.Load() != 1 {
				t.Fatalf("unexpected request counts: discovery=%d generation=%d", discoveries.Load(), generations.Load())
			}
		})
	}
}

func TestDiscoveryDecodedSizeLimit(t *testing.T) {
	for _, encoding := range []string{"plain", "transport_gzip", "explicit_gzip"} {
		for _, extra := range []int{0, 1} {
			name := encoding + "/at_limit"
			if extra != 0 {
				name = encoding + "/over_limit"
			}
			t.Run(name, func(t *testing.T) {
				prefix, suffix := `{"data":[{"id":"bounded-model"}],"padding":"`, `"}`
				data := []byte(prefix + strings.Repeat("x", maxModelDiscoveryBytes+extra-len(prefix)-len(suffix)) + suffix)
				if encoding != "plain" {
					data = gzipSSEFixture(t, string(data))
				}
				var generations atomic.Int64
				u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodGet {
						if encoding != "plain" {
							w.Header().Set("Content-Encoding", "gzip")
						}
						_, _ = w.Write(data)
						return
					}
					generations.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, completionEvent(0, map[string]any{"content": "ok"}, "stop")+"data: [DONE]\n\n")
				}))
				defer u.Close()
				c := controlledFixture(t)
				snapshot := snapshotV2("r1", u.URL, 1)
				if encoding == "explicit_gzip" {
					snapshot.Backend.Headers = map[string]string{"Accept-Encoding": "gzip"}
				}
				activateV2(t, c, "bind", snapshot)
				w := httptest.NewRecorder()
				c.ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`)))
				if extra == 0 {
					if w.Code != 200 || generations.Load() != 1 || !strings.Contains(w.Body.String(), "[DONE]") {
						t.Fatal(w.Code, w.Body.String(), generations.Load())
					}
				} else if w.Code != 503 || !strings.Contains(w.Body.String(), "model_discovery_failed") || generations.Load() != 0 {
					t.Fatal(w.Code, w.Body.String(), generations.Load())
				}
			})
		}
	}
}

func TestDiscoveryResponseEncodingFailures(t *testing.T) {
	models := `{"data":[{"id":"discovered-model"}]}`
	encoded := gzipSSEFixture(t, models)
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-8] ^= 1
	for _, tc := range []struct {
		name, coding string
		body         []byte
	}{
		{"empty", "gzip", nil},
		{"bad_header", "gzip", []byte("invalid gzip header")},
		{"truncated", "gzip", encoded[:len(encoded)-4]},
		{"checksum", "gzip", corrupt},
		{"invalid_json", "gzip", gzipSSEFixture(t, "not json")},
		{"unknown", "br", []byte(models)},
		{"stacked", "gzip, br", encoded},
		{"repeated_gzip", "gzip, gzip", encoded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var generations atomic.Int64
			u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					w.Header().Set("Content-Encoding", tc.coding)
					_, _ = w.Write(tc.body)
					return
				}
				generations.Add(1)
			}))
			defer u.Close()
			cfg := fixtureConfig(t, u.URL)
			cfg.Backend.Headers = map[string]string{"Accept-Encoding": "gzip"}
			_, server, _ := serveTest(t, cfg)
			response, data := request(t, server, "POST", "/v1/chat/completions", `{"messages":[{"role":"user","content":"hello"}],"stream":true}`, "")
			requireError(t, response, data, 503, "model_discovery_failed")
			if generations.Load() != 0 {
				t.Fatal("generation was sent after invalid model discovery")
			}
		})
	}
}

func TestDiscoveryEncodedBodyClosed(t *testing.T) {
	encoded := gzipSSEFixture(t, `{"data":[{"id":"discovered-model"}]}`)
	for _, tc := range []struct {
		name, coding string
		body         []byte
		wantError    bool
	}{
		{"valid_gzip", "gzip", encoded, false},
		{"bad_gzip", "gzip", []byte("invalid gzip header"), true},
		{"truncated_gzip", "gzip", encoded[:len(encoded)-4], true},
		{"unsupported", "br", encoded, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closed := 0
			client := &http.Client{Transport: generationTransport(func(*http.Request) (*http.Response, error) {
				response := generationResponse(200, "application/json", "")
				response.Header.Set("Content-Encoding", tc.coding)
				response.Body = &encodingTestBody{Reader: bytes.NewReader(tc.body), onClose: func() { closed++ }}
				return response, nil
			})}
			cfg := fixtureConfig(t, "http://backend.invalid")
			if err := cfg.validate(); err != nil {
				t.Fatal(err)
			}
			model, err := newPool(cfg, client).fetchModel(context.Background(), cfg.Backend)
			if (err != nil) != tc.wantError || (!tc.wantError && model != "discovered-model") || closed != 1 {
				t.Fatalf("model=%q err=%v body closes=%d", model, err, closed)
			}
		})
	}
}

func TestDiscoveryGzipCancelAndTimeout(t *testing.T) {
	encoded := gzipSSEFixture(t, `{"data":[{"id":"recovered"}]}`)
	for _, cancelClient := range []bool{true, false} {
		name := "timeout"
		if cancelClient {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			started, upstreamDone := make(chan struct{}), make(chan struct{})
			var calls atomic.Int64
			u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Encoding", "gzip")
				if calls.Add(1) == 1 {
					w.WriteHeader(200)
					_ = http.NewResponseController(w).Flush()
					close(started)
					<-r.Context().Done() // wait before sending the gzip header bytes
					close(upstreamDone)
					return
				}
				_, _ = w.Write(encoded)
			}))
			defer u.Close()
			cfg := fixtureConfig(t, u.URL)
			cfg.Backend.Headers = map[string]string{"Accept-Encoding": "gzip"}
			if !cancelClient {
				cfg.DiscoveryTimeoutSeconds = 0.05
			}
			g, _, _ := serveTest(t, cfg)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan *GatewayError, 1)
			go func() { _, err := g.pool.discover(ctx); done <- err }()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("gzip model discovery did not start")
			}
			if cancelClient {
				cancel()
			}
			select {
			case err := <-done:
				if err == nil || err.Code != "model_discovery_failed" {
					t.Fatalf("unfinished gzip discovery accepted: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("gzip discovery read did not stop")
			}
			select {
			case <-upstreamDone:
			case <-time.After(3 * time.Second):
				t.Fatal("gzip discovery did not cancel upstream")
			}
			if !cancelClient {
				expireModel(g.pool, "api1")
			}
			if model, err := g.pool.discover(context.Background()); err != nil || model != "recovered" {
				t.Fatalf("gzip discovery did not recover: model=%q err=%v", model, err)
			}
		})
	}
}
