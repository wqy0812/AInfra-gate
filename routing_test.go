package aigate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSingleBackendCacheAndModels(t *testing.T) {
	var counts [2]atomic.Int64
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		i := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				counts[i].Add(1)
				_, _ = fmt.Fprintf(w, `{"data":[{"id":"model%d"}]}`, i+1)
				return
			}
			_, _ = io.WriteString(w, strings.Replace(goodResponse, `"model":"ignored"`, fmt.Sprintf(`"model":"model%d"`, i+1), 1))
		}))
		t.Cleanup(servers[i].Close)
	}
	c := controlledFixture(t)
	activateV3(t, c, "bind", switchSnapshot(t, "api1", servers[0].URL, 1))
	commandV3(t, c, "candidate", "prepare", switchSnapshot(t, "api2", servers[1].URL, 2))
	s := httptest.NewServer(c)
	defer s.Close()
	for _, expected := range []string{"model1", "model1", "model1", "model1", "model1"} {
		r, list := request(t, s, "GET", "/v1/models", "", "")
		if r.StatusCode != 200 || !strings.Contains(string(list), "model1") || strings.Contains(string(list), "model2") {
			t.Fatal("models failed")
		}
		r, data := request(t, s, "POST", "/v1/chat/completions", `{"model":"made-up","messages":[{"role":"user"}]}`, "")
		var fields rawObject
		_ = json.Unmarshal(data, &fields)
		if r.StatusCode != 200 || string(fields["model"]) != `"`+expected+`"` {
			t.Fatal("request did not stay on active backend")
		}
	}
	if counts[0].Load() != 1 || counts[1].Load() != 0 {
		t.Fatal("discovery not cached or standby accessed")
	}
}

func TestDiscoveryFailureAndNoGenerationRetry(t *testing.T) {
	for _, mode := range []string{"single", "generation_failure"} {
		t.Run(mode, func(t *testing.T) {
			var posts [2]atomic.Int64
			u1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					if mode != "generation_failure" {
						w.WriteHeader(503)
						_, _ = io.WriteString(w, "private-secret")
						return
					}
					_, _ = io.WriteString(w, `{"data":[{"id":"model1"}]}`)
					return
				}
				posts[0].Add(1)
				w.WriteHeader(500)
			}))
			defer u1.Close()
			u2 := upstreamServer(t, "model2", func(w http.ResponseWriter, r *http.Request) { posts[1].Add(1); _, _ = io.WriteString(w, goodResponse) })
			c := controlledFixture(t)
			activateV3(t, c, "bind", switchSnapshot(t, "api1", u1.URL, 1))
			commandV3(t, c, "candidate", "prepare", switchSnapshot(t, "api2", u2.URL, 2))
			s := httptest.NewServer(c)
			defer s.Close()
			r, data := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
			switch mode {
			case "single":
				requireError(t, r, data, 503, "model_discovery_failed")
				if posts[0].Load()+posts[1].Load() != 0 {
					t.Fatal("single fell back")
				}
			case "generation_failure":
				if r.StatusCode != 500 || len(data) != 0 {
					t.Fatal("generation error status/body not preserved")
				}
				if posts[0].Load() != 1 || posts[1].Load() != 0 {
					t.Fatal("generation retried")
				}
			}
		})
	}
}

func expireModel(p *BackendPool, name string) {
	c := p.runtime.cache
	c.mu.Lock()
	c.expires = time.Time{}
	c.mu.Unlock()
}

func TestDiscoverySingleflightExpiryFailureRecovery(t *testing.T) {
	var calls atomic.Int64
	responses := []string{`{"data":[{"id":"old"}]}`, `{"data":[{"id":"new"}]}`, `{"data":[]}`, `{"data":[{"id":"recovered"}]}`}
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(calls.Add(1)) - 1
		if i >= len(responses) {
			t.Error("unexpected discovery request")
			w.WriteHeader(500)
			return
		}
		_, _ = io.WriteString(w, responses[i])
	}))
	defer u.Close()
	g, _, _ := serveTest(t, fixtureConfig(t, u.URL))
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			model, err := g.pool.discover(context.Background())
			if err != nil || model != "old" {
				t.Error("singleflight result incorrect")
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("concurrent cache miss issued multiple discoveries")
	}
	expireModel(g.pool, "api1")
	if model, err := g.pool.discover(context.Background()); err != nil || model != "new" {
		t.Fatal("cache did not refresh")
	}
	expireModel(g.pool, "api1")
	for i := 0; i < 2; i++ {
		if _, err := g.pool.discover(context.Background()); err == nil {
			t.Fatal("stale model reused")
		}
	}
	if calls.Load() != 3 {
		t.Fatal("failed discovery not cached or standby accessed")
	}
	expireModel(g.pool, "api1")
	if model, err := g.pool.discover(context.Background()); err != nil || model != "recovered" {
		t.Fatal("discovery did not recover")
	}
}

func TestConcurrentSingleBackend(t *testing.T) {
	u1 := upstreamServer(t, "one", nil)
	g, _, _ := serveTest(t, fixtureConfig(t, u1.URL))
	var one, two atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name, _, _, err := g.pool.choose(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			if name == "api1" {
				one.Add(1)
			} else {
				two.Add(1)
			}
		}()
	}
	wg.Wait()
	if one.Load() != 100 || two.Load() != 0 {
		t.Fatal("concurrent requests escaped active backend")
	}
}

func TestPreferredModelCustomPathsAndDiscoveryAuth(t *testing.T) {
	t.Setenv("MODEL_KEY", "upstream-secret")
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer upstream-secret" {
			t.Error("model discovery missing auth")
		}
		_, _ = io.WriteString(w, `{"result":{"models":[{"name":"first"},{"name":"second"}]}}`)
	}))
	defer u.Close()
	for _, preferred := range []string{"", "second", "missing"} {
		t.Run(preferred, func(t *testing.T) {
			c := fixtureConfig(t, u.URL)
			b := c.Backend
			b.ModelsListPath = "result.models"
			b.ModelIDPath = "name"
			b.HeadersEnv = map[string]string{"Authorization": "MODEL_KEY"}
			b.HeaderPrefixes = map[string]string{"Authorization": "Bearer "}
			if preferred != "" {
				b.PreferredModel = &preferred
			}
			_, s, _ := serveTest(t, c)
			r, data := request(t, s, "GET", "/v1/models", "", "")
			if preferred == "missing" {
				requireError(t, r, data, 503, "model_discovery_failed")
				return
			}
			want := preferred
			if want == "" {
				want = "first"
			}
			if r.StatusCode != 200 || !strings.Contains(string(data), `"id":"`+want+`"`) {
				t.Fatal("preferred model incorrect")
			}
		})
	}
}

func TestDiscoveryInvalidResponsesAndTimeout(t *testing.T) {
	for _, body := range []string{`null`, `not json`, `{"data":{}}`, `{"data":[]}`, `{"data":[{"id":" "}]}`, `{"data":[{"id":12}]}`, `{"data":[{}]}`, `timeout`, `redirect`} {
		t.Run(body, func(t *testing.T) {
			u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if body == "timeout" {
					<-r.Context().Done()
					return
				}
				if body == "redirect" {
					w.Header().Set("Location", "/elsewhere")
					w.WriteHeader(302)
					return
				}
				_, _ = io.WriteString(w, body)
			}))
			defer u.Close()
			c := fixtureConfig(t, u.URL)
			c.DiscoveryTimeoutSeconds = 0.03
			_, s, _ := serveTest(t, c)
			r, data := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
			requireError(t, r, data, 503, "model_discovery_failed")
		})
	}
}

func TestCancelledDiscoveryDoesNotPoisonCache(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int64
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, `{"data":[{"id":"recovered"}]}`)
	}))
	defer u.Close()
	g, _, _ := serveTest(t, fixtureConfig(t, u.URL))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _, _ = g.pool.discover(ctx) }()
	<-started
	waitCtx, stop := context.WithCancel(context.Background())
	stop()
	if _, err := g.pool.discover(waitCtx); err == nil {
		t.Fatal("cancelled waiter accepted")
	}
	cancel()
	<-done
	if model, err := g.pool.discover(context.Background()); err != nil || model != "recovered" {
		t.Fatal("caller cancellation poisoned shared cache")
	}
}
