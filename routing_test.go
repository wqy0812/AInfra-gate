package main

import (
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
			_, _ = io.WriteString(w, goodResponse)
		}))
		t.Cleanup(servers[i].Close)
	}
	_, s, _ := serveTest(t, fixtureConfig(t, servers[0].URL, servers[1].URL))
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

func TestRoutingFileRecoveryAndAtomicSwitchCLI(t *testing.T) {
	u1 := upstreamServer(t, "model1", nil)
	u2 := upstreamServer(t, "model2", nil)
	c := fixtureConfig(t, u1.URL, u2.URL)
	c.RoutingFile = filepath.Join(t.TempDir(), "routing.json")
	if err := publishPolicy(c, Policy{Mode: "single", Backend: "api1"}); err != nil {
		t.Fatal(err)
	}
	_, s, _ := serveTest(t, c)
	for _, invalid := range []string{`{"mode":"single","backend":"typo"}`, `{"mode":"round_robin"}`, `{"mode":"single"}`, `{`, `null`, `{"mode":"single","backend":"api2","extra":1}`} {
		if err := os.WriteFile(c.RoutingFile, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		r, data := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
		requireError(t, r, data, 503, "invalid_routing_config")
		if err := publishPolicy(c, Policy{Mode: "single", Backend: "api2"}); err != nil {
			t.Fatal(err)
		}
		r, data = request(t, s, "POST", "/v1/chat/completions", basicBody, "")
		if r.StatusCode != 200 || !strings.Contains(string(data), `"model":"model2"`) {
			t.Fatal("routing failed to recover")
		}
	}
	if err := os.Remove(c.RoutingFile); err != nil {
		t.Fatal(err)
	}
	r, data := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
	requireError(t, r, data, 503, "invalid_routing_config")
	// The CLI resolves the path from the config directory, even without credentials.
	configFile := filepath.Join(filepath.Dir(c.RoutingFile), "config.json")
	cliConfig := *c
	cliConfig.RoutingFile = "routing.json"
	cliConfig.GatewayAPIKeyEnv = "AIGATE_TEST_MISSING_KEY"
	if err := os.WriteFile(configFile, encodeJSON(cliConfig), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := legacyRun([]string{"switch", "single", "api1", "--config", configFile}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	r, data = request(t, s, "POST", "/v1/chat/completions", basicBody, "")
	if r.StatusCode != 200 || !strings.Contains(string(data), `"model":"model1"`) {
		t.Fatal("CLI switch failed")
	}
	before, _ := os.ReadFile(c.RoutingFile)
	for _, args := range [][]string{{"single", "typo"}, {"round_robin"}, {"round_robin", "api1"}, {"single"}, {"bad"}, {"--config"}} {
		all := append([]string{"--config", configFile}, args...)
		if err := switchCommand(all, &out); err == nil {
			t.Fatal("invalid CLI accepted")
		}
		after, _ := os.ReadFile(c.RoutingFile)
		if !bytes.Equal(before, after) {
			t.Fatal("invalid switch changed file")
		}
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
			c := fixtureConfig(t, u1.URL, u2.URL)
			if mode == "single" {
				c.Routing = Policy{Mode: "single", Backend: "api1"}
			}
			_, s, _ := serveTest(t, c)
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
	c := p.snapshot(name).cache
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
			model, err := g.pool.discover(context.Background(), "api1")
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
	if model, err := g.pool.discover(context.Background(), "api1"); err != nil || model != "new" {
		t.Fatal("cache did not refresh")
	}
	expireModel(g.pool, "api1")
	for i := 0; i < 2; i++ {
		if _, err := g.pool.discover(context.Background(), "api1"); err == nil {
			t.Fatal("stale model reused")
		}
	}
	if calls.Load() != 3 {
		t.Fatal("failed discovery not cached or standby accessed")
	}
	expireModel(g.pool, "api1")
	if model, err := g.pool.discover(context.Background(), "api1"); err != nil || model != "recovered" {
		t.Fatal("discovery did not recover")
	}
}

func TestConcurrentSingleBackend(t *testing.T) {
	u1 := upstreamServer(t, "one", nil)
	u2 := upstreamServer(t, "two", nil)
	g, _, _ := serveTest(t, fixtureConfig(t, u1.URL, u2.URL))
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
			b := c.Backends["api1"]
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
	go func() { defer close(done); _, _ = g.pool.discover(ctx, "api1") }()
	<-started
	waitCtx, stop := context.WithCancel(context.Background())
	stop()
	if _, err := g.pool.discover(waitCtx, "api1"); err == nil {
		t.Fatal("cancelled waiter accepted")
	}
	cancel()
	<-done
	if model, err := g.pool.discover(context.Background(), "api1"); err != nil || model != "recovered" {
		t.Fatal("caller cancellation poisoned shared cache")
	}
}

func TestConfigValidationAndOrder(t *testing.T) {
	data := []byte(`{"routing":{"mode":"single","backend":"z"},"backends":{"z":{"adapter":"openai","url":"http://localhost/infer","models_url":"http://localhost/models"},"a":{"adapter":"openai","url":"http://localhost/infer","models_url":"http://localhost/models"}}}`)
	c, err := parseConfig(data, "config.json")
	if err != nil || c.validate() != nil || !reflect.DeepEqual(c.names, []string{"z", "a"}) {
		t.Fatal("config order/defaults changed")
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Routing.Mode = "round_robin" },
		func(c *Config) { c.TimeoutSeconds = 0 }, func(c *Config) { c.HTTPPool.MaxConnsPerHost = -1 },
		func(c *Config) { c.Backends["z"].URL = "http://user:secret@localhost/infer" },
		func(c *Config) { c.Backends["z"].Adapter = "bad" }, func(c *Config) { c.Backends["z"].UpstreamModel = json.RawMessage(`null`) },
		func(c *Config) { c.Backends["z"].Compatibility.DropFields = []string{"messages"} },
		func(c *Config) { c.Backends["z"].Compatibility.MetadataValues = "bad" },
		func(c *Config) { c.Backends["z"].Adapter = "template" },
		func(c *Config) { c.GatewayAPIKeyEnv = "AIGATE_MISSING_TEST_KEY" },
		func(c *Config) { c.Backends["z"].HeadersEnv = map[string]string{"X-Key": "AIGATE_MISSING_TEST_KEY"} },
		func(c *Config) { c.Routing = Policy{Mode: "single", Backend: "missing"} },
	} {
		c, _ := parseConfig(data, "config.json")
		mutate(c)
		if c.validate() == nil {
			t.Fatal("invalid config accepted")
		}
	}
	for _, data := range []string{`{}`, `null`, `{"backends":null}`, `{"backends":[]}`, `{"backends":{"a":{},"a":{}}}`} {
		if _, err := parseConfig([]byte(data), "config.json"); err == nil {
			t.Fatal("invalid backends accepted")
		}
	}
}
