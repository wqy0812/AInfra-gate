package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func profileFixture(t *testing.T) (*requestProfiler, *Config) {
	t.Helper()
	t.Setenv("AIGATE_PROFILE_KEY", "fixture-key")
	c := fixtureConfig(t, "http://127.0.0.1:1")
	c.Profile.Enabled = true
	c.Profile.Directory = t.TempDir()
	c.Profile.defaults(filepath.Join(c.Profile.Directory, "config.json"))
	p, e := newRequestProfiler(c.Profile)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.close)
	return p, c
}
func profileDrain(t *testing.T, p *requestProfiler) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(p.queue) > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	p.refreshFiles()
}
func TestProfileGatewayJSONAndSSE(t *testing.T) {
	t.Setenv("AIGATE_PROFILE_KEY", "fixture-key")
	u := upstreamServer(t, "actual-model", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if bytes.Contains(b, []byte(`"stream":true`)) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"private-reasoning\"}}]}\n\n")
			w.(http.Flusher).Flush()
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"private-answer\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3,\"prompt_tokens_details\":{\"cached_tokens\":8}}}\n\ndata: [DONE]\n\n")
			return
		}
		io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"private-answer"}}],"usage":{"prompt_tokens":10,"completion_tokens":3}}`)
	})
	c := fixtureConfig(t, u.URL)
	c.Profile.Enabled = true
	c.Profile.Directory = t.TempDir()
	g, s, _ := serveTest(t, c)
	for _, stream := range []string{"false", "true"} {
		res, _ := request(t, s, "POST", "/v1/chat/completions", `{"messages":[{"role":"user","content":"private-prompt"}],"tools":[{"type":"function","function":{"name":"private-tool"}}],"stream":`+stream+`}`, "")
		if res.StatusCode != 200 {
			t.Fatal(res.StatusCode)
		}
	}
	var metrics bytes.Buffer
	g.profile.metrics(&metrics)
	m := metrics.String()
	for _, want := range []string{`aigate_prompt_tokens_total{backend="api1",model="actual-model"} 20`, `aigate_completion_tokens_total{backend="api1",model="actual-model"} 6`, `aigate_cached_tokens_total{backend="api1",model="actual-model"} 8`, `aigate_prefix_repeated_total{backend="api1",model="actual-model",level="1"} 1`, `aigate_first_increment_seconds_count{backend="api1",model="actual-model"} 1`} {
		if !strings.Contains(m, want) {
			t.Errorf("missing %s", want)
		}
	}
	profileDrain(t, g.profile)
	files, _ := filepath.Glob(filepath.Join(c.Profile.Directory, "events-*.jsonl"))
	for _, f := range files {
		b, _ := os.ReadFile(f)
		for _, secret := range []string{"private-prompt", "private-answer", "private-reasoning", "private-tool"} {
			if bytes.Contains(b, []byte(secret)) {
				t.Fatalf("persisted %s", secret)
			}
		}
	}
	if strings.Contains(m, "private-") {
		t.Fatal("private label")
	}
}
func TestProfileBoundedIndexMissingUsageAndAnalysis(t *testing.T) {
	p, c := profileFixture(t)
	p.c.MaxIndexEntries = 2
	backend := c.Backends["api1"]
	_ = profileNow()
	for i, text := range []string{"a", "b", "a"} {
		r := p.begin(string(rune('a' + i)))
		p.input(r, []byte(`{"messages":[{"role":"user","content":"`+text+`"}]}`), backend, "api1", "model")
		p.end(r, &auditEntry{Outcome: "completed"}, false)
	}
	profileDrain(t, p)
	result, e := p.analyze(context.Background(), profileQuery{Start: p.oldest, End: profileNow()})
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(result)
	if !bytes.Contains(b, []byte(`"repeated":1`)) || !bytes.Contains(b, []byte(`"families":2`)) {
		t.Fatal(string(b))
	}
	var m bytes.Buffer
	p.metrics(&m)
	if !strings.Contains(m.String(), `aigate_usage_missing_total{backend="api1",model="model",field="prompt_tokens"} 3`) {
		t.Fatal(m.String())
	}
	if strings.Contains(m.String(), `aigate_prompt_tokens_total{backend="api1",model="model"} 1`) {
		t.Fatal("invented usage")
	}
	p.mu.Lock()
	if len(p.index) > 2 {
		t.Fatal("unbounded index")
	}
	p.mu.Unlock()
}
func TestProfileObserverUsageDoesNotCollectText(t *testing.T) {
	o := newSSEObserver(false)
	o.profileUsage = true
	o.Feed([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"secret\"}}]}}],\"usage\":{\"prompt_tokens\":4}}\r\n\r\n"))
	if o.Content != "" || len(o.Tools) != 0 || len(o.Usage) == 0 || o.FirstMS == nil {
		t.Fatalf("unexpected observer: %+v", o)
	}
}
func TestProfileAuthAndLimits(t *testing.T) {
	p, _ := profileFixture(t)
	for _, tc := range []struct {
		method, path, body, auth string
		code                     int
	}{{"GET", "/metrics", "", "", 401}, {"GET", "/metrics", "", "Bearer fixture-key", 200}, {"POST", "/profile/v1/analyses", `{"start":1,"end":100000}`, "Bearer fixture-key", 400}, {"POST", "/profile/v1/analyses", `{"start":1,"end":2}`, "Bearer fixture-key", 410}} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		r.Header.Set("Authorization", tc.auth)
		w := httptest.NewRecorder()
		p.handler().ServeHTTP(w, r)
		if w.Code != tc.code {
			t.Errorf("%s got %d want %d", tc.path, w.Code, tc.code)
		}
	}
}
func TestProfilePayloadLimitDoesNotRejectBusiness(t *testing.T) {
	p, c := profileFixture(t)
	p.c.MaxPayloadBytes = 4
	r := p.begin("limit")
	p.input(r, []byte(`{"messages":[]}`), c.Backends["api1"], "api1", "model")
	p.end(r, &auditEntry{Outcome: "completed"}, false)
	if r.quality != "payload_limit" {
		t.Fatal(r.quality)
	}
}

type profileBlockingWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w *profileBlockingWriter) Write(b []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-w.release
	return len(b), nil
}
func TestProfileSlowScrapeDoesNotHoldRequestLock(t *testing.T) {
	p, _ := profileFixture(t)
	w := &profileBlockingWriter{make(chan struct{}, 1), make(chan struct{})}
	done := make(chan struct{})
	go func() { p.metrics(w); close(done) }()
	<-w.entered
	finished := make(chan struct{})
	go func() {
		r := p.begin("concurrent")
		p.end(r, &auditEntry{Outcome: "completed"}, false)
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		close(w.release)
		t.Fatal("scrape blocks request instrumentation")
	}
	close(w.release)
	<-done
}
func TestProfileComparisonDomainAndRetention(t *testing.T) {
	p, c := profileFixture(t)
	b := c.Backends["api1"]
	for i, payload := range []string{`{"messages":[{"role":"user","content":"same"}],"tools":[{"name":"one"}]}`, `{"messages":[{"role":"user","content":"same"}],"tools":[{"name":"two"}]}`, `{"messages":[{"role":"user","content":"same"}],"tools":[{"name":"one"}]}`} {
		r := p.begin(strconv.Itoa(i))
		p.input(r, []byte(payload), b, "api1", "model")
		p.end(r, &auditEntry{Outcome: "completed"}, false)
	}
	var m bytes.Buffer
	p.metrics(&m)
	if !strings.Contains(m.String(), `aigate_prefix_repeated_total{backend="api1",model="model",level="1"} 1`) {
		t.Fatal(m.String())
	}
	old := filepath.Join(p.c.Directory, "events-1000000000000-old.jsonl")
	os.WriteFile(old, []byte("{}\n"), 0600)
	at := time.Now().Add(-73 * time.Hour)
	os.Chtimes(old, at, at)
	p.refreshFiles()
	if _, e := os.Stat(old); !os.IsNotExist(e) {
		t.Fatal("expired segment retained")
	}
}

func TestProfileQueueDropsInsteadOfWaiting(t *testing.T) {
	p, _ := profileFixture(t)
	p.diskMu.Lock()
	for i := 0; i < 5000; i++ {
		p.emit(profileEvent{Kind: "start", ID: "bounded", At: profileNow()})
	}
	dropped := p.dropped.Load()
	p.diskMu.Unlock()
	if dropped == 0 {
		t.Fatal("queue must be bounded")
	}
}
func TestProfileOversizedSSEOnlyDisablesObservation(t *testing.T) {
	t.Setenv("AIGATE_PROFILE_KEY", "fixture-key")
	content := strings.Repeat("a", 2<<20)
	u := upstreamServer(t, "model", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\""+content+"\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	})
	c := fixtureConfig(t, u.URL)
	c.Profile.Enabled = true
	c.Profile.Directory = t.TempDir()
	g, s, _ := serveTest(t, c)
	res, b := request(t, s, "POST", "/v1/chat/completions", `{"messages":[{"role":"user","content":"x"}],"stream":true}`, "")
	if res.StatusCode != 200 || !bytes.Contains(b, []byte(content)) || !bytes.Contains(b, []byte("[DONE]")) {
		t.Fatal("profiling changed stream")
	}
	var m bytes.Buffer
	g.profile.metrics(&m)
	if !strings.Contains(m.String(), `reason="sse_parse"} 1`) {
		t.Fatal("missing parse quality")
	}
}

func BenchmarkGatewayProfile(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(strconv.FormatBool(enabled), func(b *testing.B) {
			b.Setenv("AIGATE_PROFILE_KEY", "benchmark-only")
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					io.WriteString(w, `{"data":[{"id":"fixture"}]}`)
				} else {
					io.WriteString(w, goodResponse)
				}
			}))
			defer upstream.Close()
			raw := encodeJSON(map[string]any{"backends": map[string]any{"fixture": map[string]any{"adapter": "openai", "url": upstream.URL, "models_url": upstream.URL}}, "profile": map[string]any{"enabled": enabled, "directory": b.TempDir()}})
			c, err := parseConfig(raw, filepath.Join(b.TempDir(), "config.json"))
			if err != nil {
				b.Fatal(err)
			}
			g, err := newGateway(c, io.Discard)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			messages := make([]any, 277)
			for i := range messages {
				messages[i] = map[string]string{"role": "user", "content": strings.Repeat("x", 2000)}
			}
			body := encodeJSON(map[string]any{"messages": messages})
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(body))
				w := httptest.NewRecorder()
				g.ServeHTTP(w, r)
				if w.Code != 200 {
					b.Fatal(w.Code)
				}
			}
			b.StopTimer()
		})
	}
}

func TestProfileAnalysisSkipsIndexedUnrelatedSegments(t *testing.T) {
	p, _ := profileFixture(t)
	path := filepath.Join(p.c.Directory, "events-1000000000000-unrelated.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	size := int64(257 << 20)
	if err = f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	f.Close()
	os.WriteFile(path+".meta.json", encodeJSON(profileSegmentMeta{Min: 1, Max: 2, Bytes: size}), 0600)
	p.refreshFiles()
	_, err = p.analyze(context.Background(), profileQuery{Start: profileNow() - 10, End: profileNow()})
	if err != nil {
		t.Fatal(err)
	}
}

func TestProfileCountersSurviveGracefulRestart(t *testing.T) {
	p, c := profileFixture(t)
	r := p.begin("first")
	p.end(r, &auditEntry{Outcome: "completed"}, false)
	p.close()
	next, err := newRequestProfiler(c.Profile)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	r = next.begin("second")
	next.end(r, &auditEntry{Outcome: "completed"}, false)
	var out bytes.Buffer
	next.metrics(&out)
	if !strings.Contains(out.String(), "aigate_requests_started_total 2") {
		t.Fatal(out.String())
	}
	if next.counterStarted != p.counterStarted {
		t.Fatal("counter origin changed")
	}
}

func TestProfileSSEOutcomesWithoutManagement(t *testing.T) {
	finish := "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\n"
	done := "data: [DONE]\n\n"
	for _, tc := range []struct{ name, stream, outcome string }{
		{"completed", finish + done, "completed"},
		{"reasoning", "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"reasoning\"},\"finish_reason\":\"stop\"}]}\n\n" + done, "completed"},
		{"error", "data: {\"error\":{\"message\":\"backend failed\"}}\n\n" + done, "upstream_protocol_error"},
		{"missing_done", finish, "upstream_protocol_error"},
		{"missing_finish", "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n" + done, "upstream_protocol_error"},
		{"malformed", "data: {broken}\n\n" + done, "upstream_protocol_error"},
		{"oversized", "data: " + strings.Repeat("x", maxSSEEvent+1) + "\n\n" + done, "protocol_unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AIGATE_PROFILE_KEY", "fixture-key")
			u := upstreamServer(t, "model", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, tc.stream)
			})
			c := fixtureConfig(t, u.URL)
			c.Profile.Enabled = true
			c.Profile.Directory = t.TempDir()
			g, s, _ := serveTest(t, c)
			res, body := request(t, s, "POST", "/v1/chat/completions", `{"messages":[{"role":"user","content":"x"}],"stream":true}`, "")
			if res.StatusCode != 200 || string(body) != tc.stream {
				t.Fatal("profile changed forwarded response")
			}
			// Flush all lifecycle events before checking metrics and the detailed result.
			g.profile.close()
			g.profile.refreshFiles()
			var metrics bytes.Buffer
			g.profile.metrics(&metrics)
			want := `aigate_requests_ended_total{backend="api1",model="model",outcome="` + tc.outcome + `"} 1`
			if !strings.Contains(metrics.String(), want) {
				t.Fatalf("missing %s", want)
			}
			raw, err := g.profile.analyze(context.Background(), profileQuery{Start: g.profile.oldest, End: profileNow()})
			if err != nil {
				t.Fatal(err)
			}
			outcomes := raw.(map[string]any)["outcomes"].(map[string]int)
			if len(outcomes) != 1 || outcomes[tc.outcome] != 1 {
				t.Fatalf("unexpected persisted outcomes: %v", outcomes)
			}
		})
	}
}

func TestProfileDroppedEndAndLiveRequests(t *testing.T) {
	// No writer: a two-event queue deterministically retains both starts and drops the end.
	p := &requestProfiler{
		c:    ProfileConfig{Directory: t.TempDir(), RetentionHours: 72, MaxDiskBytes: 10 << 30},
		boot: "current", queue: make(chan []byte, 2),
		values: map[string]float64{}, types: map[string]string{}, groups: map[string]bool{},
	}
	live := p.begin("live")
	ended := p.begin("ended")
	p.end(ended, &auditEntry{Outcome: "completed"}, false)
	if p.dropped.Load() != 1 || p.values["aigate_requests_inflight"] != 1 || len(p.active) != 1 {
		t.Fatal("unexpected fault injection or live tracking")
	}
	data := append(<-p.queue, '\n')
	data = append(data, <-p.queue...)
	data = append(data, '\n')
	old := live.e
	old.Boot = "previous" // The sequence alone must not identify a live request after restart.
	data = append(data, encodeJSON(old)...)
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(p.c.Directory, "events-1000000000000-review.jsonl"), data, 0600); err != nil {
		t.Fatal(err)
	}
	// Simulate writer()'s completed-write timestamp, independently of filesystem mtime precision.
	p.lastWrite = profileNow()
	p.refreshFiles()
	query := profileQuery{Start: p.oldest, End: profileNow()}
	check := func(pending, unknown int) {
		t.Helper()
		raw, err := p.analyze(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		result := raw.(map[string]any)
		if result["requests"] != 3 || result["pending"] != pending || result["unknown"] != unknown {
			t.Fatalf("requests=%v pending=%v unknown=%v", result["requests"], result["pending"], result["unknown"])
		}
		if result["pending_as_of"].(float64) < result["as_of"].(float64) {
			t.Fatal("live snapshot precedes event cutoff")
		}
	}
	check(1, 2)
	p.end(live, &auditEntry{Outcome: "completed"}, false)
	// An end still queued for writing is also unknown, not falsely in flight.
	check(0, 3)
	if len(p.active) != 0 || p.values["aigate_requests_inflight"] != 0 {
		t.Fatal("completed requests retained in live set")
	}
}

func TestProfileSharedPrefixCoverage(t *testing.T) {
	p, c := profileFixture(t)
	payload := []byte(`{"messages":[{"role":"user","content":"same"}]}`)
	for i := 0; i < 3; i++ {
		if i == 2 {
			p.c.MaxPayloadBytes = 4
		}
		r := p.begin(strconv.Itoa(i))
		p.input(r, payload, c.Backends["api1"], "api1", "model")
		p.end(r, &auditEntry{Outcome: "completed"}, false)
	}
	p.close()
	p.refreshFiles()
	raw, err := p.analyze(context.Background(), profileQuery{Start: p.oldest, End: profileNow()})
	if err != nil {
		t.Fatal(err)
	}
	result := raw.(map[string]any)
	shared := result["observed_shared_prefix_bytes"].(map[string]any)
	quality := result["quality"].(map[string]any)
	if shared["count"] != 2 || shared["p50"] != float64(0) || shared["max"].(float64) <= 0 || shared["mean"].(float64) != shared["max"].(float64)/2 {
		t.Fatalf("unobserved prefix included or valid zero excluded: %v", shared)
	}
	if quality["shared_prefix_observed"] != 2 || quality["shared_prefix_missing"] != 1 {
		t.Fatalf("unexpected coverage: %v", quality)
	}
}

func TestProfileDiskLimitDefaultAndOverride(t *testing.T) {
	for _, tc := range []struct{ configured, want int64 }{{0, 10 << 30}, {3 << 30, 3 << 30}} {
		c := ProfileConfig{MaxDiskBytes: tc.configured}
		c.defaults(filepath.Join(t.TempDir(), "config.json"))
		if c.MaxDiskBytes != tc.want {
			t.Fatalf("configured %d: got %d, want %d", tc.configured, c.MaxDiskBytes, tc.want)
		}
	}
}

func TestProfileAnalysisAfterRestartWithCoarseFileTime(t *testing.T) {
	p, c := profileFixture(t)
	r := p.begin("before-restart")
	p.input(r, []byte(`{"messages":[{"role":"user","content":"same"}]}`), c.Backends["api1"], "api1", "model")
	p.end(r, &auditEntry{Outcome: "completed"}, false)
	p.close()
	files, err := filepath.Glob(filepath.Join(c.Profile.Directory, "events-*.jsonl"))
	if err != nil || len(files) == 0 {
		t.Fatal("missing persisted events", err)
	}
	// Emulate a filesystem that rounds mtime down below the event's arrival/end.
	coarse := r.start.Truncate(time.Second)
	for _, path := range files {
		if err := os.Chtimes(path, coarse, coarse); err != nil {
			t.Fatal(err)
		}
	}
	next, err := newRequestProfiler(c.Profile)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	raw, err := next.analyze(context.Background(), profileQuery{Start: r.e.Arrived, End: profileNow()})
	if err != nil {
		t.Fatal(err)
	}
	result := raw.(map[string]any)
	if result["requests"] != 1 || result["pending"] != 0 || result["unknown"] != 0 || result["outcomes"].(map[string]int)["completed"] != 1 {
		t.Fatalf("lost persisted lifecycle after restart: %v", result)
	}
}
