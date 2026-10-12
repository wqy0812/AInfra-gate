package aigate

import (
	"bytes"
	"context"
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

func liveValues(p *requestProfiler, now time.Time) map[string]float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	v := map[string]float64{}
	profileLiveMetrics(now, p.active, p.liveBackends, v, map[string]string{})
	return v
}

func liveValue(v map[string]float64, name, backend string, extra ...string) float64 {
	tags := profileExtra("", "backend", backend)
	for i := 0; i < len(extra); i += 2 {
		tags = profileExtra(tags, extra[i], extra[i+1])
	}
	return v["aigate_"+name+tags]
}

func TestLiveWaitIdleStagesAndRetirement(t *testing.T) {
	p, _ := profileFixture(t)
	r := p.begin("first")
	check := func(name, backend string, want float64, extra ...string) {
		t.Helper()
		got := liveValue(liveValues(p, r.start.Add(35*time.Second)), name, backend, extra...)
		if got != want {
			t.Fatalf("%s %s %v: got %v, want %v", name, backend, extra, got, want)
		}
	}
	check("streams_waiting_first_output", "unrouted", 1)
	check("stream_first_output_wait_max_seconds", "unrouted", 35)
	r.liveBackend("dcu")
	check("streams_waiting_first_output", "unrouted", 0)
	check("streams_waiting_first_output", "dcu", 1)
	check("inflight_oldest_stage", "dcu", 1, "stage", "routing_discovery")
	r.liveStage("waiting_first_output")
	o := newSSEObserver(false)
	o.Feed([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"))
	o.LastUsefulAt = r.start
	r.liveObserve(o)
	r.liveStreamRead(o)
	check("streams_waiting_first_output", "dcu", 0)
	check("stream_idle_max_seconds", "dcu", 35)
	for _, threshold := range profileIdleThresholds {
		want := float64(0)
		if threshold <= 35 {
			want = 1
		}
		check("stream_idle_requests", "dcu", want, "threshold_seconds", strconv.Itoa(threshold))
		v := liveValues(p, r.start.Add(time.Duration(threshold)*time.Second))
		if liveValue(v, "stream_idle_requests", "dcu", "threshold_seconds", strconv.Itoa(threshold)) != 1 {
			t.Fatal("inclusive threshold")
		}
		v = liveValues(p, r.start.Add(time.Duration(threshold)*time.Second-time.Nanosecond))
		if liveValue(v, "stream_idle_requests", "dcu", "threshold_seconds", strconv.Itoa(threshold)) != 0 {
			t.Fatal("below threshold")
		}
	}
	r.liveStage("writing_client")
	check("inflight_oldest_stage", "dcu", 1, "stage", "writing_client")
	check("inflight_oldest_age_seconds", "dcu", 35)
	o.LastUsefulAt = r.start.Add(34 * time.Second)
	r.liveObserve(o)
	check("stream_idle_max_seconds", "dcu", 1)
	check("stream_idle_requests", "dcu", 0, "threshold_seconds", "5")
	r.liveTerminal()
	check("stream_idle_max_seconds", "dcu", 0)
	check("inflight_oldest_age_seconds", "dcu", 35)
	p.end(r, &auditEntry{Outcome: "completed"}, false)
	r.liveStage("streaming") // Late updates cannot resurrect an ended request.
	for name, value := range liveValues(p, time.Now()) {
		if name == "aigate_live_backend_groups" {
			continue
		}
		if value != 0 {
			t.Fatal("ended request retained")
		}
	}
}

func TestLiveSSEUsefulOnlyAndCompletion(t *testing.T) {
	for _, delta := range []string{`{"content":"x"}`, `{"reasoning_content":"x"}`, `{"reasoning":"x"}`, `{"refusal":"x"}`, `{"tool_calls":[{"index":0,"function":{"arguments":"x"}}]}`} {
		t.Run(delta, func(t *testing.T) {
			o := newSSEObserver(false)
			event := []byte(`data: {"choices":[{"delta":` + delta + "}]}\n\n")
			o.Feed(event[:len(event)-1])
			if !o.LastUsefulAt.IsZero() {
				t.Fatal("partial event counted")
			}
			o.Feed(event[len(event)-1:])
			if o.LastUsefulAt.IsZero() {
				t.Fatal("useful event missed")
			}
			old := o.LastUsefulAt
			o.Feed([]byte(": heartbeat\n\ndata: {\"choices\":[{\"delta\":{}}]}\n\ndata: {\"choices\":[],\"usage\":{\"completion_tokens\":1}}\n\n"))
			if !o.LastUsefulAt.Equal(old) {
				t.Fatal("non-content reset idle")
			}
			o.LastUsefulAt = old.Add(-time.Second)
			o.Feed(event)
			if !o.LastUsefulAt.After(old.Add(-time.Second)) {
				t.Fatal("later useful event not tracked")
			}
		})
	}
	for _, tc := range []struct {
		name, wire        string
		n                 int
		terminal, unknown bool
	}{
		{"empty_complete", "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", 1, true, false},
		{"missing_done", "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n", 1, false, false},
		{"missing_choice", "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", 2, false, false},
		{"multi_complete", "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"},{\"index\":1,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", 2, true, false},
		{"error", "data: {\"error\":{\"message\":\"failed\"}}\n\n", 1, true, false},
		{"invalid", "data: {invalid}\n\n", 1, false, true},
		{"limit", "data: " + strings.Repeat("x", maxSSEEvent+1), 1, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := profileFixture(t)
			r := p.begin("wire")
			o := newSSEObserver(false)
			o.expectedChoices = tc.n
			o.Feed([]byte(tc.wire))
			r.liveObserve(o)
			r.liveStreamRead(o)
			v := liveValues(p, r.start.Add(time.Minute))
			waiting := float64(1)
			if tc.terminal || tc.unknown {
				waiting = 0
			}
			if liveValue(v, "streams_waiting_first_output", "unrouted") != waiting || liveValue(v, "stream_observation_unknown", "unrouted") != float64(profileBoolInt(tc.unknown)) {
				t.Fatal("incorrect stream classification", v)
			}
			if tc.terminal && liveValue(v, "inflight_oldest_stage", "unrouted", "stage", "finishing_stream") != 1 {
				t.Fatal("missing finishing stage")
			}
			p.end(r, &auditEntry{Outcome: "incomplete"}, true)
		})
	}
}

func TestLiveOldestBoundsAndCheckpoint(t *testing.T) {
	p, c := profileFixture(t)
	first := p.begin("a")
	first.liveBackend("same")
	second := p.begin("b")
	second.liveBackend("same")
	second.liveStage("reading_response")
	second.updateLive(func(s *profileLiveRequest) { s.start = first.start })
	v := liveValues(p, first.start.Add(time.Second))
	if liveValue(v, "inflight_oldest_stage", "same", "stage", "routing_discovery") != 1 || liveValue(v, "streams_waiting_first_output", "same") != 2 {
		t.Fatal("oldest tie")
	}
	p.end(first, &auditEntry{Outcome: "completed"}, false)
	v = liveValues(p, time.Now())
	if liveValue(v, "inflight_oldest_stage", "same", "stage", "reading_response") != 1 {
		t.Fatal("oldest not handed over")
	}
	for i := 0; i < 100; i++ {
		r := p.begin("bounded")
		r.liveBackend(fmt.Sprintf("backend-%d", i))
		p.end(r, &auditEntry{Outcome: "completed"}, false)
	}
	if len(p.liveBackends) != 66 || !p.liveBackends["overflow"] {
		t.Fatal("backend cardinality", len(p.liveBackends))
	}
	var out bytes.Buffer
	p.metrics(&out)
	p.close()
	saved, err := os.ReadFile(filepath.Join(c.Profile.Directory, "counters-metric-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(saved, []byte("aigate_stream_idle")) || bytes.Contains(saved, []byte("aigate_inflight_oldest")) {
		t.Fatal("persisted live gauges")
	}
	next, err := newRequestProfiler(c.Profile)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	for name, value := range liveValues(next, time.Now()) {
		if name == "aigate_live_backend_groups" {
			continue
		}
		if value != 0 {
			t.Fatal("restored phantom live state")
		}
	}
	if len(next.active) != 0 {
		t.Fatal("restored request")
	}
	_ = second
}

func waitLiveStage(t *testing.T, p *requestProfiler, stage string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		v := liveValues(p, time.Now())
		for key, value := range v {
			if strings.HasPrefix(key, "aigate_inflight_requests_by_stage{") && strings.Contains(key, `stage="`+stage+`"`) && value == 1 {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("stage not observed", stage, liveValues(p, time.Now()))
}

func TestLiveGatewayBeforeCompletion(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(strconv.FormatBool(stream), func(t *testing.T) {
			t.Setenv("AIGATE_PROFILE_KEY", "fixture-key")
			discovery, headers, body, more, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			releaseAll := func() {
				for _, c := range []chan struct{}{discovery, headers, body, more, finish} {
					select {
					case <-c:
					default:
						close(c)
					}
				}
			}
			defer releaseAll()
			u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/models") {
					<-discovery
					io.WriteString(w, `{"data":[{"id":"m"}]}`)
					return
				}
				<-headers
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
				} else {
					w.Header().Set("Content-Type", "application/json")
				}
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				<-body
				if stream {
					io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
					w.(http.Flusher).Flush()
					<-more
					io.WriteString(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
					w.(http.Flusher).Flush()
					<-finish
				} else {
					io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`)
				}
			}))
			defer func() { releaseAll(); u.Close() }()
			c := fixtureConfig(t, u.URL)
			c.Profile.Enabled = true
			c.Profile.Directory = t.TempDir()
			g, s, _ := serveTest(t, c)
			done := make(chan error, 1)
			go func() {
				resp, err := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(fmt.Sprintf(`{"messages":[{"role":"user","content":"x"}],"stream":%t}`, stream)))
				if err == nil {
					_, err = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				done <- err
			}()
			waitLiveStage(t, g.profile, "routing_discovery")
			v := liveValues(g.profile, time.Now().Add(30*time.Second))
			if liveValue(v, "streams_waiting_first_output", "api1") != float64(profileBoolInt(stream)) {
				t.Fatal("discovery wait not classified")
			}
			close(discovery)
			waitLiveStage(t, g.profile, "waiting_upstream_headers")
			close(headers)
			if stream {
				waitLiveStage(t, g.profile, "waiting_first_output")
				if liveValue(liveValues(g.profile, time.Now().Add(time.Minute)), "stream_backend_wait_max_seconds", "api1") < 60 {
					t.Fatal("first output read not timed")
				}
			} else {
				waitLiveStage(t, g.profile, "reading_response")
			}
			close(body)
			if stream {
				waitLiveStage(t, g.profile, "streaming")
				if liveValue(liveValues(g.profile, time.Now().Add(time.Minute)), "stream_backend_wait_max_seconds", "api1") < 60 {
					t.Fatal("subsequent output read not timed")
				}
				close(more)
				waitLiveStage(t, g.profile, "finishing_stream")
				if liveValue(liveValues(g.profile, time.Now().Add(time.Minute)), "stream_idle_requests", "api1", "threshold_seconds", "5") != 0 {
					t.Fatal("completed stream still stalled")
				}
				close(finish)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("request did not finish")
			}
		})
	}
}

type liveBlockedWriter struct {
	header           http.Header
	entered, release chan struct{}
	once             sync.Once
}

func (w *liveBlockedWriter) Header() http.Header { return w.header }
func (w *liveBlockedWriter) WriteHeader(int)     {}
func (w *liveBlockedWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(b), nil
}
func (w *liveBlockedWriter) Flush() {}

func TestLiveClientWriteAndConcurrentScrapes(t *testing.T) {
	p, _ := profileFixture(t)
	r := p.begin("write")
	w := &liveBlockedWriter{header: http.Header{}, entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { sendJSON(w, &auditEntry{profile: r}, 200, []byte(`{}`)); close(done) }()
	<-w.entered
	waitLiveStage(t, p, "writing_client")
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				p.metrics(io.Discard)
			}
		}()
	}
	for i := 0; i < 100; i++ {
		other := p.begin("concurrent")
		other.liveBackend("dcu")
		other.liveStage("streaming")
		p.end(other, &auditEntry{Outcome: "incomplete"}, true)
	}
	wg.Wait()
	close(w.release)
	<-done
	p.end(r, &auditEntry{Outcome: "completed"}, false)
	if len(p.active) != 0 {
		t.Fatal("leaked requests")
	}
}

func TestLiveGatewayCancellationAndTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(strconv.FormatBool(timeout), func(t *testing.T) {
			t.Setenv("AIGATE_PROFILE_KEY", "fixture-key")
			u := upstreamServer(t, "m", func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
			})
			c := fixtureConfig(t, u.URL)
			c.Profile.Enabled = true
			c.Profile.Directory = t.TempDir()
			if timeout {
				c.TimeoutSeconds = .1
			}
			g, s, _ := serveTest(t, c)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, "POST", s.URL+"/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"x"}],"stream":true}`))
			done := make(chan struct{})
			go func() {
				resp, err := http.DefaultClient.Do(req)
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				close(done)
			}()
			waitLiveStage(t, g.profile, "waiting_upstream_headers")
			if !timeout {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("not ended")
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				g.profile.mu.Lock()
				n := len(g.profile.active)
				g.profile.mu.Unlock()
				if n == 0 {
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Fatal("cancelled request retained")
		})
	}
}
