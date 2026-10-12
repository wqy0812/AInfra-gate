package aigate

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNonstreamContributesGeneralMetricsButNotStreamObservations(t *testing.T) {
	for _, status := range []int{200, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			reached, release := make(chan struct{}), make(chan struct{})
			g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
				close(reached)
				<-release
				return generationResponse(status, "application/json", goodResponse), nil
			})
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- generationRequest(g, false) }()
			<-reached
			g.profile.mu.Lock()
			active, groups, started, nonstream := len(g.profile.active), len(g.profile.groups), g.profile.values["aigate_requests_started_total"], g.profile.values["aigate_nonstream_requests_total"]
			g.profile.mu.Unlock()
			close(release)
			result := <-done
			if result.Code != status || active != 1 || groups != 1 || started != 1 || nonstream != 1 {
				t.Fatalf("status=%d active=%d groups=%d started=%v nonstream=%v", result.Code, active, groups, started, nonstream)
			}
			// Omitted stream is also a full non-streaming observation.
			g.client.Transport = generationTransport(func(r *http.Request) (*http.Response, error) {
				return generationResponse(200, "application/json", `{"choices":[]}`), nil
			})
			g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(basicBody)))
			m := generationMetrics(g.profile)
			if !strings.Contains(m, `aigate_nonstream_requests_total{request_scope="nonstreaming"} 2`) {
				t.Fatal(m)
			}
			for _, required := range []string{"aigate_generation_requests_ended_total{", "aigate_request_duration_seconds_count{", "aigate_requests_routed_total{"} {
				if !strings.Contains(m, required) {
					t.Fatal("missing nonstream metric", required, m)
				}
			}
			if strings.Contains(m, "aigate_first_increment_seconds_count{") {
				t.Fatal("nonstream recorded first increment")
			}
			if liveValue(liveValues(g.profile, time.Now()), "streams_waiting_first_output", "api1") != 0 {
				t.Fatal("nonstream counted as waiting stream")
			}

			profileDrain(t, g.profile)
			files, _ := filepath.Glob(filepath.Join(g.config.Profile.Directory, "events-v2-*.jsonl"))
			for _, file := range files {
				data, _ := os.ReadFile(file)
				if len(bytes.TrimSpace(data)) == 0 {
					t.Fatal("missing nonstream profile events")
				}
			}
		})
	}
}

func TestProfileCheckpointDoesNotRestoreMixedScope(t *testing.T) {
	p, c := profileFixture(t)
	p.close()
	scoped := filepath.Join(c.Profile.Directory, "counters-metric-v2.json")
	legacy := filepath.Join(c.Profile.Directory, "counters.json")
	if err := os.Rename(scoped, legacy); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(legacy)
	next, err := newRequestProfiler(c.Profile)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	if next.counterStarted == p.counterStarted {
		t.Fatal("restored mixed-scope checkpoint")
	}
	after, _ := os.ReadFile(legacy)
	if !bytes.Equal(before, after) {
		t.Fatal("changed rollback checkpoint")
	}
}

func TestProfileAnalysisIncludesBothRequestTypes(t *testing.T) {
	p, _ := profileFixture(t)
	begin := profileNow()
	// Retained legacy events contain both request types, including untyped starts.
	for i, stream := range []bool{false, true} {
		id := []string{"nonstream", "stream"}[i]
		for _, kind := range []string{"start", "input", "end"} {
			p.emit(profileEvent{Kind: kind, ID: id, Boot: "old", Seq: uint64(i + 1), At: begin, Arrived: begin, Stream: stream && kind != "start", Outcome: "completed", Duration: 1})
		}
	}
	profileDrain(t, p)
	result, err := p.analyze(context.Background(), profileQuery{Start: begin, End: profileNow() + 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.(map[string]any)["requests"] != 2 {
		t.Fatal(result)
	}
}

func TestStreamingEventsAndMetricScope(t *testing.T) {
	g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
		return generationResponse(503, "text/plain", "error"), nil
	})
	generationRequest(g, true)
	profileDrain(t, g.profile)
	files, _ := filepath.Glob(filepath.Join(g.config.Profile.Directory, "events-v2-*.jsonl"))
	count := 0
	for _, file := range files {
		data, _ := os.ReadFile(file)
		decoder := json.NewDecoder(bytes.NewReader(data))
		for {
			var e profileEvent
			err := decoder.Decode(&e)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if !e.Stream {
				t.Fatal("unscoped new event")
			}
			count++
		}
	}
	if count != 3 {
		t.Fatalf("events=%d", count)
	}
	for _, line := range strings.Split(generationMetrics(g.profile), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		scope := `request_scope="all"`
		if strings.HasPrefix(line, "aigate_stream_") || strings.HasPrefix(line, "aigate_streams_waiting_first_output") || strings.HasPrefix(line, "aigate_first_increment_seconds_") {
			scope = `request_scope="streaming"`
		}
		if strings.HasPrefix(line, "aigate_nonstream_requests_total{") {
			scope = `request_scope="nonstreaming"`
		}
		if !strings.Contains(line, scope) {
			t.Fatal(line)
		}
	}
	// Start time includes request parsing, even though admission occurs after parsing.
	p, _ := profileFixture(t)
	arrival := time.Now().Add(-time.Second)
	r := p.begin("aged", arrival)
	if liveValue(liveValues(p, time.Now()), "inflight_oldest_age_seconds", "unrouted") < 1 {
		t.Fatal("lost pre-parse request age")
	}
	p.end(r, &auditEntry{Outcome: "completed"}, false)
}

func TestTemplateProfileRetainsUpstreamUsage(t *testing.T) {
	g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
		return generationResponse(200, "application/json", `{"answer":"ok","usage":{"prompt_tokens":17,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":5},"private":"not-a-counter"}}`), nil
	})
	b := g.config.Backend
	b.Adapter = "template"
	b.RequestTemplate = json.RawMessage(`{"messages":{"$ref":"request.messages"}}`)
	b.ResponseContentPath = "answer"
	w := generationRequest(g, false)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"content":"ok"`) {
		t.Fatalf("template response: %d %s", w.Code, w.Body.String())
	}
	m := generationMetrics(g.profile)
	for _, want := range []string{
		`aigate_prompt_tokens_total{backend="api1",model="real",request_scope="all"} 17`,
		`aigate_completion_tokens_total{backend="api1",model="real",request_scope="all"} 3`,
		`aigate_cached_tokens_total{backend="api1",model="real",request_scope="all"} 5`,
	} {
		if !strings.Contains(m, want+"\n") {
			t.Errorf("missing upstream usage: %s", want)
		}
	}
	g.profile.close()
	files, _ := filepath.Glob(filepath.Join(g.config.Profile.Directory, "events-v2-*.jsonl"))
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil || bytes.Contains(data, []byte("not-a-counter")) {
			t.Fatal("profile retained arbitrary usage content", err)
		}
	}
}
