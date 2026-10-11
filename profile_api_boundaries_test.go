package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func profileAPIFixture() *requestProfiler {
	now := profileNow()
	return &requestProfiler{c: ProfileConfig{APIKey: "fixture-key"}, started: time.Now(), oldest: now - 120, lastWrite: now - 1,
		queue: make(chan []byte, 4), work: make(chan string, 1), jobs: map[string]*profileJob{}, active: map[uint64]profileLiveRequest{}}
}

func TestProfileAnalysisSkipsUnknownEventKinds(t *testing.T) {
	p := profileAPIFixture()
	stamp := profileNow() - 10
	path := filepath.Join(t.TempDir(), "events-v2-test.jsonl")
	var data []byte
	for i, kind := range []string{"", "unknown", "start", "end"} {
		id := "valid"
		if i < 2 {
			id = kind
		}
		event := profileEvent{Kind: kind, ID: id, Boot: "old", At: stamp, Arrived: stamp, Outcome: "completed", Duration: 1}
		data = append(data, append(encodeJSON(event), '\n')...)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	p.files = []string{path}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.analyzer(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	query := string(encodeJSON(profileQuery{Start: stamp - 1, End: stamp + 1}))
	w := profileHTTP(p, "POST", "/profile/v1/analyses", query, "fixture-key")
	var job profileJob
	if w.Code != 202 || json.Unmarshal(w.Body.Bytes(), &job) != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	deadline := time.Now().Add(time.Second)
	for {
		w = profileHTTP(p, "GET", "/profile/v1/analyses/"+job.ID, "", "fixture-key")
		if json.Unmarshal(w.Body.Bytes(), &job) != nil {
			t.Fatal(w.Body.String())
		}
		if job.State == "completed" {
			break
		}
		if job.State == "failed" || time.Now().After(deadline) {
			t.Fatal(w.Body.String())
		}
		time.Sleep(time.Millisecond)
	}
	result := job.Result.(map[string]any)
	if result["requests"] != float64(1) || result["quality"].(map[string]any)["invalid_records"] != float64(2) {
		t.Fatal(result)
	}
}
func profileHTTP(p *requestProfiler, method, path, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+key)
	w := httptest.NewRecorder()
	p.handler().ServeHTTP(w, r)
	return w
}

func TestProfileHTTPValidationAndRetention(t *testing.T) {
	p := profileAPIFixture()
	for _, tc := range []struct{ name, body string }{
		{"malformed", `{`}, {"unknown", `{"start":1,"end":2,"extra":true}`}, {"trailing", `{"start":1,"end":2} {}`},
		{"zero start", `{"start":0,"end":2}`}, {"reverse", `{"start":2,"end":1}`}, {"equal", `{"start":2,"end":2}`},
		{"too long", `{"start":1,"end":86402}`}, {"oversized", strings.Repeat(" ", 4097)},
		{"future", string(encodeJSON(profileQuery{Start: profileNow() - 1, End: profileNow() + 100}))},
		{"long backend", string(encodeJSON(profileQuery{Start: profileNow() - 10, End: profileNow(), Backend: strings.Repeat("b", 129)}))},
		{"long model", string(encodeJSON(profileQuery{Start: profileNow() - 10, End: profileNow(), Model: strings.Repeat("m", 257)}))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := profileHTTP(p, "POST", "/profile/v1/analyses", tc.body, "fixture-key")
			if w.Code != 400 {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
		})
	}
	if w := profileHTTP(p, "GET", "/profile/v1/status", "", "wrong"); w.Code != 401 {
		t.Fatal(w.Code)
	}
	if w := profileHTTP(p, "GET", "/profile/v1/missing", "", "fixture-key"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	if w := profileHTTP(p, "POST", "/profile/v1/analyses", `{"start":1,"end":2}`, "fixture-key"); w.Code != 410 {
		t.Fatal(w.Code)
	}
	p.lastWrite = 0
	body := string(encodeJSON(profileQuery{Start: profileNow() - 10, End: profileNow()}))
	if w := profileHTTP(p, "POST", "/profile/v1/analyses", body, "fixture-key"); w.Code != 410 {
		t.Fatal(w.Code)
	}
}

func TestProfileHTTPStatusMetricsAndJobLimits(t *testing.T) {
	p := profileAPIFixture()
	p.c.APIKey = ""
	p.c.APIKeyEnv = "AIGATE_PROFILE_TEST_KEY"
	t.Setenv(p.c.APIKeyEnv, "fixture-key")
	p.storedBytes = 12
	p.dropped.Store(2)
	p.writes.Store(3)
	w := profileHTTP(p, "GET", "/profile/v1/status", "", "fixture-key")
	var status map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || status["stored_bytes"] != float64(12) || status["dropped_events"] != float64(2) {
		t.Fatal(w.Code, w.Body)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("status can be cached")
	}
	w = profileHTTP(p, "GET", "/metrics", "", "fixture-key")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "aigate_profile_write_errors_total{request_scope=\"all\"} 3") {
		t.Fatal(w.Code, w.Body)
	}
	body := string(encodeJSON(profileQuery{Start: profileNow() - 10, End: profileNow()}))
	p.jobs["expired"] = &profileJob{ID: "expired", State: "completed", Created: profileNow() - 601}
	w = profileHTTP(p, "POST", "/profile/v1/analyses", body, "fixture-key")
	var job profileJob
	if w.Code != 202 || json.Unmarshal(w.Body.Bytes(), &job) != nil || job.State != "queued" || p.jobs["expired"] != nil {
		t.Fatal(w.Code, w.Body)
	}
	if w := profileHTTP(p, "GET", "/profile/v1/analyses/"+job.ID, "", "fixture-key"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := profileHTTP(p, "POST", "/profile/v1/analyses", body, "fixture-key"); w.Code != 429 || len(p.jobs) != 1 {
		t.Fatal("queue overflow leaked a job", w.Code, len(p.jobs))
	}
	for i := 0; i < 16; i++ {
		id := randomID()
		p.jobs[id] = &profileJob{ID: id, State: "running", Created: profileNow() - 601}
	}
	if w := profileHTTP(p, "POST", "/profile/v1/analyses", body, "fixture-key"); w.Code != 429 {
		t.Fatal("running jobs were evicted", w.Code)
	}
	if w := profileHTTP(p, "GET", "/profile/v1/analyses/missing", "", "fixture-key"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	p.jobs["old"] = &profileJob{ID: "old", State: "completed", Created: profileNow() - 601}
	if w := profileHTTP(p, "GET", "/profile/v1/analyses/old", "", "fixture-key"); w.Code != 404 {
		t.Fatal(w.Code)
	}
}

func TestProfileAnalyzerCompletesAndReportsMissingSegments(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "missing segment"}[fail], func(t *testing.T) {
			p := profileAPIFixture()
			if fail {
				p.files = []string{filepath.Join(t.TempDir(), "missing.jsonl")}
			}
			p.jobs["job"] = &profileJob{ID: "job", State: "queued", Query: profileQuery{Start: p.oldest, End: profileNow()}}
			p.work <- "job"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() { defer close(done); p.analyzer(ctx) }()
			deadline := time.Now().Add(2 * time.Second)
			for {
				p.jobsMu.Lock()
				j := *p.jobs["job"]
				p.jobsMu.Unlock()
				if j.State == "completed" || j.State == "failed" {
					if fail && (j.State != "failed" || !strings.Contains(j.Error, "segment no longer available")) {
						t.Fatal(j)
					}
					if !fail && (j.State != "completed" || j.Result == nil) {
						t.Fatal(j)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("analyzer did not complete")
				}
				time.Sleep(time.Millisecond)
			}
			cancel()
			<-done
		})
	}
}

func TestProfileAnalysisBudgetsAndCancellation(t *testing.T) {
	p := profileAPIFixture()
	q := profileQuery{Start: p.oldest, End: profileNow()}
	path := filepath.Join(t.TempDir(), "oversized.jsonl")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Truncate(256<<20 + 1); e != nil {
		t.Fatal(e)
	}
	f.Close()
	p.files = []string{path}
	if _, e := p.analyze(context.Background(), q); e == nil || !strings.Contains(e.Error(), "scan budget") {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := p.analyze(ctx, q); e != context.Canceled {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, []byte(strings.Repeat("a", 32769)), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := p.analyze(context.Background(), q); e == nil || !strings.Contains(e.Error(), "event read failed") {
		t.Fatal(e)
	}
}

func TestJSONTruthinessAndIdentifiers(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want bool
	}{{`null`, false}, {`false`, false}, {`true`, true}, {`""`, false}, {`"0"`, true}, {`[]`, false}, {`[0]`, true}, {`{}`, false}, {`{"x":0}`, true}, {`0`, false}, {`-1`, true}, {`{`, false}} {
		if got := truthy(json.RawMessage(tc.raw)); got != tc.want {
			t.Fatal(tc.raw, got)
		}
	}
	for _, value := range []string{"", strings.Repeat("x", 129), "has/slash", "中文", "has space"} {
		if validOperationID(value) {
			t.Fatal(value)
		}
	}
	if !validOperationID("ABC_xyz-012") {
		t.Fatal("valid ID rejected")
	}
	if (&GatewayError{Message: "failure"}).Error() != "failure" {
		t.Fatal("gateway error message")
	}
}
