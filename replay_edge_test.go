package aigate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func normalizedReplayRequest(t *testing.T, id, recording, segment, target string) replayRequest {
	t.Helper()
	req := replayRequest{RunID: id, Recording: replayRecordingRef{GatewayID: "g1", RecordingID: recording, Segments: []string{segment}}, Filter: replayFilter{Status: "2xx"}, Target: target, Mode: "concurrency", Concurrency: 1}
	if err := req.normalize(); err != nil {
		t.Fatal(err)
	}
	return req
}

func TestReplayStartIsIdempotentForConcurrentDeliveries(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-i", 3)
	rec.router.delay = 100 * time.Millisecond
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	for round := 0; round < 5; round++ {
		req := normalizedReplayRequest(t, fmt.Sprintf("dup-%d", round), "rec-i", summary.Segments[0].Segment, "r1")
		gate := make(chan struct{})
		codes := make([]string, 8)
		var wg sync.WaitGroup
		for i := range codes {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-gate
				_, status, code := f.svc.start(req)
				codes[i] = fmt.Sprint(status, code)
			}(i)
		}
		close(gate)
		wg.Wait()
		for _, c := range codes {
			if c != "200" {
				t.Fatalf("round %d: %v", round, codes)
			}
		}
		f.svc.stop(req.RunID)
		f.wait(t, req.RunID)
	}
}

func TestReplayKeepsRunLimitWhileServing(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-k", 1)
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	runs := filepath.Join(f.svc.dataRoot, "runs")
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < replayRunsKept; i++ {
		id := fmt.Sprintf("old-%03d", i)
		os.Mkdir(filepath.Join(runs, id), 0700)
		end := old.Add(time.Duration(i) * time.Minute)
		view := replayRunView{RunID: id, State: "completed", CreatedAt: end, EndedAt: &end}
		if _, err := recordingAtomicJSON(filepath.Join(runs, id), "state.json", view); err != nil {
			t.Fatal(err)
		}
	}
	req := normalizedReplayRequest(t, "fresh", "rec-k", summary.Segments[0].Segment, "r1")
	if _, status, code := f.svc.start(req); status != 200 {
		t.Fatal(status, code)
	}
	f.wait(t, "fresh")
	entries, _ := os.ReadDir(runs)
	if len(entries) != replayRunsKept {
		t.Fatalf("%d run directories", len(entries))
	}
	if _, err := os.Stat(filepath.Join(runs, "old-000")); !os.IsNotExist(err) {
		t.Fatal("oldest run kept", err)
	}
	if _, err := os.Stat(filepath.Join(runs, "fresh", "state.json")); err != nil {
		t.Fatal(err)
	}
}

func TestReplayStopDuringPlanningEndsStopped(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-p", 1)
	f := newReplayFixture(t, rec.root, map[string]string{"r1": rec.proxy.URL})
	req := normalizedReplayRequest(t, "planning", "rec-p", summary.Segments[0].Segment, "r1")
	dir := f.svc.runDir(req.RunID)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// This is the state stop() leaves while the plan is still being built.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &replayRun{svc: f.svc, req: req, dir: dir, cancel: cancel, done: make(chan struct{}), state: "stopping", created: time.Now().UTC()}
	r.stopped.Store(true)
	if view := r.view(); view.Progress.Elapsed != 0 {
		t.Fatalf("elapsed before sending: %v", view.Progress.Elapsed)
	}
	r.execute(ctx)
	if view := r.view(); view.State != "stopped" || view.Error != "" {
		t.Fatalf("%+v", view)
	}
}

func TestReplayAssertsTheCheckedTargetOnEveryRequest(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "rec-t", 3)
	upstream, _ := url.Parse(rec.proxy.URL)
	forward := httputil.NewSingleHostReverseProxy(upstream)
	// The status reads like the proxy before a reconfiguration; requests reach the changed proxy.
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/runtime/v1/status" {
			json.NewEncoder(w).Encode(map[string]any{"service": "aigate-proxy", "service_id": "p", "router_id": "r1", "config_digest": strings.Repeat("a", 64)})
			return
		}
		forward.ServeHTTP(w, r)
	}))
	defer front.Close()
	before := rec.router.posts.Load()
	f := newReplayFixture(t, rec.root, map[string]string{"r1": front.URL, "other": rec.proxy.URL})
	req := normalizedReplayRequest(t, "changed", "rec-t", summary.Segments[0].Segment, "r1")
	f.svc.start(req)
	view := f.wait(t, "changed")
	if view.Progress.Completed != 0 || view.Progress.Failed != 3 || view.LastError != "http_409" || rec.router.posts.Load() != before {
		t.Fatalf("%s", encodeJSON(view))
	}
	// A target name that is not the proxy's Router would lock the wrong Router on the platform.
	req = normalizedReplayRequest(t, "mismatch", "rec-t", summary.Segments[0].Segment, "other")
	f.svc.start(req)
	if view := f.wait(t, "mismatch"); view.State != "failed" || view.Error != "target_router_mismatch" {
		t.Fatalf("%+v", view)
	}
}
