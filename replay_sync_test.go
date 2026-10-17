package aigate

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func waitPrepared(t *testing.T, f *replayFixture, id string) replayRunView {
	t.Helper()
	end := time.Now().Add(3 * time.Second)
	for time.Now().Before(end) {
		view, _ := f.svc.loadRun(id)
		if view.State == "prepared" {
			return view
		}
		if view.EndedAt != nil {
			t.Fatalf("prepare failed: %+v", view)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("prepare timeout")
	return replayRunView{}
}

func overlayFixture(t *testing.T) (string, replayRequest) {
	t.Helper()
	root, req := syntheticReplay(t, 2, .08, `{"stream":true}`)
	source := filepath.Join(root, "g1", "synthetic")
	dest := filepath.Join(root, "g2", "other")
	if err := os.MkdirAll(dest, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(dest, os.DirFS(source)); err != nil {
		t.Fatal(err)
	}
	_, err := recordingAtomicJSON(dest, "session.json", recordingSessionFile{Schema: recordingSchema, GatewayID: "g2", RecordingID: "other"})
	if err != nil {
		t.Fatal(err)
	}
	req.Recordings = []replayRecordingRef{req.Recording, {GatewayID: "g2", RecordingID: "other", Segments: req.Recording.Segments}}
	req.Recording = replayRecordingRef{}
	req.PrepareOnly = true
	return root, req
}

func TestReplayOverlayPlanAndValidation(t *testing.T) {
	root, req := overlayFixture(t)
	if err := req.normalize(); err != nil {
		t.Fatal(err)
	}
	plan, err := buildReplayPlan(root, req)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Planned != 4 || plan.DurationSeconds != 1800 {
		t.Fatalf("bad overlay: %+v", plan)
	}
	for i, item := range plan.items {
		if item.Sequence != i+1 || item.OffsetSeconds != float64(i/2)*.08 || item.GatewayID == "" {
			t.Fatalf("bad item: %+v", item)
		}
	}
	req.Mode, req.Concurrency = "concurrency", 2
	if req.normalize() == nil {
		t.Fatal("concurrent overlay accepted")
	}
	req.Mode, req.Concurrency = "timed", 0
	req.Recordings[1] = req.Recordings[0]
	if req.normalize() == nil {
		t.Fatal("duplicate accepted")
	}
}

func TestReplayOverlayRejectsMixedSelectorsAndEmptySource(t *testing.T) {
	root, req := overlayFixture(t)
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) { t.Error("invalid plan sent traffic") })
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	var body map[string]any
	if err := json.Unmarshal(encodeJSON(req), &body); err != nil {
		t.Fatal(err)
	}
	if _, exists := body["recording"]; exists {
		t.Fatal("wire includes legacy selector")
	}
	body["recording"] = map[string]any{}
	if status, _ := f.call(t, "POST", "/control/replay/v1/runs", body); status != 400 {
		t.Fatal(status)
	}
	status, clock := f.call(t, "GET", "/control/replay/v1/clock", nil)
	if status != 200 || clock["boot_id"] != f.svc.boot || clock["received_at"] == nil || clock["sent_at"] == nil {
		t.Fatal(clock)
	}
	// An empty second source must not silently degrade into a single replay.
	index := filepath.Join(root, "g2", "other", req.Recordings[1].Segments[0], "index.jsonl")
	if err := os.WriteFile(index, nil, 0600); err != nil {
		t.Fatal(err)
	}
	f.svc.start(req)
	view := f.wait(t, req.RunID)
	if view.Error != "plan_empty" || view.Progress.Sent != 0 {
		t.Fatal(view)
	}
}

func TestReplayOverlayPrepareStartIdempotencyAndSources(t *testing.T) {
	root, req := overlayFixture(t)
	var received atomic.Int64
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) { received.Add(1); io.WriteString(w, `{}`) })
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	if status, body := f.call(t, "POST", "/control/replay/v1/runs", req); status != 200 {
		t.Fatal(status, body)
	}
	waitPrepared(t, f, req.RunID)
	if received.Load() != 0 {
		t.Fatal("prepare sent requests")
	}
	start := time.Now().Add(150 * time.Millisecond)
	command := replaySyncCommand{StartAt: &start, BootID: f.svc.boot}
	for i := 0; i < 2; i++ {
		if status, body := f.call(t, "POST", "/control/replay/v1/runs/"+req.RunID+"/start", command); status != 200 {
			t.Fatal(status, body)
		}
	}
	view := f.wait(t, req.RunID)
	if view.State != "completed" || received.Load() != 4 || len(view.Sources) != 2 {
		t.Fatalf("%+v received=%d", view, received.Load())
	}
	for _, source := range view.Sources {
		if source.Progress.Sent != 2 || source.Progress.Completed != 2 || source.Progress.Inflight != 0 {
			t.Fatal(source)
		}
	}
	results := readReplayResults(t, f, req.RunID)
	for _, result := range results {
		if result.GatewayID == "" || result.RecordingID == "" || result.Segment == "" {
			t.Fatal(result)
		}
	}
}

func TestReplayOverlayTwoServicesShareStart(t *testing.T) {
	var received atomic.Int64
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) { received.Add(1); io.WriteString(w, `{}`) })
	services := []*replayFixture{}
	requests := []replayRequest{}
	for i := 0; i < 2; i++ {
		root, req := syntheticReplay(t, i+1, .08, `{"stream":true}`)
		req.PrepareOnly = true
		f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
		f.svc.start(req)
		waitPrepared(t, f, req.RunID)
		services = append(services, f)
		requests = append(requests, req)
	}
	start := time.Now().Add(150 * time.Millisecond)
	for i, f := range services {
		if _, status, code := f.svc.syncRun(requests[i].RunID, "start", replaySyncCommand{StartAt: &start, BootID: f.svc.boot}); status != 200 {
			t.Fatal(code)
		}
	}
	a, b := services[0].wait(t, requests[0].RunID), services[1].wait(t, requests[1].RunID)
	if a.State != "completed" || b.State != "completed" || received.Load() != 3 || a.StartedAt.Sub(*b.StartedAt).Abs() > time.Second {
		t.Fatal(a, b)
	}
}

func TestReplayOverlayLeaseStopLateStartAndRecovery(t *testing.T) {
	for _, mode := range []string{"lease", "stop", "late", "restart", "stop-before-prepare"} {
		t.Run(mode, func(t *testing.T) {
			root, req := syntheticReplay(t, 1, 0, `{"stream":true}`)
			req.PrepareOnly = true
			var received atomic.Int64
			target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) { received.Add(1) })
			f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
			if mode == "stop-before-prepare" {
				f.svc.stop(req.RunID)
				view, _, _ := f.svc.start(req)
				if !view.StartCancelled {
					t.Fatal(view)
				}
				return
			}
			f.svc.start(req)
			waitPrepared(t, f, req.RunID)
			if mode == "restart" {
				data := t.TempDir()
				run := f.svc.runs[req.RunID]
				run.persistMu.Lock()
				err := os.CopyFS(data, os.DirFS(f.svc.dataRoot))
				run.persistMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				restored := openRecoveredReplay(t, root, data)
				defer restored.Close()
				view, _ := restored.loadRun(req.RunID)
				if view.State != "end_unknown" || len(restored.active) != 0 {
					t.Fatal(view)
				}
			}
			if mode == "late" {
				start := time.Now().Add(-time.Second)
				if _, status, _ := f.svc.syncRun(req.RunID, "start", replaySyncCommand{StartAt: &start, BootID: f.svc.boot}); status != 409 {
					t.Fatal("late start accepted")
				}
			}
			if mode == "lease" {
				r := f.svc.runs[req.RunID]
				r.mu.Lock()
				r.leaseUntil = time.Now().Add(-time.Second)
				r.mu.Unlock()
			} else {
				f.svc.stop(req.RunID)
			}
			view := f.wait(t, req.RunID)
			if received.Load() != 0 || view.Progress.Sent != 0 {
				t.Fatal(view)
			}
			if mode == "lease" && view.Error != "replay_lease_expired" {
				t.Fatal(view)
			}
		})
	}
}

func TestReplayOverlayFatalFailureIsVisibleBeforeDrain(t *testing.T) {
	root, req := syntheticReplay(t, 2, .4, `{"stream":true}`)
	req.PrepareOnly = true
	arrived, drain := make(chan struct{}), make(chan struct{})
	target := replayTarget(t, func(w http.ResponseWriter, q *http.Request) {
		io.Copy(io.Discard, q.Body)
		close(arrived)
		<-drain
		io.WriteString(w, `{}`)
	})
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	defer close(drain)
	f.svc.start(req)
	waitPrepared(t, f, req.RunID)
	start := time.Now().Add(50 * time.Millisecond)
	if _, status, code := f.svc.syncRun(req.RunID, "start", replaySyncCommand{StartAt: &start, BootID: f.svc.boot}); status != 200 {
		t.Fatal(code)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("first request never arrived")
	}
	// Exhaust the budget for the second dispatch while the accepted request
	// remains in flight. This must not wait for its response to expose failure.
	if !f.svc.bodyMemory.tryAcquire(replayBodyBudget) {
		t.Fatal("first request body was not released")
	}
	defer f.svc.bodyMemory.release(replayBodyBudget)
	f.svc.mu.Lock()
	r := f.svc.runs[req.RunID]
	f.svc.mu.Unlock()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		fatal := r.fatalError
		r.mu.Unlock()
		if fatal != "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	status, view := f.call(t, "GET", "/control/replay/v1/runs/"+req.RunID, nil)
	if status != 200 || view["state"] != "stopping" || view["error"] != "replay_resource_exhausted" || view["ended_at"] != nil {
		t.Fatalf("fatal failure hidden during drain: %v", view)
	}
	if p := view["progress"].(map[string]any); p["inflight"] != float64(1) || p["sent"] != float64(1) {
		t.Fatal(p)
	}
	// Peer-stop coordination can repeat stop/lease calls without clearing the
	// fatal reason, canceling the accepted request, or reviving dispatch.
	f.svc.stop(req.RunID)
	if view, _, _ := f.svc.syncRun(req.RunID, "lease", replaySyncCommand{BootID: f.svc.boot}); view.State != "stopping" || view.Error != "replay_resource_exhausted" {
		t.Fatal(view)
	}
	drain <- struct{}{}
	final := f.wait(t, req.RunID)
	if final.State != "failed" || final.Error != "replay_resource_exhausted" || final.Progress.Completed != 1 || final.Progress.Unsent != 1 {
		t.Fatal(final)
	}
	r.abort("late_failure")
	if after := r.view(); after.State != final.State || after.Error != final.Error {
		t.Fatal("late abort rewrote a terminal run", after)
	}
}

func TestReplayOverlayWakeupTolerance(t *testing.T) {
	for _, late := range []time.Duration{2 * time.Second, 59 * time.Second, 61 * time.Second} {
		t.Run(late.String(), func(t *testing.T) {
			root, req := syntheticReplay(t, 1, 0, `{"stream":true}`)
			req.PrepareOnly = true
			var received atomic.Int64
			target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) { received.Add(1) })
			f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
			f.svc.start(req)
			waitPrepared(t, f, req.RunID)
			f.svc.mu.Lock()
			r := f.svc.runs[req.RunID]
			f.svc.mu.Unlock()
			// Simulate a scheduler resuming after its previously armed deadline,
			// without sleeping through the tolerance in the test suite.
			r.mu.Lock()
			r.startDeadline = time.Now().Add(-late)
			r.state = "scheduled"
			r.mu.Unlock()
			close(r.startSignal)
			view := f.wait(t, req.RunID)
			if late < replayStartTolerance {
				if view.State != "completed" || received.Load() != 1 {
					t.Fatalf("valid relaxed start rejected: %+v", view)
				}
			} else if view.Error != "sync_start_late" || received.Load() != 0 {
				t.Fatalf("excessively late start dispatched: %+v", view)
			}
		})
	}
}
