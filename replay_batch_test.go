package aigate

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func batchTarget(t *testing.T, id string, send http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/runtime/v1/status" {
			_ = json.NewEncoder(w).Encode(map[string]any{"service": "aigate-proxy", "router_id": id, "service_id": "proxy-" + id, "config_digest": strings.Repeat("a", 64)})
			return
		}
		send(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func TestReplayBatchIndependentTargetsAndSharedLocks(t *testing.T) {
	for _, mode := range []string{"timed", "concurrency", "overlay"} {
		t.Run(mode, func(t *testing.T) {
			root, req := syntheticReplay(t, 6, .01, `{"stream":true}`)
			want := int64(6)
			if mode == "overlay" {
				root, req = overlayFixture(t)
				want = 4
			}
			req.BatchID, req.PrepareOnly = "batch", true
			if mode == "concurrency" {
				req.Mode, req.Concurrency = mode, 2
			}
			var slowCount, fastCount, inflight, peak atomic.Int64
			gate := make(chan struct{})
			var unblock sync.Once
			defer unblock.Do(func() { close(gate) })
			slow := batchTarget(t, "slow", func(w http.ResponseWriter, r *http.Request) {
				n := inflight.Add(1)
				defer inflight.Add(-1)
				for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
				}
				slowCount.Add(1)
				select {
				case <-gate:
				case <-r.Context().Done():
				}
				_, _ = io.WriteString(w, `{}`)
			})
			fast := batchTarget(t, "fast", func(w http.ResponseWriter, r *http.Request) { fastCount.Add(1); _, _ = io.WriteString(w, `{}`) })
			f := newReplayFixture(t, root, map[string]string{"slow": slow.URL, "fast": fast.URL})
			for _, id := range []string{"slow", "fast"} {
				req.RunID, req.Target = id, id
				if code, out := f.call(t, "POST", "/control/replay/v1/runs", req); code != 200 {
					t.Fatal(code, out)
				}
				waitPrepared(t, f, id)
			}
			if slowCount.Load()+fastCount.Load() != 0 {
				t.Fatal("preparation sent traffic")
			}
			// Both plans use the same underlying OS locks and reserve one budget.
			f.svc.mu.Lock()
			for _, lease := range f.svc.segments {
				if lease.refs != 2 {
					t.Error(lease.refs)
				}
			}
			f.svc.mu.Unlock()
			req.RunID, req.BatchID = "unrelated", "other-batch"
			if _, code, reason := f.svc.start(req); code != 409 || reason != "run_active" {
				t.Fatal(code, reason)
			}
			start := time.Now().Add(50 * time.Millisecond)
			for _, id := range []string{"slow", "fast"} {
				if _, code, reason := f.svc.syncRun(id, "start", replaySyncCommand{BootID: f.svc.boot, StartAt: &start}); code != 200 {
					t.Fatal(code, reason)
				}
			}
			if done := f.wait(t, "fast"); done.State != "completed" || done.Progress.Sent != want {
				t.Fatal(done)
			}
			if fastCount.Load() != want {
				t.Fatal("fast target waited for slow peer")
			}
			if mode == "concurrency" && peak.Load() > 2 {
				t.Fatal("per target concurrency exceeded", peak.Load())
			}
			f.svc.mu.Lock()
			if err := f.svc.pruneRuns(0); err != nil {
				t.Error(err)
			}
			for _, lease := range f.svc.segments {
				if lease.refs != 1 {
					t.Error(lease.refs)
				}
				other, err := os.Open(lease.file.Name())
				if err != nil {
					t.Fatal(err)
				}
				if lockState(other) == nil {
					t.Error("segment became evictable while slow peer was running")
					unlockState(other)
				}
				other.Close()
			}
			f.svc.mu.Unlock()
			if _, err := os.Stat(filepath.Join(f.svc.runDir("fast"), "state.json")); err != nil {
				t.Fatal("batch sibling evidence pruned", err)
			}
			// Release slow requests; it must receive its complete independent copy.
			unblock.Do(func() { close(gate) })
			if done := f.wait(t, "slow"); done.State != "completed" || done.Progress.Sent != want {
				t.Fatal(done)
			}
			if slowCount.Load() != want {
				t.Fatal(slowCount.Load())
			}
			f.svc.mu.Lock()
			if len(f.svc.segments) != 0 || len(f.svc.active) != 0 {
				t.Error("leases not released")
			}
			f.svc.mu.Unlock()
		})
	}
}

func TestReplayBatchOneTargetFailureDoesNotStopPeer(t *testing.T) {
	root, req := syntheticReplay(t, 2, 0, `{"stream":true}`)
	target := batchTarget(t, "r1", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{}`) })
	other := batchTarget(t, "r2", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{}`) })
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL, "r2": other.URL})
	req.BatchID, req.PrepareOnly = "batch", true
	for _, id := range []string{"r1", "r2"} {
		req.RunID, req.Target = id, id
		f.svc.start(req)
		waitPrepared(t, f, id)
	}
	f.svc.mu.Lock()
	r := f.svc.runs["r1"]
	f.svc.mu.Unlock()
	r.abort("replay_resource_exhausted")
	if failed := f.wait(t, "r1"); failed.Error != "replay_resource_exhausted" {
		t.Fatal(failed)
	}
	start := time.Now().Add(20 * time.Millisecond)
	f.svc.syncRun("r2", "start", replaySyncCommand{BootID: f.svc.boot, StartAt: &start})
	if done := f.wait(t, "r2"); done.State != "completed" || done.Progress.Sent != 2 {
		t.Fatal(done)
	}
}
