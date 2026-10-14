package aigate

import (
	"context"
	"time"
)

const replayOverlayCapability = "recording-overlay-sync-v1"

// Overlay comparisons use the middle of a run; second-level startup jitter
// does not invalidate that workload. Keep this in sync with the platform.
const replayStartTolerance = 10 * time.Second

type replaySyncCommand struct {
	StartAt *time.Time `json:"start_at,omitempty"`
	BootID  string     `json:"boot_id"`
}

// A lease bounds orphan dispatch after a platform failure. It never resumes a
// stopped/recovered run. Planning itself also has a fixed deadline.
func (r *replayRun) watchLease(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.mu.Lock()
			expired := time.Now().After(r.leaseUntil)
			preparing := r.state == "planning" || r.state == "prepared"
			timeout := preparing && time.Since(r.created) > 120*time.Second
			r.mu.Unlock()
			if expired || timeout {
				r.abort("replay_lease_expired")
				return
			}
		}
	}
}

func (s *replayService) syncRun(id, action string, command replaySyncCommand) (replayRunView, int, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.runs[id]
	if command.BootID != s.boot {
		return replayRunView{}, 409, "replay_service_changed"
	}
	if r == nil {
		return replayRunView{}, 404, "run_not_found"
	}
	r.mu.Lock()
	if !r.req.PrepareOnly {
		r.mu.Unlock()
		return replayRunView{}, 409, "run_not_prepared"
	}
	if r.ended != nil || r.stopped.Load() || r.fatalError != "" {
		view := r.viewLocked()
		r.mu.Unlock()
		return view, 200, ""
	}
	if time.Now().After(r.leaseUntil) {
		r.mu.Unlock()
		r.abort("replay_lease_expired")
		return replayRunView{}, 409, "replay_lease_expired"
	}
	if action == "lease" {
		r.leaseUntil = time.Now().Add(15 * time.Second)
		view := r.viewLocked()
		r.mu.Unlock()
		return view, 200, ""
	}
	if command.StartAt == nil {
		r.mu.Unlock()
		return replayRunView{}, 400, "invalid_start_at"
	}
	if r.scheduledAt != nil {
		same := r.scheduledAt.Equal(*command.StartAt)
		view := r.viewLocked()
		r.mu.Unlock()
		if !same {
			return view, 409, "run_conflict"
		}
		return view, 200, ""
	}
	if r.state != "prepared" {
		r.mu.Unlock()
		return replayRunView{}, 409, "run_not_prepared"
	}
	now := time.Now()
	delay := command.StartAt.Sub(now)
	if delay <= 0 || delay > 30*time.Second {
		r.mu.Unlock()
		return replayRunView{}, 409, "invalid_start_at"
	}
	// Attach monotonic time to the wall-clock deadline. Subsequent wall clock
	// changes cannot move an armed scheduler's start.
	deadline := now.Add(delay)
	r.startDeadline = deadline
	scheduled := command.StartAt.UTC()
	r.scheduledAt = &scheduled
	r.state = "scheduled"
	r.mu.Unlock()
	if err := r.persist(); err != nil {
		r.abort("storage_failed")
		return replayRunView{}, 500, "storage_failed"
	}
	close(r.startSignal)
	return r.view(), 200, ""
}
