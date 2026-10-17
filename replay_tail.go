package aigate

import "time"

const replayPerformanceCapability = "replay-performance-v2"

type replayTruncation struct {
	At        time.Time `json:"at"`
	Elapsed   float64   `json:"elapsed_seconds"`
	Inflight  int64     `json:"inflight"`
	Threshold float64   `json:"threshold"`
	Mode      string    `json:"mode"`
}

func replayTailEligible(mode string, concurrency int, duration, elapsed float64, planned int, sent, inflight int64) bool {
	if planned <= 0 || sent != int64(planned) || inflight <= 0 {
		return false
	}
	if mode == "timed" {
		return elapsed > duration && inflight <= 5
	}
	return mode == "concurrency" && float64(inflight) < float64(concurrency)/2
}

// Coordinators check the combined in-flight count before calling each source.
// Once dispatch is complete, that count only decreases; a stale observation
// cannot cancel work before the threshold. Natural completion wins at zero.
func (r *replayRun) truncateTail(coordinated bool) (replayRunView, int, string) {
	r.mu.Lock()
	if r.ended != nil || r.truncation != nil {
		v := r.viewLocked()
		r.mu.Unlock()
		return v, 200, ""
	}
	n := r.inflight.Load()
	eligible := r.plan != nil && replayTailEligible(r.req.Mode, r.req.Concurrency, r.plan.DurationSeconds, time.Since(r.begin).Seconds(), r.plan.Planned, r.sent.Load(), n)
	if coordinated && r.req.CoordinatedTruncation && r.req.Mode == "timed" && r.plan != nil {
		eligible = r.sent.Load() == int64(r.plan.Planned) && time.Since(r.begin).Seconds() > r.plan.DurationSeconds && n <= 5
	}
	if !r.req.AutoTruncate || r.state != "running" || r.truncateCancel == nil || !eligible {
		v := r.viewLocked()
		r.mu.Unlock()
		return v, 409, "tail_not_eligible"
	}
	if n == 0 {
		v := r.viewLocked()
		r.mu.Unlock()
		return v, 200, ""
	}
	threshold := 5.0
	if r.req.Mode == "concurrency" {
		threshold = float64(r.req.Concurrency) / 2
	}
	r.truncation = &replayTruncation{At: time.Now().UTC(), Elapsed: time.Since(r.begin).Seconds(), Inflight: n, Threshold: threshold, Mode: r.req.Mode}
	// Persist the decision before cancellation. Holding the run mutex prevents
	// completion from publishing a normal terminal state during this decision.
	r.mu.Unlock()
	if err := r.persist(); err != nil {
		r.abort("storage_failed")
		return r.view(), 500, "storage_failed"
	}
	r.truncateCancel()
	return r.view(), 200, ""
}

func (r *replayRun) watchTail(done <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			r.truncateTail(false)
		}
	}
}
