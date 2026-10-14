package aigate

import (
	"context"
	"strconv"
	"time"
)

// IO timing is independent of display stages. Never hold the profiler lock over I/O.
func (live *profileLiveRequest) beginIO(operation string, now time.Time) {
	live.ioOperation, live.ioStarted = operation, now
}

func (live *profileLiveRequest) endIO(now time.Time) {
	if live.ioOperation == "read" {
		live.readWait += max(time.Duration(0), now.Sub(live.ioStarted))
	}
	live.ioOperation, live.ioStarted = "", time.Time{}
}

func (r *profileRequest) liveWriteStart() {
	r.updateLive(func(live *profileLiveRequest) {
		live.stage = "writing_client"
		live.beginIO("write", time.Now())
	})
}

func (r *profileRequest) liveIOEnd() {
	r.updateLive(func(live *profileLiveRequest) { live.endIO(time.Now()) })
}

// Live values are never stored in the cumulative counter checkpoint.
var profileLiveStages = []string{
	"routing_discovery", "preparing_upstream",
	"waiting_upstream_headers", "waiting_first_output", "streaming",
	"reading_response", "writing_client", "finishing_stream",
}

var profileIdleThresholds = []int{5, 15, 30, 60}

type profileLiveRequest struct {
	start, lastUseful       time.Time
	backend, stage, traffic string
	terminal                bool
	unknown                 bool
	nonstream               bool
	ioOperation             string
	ioStarted               time.Time
	readWait                time.Duration
}

func profileFrom(ctx context.Context) *profileRequest {
	r, _ := ctx.Value(profileContextKey{}).(*profileRequest)
	return r
}

func (r *profileRequest) updateLive(update func(*profileLiveRequest)) {
	if r == nil || r.profiler == nil {
		return
	}
	p := r.profiler
	p.mu.Lock()
	if live, ok := p.active[r.e.Seq]; ok {
		update(&live)
		p.active[r.e.Seq] = live
	}
	p.mu.Unlock()
}

func (r *profileRequest) liveStage(stage string) {
	r.updateLive(func(live *profileLiveRequest) { live.stage = stage })
}

func (r *profileRequest) liveBackend(backend string) {
	if r == nil || r.profiler == nil {
		return
	}
	r.updateLive(func(live *profileLiveRequest) {
		groups := r.profiler.liveBackends
		if !groups[backend] && backend != "unrouted" && backend != "overflow" && len(groups)-profileBoolInt(groups["unrouted"])-profileBoolInt(groups["overflow"]) >= 64 {
			backend = "overflow"
		}
		groups[backend] = true
		live.backend = backend
	})
}

func (r *profileRequest) liveTerminal() {
	r.updateLive(func(live *profileLiveRequest) { live.terminal = true })
}

func (r *profileRequest) liveObserve(o *sseObserver) {
	r.updateLive(func(live *profileLiveRequest) {
		if !o.LastUsefulAt.Equal(live.lastUseful) {
			live.readWait = 0
		}
		live.lastUseful = o.LastUsefulAt
		live.unknown = o.Unknown || o.Invalid
		live.terminal = live.terminal || o.HasError || o.valid()
	})
}

func (r *profileRequest) liveStreamRead(o *sseObserver) {
	stage := "waiting_first_output"
	if o.HasError || o.valid() {
		stage = "finishing_stream"
	} else if o.Useful {
		stage = "streaming"
	}
	r.updateLive(func(live *profileLiveRequest) {
		live.stage = stage
		live.beginIO("read", time.Now())
	})
}

// Input maps are private scrape snapshots. Durations use Go's monotonic clock.
func profileLiveMetrics(now time.Time, active map[uint64]profileLiveRequest, backends map[string]bool, values map[string]float64, types map[string]string) {
	// A source-level inventory lets consumers reject partial backend aggregates,
	// even after a vanished series has aged out of the current query window.
	types["aigate_live_backend_groups"] = "gauge"
	values["aigate_live_backend_groups"] = float64(len(backends))
	base := []string{"streams_waiting_first_output", "stream_first_output_wait_max_seconds", "stream_idle_requests", "stream_idle_max_seconds", "inflight_requests_by_stage", "inflight_oldest_age_seconds", "inflight_oldest_stage", "stream_observation_unknown", "stream_backend_wait_max_seconds", "stream_write_active_max_seconds"}
	for _, name := range base {
		types["aigate_"+name] = "gauge"
	}
	tagsFor := func(backend, traffic string) string {
		return profileExtra(profileTrafficTags(traffic), "backend", backend)
	}
	for backend := range backends {
		for _, traffic := range []string{"", "replay"} {
			tags := tagsFor(backend, traffic)
			for _, name := range []string{base[0], base[1], base[3], base[5], base[7], base[8], base[9]} {
				values["aigate_"+name+tags] = 0
			}
			for _, threshold := range profileIdleThresholds {
				values["aigate_stream_idle_requests"+profileExtra(tags, "threshold_seconds", strconv.Itoa(threshold))] = 0
			}
			for _, stage := range profileLiveStages {
				values["aigate_inflight_requests_by_stage"+profileExtra(tags, "stage", stage)] = 0
				values["aigate_inflight_oldest_stage"+profileExtra(tags, "stage", stage)] = 0
			}
		}
	}
	oldest := make(map[string]uint64)
	for seq, live := range active {
		tags := tagsFor(live.backend, live.traffic)
		age := max(0, now.Sub(live.start).Seconds())
		values["aigate_inflight_requests_by_stage"+profileExtra(tags, "stage", live.stage)]++
		previous, found := oldest[tags]
		if !found || live.start.Before(active[previous].start) || (live.start.Equal(active[previous].start) && seq < previous) {
			oldest[tags] = seq
		}
		if !live.nonstream && live.ioOperation == "write" {
			key := "aigate_stream_write_active_max_seconds" + tags
			values[key] = max(values[key], max(0, now.Sub(live.ioStarted).Seconds()))
		}
		if !live.nonstream && !live.terminal && !live.unknown && live.ioOperation == "read" {
			key := "aigate_stream_backend_wait_max_seconds" + tags
			values[key] = max(values[key], live.readWait.Seconds()+max(0, now.Sub(live.ioStarted).Seconds()))
		}
		if live.nonstream || live.terminal {
			continue
		}
		if live.unknown {
			values["aigate_stream_observation_unknown"+tags]++
			continue
		}
		if live.lastUseful.IsZero() {
			values["aigate_streams_waiting_first_output"+tags]++
			key := "aigate_stream_first_output_wait_max_seconds" + tags
			values[key] = max(values[key], age)
		} else {
			idle := max(0, now.Sub(live.lastUseful).Seconds())
			key := "aigate_stream_idle_max_seconds" + tags
			values[key] = max(values[key], idle)
			for _, threshold := range profileIdleThresholds {
				if idle >= float64(threshold) {
					values["aigate_stream_idle_requests"+profileExtra(tags, "threshold_seconds", strconv.Itoa(threshold))]++
				}
			}
		}
	}
	for tags, seq := range oldest {
		live := active[seq]
		values["aigate_inflight_oldest_age_seconds"+tags] = max(0, now.Sub(live.start).Seconds())
		values["aigate_inflight_oldest_stage"+profileExtra(tags, "stage", live.stage)] = 1
	}
}
