package aigate

import (
	"testing"
	"time"
)

func TestProfileReplayLifecycleAndLiveMetricsStaySeparate(t *testing.T) {
	p, _ := profileFixture(t)
	business := p.beginRequest("business", true, "")
	replay := p.beginRequest("replay", false, "replay")
	replayStream := p.beginRequest("replay-stream", true, "replay")
	replayTags := profileTrafficTags("replay")
	p.mu.Lock()
	if p.values["aigate_requests_started_total"] != 1 || p.values["aigate_requests_started_total"+replayTags] != 2 || p.values["aigate_nonstream_requests_total"+replayTags] != 1 {
		t.Fatal("replay counted as business", p.values)
	}
	p.mu.Unlock()
	values := liveValues(p, time.Now())
	businessTags := profileExtra("", "backend", "unrouted")
	liveReplayTags := profileExtra(replayTags, "backend", "unrouted")
	if values["aigate_streams_waiting_first_output"+businessTags] != 1 || values["aigate_streams_waiting_first_output"+liveReplayTags] != 1 {
		t.Fatal("live metrics combined business and replay", values)
	}
	for _, r := range []*profileRequest{business, replay, replayStream} {
		p.end(r, &auditEntry{Status: 200}, false)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.values["aigate_requests_inflight"] != 0 || p.values["aigate_requests_inflight"+replayTags] != 0 {
		t.Fatal("inflight decrement used another series", p.values)
	}
}
