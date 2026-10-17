package aigate

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReplayTailThresholds(t *testing.T) {
	cases := []struct {
		mode              string
		concurrency       int
		duration, elapsed float64
		planned           int
		sent, active      int64
		want              bool
	}{
		{"timed", 0, 1800, 1800, 10, 10, 5, false}, {"timed", 0, 1800, 1801, 10, 9, 5, false},
		{"timed", 0, 1800, 1801, 10, 10, 6, false}, {"timed", 0, 1800, 1801, 10, 10, 5, true},
		{"timed", 0, 1800, 1801, 10, 10, 1, true}, {"timed", 0, 1800, 1801, 10, 10, 0, false},
		{"concurrency", 8, 1800, 10, 10, 10, 4, false}, {"concurrency", 8, 1800, 10, 10, 10, 3, true},
		{"concurrency", 5, 1800, 10, 10, 10, 2, true}, {"concurrency", 5, 1800, 10, 10, 10, 3, false},
		{"concurrency", 1, 1800, 10, 10, 10, 1, false}, {"concurrency", 8, 1800, 10, 10, 2, 2, false},
	}
	for _, c := range cases {
		if got := replayTailEligible(c.mode, c.concurrency, c.duration, c.elapsed, c.planned, c.sent, c.active); got != c.want {
			t.Fatalf("%+v got %v", c, got)
		}
	}
}

func TestReplayStreamEligibilityAndRetention(t *testing.T) {
	for body, reason := range map[string]string{`{"stream":true}`: "", `{"stream":false}`: "nonstream", `{}`: "stream_unspecified", `{"stream":"true"}`: "stream_unclassifiable", `null`: "stream_unclassifiable", `{`: "stream_unclassifiable"} {
		if got := replayStreamExclusion([]byte(body)); got != reason {
			t.Fatalf("%s %s", body, got)
		}
	}
	root, req := syntheticReplay(t, 1, 0, `{"stream":true}`)
	segment := filepath.Join(root, req.Recording.GatewayID, req.Recording.RecordingID, req.Recording.Segments[0])
	if err := os.Remove(filepath.Join(segment, "pinned")); err != nil {
		t.Fatal(err)
	}
	if _, err := buildReplayPlan(root, req); err == nil || err.Error() != "segment_not_retained" {
		t.Fatal(err)
	}
}

func TestReplayTailCancelsRequestsAndReportsCensoring(t *testing.T) {
	root, req := syntheticReplay(t, 3, 0, `{"stream":true}`)
	req.Mode, req.Concurrency, req.AutoTruncate = "concurrency", 8, true
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	f.svc.start(req)
	view := f.wait(t, req.RunID)
	if view.State != "truncated" || view.Progress.Sent != 3 || view.Progress.Truncated != 3 || view.Progress.Failed != 0 {
		t.Fatalf("%+v", view)
	}
	rows := readReplayResults(t, f, req.RunID)
	for _, row := range rows {
		if row.Error != "tail_truncated" || row.EndOffset == nil {
			t.Fatalf("%+v", row)
		}
	}
	report := buildReplayReport(f.svc.runDir(req.RunID), view)
	if report.Version != 2 || report.Reason != "tail_truncated" || report.Metrics.Success != 0 || report.Metrics.Truncated != 3 || report.Stream.E2E.Count != 0 {
		t.Fatalf("%+v", report)
	}
	// Retrying the same cancellation never changes terminal evidence.
	if v, status, code := f.svc.runs[req.RunID].truncateTail(true); status != 200 || code != "" || v.Progress.Truncated != 3 {
		t.Fatal(v, status, code)
	}
}

func TestReplayThirtySecondCohortsIncludeLateResults(t *testing.T) {
	dir := t.TempDir()
	rows := []replayResult{
		{TimingVersion: 1, Sequence: 1, PlannedOffset: 30, SendOffset: fp(31), EndOffset: fp(125), FirstOutput: fp(32), LastOutput: fp(124), Stream: true, HTTPStatus: 200, ActualTokens: up(20), Observation: &responseObservation{ProtocolComplete: true}},
		{TimingVersion: 1, Sequence: 2, PlannedOffset: 60, SendOffset: fp(60), EndOffset: fp(90), Stream: true, Error: "tail_truncated"},
	}
	f, _ := os.Create(filepath.Join(dir, "results.jsonl"))
	encoder := json.NewEncoder(f)
	for _, r := range rows {
		encoder.Encode(r)
	}
	f.Close()
	r := buildReplayReport(dir, replayRunView{State: "truncated", Request: &replayRequest{Mode: "timed"}, Plan: &replayPlan{DurationSeconds: 120}, Progress: replayProgress{Sent: 2}})
	if r.BucketSeconds != 30 || len(r.Curve) != 5 || r.Curve[1].TTFT.Count != 1 || r.Curve[1].Success != 0 || r.Curve[4].Success != 1 || r.Curve[2].Cohort.Truncated != 1 {
		t.Fatalf("%+v", r)
	}
	if r.Metrics.E2E.Count != 1 || r.Metrics.Truncated != 1 || r.Curve[4].Width != 5 {
		t.Fatalf("%+v", r)
	}
}

func TestReplayCancellationDoesNotCancelOrdinaryStopDrain(t *testing.T) {
	r := &replayRun{req: replayRequest{AutoTruncate: true, Mode: "concurrency", Concurrency: 8}, state: "running", begin: time.Now(), plan: &replayPlan{Planned: 3}, created: time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.requestContext = ctx
	r.truncateCancel = cancel
	r.sent.Store(2)
	r.inflight.Store(2)
	if _, status, _ := r.truncateTail(false); status != 409 || ctx.Err() != nil {
		t.Fatal("cancelled before dispatch completed")
	}
}
