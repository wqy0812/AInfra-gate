package aigate

import (
	"os"
	"path/filepath"
	"testing"
)

func overlayFixture(t *testing.T) (string, replayRequest) {
	return overlayFixtureCount(t, 2)
}

func overlayFixtureCount(t *testing.T, count int) (string, replayRequest) {
	t.Helper()
	root, req := syntheticReplay(t, count, .08, `{"stream":true}`)
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
	return root, req
}

func TestReplayOverlayPlanAndValidation(t *testing.T) {
	root, req := overlayFixture(t)
	if err := req.normalize(); err != nil {
		t.Fatal(err)
	}
	plan, err := buildReplayPlan(root, req, savedTestInputs(root))
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
	if err := req.normalize(); err != nil || req.localConcurrency() != 4 {
		t.Fatal("concurrent overlay rejected or wrong allowance", err)
	}
	req.Mode, req.Concurrency = "timed", 0
	req.Recordings[1] = req.Recordings[0]
	if req.normalize() == nil {
		t.Fatal("duplicate accepted")
	}
}
