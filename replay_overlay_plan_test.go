package aigate

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func recordingsFixture(t *testing.T, count, recordings int) (string, replayRequest) {
	t.Helper()
	root, req := syntheticReplay(t, count, .08, `{"stream":true}`)
	source := filepath.Join(root, "g1", "synthetic")
	refs := []replayRecordingRef{req.Recording}
	for i := 1; i < recordings; i++ {
		gateway, recording := fmt.Sprintf("g%d", i%2+1), fmt.Sprintf("other-%d", i)
		dest := filepath.Join(root, gateway, recording)
		if err := os.MkdirAll(dest, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.CopyFS(dest, os.DirFS(source)); err != nil {
			t.Fatal(err)
		}
		_, err := recordingAtomicJSON(dest, "session.json", recordingSessionFile{Schema: recordingSchema, GatewayID: gateway, RecordingID: recording})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, replayRecordingRef{GatewayID: gateway, RecordingID: recording, Segments: req.Recording.Segments})
	}
	req.Recordings = refs
	req.Recording = replayRecordingRef{}
	return root, req
}

func TestReplayOverlayPlanAndValidation(t *testing.T) {
	for count := 1; count <= 4; count++ {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			root, req := recordingsFixture(t, 2, count)
			if err := req.normalize(); err != nil {
				t.Fatal(err)
			}
			plan, err := buildReplayPlan(root, req, savedTestInputs(root))
			if err != nil {
				t.Fatal(err)
			}
			if plan.Planned != 2*count || plan.DurationSeconds != 1800 {
				t.Fatalf("bad overlay: %+v", plan)
			}
			for i, item := range plan.items {
				if item.Sequence != i+1 || item.OffsetSeconds != float64(i/count)*.08 || item.GatewayID == "" {
					t.Fatalf("bad item: %+v", item)
				}
			}
			req.Mode, req.Concurrency = "concurrency", 2
			if err := req.normalize(); err != nil || req.localConcurrency() != 2*count {
				t.Fatal("concurrent overlay rejected or wrong allowance", err)
			}
			req.Mode, req.Concurrency = "timed", 0
			if count == 1 {
				req.Recordings = append(req.Recordings, req.Recordings[0])
			} else {
				req.Recordings[count-1] = req.Recordings[0]
			}
			if req.normalize() == nil {
				t.Fatal("duplicate accepted")
			}
		})
	}
	_, req := recordingsFixture(t, 1, 5)
	if err := req.normalize(); err == nil || err.Error() != "invalid_recordings" {
		t.Fatal("more than four recordings accepted", err)
	}
}
