package aigate

import (
	"fmt"
	"io"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A worker driven directly by each test makes disk failures and half-hour
// boundaries deterministic without changing a live worker's hooks.
func recordingWorkerFixture(t *testing.T, now time.Time) (*trafficRecorder, *recordingSession) {
	t.Helper()
	r := &trafficRecorder{root: t.TempDir(), gatewayID: "g1", boot: "b", maxBytes: 1 << 30,
		now: func() time.Time { return now }, audit: log.New(io.Discard, "", 0),
		stored: map[string]*recordingStoredSegment{}, sessionBytes: map[string]int64{}, writeJSON: recordingAtomicJSON}
	s := &recordingSession{params: recordingParams{RecordingID: "rec", StartAt: halfHour(now), EndAt: halfHour(now).Add(time.Hour), SampleOneIn: 1}, created: now,
		segments: map[time.Time]*recordingSegmentState{}}
	r.sessions = []*recordingSession{s}
	r.controlJob(recordingControlJob{create: s, done: func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}})
	return r, s
}

func TestRecordingSamplingAcrossSealUsesCurrentSegment(t *testing.T) {
	start := time.Date(2026, 10, 13, 2, 0, 0, 0, time.UTC)
	r, s := recordingWorkerFixture(t, start)
	old := s.segmentLocked(start)
	boundary := start.Add(recordingSegment)
	r.now = func() time.Time { return boundary }
	var sampled *trafficRecord
	r.writeJSON = func(dir, name string, v any) (int64, error) {
		if name == "segment.json" {
			sampled = r.begin(httptest.NewRequest("POST", "/v1/chat/completions", nil), "crossing", boundary.Add(-time.Nanosecond))
		}
		return recordingAtomicJSON(dir, name, v)
	}
	r.maintain(false)
	if sampled == nil || sampled.segment.start != boundary || !old.sealed || old.requests != 0 {
		t.Fatalf("sampling must not reopen the segment being sealed: %+v", old)
	}
	var disk recordingSegmentFile
	readJSONFile(t, filepath.Join(r.sessionDir("rec"), old.name, "segment.json"), &disk)
	if disk.Requests != 0 || s.requests != 1 {
		t.Fatal(disk, s.requests)
	}
}

func TestRecordingStateWriteFailureStopsAndRetries(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprint(terminal), func(t *testing.T) {
			start := time.Date(2026, 10, 13, 2, 0, 0, 0, time.UTC)
			r, s := recordingWorkerFixture(t, start)
			s.segmentLocked(start)
			now := start.Add(time.Minute)
			r.now = func() time.Time { return now }
			if terminal {
				r.stopSessionLocked(s, now, "stopped")
			} else {
				s.persisted = "scheduled"
			}
			r.writeJSON = func(dir, name string, v any) (int64, error) {
				if name == "session.json" {
					return 0, fmt.Errorf("disk full")
				}
				return recordingAtomicJSON(dir, name, v)
			}
			r.maintain(false)
			if !s.stopped || s.finalized || s.endReason != "disk_failed" || len(r.sessions) != 1 {
				t.Fatalf("lost retry state: %+v", s)
			}
			if r.begin(httptest.NewRequest("POST", "/v1/chat/completions", nil), "after-error", now) != nil {
				t.Fatal("still sampling after disk failure")
			}
			r.writeJSON = recordingAtomicJSON
			r.maintain(false)
			var disk recordingSessionFile
			readJSONFile(t, filepath.Join(r.sessionDir("rec"), "session.json"), &disk)
			if disk.State != "ended" || disk.EndReason != "disk_failed" || len(r.sessions) != 0 {
				t.Fatalf("retry %+v", disk)
			}
			actual, err := recordingTreeBytes(r.root)
			if err != nil || actual != r.diskBytes {
				t.Fatal(actual, r.diskBytes, err)
			}
		})
	}
}

func TestRecordingEvictionRemovesEmptySessionMetadata(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 70; i++ {
		writeStoredSession(t, root, fmt.Sprintf("old-%02d", i), "ended")
		writeStoredSegment(t, root, fmt.Sprintf("old-%02d", i), "20260101T0000", 1000, false)
	}
	r := &trafficRecorder{root: root, gatewayID: "g1", boot: "b", maxBytes: 1, now: time.Now, audit: log.New(io.Discard, "", 0), stored: map[string]*recordingStoredSegment{}, sessionBytes: map[string]int64{}}
	if err := r.recover(); err != nil {
		t.Fatal(err)
	}
	r.evict()
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 || r.diskBytes != 0 || len(r.sessionBytes) != 0 {
		t.Fatal(len(entries), r.diskBytes, err)
	}
}
