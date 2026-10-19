package aigate

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDailyMaintenanceShanghaiNoCatchup(t *testing.T) {
	for _, test := range []struct{ now, next string }{
		{"2026-10-19T20:59:59Z", "2026-10-19T21:00:00Z"},
		{"2026-10-19T21:00:00Z", "2026-10-20T21:00:00Z"},
		{"2026-10-19T23:00:00Z", "2026-10-20T21:00:00Z"},
	} {
		now, _ := time.Parse(time.RFC3339, test.now)
		if got := nextArchiveMaintenance(now).UTC().Format(time.RFC3339); got != test.next {
			t.Fatal(got, test.next)
		}
	}
	stop := make(chan struct{})
	ran := make(chan struct{}, 1)
	done := dailyArchiveMaintenance(stop, func(time.Time) { ran <- struct{}{} })
	close(stop)
	<-done
	select {
	case <-ran:
		t.Fatal("startup performed maintenance")
	default:
	}
}

func TestRecordingWriterContinuesDuringArchive(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	old := recordSample(t, rec, "old-input", 1)
	r := rec.c.recorder
	r.mu.Lock()
	r.stored["old-input/"+old.Segments[0].Segment].start = time.Now().Add(-96 * time.Hour)
	r.mu.Unlock()
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r.archiveSegment = func(string, map[string]bool) error { close(entered); <-release; return nil }
	go func() { r.maintainStorage(); close(finished) }()
	<-entered
	defer func() { close(release); <-finished }()
	// This goes through the live switch, recorder queue, writer and sealing path.
	current := recordSample(t, rec, "during-archive", 3)
	if current.Recorded != 3 {
		t.Fatal(current)
	}
}

func TestArchivedDiagnosticLateRangesSeekAndCancel(t *testing.T) {
	root := t.TempDir()
	id := "11111111111111111111111111111111"
	dir := filepath.Join(root, "request-"+id)
	os.Mkdir(dir, 0700)
	body := bytes.Repeat([]byte("0123456789abcdef"), 600000)
	meta, _ := json.Marshal(map[string]any{"ended_at": time.Now().Add(-96 * time.Hour), "request_id": id})
	os.WriteFile(filepath.Join(dir, "meta.json"), meta, 0600)
	os.WriteFile(filepath.Join(dir, "response.sse"), body, 0600)
	if err := maintainDiagnosticArchives(root, time.Now(), 10<<30, nil); err != nil {
		t.Fatal(err)
	}
	ref, _ := diagnosticReference(dir)
	m, err := readSegmentArchive(filepath.Join(root, ref.Archive))
	if err != nil || len(m.Blocks) < 8 {
		t.Fatal(m, err)
	}
	// Destroy an early compressed block: a late range must not read that prefix.
	f, _ := os.OpenFile(filepath.Join(root, ref.Archive, archivePayload), os.O_RDWR, 0)
	f.WriteAt([]byte("broken-prefix"), 0)
	f.Close()
	offset := int64(len(body) - 128)
	r, _, _, err := archivedDiagnosticRange(context.Background(), dir, "response.sse", offset)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(got, body[offset:]) {
		t.Fatal("late range read prefix", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r, _, _, err := archivedDiagnosticRange(ctx, dir, "response.sse", offset); err == nil {
		r.Close()
		t.Fatal("ignored cancellation")
	}
}

func TestDisabledProfilingMaintainsOldCaptureAndRespectsOwner(t *testing.T) {
	root := t.TempDir()
	at := time.Now().Add(-96 * time.Hour)
	id := randomID()
	path := captureSeedRecord(t, filepath.Join(root, "stream-incidents", "requests"), "request-"+id,
		streamCaptureMeta{Schema: 1, RequestID: id, EndedAt: &at, Retained: true, CaptureState: "complete"},
		map[string][]byte{"response.sse": []byte("old evidence")})
	p := retentionProxy(t, root, `{"profile":{"enabled":false}}`)
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if err = lockState(directory); err != nil {
		t.Fatal(err)
	}
	p.maintainInactiveStorage(time.Now())
	if _, err := diagnosticReference(path); !os.IsNotExist(err) {
		t.Fatal("cleaned another owner's directory")
	}
	unlockState(directory)
	directory.Close()
	p.maintainInactiveStorage(time.Now())
	if _, err := diagnosticReference(path); err != nil {
		t.Fatal(err)
	}
}
