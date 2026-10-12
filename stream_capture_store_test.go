package aigate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func captureTestStore(t *testing.T) *streamCaptureStore {
	t.Helper()
	s, err := newStreamCaptureStore(filepath.Join(t.TempDir(), "stream-incidents", "requests"), "service", "router", "boot", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	return s
}

func captureWaitReleased(t *testing.T, c *streamCapture) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		c.store.mu.Lock()
		done := c.released
		c.store.mu.Unlock()
		if done {
			return
		}
		select {
		case <-deadline:
			t.Fatal("capture did not finalize")
		case <-time.After(time.Millisecond):
		}
	}
}

func captureReadMeta(t *testing.T, s *streamCaptureStore, id string) streamCaptureMeta {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(s.root, "request-"+id, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m streamCaptureMeta
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func captureAssertReleased(t *testing.T, s *streamCaptureStore) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.usedBytes != 0 || s.queued != 0 || len(s.active) != 0 {
		t.Fatalf("leaked capture resources: bytes=%d jobs=%d handles=%d", s.usedBytes, s.queued, len(s.active))
	}
	if s.peakBytes.Load() > s.maxMemory {
		t.Fatalf("memory budget exceeded: %d", s.peakBytes.Load())
	}
}

func TestStreamCaptureFullBytesAndRepeatedIncidents(t *testing.T) {
	s := captureTestStore(t)
	id, now := randomID(), time.Now()
	raw := []byte(" {\"messages\":[{\"role\":\"user\",\"content\":\"问题😀\"}],\"stream\":true} \n")
	c := s.begin(id, raw, now)
	c.setResponse(200, "text/event-stream", "gzip", "transport", true)
	first := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"前\"}}]}\r\n\r\n")
	want := append([]byte(nil), first...)
	c.response(now, 0, first)
	for i := range first {
		first[i] = 'x'
	}
	c.event(captureEvent{Type: "sse_event", At: now, HasRange: true, Start: 0, End: int64(len(want)), EventIndex: 1, PositionKnown: true})
	c.trigger("incident-one", "output_stall", now.Add(time.Minute), captureEvent{HasRange: true, Start: 0, End: int64(len(want)), LastUsefulEventIndex: 1})
	last := []byte(": heartbeat\n\ndata: [DONE]\n\n")
	c.response(now.Add(time.Minute), int64(len(want)), last)
	want = append(want, last...)
	c.trigger("incident-two", "client_write_stall", now.Add(2*time.Minute))
	c.event(captureEvent{Type: "write_return", Origin: "gateway", At: now.Add(2 * time.Minute), Data: []byte("\n\ndata: gateway-error\n\n"), WrittenKnown: true, WrittenBytes: 25})
	c.finish(now.Add(3*time.Minute), "client_cancelled", false, false)
	captureWaitReleased(t, c)
	base := filepath.Join(s.root, "request-"+id)
	stored, err := os.ReadFile(filepath.Join(base, "request.json"))
	if err != nil || !bytes.Equal(stored, raw) {
		t.Fatalf("original request changed: %v", err)
	}
	response, err := os.ReadFile(filepath.Join(base, "response.sse"))
	if err != nil || !bytes.Equal(response, want) {
		t.Fatalf("response changed: %q %v", response, err)
	}
	m := captureReadMeta(t, s, id)
	if m.CaptureState != "complete" || m.Outcome != "client_cancelled" || m.Done || m.EOF || !m.RequestSaved || m.IncidentCount != 2 || m.FirstIncidentID != "incident-one" || m.LastIncidentID != "incident-two" {
		t.Fatalf("wrong result: %+v", m)
	}
	if m.ResponseReceivedBytes != int64(len(want)) || m.ResponseSavedBytes != int64(len(want)) || m.ResponseSyncedBytes != int64(len(want)) {
		t.Fatalf("bad byte boundaries: %+v", m)
	}
	for file, hashText := range map[string]string{"request.json": m.RequestSHA256, "response.sse": m.ResponseSHA256, "timeline.jsonl": m.TimelineSHA256} {
		data, _ := os.ReadFile(filepath.Join(base, file))
		hash := sha256.Sum256(data)
		if hashText != hex.EncodeToString(hash[:]) {
			t.Fatalf("wrong %s hash", file)
		}
		info, _ := os.Stat(filepath.Join(base, file))
		if info.Mode().Perm() != 0600 {
			t.Fatalf("unsafe %s permissions", file)
		}
	}
	timeline, _ := os.ReadFile(filepath.Join(base, "timeline.jsonl"))
	if bytes.Contains(timeline, []byte("0001-01-01")) {
		t.Fatal("unused timestamp fields were serialized")
	}
	var sequence uint64
	for _, line := range bytes.Split(bytes.TrimSpace(timeline), []byte{'\n'}) {
		var event captureTimelineEvent
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		sequence++
		if event.Sequence != sequence || event.Schema != 1 {
			t.Fatalf("invalid sequence: %s", line)
		}
	}
	if sequence != 5 || m.LastTimelineSequence != sequence || m.TimelineSyncedBytes != int64(len(timeline)) {
		t.Fatalf("timeline boundaries: %+v events=%d", m, sequence)
	}
	captureAssertReleased(t, s)
}

func TestStreamCaptureNormalRequestsDeleteTemporaryData(t *testing.T) {
	s := captureTestStore(t)
	c := s.begin(randomID(), []byte(`{"stream":true}`), time.Now())
	c.response(time.Now(), 0, []byte("data: normal\n\n"))
	c.event(captureEvent{Type: "read_return", At: time.Now()})
	c.finish(time.Now(), "success", true, true)
	captureWaitReleased(t, c)
	entries, err := os.ReadDir(s.root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("normal stream retained files: %v %v", entries, err)
	}
	captureAssertReleased(t, s)
}

func TestStreamCaptureBlockedWriterStopsAtContinuousPrefix(t *testing.T) {
	s := captureTestStore(t)
	s.maxQueue = 2
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.writePayload = func(f *os.File, p []byte) (int, error) {
		once.Do(func() { close(entered); <-release })
		return f.Write(p)
	}
	id := randomID()
	c := s.begin(id, []byte(`{"stream":true}`), time.Now())
	c.response(time.Now(), 0, []byte("first"))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer never started")
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	c.response(time.Now(), 5, []byte("second"))
	completed := make(chan struct{})
	go func() {
		c.response(time.Now(), 11, []byte("MISSING"))
		c.trigger("late-incident", "output_stall", time.Now())
		c.response(time.Now(), 18, []byte("MUST-NOT-APPEND"))
		c.finish(time.Now(), "success", true, true)
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("storage backpressure blocked request completion")
	}
	s.mu.Lock()
	if !c.retained || !c.stopped || s.queued != 2 {
		t.Errorf("lost saturated trigger: retained=%v stopped=%v jobs=%d", c.retained, c.stopped, s.queued)
	}
	s.mu.Unlock()
	close(release)
	captureWaitReleased(t, c)
	data, err := os.ReadFile(filepath.Join(s.root, "request-"+id, "response.sse"))
	if err != nil || string(data) != "firstsecond" {
		t.Fatalf("noncontiguous prefix: %q %v", data, err)
	}
	m := captureReadMeta(t, s, id)
	if m.CaptureState != "truncated" || m.FirstIncidentID != "late-incident" || m.FirstMissingResponseOffset == nil || *m.FirstMissingResponseOffset != 11 || m.ResponseReceivedBytes != 33 || m.FirstMissingTimelineSequence == nil {
		t.Fatalf("bad failure boundary: %+v", m)
	}
	captureAssertReleased(t, s)
}

func TestStreamCaptureOriginalCapacityAndHandleBudgets(t *testing.T) {
	s := captureTestStore(t)
	s.maxMemory = 32
	s.maxHandles = 1
	raw := make([]byte, 4, 64)
	copy(raw, "body")
	c := s.begin(randomID(), raw, time.Now())
	if c == nil {
		t.Fatal("missing failure tombstone")
	}
	if extra := s.begin(randomID(), []byte("small"), time.Now()); extra != nil {
		t.Fatal("handle limit not enforced")
	}
	s.mu.Lock()
	if c.raw != nil || !c.stopped || s.usedBytes != 0 {
		t.Error("request capacity was not charged")
	}
	s.mu.Unlock()
	c.trigger("incident", "output_stall", time.Now())
	c.finish(time.Now(), "success", true, true)
	captureWaitReleased(t, c)
	m := captureReadMeta(t, s, c.id)
	if m.RequestSaved || m.CaptureState != "failed" || m.FailureReason != "request_memory_full" {
		t.Fatalf("unbudgeted input claimed saved: %+v", m)
	}
	if _, err := os.Stat(filepath.Join(s.root, "request-"+c.id, "request.json")); !os.IsNotExist(err) {
		t.Fatalf("unexpected request body: %v", err)
	}
	captureAssertReleased(t, s)
}

func TestStreamCaptureFinishCannotClaimCompleteWhenEndEventIsDropped(t *testing.T) {
	s := captureTestStore(t)
	s.maxQueue = 2
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.writePayload = func(f *os.File, p []byte) (int, error) {
		once.Do(func() { close(entered); <-release })
		return f.Write(p)
	}
	c := s.begin(randomID(), []byte("input"), time.Now())
	c.response(time.Now(), 0, []byte("all-response-bytes"))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not block")
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	c.trigger("incident", "output_stall", time.Now())
	c.finish(time.Now(), "success", true, true) // Accepted payloads fill both slots.
	close(release)
	captureWaitReleased(t, c)
	m := captureReadMeta(t, s, c.id)
	if m.CaptureState != "truncated" || m.ResponseSavedBytes != m.ResponseReceivedBytes || m.FirstMissingTimelineSequence == nil || *m.FirstMissingTimelineSequence != 2 || m.Outcome != "success" || !m.EOF || !m.Done {
		t.Fatalf("missing end event declared complete: %+v", m)
	}
	captureAssertReleased(t, s)
}

func TestStreamCapturePartialRequestHasTruthfulHash(t *testing.T) {
	s := captureTestStore(t)
	s.writePayload = func(f *os.File, data []byte) (int, error) {
		if filepath.Base(f.Name()) == "request.json" {
			n, _ := f.Write(data[:3])
			return n, errors.New("request disk write failed")
		}
		return f.Write(data)
	}
	c := s.begin(randomID(), []byte("complete-request"), time.Now())
	c.response(time.Now(), 0, []byte("response"))
	c.trigger("incident", "output_stall", time.Now())
	c.finish(time.Now(), "success", true, true)
	captureWaitReleased(t, c)
	m := captureReadMeta(t, s, c.id)
	h := sha256.Sum256([]byte("com"))
	if m.CaptureState != "truncated" || m.RequestSaved || m.RequestSavedBytes != 3 || m.RequestSHA256 != hex.EncodeToString(h[:]) {
		t.Fatalf("partial request misrepresented: %+v", m)
	}
	captureAssertReleased(t, s)
}

func TestStreamCaptureTimelineFailureKeepsFirstMissingEvent(t *testing.T) {
	s := captureTestStore(t)
	s.writePayload = func(f *os.File, data []byte) (int, error) {
		if filepath.Base(f.Name()) == "timeline.jsonl" {
			return 0, errors.New("timeline unavailable")
		}
		return f.Write(data)
	}
	c := s.begin(randomID(), []byte("input"), time.Now())
	c.response(time.Now(), 0, []byte("all-response"))
	c.trigger("incident", "output_stall", time.Now())
	c.finish(time.Now(), "success", true, true)
	captureWaitReleased(t, c)
	m := captureReadMeta(t, s, c.id)
	if m.CaptureState != "truncated" || m.ResponseSavedBytes != int64(len("all-response")) || m.FirstMissingTimelineSequence == nil || *m.FirstMissingTimelineSequence != 1 || m.TimelineSavedBytes != 0 {
		t.Fatalf("missing timeline hidden by response file: %+v", m)
	}
	captureAssertReleased(t, s)
}

func TestStreamCaptureCompletedRecordsLeaveNoMemoryIndex(t *testing.T) {
	s := captureTestStore(t)
	for i := 0; i < 4; i++ {
		c := s.begin(randomID(), []byte("input"), time.Now())
		c.response(time.Now(), 0, []byte("data"))
		c.trigger(fmt.Sprintf("incident-%d", i), "output_stall", time.Now())
		c.finish(time.Now(), "success", true, true)
		captureWaitReleased(t, c)
	}
	s.close()
	if len(s.stored) != 0 {
		t.Fatalf("completed history retained %d in-memory records", len(s.stored))
	}
	entries, err := os.ReadDir(s.root)
	if err != nil || len(entries) != 4 {
		t.Fatalf("completed files unexpectedly removed: %v %v", entries, err)
	}
	captureAssertReleased(t, s)
}

func TestStreamCaptureStartupRetentionKeepsRecentAndDeletesExpired(t *testing.T) {
	root := filepath.Join(t.TempDir(), "requests")
	oldID, recentID := randomID(), randomID()
	old, recent := time.Now().Add(-73*time.Hour), time.Now().Add(-time.Hour)
	oldPath := captureSeedRecord(t, root, "request-"+oldID, streamCaptureMeta{Schema: 1, RequestID: oldID, StartedAt: old, TriggeredAt: &old, EndedAt: &old, Retained: true, CaptureState: "complete"}, map[string][]byte{"response.sse": []byte("old")})
	recentPath := captureSeedRecord(t, root, "request-"+recentID, streamCaptureMeta{Schema: 1, RequestID: recentID, StartedAt: recent, TriggeredAt: &recent, EndedAt: &recent, Retained: true, CaptureState: "complete"}, map[string][]byte{"response.sse": []byte("recent")})
	s, err := newStreamCaptureStore(root, "service", "router", "boot", nil)
	if err != nil {
		t.Fatal(err)
	}
	s.close()
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("expired record remains: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(recentPath, "response.sse")); err != nil || string(data) != "recent" {
		t.Fatalf("recent record removed: %q %v", data, err)
	}
	if len(s.stored) != 0 {
		t.Fatal("startup retained a history-sized index")
	}
	actual, err := captureTreeBytes(s.root)
	if err != nil || s.diskBytes != actual {
		t.Fatalf("cleanup disk accounting differs: tracked=%d actual=%d err=%v", s.diskBytes, actual, err)
	}
}

func TestStreamCaptureShortWriteAndWorkerPanic(t *testing.T) {
	for _, kind := range []string{"partial", "panic"} {
		t.Run(kind, func(t *testing.T) {
			s := captureTestStore(t)
			var once sync.Once
			s.writePayload = func(f *os.File, p []byte) (n int, err error) {
				failed := false
				if filepath.Base(f.Name()) == "response.sse" {
					once.Do(func() { failed = true })
				}
				if failed {
					if kind == "panic" {
						panic("injected writer failure")
					}
					n, _ = f.Write(p[:2])
					return n, errors.New("injected disk failure")
				}
				return f.Write(p)
			}
			c := s.begin(randomID(), []byte("input"), time.Now())
			c.response(time.Now(), 0, []byte("abcdef"))
			c.trigger("incident", "output_stall", time.Now())
			c.finish(time.Now(), "backend_failure", false, false)
			captureWaitReleased(t, c)
			m := captureReadMeta(t, s, c.id)
			want := int64(0)
			if kind == "partial" {
				want = 2
			}
			if m.CaptureState != "truncated" || m.ResponseSavedBytes != want || m.FirstMissingResponseOffset == nil || *m.FirstMissingResponseOffset != want || s.failures.Load() == 0 {
				t.Fatalf("failure not isolated: %+v", m)
			}
			captureAssertReleased(t, s)
		})
	}
}

func captureSeedRecord(t *testing.T, root, name string, m streamCaptureMeta, files map[string][]byte) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(path, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := json.Marshal(m)
	if err := os.WriteFile(filepath.Join(path, "meta.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestStreamCaptureRecoveryPreservesEvidenceAndIncompleteTail(t *testing.T) {
	root := filepath.Join(t.TempDir(), "stream-incidents", "requests")
	id, orphan, summary := randomID(), randomID(), randomID()
	now := time.Now()
	event, _ := json.Marshal(captureTimelineEvent{Schema: 1, Sequence: 1, captureEvent: captureEvent{Type: "read_return", At: now}})
	line := append(event, '\n')
	timeline := append(append([]byte(nil), line...), []byte(`{"schema_version":1,"sequence":2`)...)
	captureSeedRecord(t, root, ".pending-"+id, streamCaptureMeta{Schema: 1, RequestID: id, StartedAt: now, Retained: true, CaptureState: "recording", RequestExpectedBytes: 4}, map[string][]byte{"request.json": []byte("body"), "response.sse": []byte("prefix"), "timeline.jsonl": timeline})
	orphanPath := captureSeedRecord(t, root, ".pending-"+orphan, streamCaptureMeta{Schema: 1, RequestID: orphan, StartedAt: now}, map[string][]byte{"response.sse": []byte("normal")})
	summaryPath := captureSeedRecord(t, root, ".pending-"+summary, streamCaptureMeta{Schema: 1, RequestID: summary, StartedAt: now}, map[string][]byte{"response.sse": []byte("summary-correlated")})
	if err := os.Remove(filepath.Join(summaryPath, "meta.json")); err != nil {
		t.Fatal(err)
	}
	hour := filepath.Join(filepath.Dir(root), now.UTC().Format("2006010215"))
	if err := os.Mkdir(hour, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hour, "summary.json"), []byte(fmt.Sprintf(`{"request_id":%q}`, summary)), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := newStreamCaptureStore(root, "service", "router", "new-boot", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	m := captureReadMeta(t, s, id)
	if m.CaptureState != "end_unknown" || m.ResponseSavedBytes != 6 || m.TimelineCompleteBytes != int64(len(line)) || m.LastTimelineSequence != 1 || m.FirstMissingTimelineSequence == nil || *m.FirstMissingTimelineSequence != 2 || !m.RequestSaved {
		t.Fatalf("bad recovered boundary: %+v", m)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("normal orphan was retained: %v", err)
	}
	if m := captureReadMeta(t, s, summary); m.CaptureState != "end_unknown" {
		t.Fatalf("summary did not retain directory: %+v", m)
	}
	retained, _ := os.ReadFile(filepath.Join(root, "request-"+id, "timeline.jsonl"))
	if !bytes.Equal(retained, timeline) {
		t.Fatal("recovery rewrote the partial evidence")
	}
}

func TestStreamCaptureUnknownFilesAndLinksNeverRemoved(t *testing.T) {
	root := filepath.Join(t.TempDir(), "requests")
	id := randomID()
	old := time.Now().Add(-100 * time.Hour)
	path := captureSeedRecord(t, root, "request-"+id, streamCaptureMeta{Schema: 1, RequestID: id, StartedAt: old, Retained: true, CaptureState: "complete", EndedAt: &old}, map[string][]byte{"unknown.bin": bytes.Repeat([]byte("x"), 200)})
	external := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(external, []byte("do not delete"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(path, "response.sse")); err != nil {
		t.Fatal(err)
	}
	s, err := newStreamCaptureStore(root, "service", "router", "boot", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if s.diskBytes < 200 {
		t.Fatalf("unknown file missing from disk budget: %d", s.diskBytes)
	}
	if _, err := os.Lstat(filepath.Join(path, "response.sse")); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(external); err != nil || string(data) != "do not delete" {
		t.Fatalf("followed symlink: %q %v", data, err)
	}
}

func TestStreamCaptureDiskLimitAndExpiredActiveInput(t *testing.T) {
	t.Run("disk", func(t *testing.T) {
		s := captureTestStore(t)
		s.maxDisk = captureMetaReserve + 8
		c := s.begin(randomID(), []byte("input"), time.Now())
		c.response(time.Now(), 0, bytes.Repeat([]byte("x"), 100))
		c.trigger("incident", "output_stall", time.Now())
		c.finish(time.Now(), "success", true, true)
		captureWaitReleased(t, c)
		m := captureReadMeta(t, s, c.id)
		if m.CaptureState != "truncated" || m.ResponseSavedBytes != 0 || m.FailureReason != "disk_budget_exhausted" {
			t.Fatalf("disk limit not enforced: %+v", m)
		}
		if size, err := captureTreeBytes(s.root); err != nil || size > s.maxDisk {
			t.Fatalf("disk cap exceeded: %d %v", size, err)
		}
		captureAssertReleased(t, s)
	})
	t.Run("active-expiry", func(t *testing.T) {
		s := captureTestStore(t)
		c := s.begin(randomID(), []byte("held original input"), time.Now().Add(-73*time.Hour))
		c.event(captureEvent{Type: "read_return", At: time.Now()})
		deadline := time.After(2 * time.Second)
		for {
			s.mu.Lock()
			expired, raw, active := c.expired, c.raw, len(s.active)
			s.mu.Unlock()
			if expired {
				if raw != nil || active != 1 {
					t.Fatal("expiry did not release body into bounded tombstone")
				}
				break
			}
			select {
			case <-deadline:
				t.Fatal("active capture did not expire")
			case <-time.After(time.Millisecond):
			}
		}
		c.trigger("too-late", "output_stall", time.Now())
		c.response(time.Now(), 0, []byte("ignored"))
		c.finish(time.Now(), "success", true, true)
		captureWaitReleased(t, c)
		entries, _ := os.ReadDir(s.root)
		if len(entries) != 0 {
			t.Fatal("expired capture recreated files")
		}
		captureAssertReleased(t, s)
	})
}

func TestStreamCaptureNilMethodsAndBoundedEventFields(t *testing.T) {
	var store *streamCaptureStore
	c := store.begin(randomID(), nil, time.Now())
	c.setResponse(200, "", "", "", false)
	c.response(time.Now(), 0, nil)
	c.event(captureEvent{})
	c.trigger("", "", time.Now())
	c.finish(time.Now(), "", false, false)
	store.close()
	s := captureTestStore(t)
	c = s.begin(randomID(), []byte("input"), time.Now())
	c.setResponse(200, strings.Repeat("x", 1<<20), strings.Repeat("g", 1<<20), "proxy", true)
	c.trigger("incident", "output_stall", time.Now())
	c.event(captureEvent{Type: "write_return", At: time.Now(), Error: strings.Repeat("e", 1<<20), Data: []byte("gateway-error")})
	c.finish(time.Now(), "success", true, true)
	captureWaitReleased(t, c)
	m := captureReadMeta(t, s, c.id)
	if len(m.ContentType) != 256 || len(m.ContentEncoding) != 128 {
		t.Fatal("unbounded response metadata")
	}
	f, err := os.Open(filepath.Join(s.root, "request-"+c.id, "timeline.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, _ := io.ReadAll(f)
	if bytes.Contains(data, []byte(strings.Repeat("e", 1025))) {
		t.Fatal("unbounded event error")
	}
	captureAssertReleased(t, s)
}

func TestStreamCaptureOptionalEventTimestampsUseUTC(t *testing.T) {
	s := captureTestStore(t)
	at := time.Now().In(time.FixedZone("test-local", 8*60*60))
	c := s.begin(randomID(), []byte("input"), at.Add(-time.Second))
	c.trigger("incident", "output_stall", at, captureEvent{StartedAt: at.Add(-time.Minute), WriteStartedAt: at.Add(-time.Second), FlushStartedAt: at, LastUsefulAt: at.Add(-2 * time.Minute)})
	c.finish(at.Add(time.Second), "success", true, true)
	captureWaitReleased(t, c)
	data, err := os.ReadFile(filepath.Join(s.root, "request-"+c.id, "timeline.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("+08:00")) {
		t.Fatalf("local time escaped into timeline: %s", data)
	}
	var event captureTimelineEvent
	if err := json.Unmarshal(bytes.SplitN(data, []byte{'\n'}, 2)[0], &event); err != nil {
		t.Fatal(err)
	}
	for _, value := range []time.Time{event.At, event.StartedAt, event.WriteStartedAt, event.FlushStartedAt, event.LastUsefulAt} {
		if value.IsZero() || value.Location() != time.UTC {
			t.Fatalf("lost UTC event time: %v", value)
		}
	}
	if event.RelativeSeconds != 1 {
		t.Fatalf("request-relative time changed: %g", event.RelativeSeconds)
	}
}
