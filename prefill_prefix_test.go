package aigate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func prefixFixture(t *testing.T) *prefillPrefixStore {
	t.Helper()
	s, err := newPrefillPrefixStore(t.TempDir(), 0, "r1", "s1", "cfg")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	return s
}
func prefixDrain(t *testing.T, s *prefillPrefixStore) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pending, _ := s.queueStatus()
		if pending == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("prefix queue did not drain")
}
func prefixExport(t *testing.T, s *prefillPrefixStore, start float64) []map[string]any {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/profile/v1/prefill-prefix/export?start="+strconv.FormatFloat(start, 'f', -1, 64)+"&end="+strconv.FormatFloat(profileNow()+1, 'f', -1, 64), nil)
	s.serveHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("export %d %s", w.Code, w.Body.String())
	}
	rows := []map[string]any{}
	for _, line := range bytes.Split(bytes.TrimSpace(w.Body.Bytes()), []byte{'\n'}) {
		var row map[string]any
		if json.Unmarshal(line, &row) != nil {
			t.Fatalf("invalid line: %s", line)
		}
		rows = append(rows, row)
	}
	return rows
}
func TestPrefillPrefixCanonicalAndChain(t *testing.T) {
	s := prefixFixture(t)
	var a, b prefixRecord
	s.fingerprint(&a, []byte(`{"messages":[{"role":"user","content":"中文🙂<&>"}],"tools":[{"b":2,"a":1}]}`))
	s.fingerprint(&b, []byte(`{"tools":[{"a":1,"b":2}],"messages":[{"content":"中文🙂<&>","role":"user"}]}`))
	if !reflect.DeepEqual(a, b) || len(a.Marks) != 1 || a.Marks[0].EndBytes != a.MessagesBytes {
		t.Fatal(a, b)
	}
	canonical, _ := prefixCanonical([]any{map[string]any{"content": "中文🙂<&>", "role": "user"}})
	if a.MessagesBytes != len(canonical) || bytes.Contains(canonical, []byte(`\u003c`)) {
		t.Fatal(string(canonical), a)
	}
	long := strings.Repeat("shared", 1000)
	for _, content := range []string{long + "a", long + "b", "changed" + long + "b"} {
		payload, _ := json.Marshal(map[string]any{"messages": []any{map[string]string{"role": "user", "content": content}}})
		var row prefixRecord
		s.fingerprint(&row, payload)
		switch content {
		case long + "a":
			a = row
		case long + "b":
			b = row
		default:
			if row.Marks[0].Digest == a.Marks[0].Digest || row.Marks[1].Digest == a.Marks[1].Digest {
				t.Fatal("lost parent context")
			}
		}
	}
	if a.Marks[0] != b.Marks[0] || a.Marks[len(a.Marks)-1].Digest == b.Marks[len(b.Marks)-1].Digest {
		t.Fatal("prefix or tail mismatch")
	}
	// Change only an early byte, keeping every later full block identical.
	// A digest of an isolated block would incorrectly match the second block.
	for i, content := range []string{strings.Repeat("a", 3000), "b" + strings.Repeat("a", 2999)} {
		payload, _ := json.Marshal(map[string]any{"messages": []any{map[string]string{"role": "user", "content": content}}})
		var row prefixRecord
		s.fingerprint(&row, payload)
		if i == 0 {
			a = row
		} else if row.Marks[1].Digest == a.Marks[1].Digest {
			t.Fatal("second block lost preceding context")
		}
	}
	for _, raw := range []string{`[]`, `{"messages":[]}`, `{"messages":[1]}`, `{"prompt":"private"}`} {
		var row prefixRecord
		s.fingerprint(&row, []byte(raw))
		if row.Reason == "" || len(row.Marks) > 0 {
			t.Fatal(raw, row)
		}
	}
}

type prefixBlockingWriter struct {
	*httptest.ResponseRecorder
	started bool
	ready   chan struct{}
	release chan struct{}
}

func (w *prefixBlockingWriter) Write(data []byte) (int, error) {
	if !w.started {
		w.started = true
		close(w.ready)
		<-w.release
	}
	return w.ResponseRecorder.Write(data)
}
func TestPrefillPrefixExportSnapshotExcludesLaterAppends(t *testing.T) {
	s := prefixFixture(t)
	e := profileEvent{ID: "first", Boot: "boot", Seq: 1, Arrived: profileNow(), At: profileNow()}
	s.input(e, []byte(`{"messages":[{}]}`))
	prefixDrain(t, s)
	w := &prefixBlockingWriter{ResponseRecorder: httptest.NewRecorder(), ready: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { s.export(w, httptest.NewRequest("GET", "/", nil), 1, profileNow()+5, "", ""); close(done) }()
	<-w.ready
	e.ID = "second"
	e.Seq = 2
	e.Arrived = profileNow()
	e.At = e.Arrived
	s.input(e, []byte(`{"messages":[{}]}`))
	prefixDrain(t, s)
	close(w.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("export did not finish")
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(`"request_id":"first"`)) || bytes.Contains(w.Body.Bytes(), []byte(`"request_id":"second"`)) {
		t.Fatal("export read beyond fixed size")
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(`"kind":"complete"`)) {
		t.Fatal("completion marker missing")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exports != 0 || s.segments[0].readers != 0 {
		t.Fatal("snapshot pins leaked")
	}
}
func TestPrefillPrefixLongInputLifecycleAndRestart(t *testing.T) {
	s := prefixFixture(t)
	start := profileNow()
	e := profileEvent{ID: "req", Boot: "boot", Seq: 1, Arrived: start, At: start, Backend: "api", Model: "actual", Bytes: 1, Stream: true, Traffic: "replay"}
	payload, _ := json.Marshal(map[string]any{"messages": []any{map[string]string{"role": "user", "content": strings.Repeat("private秘密", 30000)}}, "tools": []any{map[string]string{"name": "private-tool"}}})
	s.input(e, payload)
	e.At = profileNow()
	e.Outcome = "client_cancelled"
	e.Usage = json.RawMessage(`{"prompt_tokens":1234,"prompt_tokens_details":{"cached_tokens":0}}`)
	s.end(e, 200)
	prefixDrain(t, s)
	rows := prefixExport(t, s, start)
	if len(rows) != 4 || rows[1]["kind"] != "input" || rows[2]["kind"] != "result" || rows[2]["cached_tokens"] != float64(0) {
		t.Fatal(rows)
	}
	if rows[1]["traffic"] != "replay" || rows[2]["outcome"] != "client_cancelled" {
		t.Fatal(rows)
	}
	input, _ := json.Marshal(rows[1])
	if len(input) <= 16<<10 {
		t.Fatal("fixture did not exceed legacy limit")
	}
	checksum := sha256.New()
	for _, segment := range s.segments {
		raw, _ := os.ReadFile(segment.path)
		checksum.Write(raw)
		for _, secret := range []string{"private", "秘密", "private-tool"} {
			if bytes.Contains(raw, []byte(secret)) {
				t.Fatal("prompt persisted")
			}
		}
	}
	if rows[3]["records_sha256"] != hex.EncodeToString(checksum.Sum(nil)) || rows[3]["complete"] != true {
		t.Fatal(rows[3])
	}
	keyID, firstMark := s.keyID, rows[1]["prefix_marks"]
	root := s.root
	s.close()
	restarted, err := newPrefillPrefixStore(root, 0, "r1", "s1", "cfg")
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	if restarted.keyID != keyID || restarted.state.Counters["unclean_restart"] != 0 {
		t.Fatal("key or clean state changed")
	}
	e.Boot = "boot2"
	e.Seq = 2
	e.At = profileNow()
	e.Arrived = e.At
	e.Usage = nil
	restarted.input(e, payload)
	restarted.end(e, 500)
	prefixDrain(t, restarted)
	rows = prefixExport(t, restarted, start)
	if !reflect.DeepEqual(rows[3]["prefix_marks"], firstMark) || rows[4]["prompt_tokens"] != nil || rows[4]["usage_quality"] != "prompt_tokens_missing" {
		t.Fatal("restart or missing usage", rows[4])
	}
}
func TestPrefillPrefixCapacityAndQueueLoss(t *testing.T) {
	s := prefixFixture(t)
	s.queueMu.Lock()
	s.queued = prefixQueueBytes
	s.queueMu.Unlock()
	e := profileEvent{ID: "lost", Arrived: profileNow(), At: profileNow()}
	s.input(e, []byte(`{"messages":[{}]}`))
	s.queueMu.Lock()
	s.queued = 0
	s.queueMu.Unlock()
	s.flushDrops()
	s.mu.Lock()
	if s.state.Counters["queue_dropped"] != 1 {
		t.Fatal(s.state)
	}
	s.maxBytes = 100
	s.mu.Unlock()
	// Exercise retention independently of the queue worker with known sizes.
	for i := 0; i < 3; i++ {
		path := filepath.Join(s.root, fmt.Sprintf("fixture-%d", i))
		os.WriteFile(path, bytes.Repeat([]byte("x"), 50), 0600)
		s.mu.Lock()
		s.segments = append(s.segments, &prefixSegment{path: path, size: 50, maximum: float64(i + 1)})
		s.stored += 50
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.segments[0].readers++
	if s.roomLocked(1) {
		t.Fatal("evicted pinned segment")
	}
	s.segments[0].readers--
	if !s.roomLocked(1) || s.stored > 100 || s.state.Counters["evicted_segments"] == 0 {
		t.Fatal(s.state)
	}
	s.mu.Unlock()
	rows := prefixExport(t, s, 1)
	if rows[0]["window_partially_expired"] != true || rows[len(rows)-1]["data_complete"] != false {
		t.Fatal(rows)
	}
}

type prefixFailWriter struct {
	header http.Header
	writes int
}

func (w *prefixFailWriter) Header() http.Header { return w.header }
func (w *prefixFailWriter) WriteHeader(int)     {}
func (w *prefixFailWriter) Write(b []byte) (int, error) {
	w.writes++
	if w.writes > 1 {
		return 0, io.ErrClosedPipe
	}
	return len(b), nil
}
func TestPrefillPrefixExportReleaseAndValidation(t *testing.T) {
	s := prefixFixture(t)
	e := profileEvent{ID: "r", Boot: "b", Arrived: profileNow(), At: profileNow(), Backend: "api", Model: "m"}
	s.input(e, []byte(`{"messages":[{"content":"hi"}]}`))
	prefixDrain(t, s)
	w := &prefixFailWriter{header: http.Header{}}
	s.export(w, httptest.NewRequest("GET", "/", nil), 1, profileNow()+1, "", "")
	if s.exports != 0 || s.segments[0].readers != 0 {
		t.Fatal("export leaked pins")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.export(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil).WithContext(ctx), 1, profileNow()+1, "", "")
	if s.exports != 0 {
		t.Fatal("cancel leaked export")
	}
	for _, query := range []string{"start=NaN&end=3", "start=1&end=Inf", "start=3&end=1"} {
		out := httptest.NewRecorder()
		s.serveHTTP(out, httptest.NewRequest("GET", "/profile/v1/prefill-prefix/export?"+query, nil))
		if out.Code != 400 {
			t.Fatal(query, out.Code)
		}
	}
	s.mu.Lock()
	s.exports = 2
	s.mu.Unlock()
	out := httptest.NewRecorder()
	s.export(out, httptest.NewRequest("GET", "/", nil), 1, profileNow()+1, "", "")
	if out.Code != 429 {
		t.Fatal(out.Code)
	}
	s.mu.Lock()
	s.exports = 0
	s.mu.Unlock()
}
func TestPrefillPrefixGatewayUsesAdaptedPayload(t *testing.T) {
	t.Setenv("AIGATE_PROFILE_KEY", "fixture-key")
	u := upstreamServer(t, "actual-model", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"answer"}}],"usage":{"prompt_tokens":10}}`)
	})
	c := fixtureConfig(t, u.URL)
	c.Profile.Enabled = true
	c.Profile.Directory = t.TempDir()
	c.Backend.Overrides = map[string]json.RawMessage{"messages": json.RawMessage(`[{"role":"user","content":"adapted-secret"}]`)}
	g, server, _ := serveTest(t, c)
	response, _ := request(t, server, "POST", "/v1/chat/completions", `{"messages":[{"role":"user","content":"original-secret"}]}`, "")
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	prefixDrain(t, g.profile.prefix)
	rows := prefixExport(t, g.profile.prefix, 1)
	var expected prefixRecord
	g.profile.prefix.fingerprint(&expected, []byte(`{"messages":[{"role":"user","content":"adapted-secret"}]}`))
	want, _ := json.Marshal(expected.Marks)
	var decoded any
	json.Unmarshal(want, &decoded)
	if !reflect.DeepEqual(rows[1]["prefix_marks"], decoded) || rows[1]["model"] != "actual-model" || rows[2]["prompt_tokens"] != float64(10) {
		t.Fatal(rows)
	}
}

func TestPrefillPrefixCrashAndWriteFailure(t *testing.T) {
	s := prefixFixture(t)
	root := s.root
	s.close()
	state := s.state
	state.Clean = false
	raw, _ := json.Marshal(state)
	if err := os.WriteFile(filepath.Join(root, "state.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := newPrefillPrefixStore(root, 0, "r1", "s1", "cfg")
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	if restarted.state.Counters["unclean_restart"] != 1 {
		t.Fatal(restarted.state)
	}
	e := profileEvent{ID: "r", Boot: "b", Arrived: profileNow(), At: profileNow()}
	restarted.mu.Lock()
	restarted.root = filepath.Join(root, "missing", "unavailable")
	restarted.mu.Unlock()
	restarted.input(e, []byte(`{"messages":[{}]}`))
	prefixDrain(t, restarted)
	restarted.mu.Lock()
	if restarted.state.Counters["write_errors"] == 0 {
		t.Fatal("unreported write failure")
	}
	restarted.root = root
	restarted.mu.Unlock()
}

func TestPrefillPrefixEnqueueDoesNotWaitForStorage(t *testing.T) {
	s := prefixFixture(t)
	s.mu.Lock() // Simulate a slow disk operation owning the storage mutex.
	done := make(chan struct{})
	go func() {
		s.input(profileEvent{ID: "nonblocking", Arrived: profileNow(), At: profileNow()}, []byte(`{"messages":[{}]}`))
		close(done)
	}()
	select {
	case <-done:
		s.mu.Unlock()
	case <-time.After(time.Second):
		s.mu.Unlock()
		t.Fatal("request enqueue waited for storage")
	}
	prefixDrain(t, s)
}

func TestPrefillPrefixExportSkipsSegmentsOutsideArrivalWindow(t *testing.T) {
	s := prefixFixture(t)
	now := profileNow()
	e := profileEvent{ID: "old", Boot: "b", Seq: 1, Arrived: now - 120, At: now}
	s.input(e, []byte(`{"messages":[{}]}`))
	prefixDrain(t, s)
	s.mu.Lock()
	s.closeActiveLocked()
	old := s.segments[0].path
	s.mu.Unlock()
	e.ID = "recent"
	e.Seq = 2
	e.Arrived = now - 60
	s.input(e, []byte(`{"messages":[{}]}`))
	prefixDrain(t, s)
	// This segment cannot contribute to the window and must never be opened.
	if err := os.Remove(old); err != nil {
		t.Fatal(err)
	}
	rows := prefixExport(t, s, now-90)
	if len(rows) != 3 || rows[1]["request_id"] != "recent" || rows[2]["complete"] != true {
		t.Fatal(rows)
	}
}
