package aigate

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPrefillPrefixLossStaysVisibleDuringMerge(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)
	now := profileNow()
	s := &prefillPrefixStore{
		queue:       make(chan prefixJob, 1),
		state:       prefixState{Started: now - 100, Floor: now - 100, Counters: map[string]uint64{}},
		pendingDrop: prefixGap{Start: now - 10, End: now, Count: 1},
	}
	s.mu.Lock()
	started, done := make(chan struct{}), make(chan struct{})
	go func() { close(started); s.flushDrops(); close(done) }()
	<-started
	runtime.Gosched() // Let the merge reach the held storage mutex.
	_, pending := s.queueStatus()
	s.mu.Unlock()
	<-done
	if pending != 1 {
		t.Fatal("loss became invisible before reaching the snapshot's gap list", pending)
	}
	w := httptest.NewRecorder()
	s.export(w, httptest.NewRequest("GET", "/", nil), now-20, now-1, "", "")
	rows := bytes.Split(bytes.TrimSpace(w.Body.Bytes()), []byte{'\n'})
	var manifest, complete map[string]any
	if json.Unmarshal(rows[0], &manifest) != nil || json.Unmarshal(rows[len(rows)-1], &complete) != nil {
		t.Fatal("invalid export", w.Body.String())
	}
	if complete["data_complete"] != false || len(manifest["gaps"].([]any)) != 1 || s.state.Counters["queue_dropped"] != 1 {
		t.Fatal("loss missing from export", manifest, complete)
	}
}

// Pause a real response between manifest and data. The actual observation
// handler and HTTP server both impose ten-second deadlines on ordinary queries.
type prefixPausedResponse struct {
	http.ResponseWriter
	blocked chan struct{}
	release <-chan struct{}
	writes  int
}

func (w *prefixPausedResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *prefixPausedResponse) Write(data []byte) (int, error) {
	w.writes++
	if w.writes == 2 {
		close(w.blocked)
		<-w.release
	}
	return w.ResponseWriter.Write(data)
}

func prefixHTTPExportFixture(t *testing.T) (*prefillPrefixStore, []byte, float64) {
	t.Helper()
	s := prefixFixture(t)
	now := profileNow()
	record := s.base(profileEvent{ID: "streaming-fixture", Arrived: now - 2, At: now - 2})
	record.Kind = "input"
	s.fingerprint(&record, []byte(`{"messages":[{"role":"user","content":"`+strings.Repeat("x", 1<<20)+`"}]}`))
	row, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	row = append(row, '\n')
	path := filepath.Join(s.root, "trace-fixture.jsonl")
	if err := os.WriteFile(path, row, 0600); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.state.Floor = now - 100
	s.segments = []*prefixSegment{{path: path, size: int64(len(row)), minimum: now - 2, maximum: now - 2}}
	s.stored = int64(len(row))
	s.mu.Unlock()
	return s, row, now
}

func prefixAssertNoExportPins(t *testing.T, s *prefillPrefixStore) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exports != 0 || s.segments[0].readers != 0 {
		t.Fatal("export did not release its slot/segment pin", s.exports, s.segments[0].readers)
	}
}

func TestPrefillPrefixHTTPExportOutlivesObservationDeadline(t *testing.T) {
	s, record, now := prefixHTTPExportFixture(t)
	p := &ProxyGateway{g: &Gateway{profile: &requestProfiler{prefix: s, c: ProfileConfig{noAuth: true}}}}
	blocked, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	handlerDone := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		p.ObservationHandler().ServeHTTP(&prefixPausedResponse{ResponseWriter: w, blocked: blocked, release: release}, r)
	}))
	server.Config.ReadTimeout, server.Config.WriteTimeout = 10*time.Second, 10*time.Second
	server.Start()
	defer func() { unblock(); server.Close() }()
	client := server.Client()
	client.Timeout = 20 * time.Second
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		response, err := client.Get(fmt.Sprintf("%s/profile/v1/prefill-prefix/export?start=%.9f&end=%.9f", server.URL, now-3, now-1))
		if err != nil {
			done <- result{err: err}
			return
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		done <- result{data, err}
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		unblock()
		t.Fatal("export did not reach response body")
	}
	time.Sleep(11 * time.Second)
	unblock()
	response := <-done
	<-handlerDone
	if response.err != nil {
		t.Fatal("long export interrupted", response.err)
	}
	rows := bytes.Split(bytes.TrimSpace(response.data), []byte{'\n'})
	if len(rows) != 3 || !bytes.Equal(append(bytes.Clone(rows[1]), '\n'), record) {
		t.Fatal("long export changed records", len(rows))
	}
	var complete map[string]any
	if err := json.Unmarshal(rows[2], &complete); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(record)
	if complete["complete"] != true || complete["data_complete"] != true || complete["records_sha256"] != hex.EncodeToString(sum[:]) {
		t.Fatal("long export missing valid completion", complete)
	}
	prefixAssertNoExportPins(t, s)
}

type prefixPipeResponse struct{ net.Conn }

func (w prefixPipeResponse) Header() http.Header { return http.Header{} }
func (w prefixPipeResponse) WriteHeader(int)     {}
func (w prefixPipeResponse) FlushError() error   { return nil }

func TestPrefillPrefixExportStalledWriteTimesOut(t *testing.T) {
	connection, peer := net.Pipe()
	defer connection.Close()
	defer peer.Close()
	response := prefixPipeResponse{connection}
	w := &prefixExportWriter{writer: response, controller: http.NewResponseController(response),
		ctx: context.Background(), timeout: 20 * time.Millisecond}
	// The peer never reads: a real blocked network write must terminate.
	_, err := w.Write([]byte("stalled export"))
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatal("stalled write did not time out", err)
	}
}

type prefixFlushFailureResponse struct {
	*httptest.ResponseRecorder
	flushes int
}

func (w *prefixFlushFailureResponse) FlushError() error {
	w.flushes++
	if w.flushes > 1 {
		return io.ErrClosedPipe
	}
	return nil
}

func TestPrefillPrefixExportFlushFailureOmitsCompletion(t *testing.T) {
	s, _, now := prefixHTTPExportFixture(t)
	w := &prefixFlushFailureResponse{ResponseRecorder: httptest.NewRecorder()}
	s.export(w, httptest.NewRequest("GET", "/", nil), now-3, now-1, "", "")
	if w.flushes < 2 || bytes.Contains(w.Body.Bytes(), []byte(`"kind":"complete"`)) {
		t.Fatal("failed transport flush reported completion", w.flushes)
	}
	prefixAssertNoExportPins(t, s)
}

func TestPrefillPrefixHTTPExportCancellationReleasesPins(t *testing.T) {
	s, _, now := prefixHTTPExportFixture(t)
	p := &ProxyGateway{g: &Gateway{profile: &requestProfiler{prefix: s, c: ProfileConfig{noAuth: true}}}}
	done, blocked := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		p.ObservationHandler().ServeHTTP(&prefixPausedResponse{ResponseWriter: w, blocked: blocked, release: r.Context().Done()}, r)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/profile/v1/prefill-prefix/export?start=%.9f&end=%.9f", server.URL, now-3, now-1), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err := bufio.NewReader(response.Body).ReadBytes('\n'); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("export did not pause with a pinned segment")
	}
	cancel()
	response.Body.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled export kept running")
	}
	prefixAssertNoExportPins(t, s)
}
