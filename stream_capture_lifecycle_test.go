//go:build darwin || linux

package aigate

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The helper runs the actual server lifecycle in a separate OS process, with
// exactly the deferred ProxyGateway.Close used by RunProxy. Only its disk writer
// is replaced. It deliberately never unblocks, including after SIGTERM.
func TestCaptureLifecycleProcessHelper(t *testing.T) {
	if os.Getenv("AIGATE_CAPTURE_LIFECYCLE_HELPER") != "1" {
		return
	}
	diskEntered := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"data":[{"id":"fixture-model"}]}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, completionEvent(0, map[string]any{"content": "before"}, nil))
		http.NewResponseController(w).Flush()
		select {
		case <-diskEntered:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, completionEvent(0, map[string]any{"content": "complete"}, "stop")+"data: [DONE]\n\n")
	}))
	defer upstream.Close()
	p, err := newProxyGateway(ProxyConfig{ServiceID: "lifecycle", RouterID: "lifecycle-router",
		Backend:  &Backend{Adapter: "openai", URL: upstream.URL + "/v1/chat/completions", ModelsURL: upstream.URL + "/v1/models"},
		Settings: json.RawMessage(`{"profile":{"enabled":true}}`)}, os.Getenv("AIGATE_CAPTURE_LIFECYCLE_ROOT"), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var entered sync.Once
	p.g.captures.writePayload = func(_ *os.File, _ []byte) (int, error) {
		entered.Do(func() { fmt.Fprintln(os.Stdout, "capture-writer-blocked"); close(diskEntered) })
		select {}
	}
	servers := []*http.Server{businessHTTPServer(os.Getenv("AIGATE_CAPTURE_LIFECYCLE_ADDR"), p)}
	if err = serveProcess(servers, p.connections, p.quiesce, 2*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureSIGTERMExitsWithFrozenDiskAndHoldsDirectoryLock(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCaptureLifecycleProcessHelper$", "-test.count=1", "-test.timeout=20s")
	// Race instrumentation normally adds a one-second sleep after main returns;
	// disable that harness-only delay when measuring the service exit budget.
	cmd.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0", "AIGATE_CAPTURE_LIFECYCLE_HELPER=1", "AIGATE_CAPTURE_LIFECYCLE_ROOT="+root, "AIGATE_CAPTURE_LIFECYCLE_ADDR="+address)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	blocked := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "capture-writer-blocked" {
				close(blocked)
				return
			}
		}
	}()
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(5 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		response, requestErr := client.Get("http://" + address + "/runtime/v1/status")
		if requestErr == nil {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			ready = response.StatusCode == http.StatusOK
			if ready {
				break
			}
		}
		select {
		case err := <-exited:
			t.Fatalf("helper exited before readiness: %v: %s", err, stderr.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !ready {
		t.Fatal("helper never became ready")
	}
	response, err := client.Post("http://"+address+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("[DONE]")) {
		t.Fatalf("business did not finish while disk froze: status=%d err=%v body=%q", response.StatusCode, err, body)
	}
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("capture writer was not reached")
	}
	directory := filepath.Join(root, "stream-incidents", "requests")
	assertLocked := func() {
		t.Helper()
		file, err := os.Open(directory)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err := lockState(file); err == nil {
			unlockState(file)
			t.Fatal("capture directory lock released before frozen worker/process stopped")
		}
	}
	assertLocked()
	started := time.Now()
	if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// Business is already drained. Check ownership during the five-second close
	// budget; the test grants one second solely for process/timer scheduling.
	timer := time.NewTimer(6 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-exited:
			if err != nil {
				t.Fatalf("SIGTERM helper failed: %v: %s", err, stderr.String())
			}
			file, openErr := os.Open(directory)
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer file.Close()
			if err := lockState(file); err != nil {
				t.Fatalf("directory lock remained after process exit: %v", err)
			}
			unlockState(file)
			t.Logf("drained process exited after SIGTERM in %s with its capture writer permanently blocked", time.Since(started))
			return
		case <-ticker.C:
			// Avoid racing a final successful exit while checking a released lock.
			if time.Since(started) < 4*time.Second {
				assertLocked()
			}
		case <-timer.C:
			t.Fatal("SIGTERM exceeded the drained-service five-second close budget plus scheduling allowance")
		}
	}
}

func TestCaptureWriteTimingExcludesBetweenCallAndAfterReturnWork(t *testing.T) {
	store, request, started := incidentFixture()
	request.startWrite(started, 10)
	request.wroteAt(started.Add(time.Second), 10)
	store.tick(started.Add(90 * time.Second))
	noIncident(t, store)
	request.startFlush(started.Add(90*time.Second), 10)
	request.flushedAt(started.Add(91 * time.Second))
	store.tick(started.Add(10 * time.Minute))
	noIncident(t, store)
	value := request.snapshot(request.newRecord("client_write_stall", started, started.Add(91*time.Second)), started.Add(10*time.Minute))
	if value.WriteSeconds != 1 || value.FlushSeconds != 1 || value.DurationSeconds != 2 {
		t.Fatalf("non-I/O work counted as a write stall: %+v", value)
	}
	request.endWrite(started.Add(91*time.Second), nil)
	noIncident(t, store)
}

type captureEOFReader struct{ data []byte }

func (r *captureEOFReader) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, io.EOF
	}
	return n, nil
}
func (r *captureEOFReader) Close() error { return nil }

type captureFailureWriter struct {
	header http.Header
	store  *streamCaptureStore
}

func (w *captureFailureWriter) Header() http.Header { return w.header }
func (w *captureFailureWriter) WriteHeader(int)     {}
func (w *captureFailureWriter) Write(p []byte) (int, error) {
	return len(p) / 2, io.ErrClosedPipe
}
func (w *captureFailureWriter) FlushError() error {
	w.store.mu.Lock()
	var active *streamCapture
	for c := range w.store.active {
		active = c
	}
	w.store.mu.Unlock()
	if active != nil {
		active.trigger("header-fixture", "client_write_stall", time.Now())
	}
	return nil
}

func TestCaptureObservedEOFSurvivesDownstreamWriteFailureAndHeaderFlushFlags(t *testing.T) {
	store, err := newStreamCaptureStore(filepath.Join(t.TempDir(), "requests"), "fixture", "r1", "boot", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	g := &Gateway{captures: store}
	g.buffers.New = func() any { buffer := make([]byte, 32768); return &buffer }
	id := randomID()
	entry := &auditEntry{RequestID: id, started: time.Now(), generation: &generationResult{stream: true, observer: newSSEObserver(false)}}
	writer := &captureFailureWriter{header: make(http.Header), store: store}
	data := []byte(completionEvent(0, map[string]any{"content": "last"}, "stop") + "data: [DONE]\n\n")
	response := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: &captureEOFReader{data: data}}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if got := g.stream(writer, request, response, context.Background(), entry, []byte(`{"messages":[],"stream":true}`)); got != "client_failure" {
		t.Fatalf("unexpected outcome %q", got)
	}
	store.close()
	directory := filepath.Join(store.root, "request-"+id)
	raw, err := os.ReadFile(filepath.Join(directory, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta streamCaptureMeta
	if err = json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	if !meta.EOF || !meta.Done || meta.Outcome != "client_failure" || meta.ResponseReceivedBytes != int64(len(data)) {
		t.Fatalf("observed EOF/DONE lost after downstream failure: %+v", meta)
	}
	raw, err = os.ReadFile(filepath.Join(directory, "timeline.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var starts, returns int
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var event captureTimelineEvent
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		if event.Phase != "headers_flush" {
			continue
		}
		switch event.Type {
		case "flush_start":
			starts++
			if event.FlushReturned {
				t.Fatal("pending header flush claimed to have returned")
			}
		case "flush_return":
			returns++
			if !event.FlushReturned {
				t.Fatal("completed header flush marked unknown")
			}
		}
	}
	if starts != 1 || returns != 1 {
		t.Fatalf("header flush events: starts=%d returns=%d", starts, returns)
	}
}
