package aigate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const slowTestBody = " {\n  \"stream\":true, \"model\":\"original-model\",\n  \"messages\":[{\"role\":\"system\",\"content\":\"private system\"},{\"role\":\"user\",\"content\":\"一二三四五六七八九十秘密\\n尾部\"},{\"role\":\"assistant\",\"tool_calls\":[{\"id\":\"call1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}],\"content\":null},{\"role\":\"tool\",\"tool_call_id\":\"call1\",\"content\":\"private tool result\"}],\n  \"tools\":[{\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"parameters\":{\"type\":\"object\"}}}],\n  \"seed\":9007199254740993,\"metadata\":{\"workspace\":{\"name\":\"private workspace\"}},\"extension\":{\"raw\":1.00}\n}\n "

func slowTestConfig(t *testing.T) SlowRequestConfig {
	t.Helper()
	c := SlowRequestConfig{Enabled: true, Directory: filepath.Join(t.TempDir(), "captures")}
	c.defaults(filepath.Join(t.TempDir(), "config.json"))
	return c
}

func slowTestStore(t *testing.T, c SlowRequestConfig) (*slowRequestStore, *lockedBuffer) {
	t.Helper()
	logs := &lockedBuffer{}
	s, err := newSlowRequestStore(c, log.New(logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	return s, logs
}

func slowTestJob(id int, end time.Time, body string) slowRequestJob {
	return slowRequestJob{body: []byte(body), meta: slowRequestMeta{Schema: slowRequestSchema, RequestID: fmt.Sprintf("%032x", id), StartedAt: end.Add(-301 * time.Second), EndedAt: end, DurationSeconds: 301, InputBytes: int64(len(body)), Outcome: "completed", Status: 200}}
}

func slowReadRecord(t *testing.T, root, id string) ([]byte, slowRequestMeta) {
	t.Helper()
	path := filepath.Join(root, "request-"+id)
	body, err := os.ReadFile(filepath.Join(path, "request.json"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(path, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta slowRequestMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if meta.Schema != slowRequestSchema || meta.RequestID != id || meta.InputBytes != int64(len(body)) || meta.InputSHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("invalid saved metadata or body checksum")
	}
	for name, want := range map[string]os.FileMode{root: 0700, path: 0700, filepath.Join(path, "request.json"): 0600, filepath.Join(path, "meta.json"): 0600} {
		info, err := os.Stat(name)
		if err != nil || info.Mode().Perm() != want {
			t.Fatalf("incorrect permissions on %s", name)
		}
	}
	return body, meta
}

func slowTestGateway(t *testing.T, profile bool, elapsed time.Duration, response generationTransport) (*Gateway, *lockedBuffer) {
	t.Helper()
	c := fixtureConfig(t, "http://backend.invalid")
	c.SlowRequests = slowTestConfig(t)
	if profile {
		t.Setenv("AIGATE_PROFILE_KEY", "profile-fixture")
		c.Profile.Enabled = true
		c.Profile.Directory = t.TempDir()
	}
	logs := &lockedBuffer{}
	g, err := newGateway(c, logs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	start := time.Now()
	var calls atomic.Int64
	g.now = func() time.Time {
		if calls.Add(1)%2 == 1 {
			return start
		}
		return start.Add(elapsed)
	}
	g.client.Transport = generationTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "GET" {
			return generationResponse(200, "application/json", `{"data":[{"id":"real-model"}]}`), nil
		}
		return response(r)
	})
	return g, logs
}

func slowCompletedResponse() *http.Response {
	return generationResponse(200, "text/event-stream", completionEvent(0, map[string]any{"content": "private model output"}, "stop")+"data: [DONE]\n\n")
}

func TestSlowRequestConfig(t *testing.T) {
	c := fixtureConfig(t, "http://backend.invalid")
	want := filepath.Join(filepath.Dir(c.SlowRequests.Directory), "slow-requests")
	if c.SlowRequests.Enabled || c.SlowRequests.ThresholdSeconds != 300 || c.SlowRequests.RetentionHours != 72 || c.SlowRequests.MaxDiskBytes != 10<<30 || c.SlowRequests.MaxQueueBytes != 64<<20 || c.SlowRequests.Directory != want {
		t.Fatal("incorrect slow request defaults")
	}
	g, err := newGateway(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.Close()
	if _, err := os.Stat(c.SlowRequests.Directory); !os.IsNotExist(err) {
		t.Fatal("disabled capture created a directory")
	}
	for _, change := range []func(*SlowRequestConfig){
		func(c *SlowRequestConfig) { c.ThresholdSeconds = -1 },
		func(c *SlowRequestConfig) { c.ThresholdSeconds = math.NaN() },
		func(c *SlowRequestConfig) { c.ThresholdSeconds = math.Inf(1) },
		func(c *SlowRequestConfig) { c.ThresholdSeconds = 1e20 },
		func(c *SlowRequestConfig) { c.RetentionHours = -1 },
		func(c *SlowRequestConfig) { c.RetentionHours = math.MaxInt },
		func(c *SlowRequestConfig) { c.MaxDiskBytes = -1 },
		func(c *SlowRequestConfig) { c.MaxQueueBytes = -1 },
		func(c *SlowRequestConfig) { c.Directory = "" },
	} {
		next := c.SlowRequests
		next.Enabled = true
		change(&next)
		if next.validate() == nil {
			t.Fatal("invalid enabled configuration accepted")
		}
	}
}

func TestSlowRequestThresholdAndScope(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
		elapsed                  time.Duration
		authorized, saved        bool
	}{
		{"below", "POST", "/v1/chat/completions", slowTestBody, 299 * time.Second, true, false},
		{"exact", "POST", "/v1/chat/completions", slowTestBody, 300 * time.Second, true, false},
		{"above", "POST", "/v1/chat/completions", slowTestBody, 300*time.Second + time.Nanosecond, true, true},
		{"nonstream", "POST", "/v1/chat/completions", strings.Replace(slowTestBody, `"stream":true`, `"stream":false`, 1), 301 * time.Second, true, false},
		{"omitted", "POST", "/v1/chat/completions", basicBody, 301 * time.Second, true, false},
		{"invalid", "POST", "/v1/chat/completions", `{"stream":true}`, 301 * time.Second, true, false},
		{"unauthorized", "POST", "/v1/chat/completions", slowTestBody, 301 * time.Second, false, false},
		{"method", "PUT", "/v1/chat/completions", slowTestBody, 301 * time.Second, true, false},
		{"health", "GET", "/health", slowTestBody, 301 * time.Second, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, _ := slowTestGateway(t, false, tc.elapsed, func(*http.Request) (*http.Response, error) { return slowCompletedResponse(), nil })
			g.apiKey = "Bearer fixture-secret"
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.authorized {
				r.Header.Set("Authorization", g.apiKey)
			}
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			g.slow.close()
			paths, err := filepath.Glob(filepath.Join(g.config.SlowRequests.Directory, "request-*"))
			if err != nil || (len(paths) == 1) != tc.saved || len(paths) > 1 {
				t.Fatalf("saved=%d want=%t", len(paths), tc.saved)
			}
			if tc.saved {
				body, meta := slowReadRecord(t, g.config.SlowRequests.Directory, w.Header().Get("X-Request-ID"))
				if !bytes.Equal(body, []byte(tc.body)) || meta.DurationSeconds <= 300 {
					t.Fatal("threshold or raw input changed")
				}
			}
		})
	}
}

func TestSlowRequestOutcomesAndReplay(t *testing.T) {
	for _, profile := range []bool{false, true} {
		for _, mode := range []string{"complete", "cancel", "timeout", "broken", "upstream_error"} {
			t.Run(fmt.Sprintf("profile=%t/%s", profile, mode), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var forwarded []byte
				g, logs := slowTestGateway(t, profile, 301*time.Second, func(r *http.Request) (*http.Response, error) {
					forwarded, _ = io.ReadAll(r.Body)
					switch mode {
					case "cancel":
						cancel()
						return nil, context.Canceled
					case "timeout":
						return nil, context.DeadlineExceeded
					case "broken":
						return generationResponse(200, "text/event-stream", completionEvent(0, map[string]any{"content": "unfinished"}, nil)), nil
					case "upstream_error":
						return generationResponse(503, "text/plain", "private upstream failure"), nil
					default:
						return slowCompletedResponse(), nil
					}
				})
				// Adaptation must not leak into the retained raw body.
				g.config.Backend.Compatibility.MetadataValues = "json_string"
				r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(slowTestBody)).WithContext(ctx)
				r.Header.Set("Authorization", "Bearer private-client-key")
				w := httptest.NewRecorder()
				g.ServeHTTP(w, r)
				g.slow.close()
				body, meta := slowReadRecord(t, g.config.SlowRequests.Directory, w.Header().Get("X-Request-ID"))
				if !bytes.Equal(body, []byte(slowTestBody)) || bytes.Equal(body, forwarded) || meta.Backend == nil || *meta.Backend != "api1" || meta.Model == nil || *meta.Model != "real-model" || meta.Status != w.Code {
					t.Fatal("capture differs from original input or routing/status metadata")
				}
				for _, secret := range []string{"秘密", "private system", "private-client-key", "private model output", "private upstream failure", "private tool result"} {
					if strings.Contains(logs.String(), secret) {
						t.Fatal("capture diagnostics exposed content or credentials")
					}
				}
				if meta.GenerationResult == nil {
					t.Fatal("missing finalized generation outcome")
				}
				switch mode {
				case "complete":
					if *meta.GenerationResult != "completed" || meta.ErrorClass != nil || meta.ClientCancelled {
						t.Fatal("incorrect completion classification")
					}
					// Send the saved file through another gateway and compare adapted payloads.
					var replayed []byte
					replay, _ := slowTestGateway(t, false, time.Second, func(r *http.Request) (*http.Response, error) {
						replayed, _ = io.ReadAll(r.Body)
						return slowCompletedResponse(), nil
					})
					replay.config.Backend.Compatibility.MetadataValues = "json_string"
					rw := httptest.NewRecorder()
					replay.ServeHTTP(rw, httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(body)))
					equivalentJSON(t, forwarded, replayed)
					if rw.Code != 200 || rw.Header().Get("X-Request-ID") == meta.RequestID || !strings.Contains(rw.Body.String(), "[DONE]") {
						t.Fatal("saved request could not be replayed")
					}
				case "cancel":
					if meta.Outcome != "client_cancelled" || !meta.ClientCancelled || *meta.GenerationResult != "client_cancelled" {
						t.Fatal("missing cancellation")
					}
				case "timeout", "broken", "upstream_error":
					want := map[string]string{"timeout": "timeout", "broken": "stream_incomplete", "upstream_error": "server_error"}[mode]
					if *meta.GenerationResult != "error" || meta.ErrorClass == nil || *meta.ErrorClass != want {
						t.Fatalf("incorrect error classification: %+v", meta)
					}
					if mode == "broken" && meta.Status != 200 {
						t.Fatal("SSE HTTP status changed")
					}
				}
			})
		}
	}
}

func TestSlowRequestQueueAndConcurrentClose(t *testing.T) {
	c := slowTestConfig(t)
	c.MaxQueueBytes = 10
	s, logs := slowTestStore(t, c)
	reached, release := make(chan struct{}), make(chan struct{})
	s.write = func(job slowRequestJob) error {
		if job.meta.RequestID == fmt.Sprintf("%032x", 1) {
			close(reached)
			<-release
		}
		return s.writeRecord(job)
	}
	if !s.enqueue(slowTestJob(1, time.Now(), "123456")) {
		t.Fatal("first enqueue failed")
	}
	<-reached
	if s.enqueue(slowTestJob(2, time.Now(), "12345")) || s.enqueue(slowTestJob(3, time.Now(), "12345678901")) || !s.enqueue(slowTestJob(4, time.Now(), "1234")) {
		t.Fatal("queue limit excludes active write or rejects available bytes")
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); s.close() }()
	}
	close(release)
	wg.Wait()
	if s.queueBytes != 0 || s.enqueue(slowTestJob(5, time.Now(), "1")) {
		t.Fatal("queue not drained or accepted after close")
	}
	for _, id := range []int{1, 4} {
		slowReadRecord(t, c.Directory, fmt.Sprintf("%032x", id))
	}
	for _, reason := range []string{"queue_bytes_full", "input_exceeds_queue_limit", "closed"} {
		if !strings.Contains(logs.String(), reason) {
			t.Fatal("missing skip diagnostic", reason)
		}
	}
}

func TestSlowRequestRetentionDiskLimitAndOwnership(t *testing.T) {
	c := slowTestConfig(t)
	if err := os.Mkdir(c.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.Open(c.Directory)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	s := &slowRequestStore{c: c, directory: dir}
	// Use the writer synchronously to control completion ordering and cleanup time.
	now := time.Now()
	for id, end := range []time.Time{now.Add(-time.Hour), now.Add(-2 * time.Hour), now} {
		if err := s.writeRecord(slowTestJob(id+1, end, slowTestBody)); err != nil {
			t.Fatal(err)
		}
	}
	_, size, err := slowRecordFiles(filepath.Join(c.Directory, fmt.Sprintf("request-%032x", 1)))
	if err != nil {
		t.Fatal(err)
	}
	s.c.MaxDiskBytes = 2 * size
	if err := s.cleanup(now, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(c.Directory, fmt.Sprintf("request-%032x", 2))); !os.IsNotExist(err) {
		t.Fatal("capacity cleanup did not remove the oldest end time")
	}
	foreign := filepath.Join(c.Directory, "keep-me")
	os.WriteFile(foreign, []byte("protected"), 0600)
	bare := filepath.Join(c.Directory, fmt.Sprintf("%032x", 99))
	os.Mkdir(bare, 0700)
	os.WriteFile(filepath.Join(bare, "keep"), []byte("protected"), 0600)
	outside := t.TempDir()
	os.WriteFile(filepath.Join(outside, "keep"), []byte("protected"), 0600)
	os.Symlink(outside, filepath.Join(c.Directory, fmt.Sprintf("request-%032x", 88)))
	pending := filepath.Join(c.Directory, fmt.Sprintf(".pending-%032x", 9))
	os.Mkdir(pending, 0700)
	os.WriteFile(filepath.Join(pending, "request.json"), []byte("partial"), 0600)
	s.c.MaxDiskBytes = 10 << 30
	if err := s.cleanup(now.Add(74*time.Hour), 0); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{foreign, filepath.Join(bare, "keep"), filepath.Join(outside, "keep")} {
		if data, err := os.ReadFile(path); err != nil || string(data) != "protected" {
			t.Fatal("cleanup touched foreign data")
		}
	}
	if _, err := os.Stat(pending); !os.IsNotExist(err) {
		t.Fatal("stale temporary directory not removed")
	}
	for _, id := range []int{1, 3} {
		if _, err := diagnosticReference(filepath.Join(c.Directory, fmt.Sprintf("request-%032x", id))); err != nil {
			t.Fatal("old record not archived", err)
		}
	}
	// Restart performs cleanup and reacquires the exclusive directory lock.
	next, _ := slowTestStore(t, c)
	if other, err := newSlowRequestStore(c, log.New(io.Discard, "", 0)); err == nil {
		other.close()
		t.Fatal("two stores acquired the same directory")
	}
	next.close()
}

func TestSlowRequestWriteFailureAndAtomicPublication(t *testing.T) {
	c := slowTestConfig(t)
	s, logs := slowTestStore(t, c)
	// Failure before rename must leave no published or truncated replay record.
	s.write = func(job slowRequestJob) error {
		tmp := filepath.Join(c.Directory, ".pending-"+job.meta.RequestID)
		if err := os.Mkdir(tmp, 0700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(tmp, "request.json"), []byte("partial"), 0600); err != nil {
			return err
		}
		return fmt.Errorf("injected disk failure")
	}
	s.enqueue(slowTestJob(1, time.Now(), slowTestBody))
	s.close()
	if _, err := os.Stat(filepath.Join(c.Directory, fmt.Sprintf("request-%032x", 1))); !os.IsNotExist(err) || !strings.Contains(logs.String(), "injected disk failure") {
		t.Fatal("failed record published or no diagnostic")
	}
	next, _ := slowTestStore(t, c)
	next.enqueue(slowTestJob(2, time.Now(), slowTestBody))
	next.close()
	slowReadRecord(t, c.Directory, fmt.Sprintf("%032x", 2))
	if tmp, _ := filepath.Glob(filepath.Join(c.Directory, ".pending-*")); len(tmp) != 0 {
		t.Fatal("temporary record not recovered")
	}
	if err := next.writeRecord(slowTestJob(2, time.Now(), "replacement")); err == nil {
		t.Fatal("duplicate ID overwrote a record")
	}
}

func TestSlowRequestConcurrentPublication(t *testing.T) {
	c := slowTestConfig(t)
	s, _ := slowTestStore(t, c)
	var writers sync.WaitGroup
	for id := 1; id <= 32; id++ {
		writers.Add(1)
		go func(id int) {
			defer writers.Done()
			if !s.enqueue(slowTestJob(id, time.Now(), slowTestBody)) {
				t.Error("unexpected queue rejection")
			}
		}(id)
	}
	writers.Wait()
	closed := make(chan struct{})
	go func() { s.close(); close(closed) }()
	for {
		// A concurrently visible published directory always has both complete files.
		paths, err := filepath.Glob(filepath.Join(c.Directory, "request-*"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			slowReadRecord(t, c.Directory, strings.TrimPrefix(filepath.Base(path), "request-"))
		}
		select {
		case <-closed:
			paths, _ := filepath.Glob(filepath.Join(c.Directory, "request-*"))
			if len(paths) != 32 {
				t.Fatalf("drained records=%d want=32", len(paths))
			}
			return
		default:
		}
	}
}

func TestSlowRequestRealStorageFailurePreservesResponse(t *testing.T) {
	g, logs := slowTestGateway(t, false, 301*time.Second, func(*http.Request) (*http.Response, error) { return slowCompletedResponse(), nil })
	captureDir := g.config.SlowRequests.Directory
	// Make the directory unavailable using a real filesystem error, without changing HTTP behavior.
	if err := os.Rename(captureDir, captureDir+"-offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(captureDir, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(slowTestBody)))
	g.slow.close()
	if w.Code != 200 || !strings.Contains(w.Body.String(), "[DONE]") || !strings.Contains(logs.String(), `"result":"failed"`) || !strings.Contains(logs.String(), w.Header().Get("X-Request-ID")) {
		t.Fatal("storage error changed response or lost diagnostic")
	}
}

func TestSlowRequestOnlyAfterStreamEnd(t *testing.T) {
	reader, writer := io.Pipe()
	reached := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, _ := slowTestGateway(t, false, 301*time.Second, func(*http.Request) (*http.Response, error) {
		close(reached)
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}, nil
	})
	done := make(chan struct{})
	w := httptest.NewRecorder()
	go func() {
		defer close(done)
		g.ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(slowTestBody)).WithContext(ctx))
	}()
	<-reached
	if paths, _ := filepath.Glob(filepath.Join(g.config.SlowRequests.Directory, "request-*")); len(paths) != 0 {
		t.Fatal("captured before request completed")
	}
	cancel()
	writer.CloseWithError(context.Canceled)
	<-done
	g.slow.close()
	_, meta := slowReadRecord(t, g.config.SlowRequests.Directory, w.Header().Get("X-Request-ID"))
	if !meta.ClientCancelled || meta.Outcome != "client_cancelled" || meta.Status != 200 {
		t.Fatal("mid-stream cancellation lost")
	}
}
