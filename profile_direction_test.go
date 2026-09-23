package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDirectionReadWaitExcludesWrites(t *testing.T) {
	p, _ := profileFixture(t)
	r := p.begin("direction")
	r.liveBackend("dcu")
	base := time.Now()
	change := func(f func(*profileLiveRequest)) { r.updateLive(f) }
	check := func(at time.Duration, read, write float64) {
		t.Helper()
		v := liveValues(p, base.Add(at))
		if got := liveValue(v, "stream_backend_wait_max_seconds", "dcu"); got != read {
			t.Fatalf("read %v want %v", got, read)
		}
		if got := liveValue(v, "stream_write_active_max_seconds", "dcu"); got != write {
			t.Fatalf("write %v want %v", got, write)
		}
	}
	change(func(l *profileLiveRequest) { l.beginIO("read", base) })
	check(2*time.Second, 2, 0)
	change(func(l *profileLiveRequest) {
		l.endIO(base.Add(2 * time.Second))
		l.beginIO("write", base.Add(2*time.Second))
	})
	check(22*time.Second, 0, 20)
	change(func(l *profileLiveRequest) {
		l.endIO(base.Add(22 * time.Second))
		l.beginIO("read", base.Add(22*time.Second))
	})
	check(25*time.Second, 5, 0)
	change(func(l *profileLiveRequest) { l.endIO(base.Add(25 * time.Second)) })
	o := newSSEObserver(false)
	o.Feed([]byte(": ping\n\n"))
	r.liveObserve(o)
	change(func(l *profileLiveRequest) { l.beginIO("read", base.Add(25*time.Second)) })
	check(26*time.Second, 6, 0)
	change(func(l *profileLiveRequest) { l.endIO(base.Add(26 * time.Second)) })
	o.Feed([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"))
	r.liveObserve(o)
	change(func(l *profileLiveRequest) { l.beginIO("read", base.Add(26*time.Second)) })
	check(27*time.Second, 1, 0)
	for _, flag := range []string{"unknown", "terminal", "nonstream"} {
		change(func(l *profileLiveRequest) {
			l.unknown = flag == "unknown"
			l.terminal = flag == "terminal"
			l.nonstream = flag == "nonstream"
		})
		check(28*time.Second, 0, 0)
	}
	change(func(l *profileLiveRequest) {
		l.nonstream = false
		l.terminal = true
		l.beginIO("write", base.Add(28*time.Second))
	})
	check(30*time.Second, 0, 2) // terminal streams still flush their final bytes
	p.end(r, &auditEntry{Outcome: "completed"}, false)
	check(40*time.Second, 0, 0)
}

func TestDirectionMetricsAggregateMaxAndScope(t *testing.T) {
	p, _ := profileFixture(t)
	base := time.Now()
	for i, backend := range []string{"dcu", "dcu", "a3"} {
		r := p.begin(fmt.Sprint(i))
		r.liveBackend(backend)
		r.updateLive(func(l *profileLiveRequest) { l.beginIO("read", base.Add(-time.Duration(i+1)*time.Second)) })
	}
	v := liveValues(p, base)
	if liveValue(v, "stream_backend_wait_max_seconds", "dcu") != 2 || liveValue(v, "stream_backend_wait_max_seconds", "a3") != 3 {
		t.Fatal(v)
	}
	var out strings.Builder
	p.metrics(&out)
	for _, name := range []string{"backend_wait", "write_active"} {
		if !strings.Contains(out.String(), "aigate_stream_"+name+"_max_seconds{backend=\"dcu\",request_scope=\"streaming\"}") {
			t.Fatal(out.String())
		}
	}
}

type directionWriter struct {
	*httptest.ResponseRecorder
	blockWrite       bool
	flushAt, flushes int
	entered, release chan struct{}
	fail             bool
}

func (w *directionWriter) block() error {
	close(w.entered)
	<-w.release
	if w.fail {
		return io.ErrClosedPipe
	}
	return nil
}
func (w *directionWriter) Write(b []byte) (int, error) {
	if w.blockWrite {
		w.blockWrite = false
		if err := w.block(); err != nil {
			return 0, err
		}
	}
	return w.ResponseRecorder.Write(b)
}
func (w *directionWriter) FlushError() error {
	w.flushes++
	if w.flushes == w.flushAt {
		return w.block()
	}
	return nil
}

func TestDirectionStreamBlockedWrites(t *testing.T) {
	for _, tc := range []struct {
		name   string
		write  bool
		flush  int
		fail   bool
		broken bool
	}{
		{"initial-flush", false, 1, false, false},
		{"initial-flush-failure", false, 1, true, false},
		{"write", true, 0, false, false},
		{"final-flush", false, 2, false, false},
		{"write-failure", true, 0, true, false},
		{"flush-failure", false, 2, true, false},
		{"error-write", true, 0, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
				if tc.broken {
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: &generationBrokenBody{err: io.ErrUnexpectedEOF}}, nil
				}
				return generationResponse(200, "text/event-stream", "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"), nil
			})
			w := &directionWriter{ResponseRecorder: httptest.NewRecorder(), blockWrite: tc.write, flushAt: tc.flush, fail: tc.fail, entered: make(chan struct{}), release: make(chan struct{})}
			done := make(chan struct{})
			go func() {
				defer close(done)
				g.ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"x"}],"stream":true}`)))
			}()
			defer func() {
				close(w.release)
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("request did not exit")
				}
			}()
			select {
			case <-w.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("write did not block")
			}
			v := liveValues(g.profile, time.Now().Add(time.Minute))
			var writes float64
			for k, n := range v {
				if strings.HasPrefix(k, "aigate_stream_write_active_max_seconds{") {
					writes = max(writes, n)
				}
				if strings.HasPrefix(k, "aigate_stream_backend_wait_max_seconds{") && n != 0 {
					t.Fatal("blocked write counted as backend wait", k, n)
				}
			}
			if writes < 60 {
				t.Fatal("missing live write duration", v)
			}
			g.profile.metrics(io.Discard) // scrape must remain possible while the writer is blocked
		})
	}
}

type directionReadBody struct {
	ctx     context.Context
	entered chan struct{}
}

func (b *directionReadBody) Read(p []byte) (int, error) {
	close(b.entered)
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (b *directionReadBody) Close() error { return nil }

func TestDirectionBlockedReadCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: &directionReadBody{ctx: r.Context(), entered: entered}}, nil
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"x"}],"stream":true}`)).WithContext(ctx))
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("read not entered")
	}
	v := liveValues(g.profile, time.Now().Add(time.Minute))
	var read float64
	for k, n := range v {
		if strings.HasPrefix(k, "aigate_stream_backend_wait_max_seconds{") {
			read = max(read, n)
		}
	}
	if read < 60 {
		t.Fatal("blocked read missing", v)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not exit")
	}
	v = liveValues(g.profile, time.Now().Add(time.Minute))
	for k, n := range v {
		if (strings.HasPrefix(k, "aigate_stream_backend_wait_max_seconds{") || strings.HasPrefix(k, "aigate_stream_write_active_max_seconds{")) && n != 0 {
			t.Fatal("cancel left stale timing", v)
		}
	}
}
