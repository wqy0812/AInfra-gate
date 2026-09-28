package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func diagnosticAudit(t *testing.T, logs *lockedBuffer) auditEntry {
	t.Helper()
	var e auditEntry
	if err := json.Unmarshal([]byte(strings.TrimSpace(logs.String())), &e); err != nil {
		t.Fatalf("audit: %v %s", err, logs.String())
	}
	return e
}

func TestTransportDiagnosticRealHeaderTimeout(t *testing.T) {
	for _, reused := range []bool{false, true} {
		t.Run(fmt.Sprint(reused), func(t *testing.T) {
			var called atomic.Int32
			logs := &lockedBuffer{}
			d := newConnectionDiagnostics(logs, "boot-test")
			s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called.Add(1); fmt.Fprint(w, "ok") }))
			s.Config.ReadHeaderTimeout = 60 * time.Millisecond
			s.Listener = installConnectionDiagnostics(s.Config, s.Listener, d)
			s.Start()
			defer func() { s.Close(); d.close() }()
			conn, err := net.Dial("tcp", s.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(2 * time.Second))
			reader := bufio.NewReader(conn)
			if reused {
				fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: local\r\n\r\n")
				r, err := http.ReadResponse(reader, nil)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, r.Body)
				r.Body.Close()
			}
			fmt.Fprint(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: local\r\nAuthorization: SECRET-HEADER")
			io.Copy(io.Discard, reader)
			deadline := time.Now().Add(time.Second)
			for logs.String() == "" && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			var v struct {
				Reason     string                 `json:"reason"`
				Connection connectionSummary      `json:"connection"`
				Errors     []transportErrorDetail `json:"errors"`
				Entered    bool                   `json:"entered_handler"`
			}
			if err := json.Unmarshal([]byte(logs.String()), &v); err != nil {
				t.Fatal(err, logs.String())
			}
			want := int32(0)
			if reused {
				want = 1
			}
			if called.Load() != want || v.Reason != "request_header_read_timeout" || v.Entered || v.Connection.ID == "" || len(v.Errors) == 0 || !v.Errors[0].Timeout {
				t.Fatalf("missing header timeout evidence: %+v calls=%d", v, called.Load())
			}
			if strings.Contains(logs.String(), "SECRET-HEADER") {
				t.Fatal("header leaked")
			}
		})
	}
}

func TestTransportDiagnosticCleanKeepAlive(t *testing.T) {
	logs := &lockedBuffer{}
	d := newConnectionDiagnostics(logs, "boot-test")
	var summaries []connectionSummary
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		summaries = append(summaries, r.Context().Value(connectionContextKey{}).(connectionSummary))
		fmt.Fprint(w, "ok")
	}))
	s.Listener = installConnectionDiagnostics(s.Config, s.Listener, d)
	s.Start()
	conn, err := net.Dial("tcp", s.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	for i := 0; i < 2; i++ {
		fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: local\r\n\r\n")
		r, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	conn.Close()
	s.Close()
	d.close()
	if logs.String() != "" {
		t.Fatal("normal keep-alive closure logged as failure", logs.String())
	}
	if len(summaries) != 2 || summaries[0].ID != summaries[1].ID || summaries[1].RequestNumber != 2 {
		t.Fatal(summaries)
	}
}

func TestConnectionDiagnosticRejectionClosesWrite(t *testing.T) {
	for _, status := range []int{http.StatusRequestEntityTooLarge, http.StatusRequestHeaderFieldsTooLarge} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			d := newConnectionDiagnostics(io.Discard, "test")
			s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, err := readChatBody(w, r, time.Second)
				if err == nil {
					t.Error("oversize body accepted")
					return
				}
				sendError(w, &auditEntry{}, err)
			}))
			s.Config.MaxHeaderBytes = 128
			s.Listener = installConnectionDiagnostics(s.Config, s.Listener, d)
			s.Start()
			defer func() { s.Close(); d.close() }()
			conn, err := net.Dial("tcp", s.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			// Leave unread bytes in the socket so a full close without a preceding
			// CloseWrite would reset the connection instead of delivering EOF.
			extra := strings.Repeat("x", 16*1024)
			if status == http.StatusRequestEntityTooLarge {
				_, err = fmt.Fprintf(conn, "POST / HTTP/1.1\r\nHost: local\r\nContent-Length: %d\r\n\r\n%s", maxRequestBodyBytes+1, extra)
			} else {
				_, err = fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: local\r\nX-Big: %s\r\n\r\n", extra)
			}
			if err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			res, err := http.ReadResponse(reader, nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil || res.StatusCode != status || len(body) == 0 {
				t.Fatalf("rejection interrupted: status=%d body=%q err=%v", res.StatusCode, body, err)
			}
			if _, err := reader.ReadByte(); err != io.EOF {
				t.Fatalf("rejection must end with EOF, got %v", err)
			}
		})
	}
}

// Deliver both requests in the initial read, independent of TCP packet timing.
// The next request must be parsed from net/http's buffer to exercise the gap.
type diagnosticBufferedListener struct {
	net.Listener
	initialBytes int
}

func (l diagnosticBufferedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &diagnosticBufferedConn{TCPConn: c.(*net.TCPConn), initialBytes: l.initialBytes}, nil
}

type diagnosticBufferedConn struct {
	*net.TCPConn
	initialBytes int
}

func (c *diagnosticBufferedConn) Read(p []byte) (int, error) {
	if c.initialBytes > 0 {
		n, err := io.ReadFull(c.TCPConn, p[:min(len(p), c.initialBytes)])
		c.initialBytes -= n
		return n, err
	}
	return c.TCPConn.Read(p)
}

func TestConnectionDiagnosticBufferedRequests(t *testing.T) {
	for _, tc := range []struct {
		name, next string
		status     int
	}{
		{"normal", "GET / HTTP/1.1\r\nHost: local\r\n\r\n", 200},
		{"malformed_header", "GET / HTTP/1.1\r\nHost: local\r\nBad Header: SECRET-HEADER\r\n\r\n", 400},
		{"unsupported_expectation", "POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\nExpect: unsupported\r\n\r\n", 417},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &lockedBuffer{}
			d := newConnectionDiagnostics(logs, "test")
			var summaries []connectionSummary
			s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				summaries = append(summaries, r.Context().Value(connectionContextKey{}).(connectionSummary))
				fmt.Fprint(w, "ok")
			}))
			payload := "GET / HTTP/1.1\r\nHost: local\r\n\r\n" + tc.next
			s.Listener = diagnosticBufferedListener{s.Listener, len(payload)}
			s.Listener = installConnectionDiagnostics(s.Config, s.Listener, d)
			s.Start()
			defer func() { s.Close(); d.close() }()
			conn, err := net.Dial("tcp", s.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			if _, err := io.WriteString(conn, payload); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			for _, status := range []int{200, tc.status} {
				res, err := http.ReadResponse(reader, nil)
				if err != nil {
					t.Fatal(err)
				}
				_, err = io.Copy(io.Discard, res.Body)
				res.Body.Close()
				if err != nil || res.StatusCode != status {
					t.Fatalf("status=%d want=%d err=%v", res.StatusCode, status, err)
				}
			}
			conn.Close()
			s.Close()
			d.close()
			if tc.status == 200 {
				if logs.String() != "" || len(summaries) != 2 || summaries[0].ID != summaries[1].ID || summaries[1].RequestNumber != 2 {
					t.Fatalf("normal pipelining: summaries=%+v logs=%s", summaries, logs.String())
				}
				return
			}
			var event struct {
				Event                 string            `json:"event"`
				Reason                string            `json:"reason"`
				Connection            connectionSummary `json:"connection"`
				EnteredHandler        bool              `json:"entered_handler"`
				ResponseBeforeHandler bool              `json:"response_before_handler"`
				HeaderBytes           int64             `json:"pending_header_bytes"`
			}
			if err := json.Unmarshal([]byte(logs.String()), &event); err != nil {
				t.Fatalf("missing single diagnostic: %v logs=%s", err, logs.String())
			}
			if event.Event != "connection_diagnostic" || event.Reason != "closed_before_handler" || event.EnteredHandler || !event.ResponseBeforeHandler || event.HeaderBytes != 0 || len(summaries) != 1 || event.Connection != summaries[0] {
				t.Fatalf("buffered rejection evidence: %+v summaries=%+v", event, summaries)
			}
			if strings.Contains(logs.String(), "SECRET-HEADER") {
				t.Fatal("header leaked")
			}
		})
	}
}

func TestTransportDiagnosticUpstreamReset(t *testing.T) {
	u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
		c, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		c.(*net.TCPConn).SetLinger(0)
		c.Close()
	})
	_, s, logs := serveTest(t, fixtureConfig(t, u.URL))
	r, _ := http.NewRequest("POST", s.URL+"/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"test"}],"stream":true}`))
	r.Header.Set("X-Request-ID", "attempt-123")
	response, err := s.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	e := diagnosticAudit(t, logs)
	if e.ClientRequestID != "attempt-123" || e.Transport == nil || e.ErrorClass != "connection_error" {
		t.Fatalf("missing failure: %+v", e)
	}
	v := string(encodeJSON(e.Transport))
	for _, required := range []string{"get_connection", "request_written", "upstream_round_trip", "errno"} {
		if !strings.Contains(v, required) {
			t.Fatalf("missing %s: %s", required, v)
		}
	}
	if e.Transport.UpstreamRemote == "" {
		t.Fatal("missing actual peer")
	}
}

func TestTransportDiagnosticReadAndCancel(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprint(cancel), func(t *testing.T) {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
				res := generationResponse(200, "text/event-stream", "")
				err := error(io.ErrUnexpectedEOF)
				if cancel {
					stop()
					err = context.Canceled
				}
				res.Body = &generationBrokenBody{data: "data: {}\n\n", err: err}
				return res, nil
			})
			logs := &lockedBuffer{}
			g.audit = log.New(logs, "", 0)
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"test"}],"stream":true}`)).WithContext(ctx)
			g.ServeHTTP(httptest.NewRecorder(), r)
			e := diagnosticAudit(t, logs)
			if e.Transport == nil || len(e.Transport.Failures) == 0 || e.Transport.Failures[0].Operation != "upstream_body_read" {
				t.Fatalf("missing read evidence: %+v", e)
			}
			if cancel {
				if e.ErrorClass != "" || e.GenerationResult != "client_cancelled" || e.Transport.ClientContext != "context canceled" {
					t.Fatalf("cancellation misclassified: %+v", e)
				}
			} else if e.ErrorClass != "response_read_error" || e.Transport.UpstreamStatus != 200 || e.Transport.ReadBytes == 0 {
				t.Fatalf("read error not retained: %+v", e)
			}
		})
	}
}

func TestTransportDiagnosticSuccessUnchanged(t *testing.T) {
	body := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
	u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, body)
	})
	_, s, logs := serveTest(t, fixtureConfig(t, u.URL))
	res, data := request(t, s, "POST", "/v1/chat/completions", `{"messages":[{"role":"user","content":"test"}],"stream":true}`, "")
	if res.StatusCode != 200 || string(data) != body {
		t.Fatal("forwarded SSE changed")
	}
	e := diagnosticAudit(t, logs)
	if e.Transport != nil || e.GenerationResult != "completed" || strings.Count(strings.TrimSpace(logs.String()), "\n") != 0 {
		t.Fatal("normal request emitted verbose diagnostics", logs.String())
	}
}

type diagnosticFailWriter struct {
	*httptest.ResponseRecorder
	failWrite bool
	flushes   int
}

func (w *diagnosticFailWriter) Write(p []byte) (int, error) {
	if w.failWrite {
		return 0, syscall.EPIPE
	}
	return w.ResponseRecorder.Write(p)
}
func (w *diagnosticFailWriter) FlushError() error {
	w.flushes++
	if !w.failWrite && w.flushes > 1 {
		return syscall.ECONNRESET
	}
	w.ResponseRecorder.Flush()
	return nil
}
func TestTransportDiagnosticClientWriteDirection(t *testing.T) {
	for _, write := range []bool{true, false} {
		t.Run(fmt.Sprint(write), func(t *testing.T) {
			g := generationGateway(t, func(r *http.Request) (*http.Response, error) {
				return generationResponse(200, "text/event-stream", "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n"), nil
			})
			logs := &lockedBuffer{}
			g.audit = log.New(logs, "", 0)
			w := &diagnosticFailWriter{ResponseRecorder: httptest.NewRecorder(), failWrite: write}
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"test"}],"stream":true}`))
			g.ServeHTTP(w, r)
			e := diagnosticAudit(t, logs)
			operation := "client_body_flush"
			if write {
				operation = "client_body_write"
			}
			if e.Transport == nil || len(e.Transport.Failures) != 1 || e.Transport.Failures[0].Operation != operation || e.GenerationResult != "client_disconnected" || e.ErrorClass != "" {
				t.Fatalf("wrong error direction: %+v", e)
			}
			if e.Transport.Failures[0].Errors[0].Errno == 0 {
				t.Fatal("errno lost")
			}
		})
	}
}

func TestTransportDiagnosticBoundedAndSafe(t *testing.T) {
	err := &url.Error{Op: "Post", URL: "https://user:SECRET@private/path?api_key=SECRET", Err: &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}}
	d := newRequestTransport()
	for i := 0; i < 100; i++ {
		d.stage("stage")
		d.failure("read", err, nil, nil)
	}
	s := d.snapshot()
	raw := string(encodeJSON(s))
	if len(s.Steps) != 24 || len(s.Failures) != 3 || !s.Truncated || len(raw) > 16*1024 || strings.Contains(raw, "SECRET") || !strings.Contains(raw, `"errno"`) {
		t.Fatal(raw)
	}
	if diagnosticClientID("bad\nheader") != "" || diagnosticClientID(strings.Repeat("x", 129)) != "" {
		t.Fatal("unsafe ID accepted")
	}
}

type diagnosticBlockedWriter struct {
	started chan struct{}
	release chan struct{}
}

func (w diagnosticBlockedWriter) Write(p []byte) (int, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}
	<-w.release
	return 0, io.ErrClosedPipe
}

func TestConnectionDiagnosticBackpressure(t *testing.T) {
	w := diagnosticBlockedWriter{make(chan struct{}, 1), make(chan struct{})}
	d := newConnectionDiagnostics(w, "test")
	d.emit(map[string]string{"event": "test"})
	<-w.started
	for i := 0; i < 300; i++ {
		d.emit(map[string]string{"event": "test"})
	}
	if d.dropped.Load() == 0 || len(d.queue) != 256 {
		t.Fatal("queue is not bounded")
	}
	close(w.release)
	d.close()
	if d.writeErrors.Load() == 0 {
		t.Fatal("writer failures invisible")
	}
}

func TestBusinessHeaderTimeoutThirtySeconds(t *testing.T) {
	// Use the production server factory: a header arriving after the old 10s
	// deadline must still enter the handler with the new 30s deadline.
	s := businessHTTPServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") }))
	if s.ReadHeaderTimeout != 30*time.Second {
		t.Fatal(s.ReadHeaderTimeout)
	}
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	d := newConnectionDiagnostics(logs, "test")
	l = installConnectionDiagnostics(s, l, d)
	go s.Serve(l)
	defer func() { s.Close(); d.close() }()
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: local\r\n")
	time.Sleep(10200 * time.Millisecond)
	fmt.Fprint(conn, "Connection: close\r\n\r\n")
	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != 200 || string(data) != "ok" {
		t.Fatalf("delayed headers rejected: %v %s", err, data)
	}
}
