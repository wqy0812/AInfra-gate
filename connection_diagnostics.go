package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Only abnormal connections that never reach the next HTTP handler get an
// extra record. Normal connections are described by the existing request audit.
// The queue bounds memory and keeps a slow diagnostic writer off the I/O path.
type connectionDiagnostics struct {
	mu          sync.Mutex
	queue       chan []byte
	done        chan struct{}
	closed      bool
	dropped     atomic.Uint64
	writeErrors atomic.Uint64
	boot        string
}

func newConnectionDiagnostics(w io.Writer, boot string) *connectionDiagnostics {
	if w == nil {
		w = io.Discard
	}
	d := &connectionDiagnostics{queue: make(chan []byte, 256), done: make(chan struct{}), boot: boot}
	go func() {
		defer close(d.done)
		for record := range d.queue {
			if n, err := w.Write(record); err != nil || n != len(record) {
				d.writeErrors.Add(1)
			}
		}
	}()
	return d
}
func (d *connectionDiagnostics) emit(v any) {
	if d == nil {
		return
	}
	data, err := json.Marshal(v)
	if err != nil || len(data) > 16*1024 {
		d.dropped.Add(1)
		return
	}
	data = append(data, '\n')
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		d.dropped.Add(1)
		return
	}
	select {
	case d.queue <- data:
	default:
		d.dropped.Add(1)
	}
}
func (d *connectionDiagnostics) close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		close(d.queue)
	}
	d.mu.Unlock()
	select {
	case <-d.done:
	case <-time.After(time.Second):
	}
}

type connectionContextKey struct{}
type connectionSummary struct {
	ID            string    `json:"connection_id"`
	AcceptedAt    time.Time `json:"accepted_at"`
	Local         string    `json:"local"`
	Remote        string    `json:"remote"`
	RequestNumber uint64    `json:"request_number"`
	HandlerAt     time.Time `json:"handler_at"`
}
type diagnosticConn struct {
	net.Conn
	mu                    sync.Mutex
	diagnostics           *connectionDiagnostics
	summary               connectionSummary
	state                 http.ConnState
	pendingBytes          int64
	readErrors            []transportErrorDetail
	readErrorAt           time.Time
	readDeadline          time.Time
	failureDeadline       time.Time
	readTimeout           bool
	readEOF               bool
	readClosed            bool
	closed                bool
	handled               bool // current HTTP request, reset at StateIdle
	responseBeforeHandler bool
}
type diagnosticListener struct {
	net.Listener
	diagnostics *connectionDiagnostics
}

func (l diagnosticListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &diagnosticConn{Conn: c, diagnostics: l.diagnostics, summary: connectionSummary{ID: randomID(), AcceptedAt: time.Now().UTC(), Local: c.LocalAddr().String(), Remote: c.RemoteAddr().String()}}, nil
}
func (c *diagnosticConn) CloseWrite() error {
	// net/http uses this to deliver a final rejection before closing a TCP
	// connection with unread request data, which could otherwise cause a reset.
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return http.ErrNotSupported
}
func (c *diagnosticConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if !c.handled && len(p) > 0 {
		// net/http may reject a pipelined request entirely from its read buffer.
		// In that case there need not be any new Read calls or pendingBytes.
		c.responseBeforeHandler = true
	}
	c.mu.Unlock()
	return c.Conn.Write(p)
}
func (c *diagnosticConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(t)
}
func (c *diagnosticConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.mu.Lock()
	// net/http intentionally aborts a background read after a handler returns.
	// Those active-request read timeouts must not look like header failures.
	if c.state == http.StateNew || c.state == http.StateIdle {
		c.pendingBytes += int64(n)
		if err != nil {
			c.readErrors, c.readErrorAt, c.failureDeadline = transportErrors(err), time.Now().UTC(), c.readDeadline
			var timeout net.Error
			c.readTimeout = errors.As(err, &timeout) && timeout.Timeout()
			c.readEOF = errors.Is(err, io.EOF)
			c.readClosed = errors.Is(err, net.ErrClosed)
		}
	}
	c.mu.Unlock()
	return n, err
}
func (c *diagnosticConn) enter() connectionSummary {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handled = true
	c.summary.RequestNumber++
	c.summary.HandlerAt = time.Now().UTC()
	return c.summary
}
func (c *diagnosticConn) changeState(state http.ConnState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = state
	if state == http.StateIdle {
		c.handled, c.pendingBytes, c.readErrors = false, 0, nil
		c.responseBeforeHandler = false
		c.readTimeout, c.readEOF = false, false
		c.readClosed = false
		c.readErrorAt, c.failureDeadline = time.Time{}, time.Time{}
	}
	if state != http.StateClosed || c.closed {
		return
	}
	c.closed = true
	// Clean EOF with no next request is normal keep-alive/probe cleanup.
	if c.handled || (!c.responseBeforeHandler && c.pendingBytes == 0 && (c.readEOF || c.readClosed || len(c.readErrors) == 0)) {
		return
	}
	reason := "closed_before_handler"
	if c.readTimeout {
		reason = "request_header_read_timeout"
	} else if c.readEOF {
		reason = "request_header_incomplete"
	} else if len(c.readErrors) > 0 {
		reason = "request_header_read_error"
	}
	c.diagnostics.emit(struct {
		Event                 string                 `json:"event"`
		Boot                  string                 `json:"gateway_boot_id"`
		Connection            connectionSummary      `json:"connection"`
		ClosedAt              time.Time              `json:"closed_at"`
		Reason                string                 `json:"reason"`
		EnteredHandler        bool                   `json:"entered_handler"`
		ResponseBeforeHandler bool                   `json:"response_before_handler,omitempty"`
		HeaderBytes           int64                  `json:"pending_header_bytes"`
		ErrorAt               time.Time              `json:"read_error_at"`
		ReadDeadline          time.Time              `json:"read_deadline"`
		Errors                []transportErrorDetail `json:"errors,omitempty"`
	}{"connection_diagnostic", c.diagnostics.boot, c.summary, time.Now().UTC(), reason, false, c.responseBeforeHandler, c.pendingBytes, c.readErrorAt, c.failureDeadline, c.readErrors})
}

func installConnectionDiagnostics(s *http.Server, l net.Listener, d *connectionDiagnostics) net.Listener {
	s.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		return context.WithValue(ctx, connectionContextKey{}, conn.(*diagnosticConn))
	}
	s.ConnState = func(conn net.Conn, state http.ConnState) { conn.(*diagnosticConn).changeState(state) }
	handler := s.Handler
	s.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if conn, ok := r.Context().Value(connectionContextKey{}).(*diagnosticConn); ok {
			summary := conn.enter()
			r = r.WithContext(context.WithValue(r.Context(), connectionContextKey{}, summary))
		}
		handler.ServeHTTP(w, r)
	})
	return diagnosticListener{l, d}
}
