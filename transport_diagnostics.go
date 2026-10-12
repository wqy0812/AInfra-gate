package aigate

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptrace"
	"net/url"
	"os"
	"sync"
	"syscall"
	"time"
)

// Transport diagnostics never include request/response bodies, headers or URLs.
// Unknown errors retain their concrete type, not potentially sensitive text.
type transportErrorDetail struct {
	Type      string `json:"type"`
	Message   string `json:"message,omitempty"`
	Errno     int    `json:"errno,omitempty"`
	Operation string `json:"operation,omitempty"`
	Timeout   bool   `json:"timeout,omitempty"`
}

func transportErrors(err error) []transportErrorDetail {
	var out []transportErrorDetail
	for i := 0; err != nil && i < 6; i++ {
		d := transportErrorDetail{Type: fmt.Sprintf("%T", err)}
		switch e := err.(type) {
		case syscall.Errno:
			d.Errno, d.Message = int(e), e.Error()
		case *net.OpError:
			d.Operation, d.Timeout = e.Op, e.Timeout()
		case *os.SyscallError:
			d.Operation = e.Syscall
		case *url.Error:
			d.Operation, d.Timeout = e.Op, e.Timeout() // deliberately omit URL
		default:
			if err == io.EOF || err == io.ErrUnexpectedEOF || err == context.Canceled || err == context.DeadlineExceeded || err == os.ErrDeadlineExceeded || err == net.ErrClosed {
				d.Message = err.Error()
			}
			var timeout net.Error
			if errors.As(err, &timeout) {
				d.Timeout = timeout.Timeout()
			}
		}
		if len(d.Operation) > 64 {
			d.Operation = d.Operation[:64]
		}
		out = append(out, d)
		err = errors.Unwrap(err)
	}
	return out
}

type transportStep struct {
	Stage string  `json:"stage"`
	MS    float64 `json:"ms"`
}
type transportFailure struct {
	Operation       string                 `json:"operation"`
	MS              float64                `json:"ms"`
	Errors          []transportErrorDetail `json:"errors,omitempty"`
	ClientContext   string                 `json:"client_context,omitempty"`
	UpstreamContext string                 `json:"upstream_context,omitempty"`
}
type transportSnapshot struct {
	StartedAt       time.Time          `json:"started_at"`
	Steps           []transportStep    `json:"steps"`
	Truncated       bool               `json:"truncated,omitempty"`
	Failures        []transportFailure `json:"failures,omitempty"`
	UpstreamStatus  int                `json:"upstream_status,omitempty"`
	UpstreamLocal   string             `json:"upstream_local,omitempty"`
	UpstreamRemote  string             `json:"upstream_remote,omitempty"`
	Reused          bool               `json:"connection_reused"`
	IdleMS          float64            `json:"connection_idle_ms,omitempty"`
	ReadBytes       int64              `json:"upstream_read_bytes"`
	WrittenBytes    int64              `json:"client_written_bytes"`
	LastReadMS      float64            `json:"last_read_ms,omitempty"`
	LastWriteMS     float64            `json:"last_write_ms,omitempty"`
	ClientContext   string             `json:"client_context,omitempty"`
	UpstreamContext string             `json:"upstream_context,omitempty"`
}
type requestTransport struct {
	mu    sync.Mutex // httptrace callbacks may run concurrently
	start time.Time
	state transportSnapshot
}
type requestTransportKey struct{}

func transportFrom(ctx context.Context) *requestTransport {
	d, _ := ctx.Value(requestTransportKey{}).(*requestTransport)
	return d
}
func newRequestTransport() *requestTransport {
	now := time.Now()
	return &requestTransport{start: now, state: transportSnapshot{StartedAt: now.UTC()}}
}
func (d *requestTransport) stage(name string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.state.Steps) >= 24 {
		d.state.Truncated = true
		return
	}
	d.state.Steps = append(d.state.Steps, transportStep{name, float64(time.Since(d.start).Microseconds()) / 1000})
}
func contextStatus(ctx context.Context) string {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err().Error()
	}
	return ""
}
func (d *requestTransport) failure(operation string, err error, upstream, client context.Context) {
	if d == nil || err == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.state.Failures) >= 3 {
		d.state.Truncated = true
		return
	}
	d.state.Failures = append(d.state.Failures, transportFailure{operation, float64(time.Since(d.start).Microseconds()) / 1000, transportErrors(err), contextStatus(client), contextStatus(upstream)})
}
func (d *requestTransport) contexts(upstream, client context.Context) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.state.ClientContext, d.state.UpstreamContext = contextStatus(client), contextStatus(upstream)
}
func (d *requestTransport) status(status int) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.state.UpstreamStatus = status
	d.mu.Unlock()
}
func (d *requestTransport) bytes(read bool, n int) {
	if d == nil || n <= 0 {
		return
	}
	ms := float64(time.Since(d.start).Microseconds()) / 1000
	d.mu.Lock()
	defer d.mu.Unlock()
	if read {
		d.state.ReadBytes += int64(n)
		d.state.LastReadMS = ms
	} else {
		d.state.WrittenBytes += int64(n)
		d.state.LastWriteMS = ms
	}
}
func (d *requestTransport) snapshot() *transportSnapshot {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.state
	s.Steps = append([]transportStep(nil), s.Steps...)
	s.Failures = append([]transportFailure(nil), s.Failures...)
	return &s
}
func (d *requestTransport) trace(ctx context.Context) context.Context {
	if d == nil {
		return ctx
	}
	d.stage("upstream_prepare")
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn:  func(string) { d.stage("get_connection") },
		DNSStart: func(httptrace.DNSStartInfo) { d.stage("dns_start") },
		DNSDone:  func(i httptrace.DNSDoneInfo) { d.stage("dns_done"); d.failure("dns", i.Err, ctx, nil) },
		ConnectStart: func(_, address string) {
			d.mu.Lock()
			d.state.UpstreamRemote = address
			d.mu.Unlock()
			d.stage("connect_start")
		},
		ConnectDone:       func(_, _ string, err error) { d.stage("connect_done"); d.failure("connect", err, ctx, nil) },
		TLSHandshakeStart: func() { d.stage("tls_start") },
		TLSHandshakeDone:  func(_ tls.ConnectionState, err error) { d.stage("tls_done"); d.failure("tls", err, ctx, nil) },
		GotConn: func(i httptrace.GotConnInfo) {
			d.mu.Lock()
			d.state.UpstreamLocal, d.state.UpstreamRemote = i.Conn.LocalAddr().String(), i.Conn.RemoteAddr().String()
			d.state.Reused, d.state.IdleMS = i.Reused, float64(i.IdleTime.Microseconds())/1000
			d.mu.Unlock()
			d.stage("got_connection")
		},
		WroteHeaders: func() { d.stage("request_headers_written") },
		WroteRequest: func(i httptrace.WroteRequestInfo) {
			d.stage("request_written")
			d.failure("upstream_write", i.Err, ctx, nil)
		},
		GotFirstResponseByte: func() { d.stage("response_first_byte") },
	})
}

func diagnosticClientID(s string) string {
	if len(s) == 0 || len(s) > 128 {
		return ""
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return ""
		}
	}
	return s
}
