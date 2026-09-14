package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Only public chat generation installs this state. Discovery and probes do not.
type generationContextKey struct{}
type generationResult struct {
	observer   *sseObserver
	started    bool
	stream     bool
	status     int
	errorClass string
	result     string
}

func generationFrom(ctx context.Context) *generationResult {
	s, _ := ctx.Value(generationContextKey{}).(*generationResult)
	return s
}

func (s *generationResult) fail(class string) {
	if s != nil && s.errorClass == "" {
		s.errorClass = class
	}
}

func upstreamHTTPClass(status int) string {
	switch {
	case status == 400 || status == 413 || status == 422:
		return "request_rejected"
	case status == 401 || status == 403:
		return "auth"
	case status == 404:
		return "not_found"
	case status == 429:
		return "rate_limited"
	case status >= 500 && status <= 599:
		return "server_error"
	case status >= 300 && status <= 399:
		return "unexpected_redirect"
	default:
		return "http_other"
	}
}

func (s *generationResult) transportError(err error, ctx, client context.Context, fallback string) {
	if s == nil || client.Err() != nil {
		return
	}
	if upstreamError(err, ctx).Code == "upstream_timeout" {
		s.fail("timeout")
	} else {
		s.fail(fallback)
	}
}

func (s *generationResult) writeFailure(ctx, client context.Context) {
	deadline, bounded := ctx.Deadline()
	if client.Err() == nil && bounded && !time.Now().Before(deadline) {
		s.fail("timeout")
	} else if client.Err() != nil {
		s.result = "client_cancelled"
	} else {
		s.result = "client_disconnected"
	}
}

func (s *generationResult) finish(entry *auditEntry, cancelled bool) {
	if s == nil || !s.started {
		return
	}
	switch {
	case s.errorClass != "":
		s.result = "error"
	case s.result == "client_disconnected":
	case cancelled:
		s.result = "client_cancelled"
	case s.result == "unknown":
	case entry.Outcome == "completed":
		s.result = "completed"
	default:
		s.result = "unknown"
	}
}

func (s *generationResult) observeStream(o *sseObserver, complete bool) {
	if s == nil {
		return
	}
	if o.HasError {
		s.fail("stream_error")
	} else if complete && s.errorClass == "" {
		class := o.completionClass()
		if class == "unknown" {
			s.result = "unknown"
		} else if class != "" {
			s.fail(class)
		}
	}
}

// Preserve representation headers, never hop-by-hop headers or upstream cookies.
func copyErrorHeaders(dst, src http.Header) {
	blocked := map[string]bool{}
	for _, line := range src.Values("Connection") {
		for _, name := range strings.Split(line, ",") {
			blocked[http.CanonicalHeaderKey(strings.TrimSpace(name))] = true
		}
	}
	for _, name := range []string{"Content-Type", "Content-Encoding", "Retry-After", "WWW-Authenticate", "Location"} {
		name = http.CanonicalHeaderKey(name)
		if !blocked[name] {
			for _, value := range src.Values(name) {
				dst.Add(name, value)
			}
		}
	}
	// A missing upstream Content-Type must not trigger downstream content sniffing.
	if _, ok := dst["Content-Type"]; !ok {
		dst["Content-Type"] = nil
	}
}

func (g *Gateway) forwardGenerationError(w http.ResponseWriter, r *http.Request, response *http.Response, ctx context.Context, entry *auditEntry) {
	s := entry.generation
	s.fail(upstreamHTTPClass(response.StatusCode))
	entry.Status, entry.Outcome = response.StatusCode, "rejected"
	copyErrorHeaders(w.Header(), response.Header)
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	_ = controller.SetWriteDeadline(deadline)
	defer controller.SetWriteDeadline(time.Time{})
	w.WriteHeader(response.StatusCode)
	if controller.Flush() != nil {
		s.writeFailure(ctx, r.Context())
		return
	}
	buffer := g.buffers.Get().(*[]byte)
	defer g.buffers.Put(buffer)
	for {
		n, err := response.Body.Read(*buffer)
		if n > 0 {
			if _, writeErr := w.Write((*buffer)[:n]); writeErr != nil {
				s.writeFailure(ctx, r.Context())
				return
			}
			if controller.Flush() != nil {
				s.writeFailure(ctx, r.Context())
				return
			}
		}
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			// Headers are committed. Abort so a truncated body cannot appear complete.
			panic(http.ErrAbortHandler)
		}
	}
}

func (p *requestProfiler) generationEnd(s *generationResult, tags string) {
	if s == nil || !s.started {
		return
	}
	tags = profileExtra(tags, "stream", strconv.FormatBool(s.stream))
	p.add("generation_requests_ended_total", profileExtra(tags, "result", s.result), 1)
	if s.errorClass != "" {
		status := "none"
		if s.status != 0 {
			status = strconv.Itoa(s.status)
		}
		tags = profileExtra(tags, "error_class", s.errorClass)
		p.add("upstream_errors_total", profileExtra(tags, "upstream_status", status), 1)
	}
}
