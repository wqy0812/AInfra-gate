package aigate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SwitchSnapshot struct {
	RouterID             string `json:"router_id"`
	RouterVersion        uint64 `json:"router_version"`
	GatewayConfigVersion uint64 `json:"gateway_config_version"`
	BindingRevision      uint64 `json:"binding_revision"`
	TargetURL            string `json:"target_url"`
	ConfigDigest         string `json:"config_digest"`
}

func newSwitchGateway(id, key string, cidrs []string, audit io.Writer) (*ControlledGateway, error) {
	return newRuntimeGateway(id, key, cidrs, audit)
}

func (c *ControlledGateway) prepareSwitchLocked(cmd RuntimeCommand) error {
	if c.test != nil {
		return fmt.Errorf("test_occupied")
	}
	if c.candidate != nil && c.candidate.owner != cmd.OperationID {
		return fmt.Errorf("candidate_owned")
	}
	var s SwitchSnapshot
	if strictDecode(cmd.Payload, &s) != nil || !validOperationID(s.RouterID) || s.RouterVersion == 0 || s.GatewayConfigVersion == 0 || s.BindingRevision <= c.revisionLocked() || !validDigest(s.ConfigDigest) {
		return fmt.Errorf("invalid_snapshot")
	}
	base, err := controlBase(s.TargetURL)
	if err != nil {
		return fmt.Errorf("invalid_target")
	}
	u, _ := url.Parse(base)
	// A switch is never an adapter. This client is used exclusively by protocol
	// validation, which makes real HTTP requests to the target proxy API.
	cfg := &RuntimeConfig{RuntimeSettings: defaultRuntimeSettings(), RouterID: s.RouterID,
		Backend: &Backend{Adapter: "openai", URL: base + "/v1/chat/completions", ModelsURL: base + "/v1/models",
			Headers: map[string]string{headerRouter: s.RouterID, headerExpectedConfig: s.ConfigDigest}}}
	probe, err := newGateway(cfg, c.audit)
	if err != nil {
		return fmt.Errorf("target_prepare_failed")
	}
	probe.transport.DisableCompression = true
	forward := &switchForwarder{target: u, transport: probe.transport, router: s.RouterID, revision: s.BindingRevision, audit: probe.audit, timeout: c.forwardTimeout}
	snapshot := RuntimeSnapshot{RouterID: s.RouterID, RouterVersion: s.RouterVersion, GatewayConfigVersion: s.GatewayConfigVersion,
		BindingRevision: s.BindingRevision, TargetURL: base, ConfigDigest: s.ConfigDigest}
	gen := &runtimeGeneration{snapshot: snapshot, g: probe, forward: forward, owner: cmd.OperationID, counts: RuntimeCounts{RouterID: s.RouterID, Revision: s.BindingRevision}}
	c.candidate = gen
	c.generations = append(c.generations, gen)
	return nil
}

func validDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

type switchForwarder struct {
	timeout   time.Duration
	target    *url.URL
	transport *http.Transport
	router    string
	revision  uint64
	audit     *log.Logger
}

// Streaming uploads keep the outer five-minute deadline and size limit. The
// inner proxy receives exactly the original bytes, with no JSON transformation.
type switchUpload struct {
	io.ReadCloser
	controller *http.ResponseController
	mu         sync.Mutex
	err        error
	remaining  int64
	complete   bool
}

func (b *switchUpload) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	if b.remaining >= 0 {
		b.remaining -= int64(n)
	}
	done := err == io.EOF || b.remaining == 0
	b.complete = done
	if err != nil && err != io.EOF {
		b.err = err
	}
	b.mu.Unlock()
	if done {
		_ = b.controller.SetReadDeadline(time.Time{})
	}
	return n, err
}
func (b *switchUpload) failure() error { b.mu.Lock(); defer b.mu.Unlock(); return b.err }
func (b *switchUpload) finished() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.complete }

func (f *switchForwarder) serve(w http.ResponseWriter, r *http.Request, meta *runtimeRequest, lease *runtimeTest) {
	ctx, cancel := context.WithTimeout(r.Context(), f.timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	_ = http.NewResponseController(w).SetWriteDeadline(deadline)
	defer http.NewResponseController(w).SetWriteDeadline(time.Time{})
	want := http.MethodPost
	if r.URL.Path == "/v1/models" {
		want = http.MethodGet
	}
	if r.Method != want {
		abandonRequestBody(w, r)
		w.Header().Set("Allow", want)
		runtimeError(w, 405, "method_not_allowed")
		return
	}
	if r.Header.Get("X-Aigate-Proxy-Hop") != "" {
		abandonRequestBody(w, r)
		runtimeError(w, 508, "proxy_loop")
		return
	}
	id := randomID()
	upstreamRequestID := ""
	responseStatus := 0
	defer func() {
		failure := recover()
		if failure != nil || ctx.Err() != nil || responseStatus >= 400 {
			class := "upstream_response_error"
			if failure != nil {
				class = "stream_aborted"
			} else if ctx.Err() != nil {
				class = "forward_context_ended"
			}
			f.audit.Print(string(encodeJSON(map[string]any{"event": "switch_forward_ended", "request_id": id, "upstream_request_id": upstreamRequestID, "router_id": f.router, "binding_revision": f.revision, "status": responseStatus, "error_class": class})))
		}
		if failure != nil {
			panic(failure)
		}
	}()
	var upload *switchUpload
	inbound := r
	request := r.Clone(ctx)
	if want == http.MethodPost {
		if r.ContentLength > maxRequestBodyBytes {
			abandonRequestBody(w, r)
			runtimeError(w, 413, "request_body_too_large")
			return
		}
		controller := http.NewResponseController(w)
		_ = controller.SetReadDeadline(time.Now().Add(requestBodyTimeout))
		upload = &switchUpload{ReadCloser: http.MaxBytesReader(w, r.Body, maxRequestBodyBytes), controller: controller, remaining: r.ContentLength}
		request.Body = upload
		request.GetBody = nil
	} else {
		request.Body = nil
		request.ContentLength = 0
	}
	proxy := httputil.ReverseProxy{
		Transport: f.transport, FlushInterval: -1, ErrorLog: f.audit,
		Rewrite: func(p *httputil.ProxyRequest) {
			p.SetURL(f.target)
			p.Out.Host = f.target.Host
			// Caller-controlled metadata cannot claim a platform lease or route.
			for _, key := range []string{headerRoute, headerTask, headerExpectedModel, headerExpectedConfig, headerRouter, "X-Code-Eval-Token", "Authorization"} {
				p.Out.Header.Del(key)
			}
			p.Out.Header.Set("X-Aigate-Proxy-Hop", "1")
			p.Out.Header.Set("X-Request-ID", id)
			p.Out.Header.Set(headerRoute, strconv.FormatUint(f.revision, 10))
			p.Out.Header.Set(headerRouter, f.router)
			if lease != nil {
				p.Out.Header.Set(headerTask, lease.TaskID)
				p.Out.Header.Set(headerExpectedModel, lease.Model)
				p.Out.Header.Set(headerExpectedConfig, lease.ConfigDigest)
			}
		},
		ModifyResponse: func(response *http.Response) error {
			// Complete the small discovery/early-rejection response before abandoning
			// an unread client body: closing it cancels the inbound HTTP context.
			if inbound.Body != nil && inbound.Body != http.NoBody && (want == http.MethodGet || (upload != nil && !upload.finished())) {
				data, err := io.ReadAll(io.LimitReader(response.Body, maxModelDiscoveryBytes+1))
				response.Body.Close()
				abandonRequestBody(w, inbound)
				if err != nil {
					return err
				}
				if len(data) > maxModelDiscoveryBytes {
					return fmt.Errorf("early response exceeds limit")
				}
				response.Body = io.NopCloser(bytes.NewReader(data))
				response.ContentLength = int64(len(data))
				response.Header.Set("Content-Length", strconv.Itoa(len(data)))
			}
			responseStatus = response.StatusCode
			upstreamRequestID = diagnosticClientID(response.Header.Get("X-Request-ID"))
			response.Header.Set("X-Aigate-Backend", f.router)
			response.Header.Set(headerRoute, strconv.FormatUint(f.revision, 10))
			if strings.HasPrefix(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
				meta.stream()
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			code, status := "upstream_unavailable", 502
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				code, status = "upstream_timeout", 504
			}
			if upload != nil {
				var limit *http.MaxBytesError
				var timeout net.Error
				switch e := upload.failure(); {
				case errors.As(e, &limit):
					code, status = "request_body_too_large", 413
				case errors.As(e, &timeout) && timeout.Timeout():
					code, status = "request_body_timeout", 408
				case e != nil:
					code, status = "invalid_request", 400
				}
			}
			abandonRequestBody(w, inbound)
			f.audit.Print(string(encodeJSON(map[string]any{"event": "switch_forward_error", "request_id": id, "router_id": f.router, "binding_revision": f.revision, "error_class": code})))
			runtimeError(w, status, code)
		},
	}
	proxy.ServeHTTP(w, request)
}

func validateSwitchTarget(parent context.Context, g *runtimeGeneration, policy WarmupPolicy) (*WarmupReport, error) {
	ctx, cancel := context.WithTimeout(parent, seconds(policy.TimeoutSeconds))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.snapshot.TargetURL+"/runtime/v1/status", nil)
	if err != nil {
		return nil, err
	}
	response, err := g.g.client.Do(req)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	response.Body.Close()
	var status struct {
		Service      string `json:"service"`
		RouterID     string `json:"router_id"`
		ConfigDigest string `json:"config_digest"`
	}
	if readErr != nil || len(data) > 1<<20 || response.StatusCode != 200 || json.Unmarshal(data, &status) != nil || status.Service != "aigate-proxy" || status.RouterID != g.snapshot.RouterID || status.ConfigDigest != g.snapshot.ConfigDigest {
		return nil, fmt.Errorf("target_identity_changed")
	}
	return validateControlledRuntime(ctx, g.g, policy)
}
