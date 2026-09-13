package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Gateway struct {
	config     *Config
	pool       *BackendPool
	client     *http.Client
	transport  *http.Transport
	audit      *log.Logger
	apiKey     string
	buffers    sync.Pool
	management *Management
	profile    *requestProfiler
}

func newGateway(c *Config, audit io.Writer) (*Gateway, error) {
	effective, err := effectiveManagedConfig(c)
	if err != nil {
		return nil, err
	}
	c = effective
	if err := c.validate(); err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: c.HTTPPool.MaxIdleConns,
		MaxIdleConnsPerHost: c.HTTPPool.MaxIdleConnsPerHost, MaxConnsPerHost: c.HTTPPool.MaxConnsPerHost,
		IdleConnTimeout: seconds(c.HTTPPool.IdleTimeoutSeconds), TLSHandshakeTimeout: 10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if audit == nil {
		audit = io.Discard
	}
	g := &Gateway{config: c, client: client, transport: transport, audit: log.New(audit, "", 0)}
	if c.GatewayAPIKeyEnv != "" {
		g.apiKey = "Bearer " + os.Getenv(c.GatewayAPIKeyEnv)
	}
	g.buffers.New = func() any { b := make([]byte, 32*1024); return &b }
	g.pool = newPool(c, client)
	if _, err := g.pool.policy(); err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	if c.Management.Enabled {
		var err error
		g.management, err = newManagement(g)
		if err != nil {
			transport.CloseIdleConnections()
			return nil, err
		}
	}
	if c.Profile.Enabled {
		g.profile, err = newRequestProfiler(c.Profile)
		if err != nil {
			g.Close()
			return nil, err
		}
	}
	return g, nil
}

func (g *Gateway) Close() {
	if g.profile != nil {
		g.profile.close()
	}
	if g.management != nil {
		g.management.close()
	}
	g.transport.CloseIdleConnections()
}

type auditEntry struct {
	RequestID      string  `json:"request_id"`
	QuestionPrefix string  `json:"question_prefix"`
	Status         int     `json:"status"`
	Outcome        string  `json:"outcome"`
	Backend        *string `json:"backend"`
	Model          *string `json:"model"`
	DurationMS     float64 `json:"duration_ms"`
}

func sendJSON(w http.ResponseWriter, entry *auditEntry, status int, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	entry.Status = status
	if _, err := w.Write(data); err == nil {
		entry.Outcome = "completed"
		if status >= 400 {
			entry.Outcome = "rejected"
		}
	}
}

func errorJSON(err *GatewayError, id string) []byte {
	return encodeJSON(map[string]any{"error": map[string]any{"code": err.Code, "message": err.Message, "request_id": id}})
}

func sendError(w http.ResponseWriter, entry *auditEntry, err *GatewayError) {
	sendJSON(w, entry, err.Status, errorJSON(err, entry.RequestID))
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		http.Error(w, "request ID unavailable", 500)
		return
	}
	entry := &auditEntry{RequestID: hex.EncodeToString(id[:]), Status: 500, Outcome: "incomplete"}
	w.Header().Set("X-Request-ID", entry.RequestID)
	started := time.Now()
	if r.URL.Path == "/v1/chat/completions" {
		if g.profile != nil {
			pr := g.profile.begin(entry.RequestID)
			r = r.WithContext(context.WithValue(r.Context(), profileContextKey{}, pr))
			defer func() { g.profile.end(pr, entry, r.Context().Err() != nil) }()
		}
		defer func() {
			entry.DurationMS = float64(time.Since(started).Microseconds()) / 1000
			g.audit.Print(string(encodeJSON(entry)))
		}()
	}
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		sendJSON(w, entry, 200, []byte(`{"status":"ok"}`))
		return
	}
	if r.URL.Path == "/docs" && r.Method == http.MethodGet {
		serveDocs(w)
		return
	}
	if r.URL.Path == "/openapi.json" && r.Method == http.MethodGet {
		serveOpenAPI(w)
		return
	}
	wantMethod := ""
	switch r.URL.Path {
	case "/v1/models":
		wantMethod = http.MethodGet
	case "/v1/chat/completions":
		wantMethod = http.MethodPost
	}
	if wantMethod == "" {
		sendError(w, entry, fault(404, "not_found", "接口不存在"))
		return
	}
	if r.Method != wantMethod {
		w.Header().Set("Allow", wantMethod)
		sendError(w, entry, fault(405, "method_not_allowed", "请求方法不支持"))
		return
	}
	if g.apiKey != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(g.apiKey)) != 1 {
		sendError(w, entry, fault(401, "unauthorized", "网关凭据无效"))
		return
	}
	if r.URL.Path == "/v1/models" {
		var ids []string
		var err *GatewayError
		if g.management != nil {
			var lease *backendLease
			lease, err = g.management.acquire("", true, false, false)
			if err == nil {
				defer lease.release("ignored")
				var model string
				model, err = g.pool.discoverRuntime(r.Context(), lease.runtime)
				g.management.noteModel(lease.name, model)
				if err == nil {
					ids = []string{model}
				}
			}
		} else {
			ids, err = g.pool.models(r.Context())
		}
		if err != nil {
			sendError(w, entry, err)
			return
		}
		models := make([]any, 0, len(ids))
		for _, id := range ids {
			models = append(models, map[string]any{"id": g.config.publicModel(id), "object": "model", "created": 0, "owned_by": "aigate"})
		}
		sendJSON(w, entry, 200, encodeJSON(map[string]any{"object": "list", "data": models}))
		return
	}
	g.chat(w, r, entry)
}

func upstreamError(err error, ctx context.Context) *GatewayError {
	var timeout net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
		return fault(504, "upstream_timeout", "下游请求超时")
	}
	return fault(502, "upstream_unavailable", "无法连接下游服务")
}

func questionPrefix(question string) string {
	count := 0
	for index := range question {
		if count == 10 {
			return question[:index]
		}
		count++
	}
	return question
}

func (g *Gateway) chat(w http.ResponseWriter, r *http.Request, entry *auditEntry) {
	data, readErr := io.ReadAll(r.Body)
	if readErr != nil {
		sendError(w, entry, fault(400, "invalid_request", "请求不符合固定 JSON 格式"))
		return
	}
	body, err := parseChat(data)
	if err != nil {
		sendError(w, entry, err)
		return
	}
	entry.QuestionPrefix = questionPrefix(body.question)
	var name, model string
	var backend *Backend
	outcome := "ignored"
	if g.management != nil {
		var lease *backendLease
		lease, err = g.management.acquire("", false, body.stream, false)
		if err == nil {
			defer func() {
				if r.Context().Err() != nil {
					outcome = "client_cancelled"
				}
				lease.release(outcome)
			}()
			name = lease.name
			backend = lease.runtime.backend
			model, err = g.pool.discoverRuntime(r.Context(), lease.runtime)
			g.management.noteModel(name, model)
			if err != nil {
				outcome = "backend_failure"
			}
		}
	} else {
		name, backend, model, err = g.pool.choose(r.Context())
	}
	if err != nil {
		sendError(w, entry, err)
		return
	}
	entry.Backend, entry.Model = &name, &model
	if pr, ok := r.Context().Value(profileContextKey{}).(*profileRequest); ok {
		pr.e.Backend = name
		pr.e.Model = model
	}
	if body.stream && backend.Adapter != "openai" {
		sendError(w, entry, fault(400, "stream_not_supported", "模板适配器尚不支持下游流式协议转换"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), seconds(g.config.TimeoutSeconds))
	defer cancel()
	response, err := g.generate(ctx, body, backend, model)
	if err != nil {
		if err.Status >= 500 || err.Status == 429 {
			outcome = "backend_failure"
		}
		if err.UpstreamStatus >= 400 && err.UpstreamStatus < 500 && err.UpstreamStatus != 429 {
			outcome = "upstream_rejected"
		}
		sendError(w, entry, err)
		return
	}
	defer response.Body.Close()
	if body.stream {
		outcome = g.stream(w, r, response, ctx, entry)
		return
	}
	data, readErr = io.ReadAll(response.Body)
	if readErr != nil {
		outcome = "backend_failure"
		sendError(w, entry, upstreamError(readErr, ctx))
		return
	}
	if pr, ok := r.Context().Value(profileContextKey{}).(*profileRequest); ok {
		var v struct {
			Usage json.RawMessage `json:"usage"`
		}
		if json.Unmarshal(data, &v) == nil {
			pr.usage = v.Usage
		} else {
			pr.quality = "response_parse"
		}
	}
	result, err := normalizeResponse(data, backend, g.config.publicModel(model), entry.RequestID)
	if err != nil {
		outcome = "backend_failure"
		sendError(w, entry, err)
		return
	}
	sendJSON(w, entry, 200, result)
	if entry.Outcome == "completed" {
		outcome = "success"
	} else {
		outcome = "client_failure"
	}
}

func (g *Gateway) generate(ctx context.Context, body *chatBody, backend *Backend, model string) (*http.Response, *GatewayError) {
	payload, err := makePayload(body, backend, model)
	if err != nil {
		return nil, err
	}
	if pr, ok := ctx.Value(profileContextKey{}).(*profileRequest); ok && g.profile != nil {
		g.profile.input(pr, payload, backend, pr.e.Backend, model)
	}
	req, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, backend.URL, bytes.NewReader(payload))
	if requestErr != nil {
		return nil, upstreamError(requestErr, ctx)
	}
	// Never replay generation, even when configured with idempotency headers.
	req.GetBody = nil
	req.Header = backend.headers.Clone()
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	response, requestErr := g.client.Do(req)
	if requestErr != nil {
		return nil, upstreamError(requestErr, ctx)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		response.Body.Close()
		status := 502
		if response.StatusCode == 429 {
			status = 429
		}
		err := fault(status, "upstream_http_error", fmt.Sprintf("下游返回 HTTP %d", response.StatusCode))
		err.UpstreamStatus = response.StatusCode
		return nil, err
	}
	if body.stream && !strings.EqualFold(strings.TrimSpace(strings.SplitN(response.Header.Get("Content-Type"), ";", 2)[0]), "text/event-stream") {
		response.Body.Close()
		return nil, fault(502, "invalid_upstream_stream", "下游未返回 SSE 流")
	}
	return response, nil
}

func (g *Gateway) stream(w http.ResponseWriter, r *http.Request, response *http.Response, ctx context.Context, entry *auditEntry) string {
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	_ = controller.SetWriteDeadline(deadline)
	defer controller.SetWriteDeadline(time.Time{})
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(200)
	entry.Status = 200
	if controller.Flush() != nil {
		return "client_failure"
	}
	buffer := g.buffers.Get().(*[]byte)
	defer g.buffers.Put(buffer)
	var observer *sseObserver
	if g.management != nil {
		observer = newSSEObserver(false)
	}
	var profileObserver *sseObserver
	if pr, ok := r.Context().Value(profileContextKey{}).(*profileRequest); ok {
		profileObserver = newSSEObserver(false)
		profileObserver.started = pr.start
		profileObserver.profileUsage = true
		defer func() {
			pr.usage = profileObserver.Usage
			if profileObserver.FirstMS != nil {
				n := *profileObserver.FirstMS / 1000
				pr.first = &n
			}
			if profileObserver.Unknown || profileObserver.Invalid {
				pr.quality = "sse_parse"
			}
			// Keep forwarding semantics unchanged; profile success requires a complete stream.
			if entry.Outcome == "completed" && !profileObserver.valid() {
				pr.outcome = "upstream_protocol_error"
				if profileObserver.Unknown {
					pr.outcome = "protocol_unknown"
				}
			}
		}()
	}
	var rewrite *sseModelWriter
	if g.config.PublicModelName != "" {
		rewrite = &sseModelWriter{model: g.config.PublicModelName}
	}
	writeFailed := false
	emit := func(data []byte) error {
		if _, err := w.Write(data); err != nil {
			writeFailed = true
			return err
		}
		if err := controller.Flush(); err != nil {
			writeFailed = true
			return err
		}
		return nil
	}
	for {
		n, readErr := response.Body.Read(*buffer)
		if n > 0 {
			data := (*buffer)[:n]
			if profileObserver != nil {
				profileObserver.Feed(data)
			}
			var err error
			if rewrite != nil {
				err = rewrite.feed(data, emit)
			} else {
				err = emit(data)
			}
			if writeFailed {
				return "client_failure"
			}
			if observer != nil {
				observer.Feed(data)
			}
			if err != nil {
				readErr = err
			}
		}
		if readErr == io.EOF && rewrite != nil {
			if err := rewrite.finish(); err != nil {
				readErr = err
			}
		}
		if readErr == nil {
			continue
		}
		if r.Context().Err() != nil {
			return "client_cancelled"
		}
		if readErr == io.EOF {
			if observer != nil && !observer.valid() {
				entry.Outcome = "upstream_protocol_error"
				if observer.Unknown {
					entry.Outcome = "protocol_unknown"
					return "protocol_unknown"
				}
				return "backend_failure"
			}
			entry.Outcome = "completed"
			return "success"
		}
		code := "upstream_stream_error"
		if errors.Is(readErr, errSSEModel) {
			code = "invalid_upstream_stream"
		}
		if upstreamError(readErr, ctx).Code == "upstream_timeout" {
			code = "upstream_timeout"
		}
		entry.Outcome = code
		// Permit a bounded final error write after the generation deadline expires.
		_ = controller.SetWriteDeadline(time.Now().Add(time.Second))
		_, _ = fmt.Fprintf(w, "\n\ndata: %s\n\n", errorJSON(fault(502, code, "下游流中断"), entry.RequestID))
		_ = controller.Flush()
		return "backend_failure"
	}
}
