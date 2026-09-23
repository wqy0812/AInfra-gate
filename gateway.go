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
	slow       *slowRequestStore
	now        func() time.Time
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
	g := &Gateway{config: c, client: client, transport: transport, audit: log.New(audit, "", 0), now: time.Now}
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
	if c.SlowRequests.Enabled {
		g.slow, err = newSlowRequestStore(c.SlowRequests, g.audit)
		if err != nil {
			g.Close()
			return nil, err
		}
	}
	return g, nil
}

func (g *Gateway) Close() {
	if g.slow != nil {
		g.slow.close()
	}
	if g.profile != nil {
		g.profile.close()
	}
	if g.management != nil {
		g.management.close()
	}
	g.transport.CloseIdleConnections()
}

type auditEntry struct {
	slowInput      []byte
	profile        *profileRequest
	generation     *generationResult
	RequestID      string  `json:"request_id"`
	QuestionPrefix string  `json:"question_prefix"`
	TestTask       string  `json:"test_task,omitempty"`
	RouteVersion   uint64  `json:"route_version,omitempty"`
	Status         int     `json:"status"`
	Outcome        string  `json:"outcome"`
	Backend        *string `json:"backend"`
	Model          *string `json:"model"`
	DurationMS     float64 `json:"duration_ms"`
}

func sendJSON(w http.ResponseWriter, entry *auditEntry, status int, data []byte) {
	if status >= 400 {
		entry.profile.liveTerminal()
	}
	entry.profile.liveStage("writing_client")
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
	started := g.now()
	if r.URL.Path == "/v1/chat/completions" {
		defer func() {
			ended := g.now()
			entry.DurationMS = float64(ended.Sub(started).Microseconds()) / 1000
			g.audit.Print(string(encodeJSON(entry)))
			if g.slow != nil {
				g.slow.capture(entry, started, ended, r.Context().Err() != nil)
			}
		}()
	}
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		sendJSON(w, entry, 200, []byte(`{"status":"ok"}`))
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

const requestBodyTimeout = 5 * time.Minute
const maxRequestBodyBytes int64 = 30_000_000

func readChatBody(w http.ResponseWriter, r *http.Request, timeout time.Duration) ([]byte, *GatewayError) {
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(timeout))
	defer controller.SetReadDeadline(time.Time{})
	tooLarge := fault(http.StatusRequestEntityTooLarge, "request_body_too_large", "请求体不能超过 30 MB")
	if r.ContentLength > maxRequestBodyBytes {
		w.Header().Set("Connection", "close")
		return nil, tooLarge
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	data, err := io.ReadAll(r.Body)
	if err == nil {
		return data, nil
	}
	// Do not let net/http drain an unfinished upload while writing the error.
	w.Header().Set("Connection", "close")
	var limitErr *http.MaxBytesError
	if errors.As(err, &limitErr) {
		return nil, tooLarge
	}
	var timeoutErr net.Error
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return nil, fault(http.StatusRequestTimeout, "request_body_timeout", "请求体上传超时")
	}
	return nil, fault(400, "invalid_request", "请求不符合固定 JSON 格式")
}

func (g *Gateway) chat(w http.ResponseWriter, r *http.Request, entry *auditEntry) {
	arrived := time.Now()
	data, bodyErr := readChatBody(w, r, requestBodyTimeout)
	if bodyErr != nil {
		sendError(w, entry, bodyErr)
		return
	}
	body, err := parseChat(data)
	if err != nil {
		sendError(w, entry, err)
		return
	}
	entry.QuestionPrefix = questionPrefix(body.question)
	if meta, ok := r.Context().Value(runtimeRequestKey{}).(*runtimeRequest); ok {
		entry.RouteVersion, entry.TestTask = meta.revision, meta.task
		if body.stream {
			meta.stream()
		}
	}
	if body.stream && g.slow != nil {
		// Keep the original bytes alive; makePayload may change the parsed fields.
		entry.slowInput = data
	}
	if g.profile != nil {
		pr := g.profile.beginRequest(entry.RequestID, body.stream, arrived)
		entry.profile = pr
		r = r.WithContext(context.WithValue(r.Context(), profileContextKey{}, pr))
		defer func() { g.profile.end(pr, entry, r.Context().Err() != nil) }()
	}
	var name, model string
	var backend *Backend
	outcome := "ignored"
	if g.management != nil {
		var lease *backendLease
		lease, err = g.management.acquireRequest(r.Header.Get("X-Code-Eval-Token"), body.stream)
		if err == nil {
			defer func() {
				if r.Context().Err() != nil {
					outcome = "client_cancelled"
				}
				lease.release(outcome)
			}()
			name = lease.name
			entry.RouteVersion = lease.version
			if lease.test {
				entry.TestTask = g.management.status().TestOccupancy.TaskID
			}
			w.Header().Set("X-Aigate-Backend", name)
			w.Header().Set("X-Aigate-Route-Version", fmt.Sprint(lease.version))
			entry.profile.liveBackend(name)
			backend = lease.runtime.backend
			model, err = g.pool.discoverRuntime(r.Context(), lease.runtime)
			g.management.noteModel(name, model)
			if err == nil && lease.test && model != g.management.status().TestOccupancy.Model {
				err = fault(409, "test_model_changed", "实际模型与测试快照不一致")
			}
			if err != nil {
				outcome = "backend_failure"
			}
		}
	} else {
		name, backend, model, err = g.pool.choose(r.Context())
	}
	if meta, ok := r.Context().Value(runtimeRequestKey{}).(*runtimeRequest); ok && err == nil && meta.model != "" && meta.model != model {
		err = fault(409, "test_model_changed", "实际模型与测试快照不一致")
	}
	if err != nil {
		sendError(w, entry, err)
		return
	}
	entry.Backend, entry.Model = &name, &model
	w.Header().Set("X-Aigate-Model", model)
	entry.profile.liveStage("preparing_upstream")
	if pr, ok := r.Context().Value(profileContextKey{}).(*profileRequest); ok {
		pr.e.TestTask = entry.TestTask
		pr.e.RouteVersion = entry.RouteVersion
		pr.e.Backend = name
		pr.e.Model = model
	}
	if body.stream && backend.Adapter != "openai" {
		sendError(w, entry, fault(400, "stream_not_supported", "模板适配器尚不支持下游流式协议转换"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), seconds(g.config.TimeoutSeconds))
	defer cancel()
	entry.generation = &generationResult{stream: body.stream}
	state := entry.generation
	if body.stream {
		state.observer = newSSEObserver(false)
		ctx = context.WithValue(ctx, sseObserverContextKey{}, state.observer)
	}
	ctx = context.WithValue(ctx, generationContextKey{}, state)
	defer func() { state.finish(entry, r.Context().Err() != nil) }()
	response, err := g.generate(ctx, body, backend, model)
	if err != nil {
		if state.started && state.errorClass == "" && r.Context().Err() == nil {
			if err.Code == "upstream_timeout" {
				state.fail("timeout")
			} else {
				state.fail("connection_error")
			}
		}
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
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		outcome = "backend_failure"
		if response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != 429 {
			outcome = "upstream_rejected"
		}
		g.forwardGenerationError(w, r, response, ctx, entry)
		return
	}
	if body.stream {
		outcome = g.stream(w, r, response, ctx, entry)
		return
	}
	entry.profile.liveStage("reading_response")
	data, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		state.transportError(readErr, ctx, r.Context(), "response_read_error")
		outcome = "backend_failure"
		sendError(w, entry, upstreamError(readErr, ctx))
		return
	}

	result := data
	if backend.Adapter == "openai" {
		state.observeNonstream(data, response.Header)
		// The OpenAI backend owns its response format. Only the configured public
		// model alias may change its body; an unrecognized body passes through.
		if g.config.PublicModelName != "" {
			result = rewriteChatResponseModel(data, g.config.PublicModelName)
		}
	} else {
		result, err = normalizeResponse(data, backend, g.config.publicModel(model), entry.RequestID)
		if err != nil {
			state.fail("protocol_error")
			outcome = "backend_failure"
			sendError(w, entry, err)
			return
		}
	}
	if entry.profile != nil {
		var decoded struct {
			Usage json.RawMessage `json:"usage"`
		}
		// Template normalization creates a new response envelope without usage.
		// Observe upstream counters without requiring a particular response shape.
		if json.Unmarshal(data, &decoded) == nil {
			entry.profile.usage = usageCounters(decoded.Usage)
		}
	}
	if backend.Adapter == "openai" {
		entry.profile.liveStage("writing_client")
		copyRepresentationHeaders(w.Header(), response.Header)
		w.WriteHeader(response.StatusCode)
		entry.Status = response.StatusCode
		if _, err := w.Write(result); err == nil {
			entry.Outcome = "completed"
		}
	} else {
		sendJSON(w, entry, 200, result)
	}
	if entry.Outcome == "completed" {
		outcome = "success"
	} else {
		state.writeFailure(ctx, r.Context())
		outcome = "client_failure"
	}
}

func (g *Gateway) generate(ctx context.Context, body *chatBody, backend *Backend, model string) (*http.Response, *GatewayError) {
	profileFrom(ctx).liveStage("preparing_upstream")
	payload, err := makePayload(body, backend, model)
	if err != nil {
		return nil, err
	}
	if observer, ok := ctx.Value(sseObserverContextKey{}).(*sseObserver); ok {
		observer.expectPayload(payload)
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
	state := generationFrom(ctx)
	if state != nil {
		state.started = true
	}
	profileFrom(ctx).liveStage("waiting_upstream_headers")
	response, requestErr := g.client.Do(req)
	if requestErr != nil {
		return nil, upstreamError(requestErr, ctx)
	}
	if state != nil {
		state.status = response.StatusCode
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			state.fail(upstreamHTTPClass(response.StatusCode))
			return response, nil
		}
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
		state.fail("protocol_error")
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
	entry.profile.liveWriteStart()
	w.WriteHeader(200)
	entry.Status = 200
	flushErr := controller.Flush()
	entry.profile.liveIOEnd()
	if flushErr != nil {
		entry.generation.writeFailure(ctx, r.Context())
		return "client_failure"
	}
	buffer := g.buffers.Get().(*[]byte)
	defer g.buffers.Put(buffer)
	observer := entry.generation.observer
	var profileObserver *sseObserver
	if pr, ok := r.Context().Value(profileContextKey{}).(*profileRequest); ok {
		profileObserver = observer
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
				if profileObserver.completionClass() == "unknown" {
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
		entry.profile.liveWriteStart()
		defer entry.profile.liveIOEnd()
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
		entry.profile.liveStreamRead(observer)
		n, readErr := response.Body.Read(*buffer)
		entry.profile.liveIOEnd()
		if n > 0 {
			data := (*buffer)[:n]
			observer.Feed(data)
			entry.profile.liveObserve(observer)
			entry.generation.observeStream(observer, false)
			var err error
			if rewrite != nil {
				err = rewrite.feed(data, emit)
			} else {
				err = emit(data)
			}
			if writeFailed {
				entry.generation.writeFailure(ctx, r.Context())
				return "client_failure"
			}
			if err != nil {
				readErr = err
			}
		}
		if readErr == io.EOF && r.Context().Err() == nil {
			entry.generation.observeStream(observer, true)
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
				if observer.completionClass() == "unknown" {
					entry.Outcome = "protocol_unknown"
					return "protocol_unknown"
				}
				return "backend_failure"
			}
			entry.Outcome = "completed"
			return "success"
		}
		if errors.Is(readErr, errSSEModel) {
			if observer.Unknown {
				entry.generation.result = "unknown"
			} else {
				entry.generation.fail("protocol_error")
			}
		} else {
			entry.generation.transportError(readErr, ctx, r.Context(), "response_read_error")
		}
		code := "upstream_stream_error"
		if errors.Is(readErr, errSSEModel) {
			code = "invalid_upstream_stream"
		}
		if upstreamError(readErr, ctx).Code == "upstream_timeout" {
			code = "upstream_timeout"
		}
		entry.Outcome = code
		entry.profile.liveTerminal()
		// Permit a bounded final error write after the generation deadline expires.
		_ = controller.SetWriteDeadline(time.Now().Add(time.Second))
		entry.profile.liveWriteStart()
		_, _ = fmt.Fprintf(w, "\n\ndata: %s\n\n", errorJSON(fault(502, code, "下游流中断"), entry.RequestID))
		_ = controller.Flush()
		entry.profile.liveIOEnd()
		return "backend_failure"
	}
}
