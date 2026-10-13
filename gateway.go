package aigate

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
	"strings"
	"sync"
	"time"
)

type Gateway struct {
	config    *RuntimeConfig
	pool      *BackendPool
	client    *http.Client
	transport *http.Transport
	audit     *log.Logger
	apiKey    string
	buffers   sync.Pool
	profile   *requestProfiler
	slow      *slowRequestStore
	now       func() time.Time
	incidents *incidentStore      // owned by ControlledGateway, shared across route snapshots
	captures  *streamCaptureStore // proxy only; never installed on switch snapshots
}

func newGateway(c *RuntimeConfig, audit io.Writer) (*Gateway, error) {
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
	g.buffers.New = func() any { b := make([]byte, 32*1024); return &b }
	g.pool = newPool(c, client)
	var err error
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
	var group sync.WaitGroup
	if g.slow != nil {
		group.Add(1)
		go func() { defer group.Done(); g.slow.close() }()
	}
	if g.profile != nil {
		group.Add(1)
		go func() { defer group.Done(); g.profile.close() }()
	}
	g.transport.CloseIdleConnections()
	group.Wait()
}

type auditEntry struct {
	started          time.Time
	slowInput        []byte
	profile          *profileRequest
	generation       *generationResult
	RequestID        string             `json:"request_id"`
	TestTask         string             `json:"test_task,omitempty"`
	RouteVersion     uint64             `json:"route_version,omitempty"`
	Traffic          string             `json:"traffic,omitempty"`
	Status           int                `json:"status"`
	Outcome          string             `json:"outcome"`
	Backend          *string            `json:"backend"`
	Model            *string            `json:"model"`
	DurationMS       float64            `json:"duration_ms"`
	ClientRequestID  string             `json:"client_request_id,omitempty"`
	Connection       *connectionSummary `json:"connection,omitempty"`
	Transport        *transportSnapshot `json:"transport,omitempty"`
	GenerationResult string             `json:"generation_result,omitempty"`
	ErrorClass       string             `json:"error_class,omitempty"`
	diagnostic       *requestTransport
}

func sendJSON(w http.ResponseWriter, entry *auditEntry, status int, data []byte) {
	if status >= 400 {
		entry.profile.liveTerminal()
	}
	entry.profile.liveStage("writing_client")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	entry.Status = status
	n, writeErr := w.Write(data)
	entry.diagnostic.bytes(false, n)
	entry.diagnostic.failure("client_response_write", writeErr, nil, nil)
	if writeErr == nil {
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
		abandonRequestBody(w, r)
		http.Error(w, "request ID unavailable", 500)
		return
	}
	entry := &auditEntry{RequestID: hex.EncodeToString(id[:]), Status: 500, Outcome: "incomplete"}
	w.Header().Set("X-Request-ID", entry.RequestID)
	started := g.now()
	entry.started = started
	if r.URL.Path == "/v1/chat/completions" {
		entry.ClientRequestID = diagnosticClientID(r.Header.Get("X-Request-ID"))
		if conn, ok := r.Context().Value(connectionContextKey{}).(connectionSummary); ok {
			entry.Connection = &conn
		}
		entry.diagnostic = newRequestTransport()
		r = r.WithContext(context.WithValue(r.Context(), requestTransportKey{}, entry.diagnostic))
		entry.diagnostic.stage("handler_entered")
		defer func() {
			ended := g.now()
			entry.DurationMS = float64(ended.Sub(started).Microseconds()) / 1000
			if entry.generation != nil {
				entry.GenerationResult, entry.ErrorClass = entry.generation.result, entry.generation.errorClass
			}
			if entry.Outcome != "completed" || entry.Status >= 400 || entry.ErrorClass != "" || entry.GenerationResult == "client_cancelled" || entry.GenerationResult == "client_disconnected" || r.Context().Err() != nil {
				entry.diagnostic.stage("handler_finished")
				entry.Transport = entry.diagnostic.snapshot()
				entry.Transport.ClientContext = contextStatus(r.Context())
			}
			g.audit.Print(string(encodeJSON(entry)))
			if g.slow != nil {
				g.slow.capture(entry, started, ended, r.Context().Err() != nil)
			}
		}()
	}
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		abandonRequestBody(w, r)
		sendJSON(w, entry, 200, []byte(`{"status":"ok"}`))
		return
	}
	if r.URL.Path == "/openapi.json" && r.Method == http.MethodGet {
		abandonRequestBody(w, r)
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
		abandonRequestBody(w, r)
		sendError(w, entry, fault(404, "not_found", "接口不存在"))
		return
	}
	if r.Method != wantMethod {
		abandonRequestBody(w, r)
		w.Header().Set("Allow", wantMethod)
		sendError(w, entry, fault(405, "method_not_allowed", "请求方法不支持"))
		return
	}
	if g.apiKey != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(g.apiKey)) != 1 {
		abandonRequestBody(w, r)
		sendError(w, entry, fault(401, "unauthorized", "网关凭据无效"))
		return
	}
	if r.URL.Path == "/v1/models" {
		ids, err := g.pool.models(r.Context())
		abandonRequestBody(w, r)
		if err != nil {
			sendError(w, entry, err)
			return
		}
		models := make([]any, 0, len(ids))
		for _, id := range ids {
			models = append(models, map[string]any{"id": id, "object": "model", "created": 0, "owned_by": "aigate"})
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

const requestBodyTimeout = 5 * time.Minute
const maxRequestBodyBytes int64 = 30_000_000

func readChatBody(w http.ResponseWriter, r *http.Request, timeout time.Duration) ([]byte, *GatewayError) {
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(timeout))
	tooLarge := fault(http.StatusRequestEntityTooLarge, "request_body_too_large", "请求体不能超过 30 MB")
	if r.ContentLength > maxRequestBodyBytes {
		abandonRequestBody(w, r)
		return nil, tooLarge
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	data, err := io.ReadAll(r.Body)
	if err == nil {
		_ = controller.SetReadDeadline(time.Time{})
		return data, nil
	}
	transportFrom(r.Context()).failure("client_request_body_read", err, nil, r.Context())
	abandonRequestBody(w, r)
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
	entry.diagnostic.stage("request_body_read")
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
	entry.diagnostic.stage("request_body_ready")
	if body.stream && g.incidents != nil {
		head, tail, length := questionEdges(incidentQuestionText(body.question))
		r = r.WithContext(context.WithValue(r.Context(), incidentQuestionKey{}, incidentQuestion{head, tail, length}))
	}
	if meta, ok := r.Context().Value(runtimeRequestKey{}).(*runtimeRequest); ok {
		entry.RouteVersion, entry.TestTask, entry.Traffic = meta.revision, meta.task, meta.traffic
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
		pr.e.Traffic = entry.Traffic
		entry.profile = pr
		r = r.WithContext(context.WithValue(r.Context(), profileContextKey{}, pr))
		defer func() { g.profile.end(pr, entry, r.Context().Err() != nil) }()
	}
	entry.diagnostic.stage("routing_discovery")
	name, backend, model, err := g.pool.choose(r.Context())
	if meta, ok := r.Context().Value(runtimeRequestKey{}).(*runtimeRequest); ok && err == nil && meta.model != "" && meta.model != model {
		err = fault(409, "test_model_changed", "实际模型与测试快照不一致")
	}
	if err != nil {
		sendError(w, entry, err)
		return
	}
	entry.Backend, entry.Model = &name, &model
	entry.diagnostic.stage("routing_ready")
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
	defer func() { entry.diagnostic.contexts(ctx, r.Context()) }()
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
		sendError(w, entry, err)
		return
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		g.forwardGenerationError(w, r, response, ctx, entry)
		return
	}
	if body.stream {
		g.stream(w, r, response, ctx, entry, data)
		return
	}
	entry.profile.liveStage("reading_response")
	data, readErr := io.ReadAll(response.Body)
	entry.diagnostic.bytes(true, len(data))
	if readErr != nil {
		entry.diagnostic.failure("upstream_body_read", readErr, ctx, r.Context())
		state.transportError(readErr, ctx, r.Context(), "response_read_error")
		sendError(w, entry, upstreamError(readErr, ctx))
		return
	}

	result := data
	if backend.Adapter == "openai" {
		state.observeNonstream(data, response.Header)
	} else {
		result, err = normalizeResponse(data, backend, model, entry.RequestID)
		if err != nil {
			state.fail("protocol_error")
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
		n, writeErr := w.Write(result)
		entry.diagnostic.bytes(false, n)
		entry.diagnostic.failure("client_response_write", writeErr, ctx, r.Context())
		if writeErr == nil {
			entry.Outcome = "completed"
		}
	} else {
		sendJSON(w, entry, 200, result)
	}
	if entry.Outcome != "completed" {
		state.writeFailure(ctx, r.Context())
	}
}

func (g *Gateway) generate(ctx context.Context, body *chatBody, backend *Backend, model string) (*http.Response, *GatewayError) {
	diagnostic := transportFrom(ctx)
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
	req, requestErr := http.NewRequestWithContext(diagnostic.trace(ctx), http.MethodPost, backend.URL, bytes.NewReader(payload))
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
		diagnostic.failure("upstream_round_trip", requestErr, ctx, nil)
		return nil, upstreamError(requestErr, ctx)
	}
	diagnostic.stage("response_headers_received")
	diagnostic.status(response.StatusCode)
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
	if body.stream {
		if state != nil {
			state.responseEncoding = response.Header.Get("Content-Encoding")
			if response.Uncompressed {
				state.responseEncoding = "gzip"
				state.decodeOrigin = "transport"
				state.responseDecoded = true
			}
		}
		if err := decodeSSEBody(response); err != nil {
			response.Body.Close()
			diagnostic.failure("upstream_content_decode", err, ctx, nil)
			state.fail("protocol_error")
			return nil, err
		}
		if state != nil && response.Uncompressed && state.decodeOrigin == "" {
			state.decodeOrigin = "proxy"
			state.responseDecoded = true
		}
	}
	return response, nil
}

func (g *Gateway) stream(w http.ResponseWriter, r *http.Request, response *http.Response, ctx context.Context, entry *auditEntry, raw ...[]byte) (outcome string) {
	entry.diagnostic.stage("stream_started")
	var capture *streamCapture
	if len(raw) > 0 {
		capture = g.captures.begin(entry.RequestID, raw[0], entry.started)
	}
	state := entry.generation
	capture.setResponse(response.StatusCode, response.Header.Get("Content-Type"), state.responseEncoding, state.decodeOrigin, state.responseDecoded)
	var eof bool
	defer func() {
		reason := outcome
		if r.Context().Err() != nil {
			reason = "client_cancelled"
		} else if ctx.Err() != nil {
			reason = "timeout"
		}
		capture.finish(time.Now(), reason, eof, state.observer.Done)
	}()
	var diagnostic *incidentRequest
	if g.incidents != nil {
		base := streamIncident{RequestID: entry.RequestID, BindingRevision: entry.RouteVersion, RequestStartedAt: entry.started.UTC(), ConnectionPeer: r.RemoteAddr, Traffic: entry.Traffic}
		if entry.Backend != nil {
			base.Router = *entry.Backend
		}
		if entry.Model != nil {
			base.Model = *entry.Model
		}
		if question, ok := r.Context().Value(incidentQuestionKey{}).(incidentQuestion); ok {
			base.QuestionHead, base.QuestionTail, base.QuestionLength = question.head, question.tail, question.length
		}
		diagnostic = g.incidents.begin(base, capture)
		defer func() {
			reason := outcome
			if r.Context().Err() != nil {
				reason = "client_cancelled"
			} else if ctx.Err() != nil {
				reason = "timeout"
			}
			diagnostic.finish(time.Now(), reason)
		}()
	}
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	_ = controller.SetWriteDeadline(deadline)
	defer controller.SetWriteDeadline(time.Time{})
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	entry.profile.liveWriteStart()
	headerStart := time.Now()
	diagnostic.setWritePosition(captureEvent{Origin: "upstream", Phase: "headers_flush", IOSequence: 1})
	diagnostic.startWrite(headerStart, 0)
	w.WriteHeader(200)
	entry.Status = 200
	headerFlushStart := time.Now()
	diagnostic.wroteAt(headerFlushStart, 0)
	diagnostic.startFlush(headerFlushStart, 0)
	flushErr := controller.Flush()
	headerReturned := time.Now()
	diagnostic.flushedAt(headerReturned)
	capture.event(captureEvent{Type: "flush_start", Origin: "upstream", Phase: "headers_flush", IOSequence: 1, At: headerFlushStart})
	capture.event(captureEvent{Type: "flush_return", Origin: "upstream", Phase: "headers_flush", IOSequence: 1, At: headerReturned, FlushReturned: true, Error: captureError(flushErr)})
	entry.diagnostic.failure("client_headers_flush", flushErr, ctx, r.Context())
	diagnostic.endWrite(headerReturned, flushErr)
	entry.profile.liveIOEnd()
	if flushErr != nil {
		entry.generation.writeFailure(ctx, r.Context())
		return "client_failure"
	}
	buffer := g.buffers.Get().(*[]byte)
	defer g.buffers.Put(buffer)
	observer := entry.generation.observer
	if capture != nil {
		observer.onEvent = func(p sseEventPosition) {
			diagnostic.eventPosition(p)
			capture.event(captureEvent{Type: "sse_event", Origin: "upstream", At: p.At, HasRange: true, Start: p.Start, End: p.End, EventIndex: p.Index, PositionKnown: p.Known})
		}
	}
	defer func() { observer.onEvent = nil }()
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
	writeFailed := false
	var received, totalWritten int64
	var ioSequence uint64 = 1
	emit := func(data []byte, origin string, start, end int64, alwaysFlush bool) error {
		entry.profile.liveWriteStart()
		defer entry.profile.liveIOEnd()
		ioSequence++
		position := captureEvent{Origin: origin, Phase: "body_write", IOSequence: ioSequence, HasRange: origin == "upstream", Start: start, End: end, PlannedBytes: len(data), TotalWritten: totalWritten}
		diagnostic.setWritePosition(position)
		writeStart := time.Now()
		diagnostic.startWrite(writeStart, len(data))
		n, err := w.Write(data)
		writeReturned := time.Now()
		diagnostic.wroteAt(writeReturned, n)
		totalWritten += int64(n)
		position.Type = "write_start"
		position.At = writeStart
		if origin == "gateway" {
			position.Data = data
		}
		capture.event(position)
		position.Type = "write_return"
		position.At = writeReturned
		position.Data = nil
		position.WrittenKnown = true
		position.WrittenBytes = n
		position.TotalWritten = totalWritten
		position.Error = captureError(err)
		capture.event(position)
		diagnostic.setWritePosition(position)
		entry.diagnostic.bytes(false, n)
		writeStage, flushStage := "client_body_write", "client_body_flush"
		if origin == "gateway" {
			writeStage, flushStage = "client_error_write", "client_error_flush"
		}
		entry.diagnostic.failure(writeStage, err, ctx, r.Context())
		diagnostic.wrote(n)
		if err != nil && !alwaysFlush {
			diagnostic.endWrite(writeReturned, err)
			writeFailed = true
			return err
		}
		flushStart := time.Now()
		diagnostic.startFlush(flushStart, n)
		flushErr := controller.Flush()
		flushReturned := time.Now()
		diagnostic.flushedAt(flushReturned)
		position.Phase = "body_flush"
		position.Type = "flush_start"
		position.At = flushStart
		position.Error = ""
		capture.event(position)
		position.Type = "flush_return"
		position.At = flushReturned
		position.FlushReturned = true
		position.Error = captureError(flushErr)
		capture.event(position)
		entry.diagnostic.failure(flushStage, flushErr, ctx, r.Context())
		if err == nil {
			err = flushErr
		}
		diagnostic.endWrite(flushReturned, err)
		if err != nil {
			writeFailed = true
			return err
		}
		return nil
	}
	for {
		entry.profile.liveStreamRead(observer)
		readStart := time.Now()
		diagnostic.startRead(readStart)
		n, readErr := response.Body.Read(*buffer)
		readReturned := time.Now()
		if readErr == io.EOF {
			eof = true
		}
		start := received
		received += int64(n)
		capture.response(readReturned, start, (*buffer)[:n])
		capture.event(captureEvent{Type: "read_start", Origin: "upstream", At: readStart, Start: start, End: start, HasRange: true})
		capture.event(captureEvent{Type: "read_return", Origin: "upstream", At: readReturned, Start: start, End: received, HasRange: true, Error: captureError(readErr)})
		entry.diagnostic.bytes(true, n)
		if readErr != nil && readErr != io.EOF {
			entry.diagnostic.failure("upstream_body_read", readErr, ctx, r.Context())
		}
		diagnostic.readReturned(readReturned, n)
		entry.profile.liveIOEnd()
		if n > 0 {
			data := (*buffer)[:n]
			observer.observedAt = readReturned
			observer.Feed(data)
			diagnostic.observe(readReturned, observer)
			entry.profile.liveObserve(observer)
			entry.generation.observeStream(observer, false)
			err := emit(data, "upstream", start, received, false)
			if writeFailed {
				entry.generation.writeFailure(ctx, r.Context())
				return "client_failure"
			}
			if err != nil {
				readErr = err
			}
		}
		if readErr == io.EOF && r.Context().Err() == nil {
			eof = true
			entry.generation.observeStream(observer, true)
		}
		if readErr == nil {
			continue
		}
		if r.Context().Err() != nil {
			return "client_cancelled"
		}
		if readErr == io.EOF {
			if observer != nil && !observer.valid() {
				entry.diagnostic.failure("upstream_incomplete_eof", readErr, ctx, r.Context())
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
		entry.generation.transportError(readErr, ctx, r.Context(), "response_read_error")
		code := "upstream_stream_error"
		if upstreamError(readErr, ctx).Code == "upstream_timeout" {
			code = "upstream_timeout"
		}
		entry.Outcome = code
		entry.profile.liveTerminal()
		if observer != nil && observer.Done {
			// The read failure is still recorded, but nothing may follow [DONE].
			return "backend_failure"
		}
		// Permit a bounded final error write after the generation deadline expires.
		_ = controller.SetWriteDeadline(time.Now().Add(time.Second))
		finalData := []byte(fmt.Sprintf("\n\ndata: %s\n\n", errorJSON(fault(502, code, "下游流中断"), entry.RequestID)))
		_ = emit(finalData, "gateway", 0, 0, true)
		return "backend_failure"
	}
}

func captureError(err error) string {
	if err == nil {
		return ""
	}
	// Diagnostic files must not copy arbitrary network errors (which may contain
	// upstream URLs or credentials). Preserve only a stable bounded classification.
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return "timeout"
	}
	return "io_error"
}
