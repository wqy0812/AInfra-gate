package aigate

// Platform-controlled runtime. Configuration and control state never leave memory.
import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RuntimeSnapshot contains only the immutable target binding of a switch.
type RuntimeSnapshot = SwitchSnapshot
type RuntimeCommand struct {
	ProtocolVersion  int             `json:"protocol_version"`
	OperationID      string          `json:"operation_id"`
	GatewayID        string          `json:"gateway_id"`
	ExpectedBootID   string          `json:"expected_boot_id"`
	Sequence         uint64          `json:"command_seq"`
	ExpectedRevision uint64          `json:"expected_binding_revision"`
	Action           string          `json:"action"`
	Payload          json.RawMessage `json:"payload"`
}
type RuntimeResult struct {
	Sequence    uint64            `json:"command_seq"`
	OperationID string            `json:"operation_id"`
	Status      string            `json:"status"`
	Error       string            `json:"error,omitempty"`
	Report      *WarmupReport     `json:"report,omitempty"`
	Recording   *recordingSummary `json:"recording,omitempty"`
	FinishedAt  *time.Time        `json:"finished_at,omitempty"`
	digest      string
}
type RuntimeCounts struct {
	RouterID string `json:"router_id"`
	Revision uint64 `json:"binding_revision"`
	Business int    `json:"business"`
	Streams  int    `json:"streams"`
	Tests    int    `json:"tests"`
	Probes   int    `json:"probes"`
}
type runtimeGeneration struct {
	forward  *switchForwarder
	snapshot RuntimeSnapshot
	g        *Gateway
	counts   RuntimeCounts
	owner    string
	refs     int
	closed   bool
}
type runtimeTest struct {
	ConfigDigest    string `json:"config_digest,omitempty"`
	TaskID          string `json:"task_id"`
	Model           string `json:"model"`
	RouterID        string `json:"router_id"`
	BindingRevision uint64 `json:"binding_revision"`
	Inflight        int    `json:"inflight"`
	Releasing       bool   `json:"releasing"`
	hash            string
}

// Lock order is cmdMu, then mu. Commands serialize independently from business
// admission; switching never opens or owns observation storage.
type ControlledGateway struct {
	forwardTimeout     time.Duration
	cmdMu              sync.Mutex
	mu                 sync.Mutex
	id, boot, key      string
	cidrs              []netip.Prefix
	audit              io.Writer
	connections        *connectionDiagnostics
	active, candidate  *runtimeGeneration
	generations        []*runtimeGeneration
	admission, retired bool
	quiesceOwner       string
	sequence           uint64
	results            map[uint64]*RuntimeResult
	test               *runtimeTest
	released           map[string]bool
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	recorder           *trafficRecorder // nil unless --record-root is configured
}
type runtimeRequestKey struct{}
type runtimeRequest struct {
	revision    uint64
	task, model string
	stream      func()
	id          string
	record      *trafficRecord
	traffic     string // "replay" for verified replay requests on a proxy
}

func newRuntimeGateway(id, key string, cidrs []string, audit io.Writer) (*ControlledGateway, error) {
	if !validOperationID(id) || len(key) < 32 || len(cidrs) == 0 {
		return nil, fmt.Errorf("gateway ID, control key (32+ bytes) and allowed CIDRs are required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &ControlledGateway{forwardTimeout: 62 * time.Minute, id: id, boot: randomID(), key: key, audit: audit, ctx: ctx, cancel: cancel, results: map[uint64]*RuntimeResult{}, released: map[string]bool{}}
	for _, v := range cidrs {
		p, e := netip.ParsePrefix(v)
		if e != nil {
			cancel()
			return nil, e
		}
		c.cidrs = append(c.cidrs, p)
	}
	c.connections = newConnectionDiagnostics(audit, c.boot)
	return c, nil
}
func testHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}
func runtimeDigest(v any) string { s := sha256.Sum256(encodeJSON(v)); return hex.EncodeToString(s[:]) }
func (c *ControlledGateway) revisionLocked() uint64 {
	if c.active != nil {
		return c.active.snapshot.BindingRevision
	}
	return 0
}
func (c *ControlledGateway) cleanupLocked() {
	for _, g := range c.generations {
		if g != c.active && g != c.candidate && g.refs == 0 && !g.closed {
			g.closed = true
			c.closeGenerationLocked(g)
		}
	}
	live := c.generations[:0]
	for _, g := range c.generations {
		if !g.closed {
			live = append(live, g)
		}
	}
	for i := len(live); i < len(c.generations); i++ {
		c.generations[i] = nil
	}
	c.generations = live
}
func (c *ControlledGateway) closeGenerationLocked(g *runtimeGeneration) {
	g.g.transport.CloseIdleConnections()
}

func (c *ControlledGateway) Close() {
	defer c.connections.close()
	c.mu.Lock()
	c.admission = false
	c.retired = true
	c.cancel()
	c.mu.Unlock()
	c.wg.Wait()
	// Recording completion callbacks take c.mu, so close it before locking.
	c.recorder.close("process_exit")
	// An in-flight command finishes first; later ones see the retired instance.
	c.cmdMu.Lock()
	defer c.cmdMu.Unlock()
	c.mu.Lock()
	c.active = nil
	c.candidate = nil
	for _, g := range c.generations {
		if !g.closed && g.refs == 0 {
			g.closed = true
			c.closeGenerationLocked(g)
		}
	}
	c.mu.Unlock()
}
func (c *ControlledGateway) statusLocked() map[string]any {
	var current any
	if c.active != nil {
		s := c.active.snapshot
		current = map[string]any{"router_id": s.RouterID, "router_version": s.RouterVersion,
			"gateway_config_version": s.GatewayConfigVersion, "binding_revision": s.BindingRevision,
			"model": "", "target_url": s.TargetURL, "config_digest": s.ConfigDigest}
	}
	counts := []RuntimeCounts{}
	for _, g := range c.generations {
		if g == c.active || g.refs > 0 {
			counts = append(counts, g.counts)
		}
	}
	var candidate any
	if c.candidate != nil {
		candidate = map[string]any{"operation_id": c.candidate.owner, "binding_revision": c.candidate.snapshot.BindingRevision, "router_id": c.candidate.snapshot.RouterID}
	}
	return map[string]any{"protocol_version": 2, "gateway_id": c.id, "boot_id": c.boot, "admission": c.admission, "retired": c.retired, "binding_revision": c.revisionLocked(), "last_command_seq": c.sequence, "current": current, "candidate": candidate, "counts": counts, "test_occupancy": c.test, "quiesce_owner": c.quiesceOwner, "business_read_header_timeout_seconds": businessReadHeaderTimeout.Seconds(), "recording": c.recorder.status(time.Now())}
}
func runtimeResponse(w http.ResponseWriter, status int, value any) {
	newRuntimeResponse(status, value).write(w)
}

// Encode while state is locked, then write the detached bytes after unlocking.
// Results and test occupancy may otherwise change during network backpressure.
type runtimeResponseData struct {
	status int
	body   []byte
}

func newRuntimeResponse(status int, value any) runtimeResponseData {
	return runtimeResponseData{status, append(encodeJSON(value), '\n')}
}

func newRuntimeError(status int, code string) runtimeResponseData {
	return newRuntimeResponse(status, map[string]any{"error": map[string]string{"code": code, "message": code}})
}

func (response runtimeResponseData) write(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(response.status)
	_, _ = w.Write(response.body)
}
func runtimeError(w http.ResponseWriter, status int, code string) {
	newRuntimeError(status, code).write(w)
}

// controlAuthorized checks the actual TCP peer against the allowed CIDRs and
// the bearer key. Forwarded-address headers are never trusted.
func controlAuthorized(r *http.Request, cidrs []netip.Prefix, key string) bool {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	ip, err := netip.ParseAddr(host)
	allowed := false
	if err == nil {
		for _, p := range cidrs {
			allowed = allowed || p.Contains(ip.Unmap())
		}
	}
	return allowed && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+key)) == 1
}

func parseControlCIDRs(values []string) ([]netip.Prefix, error) {
	out := []netip.Prefix{}
	for _, v := range values {
		p, err := netip.ParsePrefix(strings.TrimSpace(v))
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

func (c *ControlledGateway) ControlHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		version := 2
		path := r.URL.Path
		if strings.HasPrefix(path, "/control/v3/") {
			version = 3
			path = strings.Replace(path, "/control/v3/", "/control/v2/", 1)
		}
		if !controlAuthorized(r, c.cidrs, c.key) {
			runtimeError(w, 403, "unauthorized")
			return
		}
		if r.Method == "GET" && path == "/control/v2/status" {
			c.mu.Lock()
			status := c.statusLocked()
			status["protocol_version"] = version
			status["service"] = "aigate-switch"
			if version == 2 {
				// Fixed execution images interpret this list as historical
				// snapshots still draining on another entry. Current target
				// business traffic may be shared and must not block them.
				counts := []RuntimeCounts{}
				for _, generation := range c.generations {
					if generation != c.active && generation.refs > 0 {
						counts = append(counts, generation.counts)
					}
				}
				status["counts"] = counts
				status["counts_scope"] = "retiring_snapshots"
			}
			response := newRuntimeResponse(200, status)
			c.mu.Unlock()
			response.write(w)
			return
		}
		if r.Method == "GET" && strings.HasPrefix(path, "/control/v2/commands/") {
			n, e := strconv.ParseUint(strings.TrimPrefix(path, "/control/v2/commands/"), 10, 64)
			c.mu.Lock()
			var response runtimeResponseData
			if e != nil {
				response = newRuntimeError(400, "invalid_sequence")
			} else if v := c.results[n]; v != nil {
				response = newRuntimeResponse(200, v)
			} else {
				response = newRuntimeError(404, "result_unavailable")
			}
			c.mu.Unlock()
			response.write(w)
			return
		}
		if r.Method != "POST" || path != "/control/v2/commands" {
			runtimeError(w, 404, "not_found")
			return
		}
		data, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		var cmd RuntimeCommand
		if e != nil || strictDecode(data, &cmd) != nil {
			runtimeError(w, 400, "invalid_request")
			return
		}
		if cmd.ProtocolVersion != version {
			runtimeError(w, 400, "protocol_mismatch")
			return
		}
		if version == 2 && cmd.Action != "test-acquire" && cmd.Action != "test-release" && cmd.Action != "test-status" {
			runtimeError(w, 400, "legacy_command_removed")
			return
		}
		c.controlCommand(cmd).write(w)
	})
}

func (c *ControlledGateway) controlCommand(cmd RuntimeCommand) runtimeResponseData {
	c.cmdMu.Lock()
	defer c.cmdMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if (cmd.ProtocolVersion != 2 && cmd.ProtocolVersion != 3) || cmd.GatewayID != c.id || !validOperationID(cmd.OperationID) || cmd.Sequence == 0 {
		return newRuntimeError(400, "invalid_command")
	}
	if cmd.ExpectedBootID != c.boot {
		return newRuntimeError(409, "boot_changed")
	}
	digest := runtimeDigest(cmd)
	if old := c.results[cmd.Sequence]; old != nil {
		if old.digest != digest {
			return newRuntimeError(409, "command_conflict")
		}
		return newRuntimeResponse(200, old)
	}
	if cmd.Sequence <= c.sequence {
		return newRuntimeError(409, "stale_command")
	}
	if cmd.ExpectedRevision != c.revisionLocked() {
		return newRuntimeError(409, "binding_changed")
	}
	if c.retired {
		return newRuntimeError(409, "instance_retired")
	}
	c.sequence = cmd.Sequence
	result := &RuntimeResult{Sequence: cmd.Sequence, OperationID: cmd.OperationID, Status: "completed", digest: digest}
	c.results[cmd.Sequence] = result
	if err := c.executeLocked(cmd, result); err != nil {
		result.Status = "failed"
		result.Error = err.Error()
	}
	if result.Status != "running" {
		now := time.Now()
		result.FinishedAt = &now
	}
	for n, v := range c.results {
		if n+256 <= c.sequence && v.Status != "running" {
			delete(c.results, n)
		}
	}
	c.cleanupLocked()
	return newRuntimeResponse(200, result)
}

func (c *ControlledGateway) executeLocked(cmd RuntimeCommand, result *RuntimeResult) error {
	switch cmd.Action {
	case "quiesce", "resume", "discard", "retire":
		if strictDecode(cmd.Payload, &struct{}{}) != nil {
			return fmt.Errorf("invalid_payload")
		}
	}
	switch cmd.Action {
	case "prepare":
		return c.prepareSwitchLocked(cmd)
	case "validate":
		g := c.candidate
		if g == nil || g.owner != cmd.OperationID {
			return fmt.Errorf("candidate_missing")
		}
		var policy WarmupPolicy
		if len(cmd.Payload) > 0 && string(cmd.Payload) != "{}" && strictDecode(cmd.Payload, &policy) != nil {
			return fmt.Errorf("invalid_validation_policy")
		}
		policy.defaults()
		if policy.validate() != nil {
			return fmt.Errorf("invalid_validation_policy")
		}
		g.refs++
		g.counts.Probes++
		result.Status = "running"
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			report, e := validateSwitchTarget(c.ctx, g, policy)
			c.mu.Lock()
			defer c.mu.Unlock()
			g.refs--
			g.counts.Probes--
			result.Report = report
			now := time.Now()
			result.FinishedAt = &now
			if c.candidate != g || c.sequence != cmd.Sequence || c.retired {
				result.Status = "failed"
				result.Error = "validation_superseded"
			} else if e != nil {
				result.Status = "failed"
				result.Error = "validation_failed"
			} else {
				result.Status = "completed"
			}
			c.cleanupLocked()
		}()
		return nil
	case "quiesce":
		if c.test != nil {
			return fmt.Errorf("test_occupied")
		}
		c.admission = false
		c.quiesceOwner = cmd.OperationID
	case "activate":
		var p struct {
			Admission bool `json:"admission"`
		}
		if strictDecode(cmd.Payload, &p) != nil {
			return fmt.Errorf("invalid_activation")
		}
		if c.admission || c.test != nil {
			return fmt.Errorf("not_quiesced")
		}
		if c.active != nil && c.quiesceOwner != cmd.OperationID {
			return fmt.Errorf("quiesce_owner_changed")
		}
		if c.candidate == nil || c.candidate.owner != cmd.OperationID {
			return fmt.Errorf("candidate_missing")
		}
		c.active = c.candidate
		c.candidate = nil
		c.admission = p.Admission
		c.quiesceOwner = ""

	case "resume":
		if c.active == nil || c.quiesceOwner != cmd.OperationID {
			return fmt.Errorf("resume_not_owned")
		}
		c.admission = true
		c.quiesceOwner = ""
	case "discard":
		if c.candidate != nil && c.candidate.owner != cmd.OperationID {
			return fmt.Errorf("candidate_owned")
		}
		c.candidate = nil
	case "retire":
		if c.test != nil {
			return fmt.Errorf("test_occupied")
		}
		c.admission = false
		c.retired = true
		c.cancel()
		c.recorder.endAll("retired")
	case "test-acquire", "test-release", "test-status":
		return c.testCommandLocked(cmd, result)
	case "record-start", "record-stop", "record-status", "record-pin", "record-unpin":
		return c.recordCommandLocked(cmd, result)
	default:
		return fmt.Errorf("unsupported_command")
	}
	return nil
}
func (c *ControlledGateway) testCommandLocked(cmd RuntimeCommand, result *RuntimeResult) error {
	var p struct {
		ConfigDigest    string `json:"config_digest,omitempty"`
		TaskID          string `json:"task_id"`
		Token           string `json:"token"`
		Model           string `json:"model,omitempty"`
		RouterID        string `json:"router_id,omitempty"`
		BindingRevision uint64 `json:"binding_revision,omitempty"`
	}
	if strictDecode(cmd.Payload, &p) != nil || !validOperationID(p.TaskID) || len(p.Token) < 32 || len(p.Token) > 256 {
		return fmt.Errorf("invalid_test_identity")
	}
	hash := testHash(p.Token)
	same := c.test != nil && c.test.hash == hash && c.test.TaskID == p.TaskID
	if cmd.Action == "test-status" {
		if c.test != nil && !same {
			return fmt.Errorf("test_occupied")
		}
		return nil
	}
	if cmd.Action == "test-release" {
		c.released[hash] = true
		if same {
			c.test.Releasing = true
			if c.test.Inflight == 0 {
				c.test = nil
			}
		}
		return nil
	}
	if c.released[hash] {
		return fmt.Errorf("test_already_released")
	}
	if same && !c.test.Releasing {
		return nil
	}
	if c.test != nil {
		return fmt.Errorf("test_occupied")
	}
	if !c.admission || c.active == nil || c.candidate != nil || p.RouterID != c.active.snapshot.RouterID || p.BindingRevision != c.revisionLocked() || p.Model == "" {
		return fmt.Errorf("test_target_changed")
	}
	if p.ConfigDigest == "" {
		p.ConfigDigest = c.active.snapshot.ConfigDigest
	}
	if p.ConfigDigest != c.active.snapshot.ConfigDigest {
		return fmt.Errorf("test_config_changed")
	}
	c.test = &runtimeTest{TaskID: p.TaskID, Model: p.Model, RouterID: p.RouterID, BindingRevision: p.BindingRevision, hash: hash, ConfigDigest: p.ConfigDigest}
	return nil
}

func (c *ControlledGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/chat/completions" && r.URL.Path != "/v1/models" {
		abandonRequestBody(w, r)
	}
	if r.URL.Path == "/health" {
		runtimeResponse(w, 200, map[string]string{"status": "ok"})
		return
	}
	if r.URL.Path == "/openapi.json" {
		serveOpenAPI(w)
		return
	}
	if r.URL.Path != "/v1/chat/completions" && r.URL.Path != "/v1/models" {
		runtimeError(w, 404, "not_found")
		return
	}
	id := randomID()
	// Sampling happens before admission so entry rejections are recorded too.
	record := c.recorder.begin(r, id, time.Now())
	if record != nil {
		w = &recordingWriter{ResponseWriter: w, record: record}
		defer func() { record.finish(r.Context().Err()) }()
	}
	c.mu.Lock()
	g := c.active
	if g == nil || !c.admission || c.retired {
		c.mu.Unlock()
		abandonRequestBody(w, r)
		w.Header().Set("Retry-After", "1")
		runtimeError(w, 503, "gateway_not_accepting")
		return
	}
	token := r.Header.Get("X-Code-Eval-Token")
	var lease *runtimeTest
	if token != "" {
		lease = c.test
		if lease == nil || lease.Releasing || subtle.ConstantTimeCompare([]byte(lease.hash), []byte(testHash(token))) != 1 {
			c.mu.Unlock()
			abandonRequestBody(w, r)
			runtimeError(w, 409, "test_target_changed")
			return
		}
		lease.Inflight++
		g.counts.Tests++
	}
	isModel := r.URL.Path == "/v1/models"
	g.refs++
	if isModel {
		g.counts.Probes++
	} else {
		g.counts.Business++
	}
	stream := false
	meta := &runtimeRequest{revision: g.snapshot.BindingRevision, id: id, record: record}
	record.route(recordingRouter{RouterID: g.snapshot.RouterID, BindingRevision: g.snapshot.BindingRevision, ConfigDigest: g.snapshot.ConfigDigest})
	if lease != nil {
		meta.task = lease.TaskID
		meta.model = lease.Model
	}
	meta.stream = func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !stream {
			stream = true
			g.counts.Streams++
		}
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		g.refs--
		if isModel {
			g.counts.Probes--
		} else {
			g.counts.Business--
		}
		if stream {
			g.counts.Streams--
		}
		if lease != nil {
			lease.Inflight--
			g.counts.Tests--
			if lease.Releasing && lease.Inflight == 0 && c.test == lease {
				c.test = nil
			}
		}
		c.cleanupLocked()
	}()
	g.forward.serve(w, r, meta, lease)
}

func (c *ControlledGateway) enableRecording(root string, maxBytes int64, markKey []byte) error {
	r, err := newTrafficRecorder(root, c.id, c.boot, maxBytes, markKey, c.audit)
	if err != nil {
		return err
	}
	c.recorder = r
	return nil
}

// recordCommandLocked handles record-* actions. They never change admission,
// bindings or test occupancy; disk work completes asynchronously.
func (c *ControlledGateway) recordCommandLocked(cmd RuntimeCommand, result *RuntimeResult) error {
	if c.recorder == nil {
		return fmt.Errorf("recording_disabled")
	}
	now := time.Now()
	complete := func(err error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		at := time.Now()
		result.FinishedAt = &at
		if err != nil {
			result.Status, result.Error = "failed", err.Error()
		} else {
			result.Status = "completed"
		}
	}
	switch cmd.Action {
	case "record-start":
		var p recordingParams
		if strictDecode(cmd.Payload, &p) != nil {
			return fmt.Errorf("invalid_payload")
		}
		if err := p.normalize(c.recorder.maxBytes, now); err != nil {
			return err
		}
		if err := c.recorder.startLocked(p, now, complete); err != nil {
			return err
		}
		result.Status = "running"
		return nil
	case "record-pin", "record-unpin":
		var p struct {
			RecordingID string `json:"recording_id"`
			Segment     string `json:"segment"`
		}
		if strictDecode(cmd.Payload, &p) != nil {
			return fmt.Errorf("invalid_payload")
		}
		if err := c.recorder.pinLocked(p.RecordingID, p.Segment, cmd.Action == "record-pin", complete); err != nil {
			return err
		}
		result.Status = "running"
		return nil
	}
	var p struct {
		RecordingID string `json:"recording_id"`
	}
	if strictDecode(cmd.Payload, &p) != nil || !validOperationID(p.RecordingID) {
		return fmt.Errorf("invalid_payload")
	}
	if cmd.Action == "record-stop" {
		if err := c.recorder.stopLocked(p.RecordingID, now); err != nil {
			return err
		}
	}
	summary, err := c.recorder.statusOf(p.RecordingID, now)
	if err != nil {
		return err
	}
	result.Recording = summary
	return nil
}
