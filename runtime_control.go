package main

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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RuntimeSnapshot struct {
	RouterID             string          `json:"router_id"`
	RouterVersion        uint64          `json:"router_version"`
	GatewayConfigVersion uint64          `json:"gateway_config_version"`
	BindingRevision      uint64          `json:"binding_revision"`
	Backend              *Backend        `json:"backend"`
	Settings             json.RawMessage `json:"settings"`
	APIKey               string          `json:"api_key,omitempty"`
	GatewayAddresses     []string        `json:"gateway_addresses,omitempty"`
}
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
	Sequence    uint64        `json:"command_seq"`
	OperationID string        `json:"operation_id"`
	Status      string        `json:"status"`
	Error       string        `json:"error,omitempty"`
	Report      *WarmupReport `json:"report,omitempty"`
	FinishedAt  *time.Time    `json:"finished_at,omitempty"`
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
	snapshot RuntimeSnapshot
	g        *Gateway
	counts   RuntimeCounts
	owner    string
	refs     int
	closed   bool
}
type runtimeTest struct {
	TaskID          string `json:"task_id"`
	Model           string `json:"model"`
	RouterID        string `json:"router_id"`
	BindingRevision uint64 `json:"binding_revision"`
	Inflight        int    `json:"inflight"`
	Releasing       bool   `json:"releasing"`
	hash            string
}
type ControlledGateway struct {
	incidents          *incidentStore
	mu                 sync.Mutex
	id, boot, key      string
	cidrs              []netip.Prefix
	audit              io.Writer
	active, candidate  *runtimeGeneration
	generations        []*runtimeGeneration
	admission, retired bool
	quiesceOwner       string
	dataRoot           string
	sequence           uint64
	results            map[uint64]*RuntimeResult
	test               *runtimeTest
	released           map[string]bool
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	retentionStop      chan struct{}
	retentionWake      chan struct{}
	retentionOnce      sync.Once
}
type runtimeRequestKey struct{}
type runtimeRequest struct {
	revision    uint64
	task, model string
	stream      func()
}

func newControlledGateway(id, key string, cidrs []string, audit io.Writer) (*ControlledGateway, error) {
	if !validOperationID(id) || len(key) < 32 || len(cidrs) == 0 {
		return nil, fmt.Errorf("gateway ID, control key (32+ bytes) and allowed CIDRs are required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &ControlledGateway{id: id, boot: randomID(), key: key, audit: audit, ctx: ctx, cancel: cancel, dataRoot: "/var/lib/aigate", results: map[uint64]*RuntimeResult{}, released: map[string]bool{}}
	for _, v := range cidrs {
		p, e := netip.ParsePrefix(v)
		if e != nil {
			cancel()
			return nil, e
		}
		c.cidrs = append(c.cidrs, p)
	}
	c.retentionStop = make(chan struct{})
	c.retentionWake = make(chan struct{}, 1)
	c.wg.Add(1)
	go c.retainObservations()
	return c, nil
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
	profileUsed, slowUsed := false, false
	for _, other := range c.generations {
		if other != g && !other.closed {
			profileUsed = profileUsed || other.g.profile == g.g.profile
			slowUsed = slowUsed || other.g.slow == g.g.slow
		}
	}
	if g.g.profile != nil && !profileUsed {
		g.g.profile.close()
	}
	if g.g.slow != nil && !slowUsed {
		g.g.slow.close()
	}
}
func (c *ControlledGateway) Close() {
	c.retentionOnce.Do(func() { close(c.retentionStop) })
	c.mu.Lock()
	c.admission = false
	c.retired = true
	c.cancel()
	c.mu.Unlock()
	c.wg.Wait()
	c.incidents.close()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active = nil
	c.candidate = nil
	for _, g := range c.generations {
		if !g.closed && g.refs == 0 {
			g.closed = true
			c.closeGenerationLocked(g)
		}
	}
}
func (c *ControlledGateway) statusLocked() map[string]any {
	var current any
	if c.active != nil {
		s := c.active.snapshot
		model := ""
		if s.Backend.PreferredModel != nil {
			model = *s.Backend.PreferredModel
		}
		current = map[string]any{"router_id": s.RouterID, "router_version": s.RouterVersion, "gateway_config_version": s.GatewayConfigVersion, "binding_revision": s.BindingRevision, "model": model}
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
	return map[string]any{"protocol_version": 2, "gateway_id": c.id, "boot_id": c.boot, "admission": c.admission, "retired": c.retired, "binding_revision": c.revisionLocked(), "last_command_seq": c.sequence, "current": current, "candidate": candidate, "counts": counts, "test_occupancy": c.test, "quiesce_owner": c.quiesceOwner}
}
func runtimeResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func runtimeError(w http.ResponseWriter, status int, code string) {
	runtimeResponse(w, status, map[string]any{"error": map[string]string{"code": code, "message": code}})
}
func (c *ControlledGateway) ControlHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		ip, e := netip.ParseAddr(host)
		allowed := false
		if e == nil {
			for _, p := range c.cidrs {
				allowed = allowed || p.Contains(ip.Unmap())
			}
		}
		if !allowed || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+c.key)) != 1 {
			runtimeError(w, 403, "unauthorized")
			return
		}
		if r.Method == "GET" && r.URL.Path == "/control/v2/status" {
			c.mu.Lock()
			defer c.mu.Unlock()
			runtimeResponse(w, 200, c.statusLocked())
			return
		}
		if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/control/v2/commands/") {
			n, e := strconv.ParseUint(strings.TrimPrefix(r.URL.Path, "/control/v2/commands/"), 10, 64)
			c.mu.Lock()
			defer c.mu.Unlock()
			if e != nil {
				runtimeError(w, 400, "invalid_sequence")
			} else if v := c.results[n]; v != nil {
				runtimeResponse(w, 200, v)
			} else {
				runtimeError(w, 404, "result_unavailable")
			}
			return
		}
		if r.Method != "POST" || r.URL.Path != "/control/v2/commands" {
			runtimeError(w, 404, "not_found")
			return
		}
		data, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		var cmd RuntimeCommand
		if e != nil || strictDecode(data, &cmd) != nil {
			runtimeError(w, 400, "invalid_request")
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if cmd.ProtocolVersion != 2 || cmd.GatewayID != c.id || !validOperationID(cmd.OperationID) || cmd.Sequence == 0 {
			runtimeError(w, 400, "invalid_command")
			return
		}
		if cmd.ExpectedBootID != c.boot {
			runtimeError(w, 409, "boot_changed")
			return
		}
		digest := runtimeDigest(cmd)
		if old := c.results[cmd.Sequence]; old != nil {
			if old.digest != digest {
				runtimeError(w, 409, "command_conflict")
			} else {
				runtimeResponse(w, 200, old)
			}
			return
		}
		if cmd.Sequence <= c.sequence {
			runtimeError(w, 409, "stale_command")
			return
		}
		if cmd.ExpectedRevision != c.revisionLocked() {
			runtimeError(w, 409, "binding_changed")
			return
		}
		if c.retired {
			runtimeError(w, 409, "instance_retired")
			return
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
		runtimeResponse(w, 200, result)
	})
}

func (c *ControlledGateway) ProfileHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		g := c.active
		if g == nil || g.g.profile == nil {
			c.mu.Unlock()
			runtimeError(w, 503, "profile_unavailable")
			return
		}
		g.refs++
		c.mu.Unlock()
		defer func() { c.mu.Lock(); defer c.mu.Unlock(); g.refs--; c.cleanupLocked() }()
		if r.URL.Path == "/profile/v1/stream-incidents" {
			if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+g.g.profile.c.APIKey)) != 1 {
				runtimeError(w, 401, "unauthorized")
				return
			}
			if g.g.incidents == nil {
				runtimeError(w, 503, "incident_storage_unavailable")
				return
			}
			g.g.incidents.serveHTTP(w, r)
			return
		}
		g.g.profile.handler().ServeHTTP(w, r)
		if r.URL.Path == "/metrics" && r.Method == http.MethodGet && g.g.incidents != nil &&
			subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+g.g.profile.c.APIKey)) == 1 {
			fmt.Fprintf(w, "aigate_stream_incident_dropped_total %d\naigate_stream_incident_write_errors_total %d\n", g.g.incidents.dropped.Load(), g.g.incidents.failures.Load())
		}
	})
}

func (c *ControlledGateway) prepareLocked(cmd RuntimeCommand) error {
	if c.test != nil {
		return fmt.Errorf("test_occupied")
	}
	if c.candidate != nil && c.candidate.owner != cmd.OperationID {
		return fmt.Errorf("candidate_owned")
	}
	var s RuntimeSnapshot
	if strictDecode(cmd.Payload, &s) != nil || s.Backend == nil || !validOperationID(s.RouterID) || s.BindingRevision <= c.revisionLocked() || s.RouterVersion == 0 || s.GatewayConfigVersion == 0 {
		return fmt.Errorf("invalid_snapshot")
	}
	if s.Backend.Adapter != "openai" || len(s.Backend.HeadersEnv) > 0 || len(s.Backend.HeaderPrefixes) > 0 {
		return fmt.Errorf("invalid_backend")
	}
	guard := ManagementHTTPConfig{Listen: "guard", GatewayURLs: s.GatewayAddresses}
	if e := guard.checkRouter(s.Backend); e != nil {
		return fmt.Errorf("gateway_target_forbidden")
	}
	var settings map[string]json.RawMessage
	if decodeJSON(s.Settings, &settings) != nil || settings == nil {
		return fmt.Errorf("invalid_settings")
	}
	// Historical snapshots may contain the retired alias. Ignore it on load.
	delete(settings, "public_model_name")
	s.Settings = encodeJSON(settings)
	allowedSettings := map[string]bool{"timeout_seconds": true, "discovery_timeout_seconds": true, "model_cache_seconds": true, "discovery_retry_seconds": true, "http_pool": true, "profile": true, "slow_requests": true}
	for key := range settings {
		if !allowedSettings[key] {
			return fmt.Errorf("unknown_setting")
		}
	}
	var typedSettings Config
	if strictDecode(s.Settings, &typedSettings) != nil {
		return fmt.Errorf("invalid_settings")
	}
	if typedSettings.Profile.Directory != "" || typedSettings.Profile.Listen != "" || typedSettings.Profile.APIKeyEnv != "" || typedSettings.SlowRequests.Directory != "" {
		return fmt.Errorf("bootstrap_setting_forbidden")
	}
	for _, key := range []string{"backends", "routing", "routing_file", "management", "gateway_api_key_env"} {
		if _, ok := settings[key]; ok {
			return fmt.Errorf("forbidden_setting")
		}
	}
	settings["backends"] = encodeJSON(map[string]*Backend{s.RouterID: s.Backend})
	settings["routing"] = encodeJSON(Policy{Mode: "single", Backend: s.RouterID})
	cfg, e := parseConfig(encodeJSON(settings), "/runtime/config.json")
	if e != nil {
		return fmt.Errorf("invalid_settings")
	}
	if cfg.Profile.Enabled && cfg.Profile.APIKey == "" {
		return fmt.Errorf("profile_key_required")
	}
	cfg.Profile.APIKeyEnv = ""
	// Observation paths are deployment-scoped. A route swap reuses observers;
	// actual observation-policy changes get independent generations.
	profileEnabled, slowEnabled := cfg.Profile.Enabled, cfg.SlowRequests.Enabled
	cfg.Profile.Directory = ""
	cfg.SlowRequests.Directory = ""
	profilePolicy := runtimeDigest(cfg.Profile)[:16]
	slowPolicy := runtimeDigest(cfg.SlowRequests)[:16]
	cfg.Profile.Directory = filepath.Join(c.dataRoot, "request-profile", profilePolicy)
	cfg.SlowRequests.Directory = filepath.Join(c.dataRoot, "slow-requests", slowPolicy)
	var sharedProfile *requestProfiler
	var sharedSlow *slowRequestStore
	for _, old := range c.generations {
		if old.closed {
			continue
		}
		if runtimeDigest(old.g.config.Profile) == runtimeDigest(cfg.Profile) {
			sharedProfile = old.g.profile
		}
		if runtimeDigest(old.g.config.SlowRequests) == runtimeDigest(cfg.SlowRequests) {
			sharedSlow = old.g.slow
		}
	}
	if sharedProfile != nil {
		cfg.Profile.Enabled = false
	}
	if sharedSlow != nil {
		cfg.SlowRequests.Enabled = false
	}
	// No environment mutation: secrets arrive in the authenticated snapshot.
	g, e := newGateway(cfg, c.audit)
	if e != nil {
		return fmt.Errorf("runtime_prepare_failed")
	}
	cfg.Profile.Enabled = profileEnabled
	cfg.SlowRequests.Enabled = slowEnabled
	if sharedProfile != nil {
		g.profile = sharedProfile
	}
	if sharedSlow != nil {
		g.slow = sharedSlow
	}
	if profileEnabled {
		if c.incidents == nil {
			var incidentErr error
			c.incidents, incidentErr = newIncidentStore(filepath.Join(c.dataRoot, "stream-incidents"), c.id, c.boot, c.audit)
			if incidentErr != nil {
				fmt.Fprintln(c.audit, `{"event":"stream_incident_storage_unavailable"}`)
			}
		}
		g.incidents = c.incidents
	}
	if s.APIKey != "" {
		g.apiKey = "Bearer " + s.APIKey
	}
	gen := &runtimeGeneration{snapshot: s, g: g, owner: cmd.OperationID, counts: RuntimeCounts{RouterID: s.RouterID, Revision: s.BindingRevision}}
	c.candidate = gen
	c.generations = append(c.generations, gen)
	return nil
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
		return c.prepareLocked(cmd)
	case "validate":
		g := c.candidate
		if g == nil || g.owner != cmd.OperationID {
			return fmt.Errorf("candidate_missing")
		}
		var policy struct {
			Rounds         int     `json:"rounds"`
			StepSeconds    float64 `json:"step_seconds"`
			TimeoutSeconds float64 `json:"timeout_seconds"`
			MaxTokens      int     `json:"max_tokens"`
		}
		if len(cmd.Payload) > 0 && string(cmd.Payload) != "{}" && strictDecode(cmd.Payload, &policy) != nil {
			return fmt.Errorf("invalid_validation_policy")
		}
		if policy.Rounds == 0 {
			policy.Rounds = 2
		}
		if policy.StepSeconds == 0 {
			policy.StepSeconds = 120
		}
		if policy.TimeoutSeconds == 0 {
			policy.TimeoutSeconds = 900
		}
		if policy.MaxTokens == 0 {
			policy.MaxTokens = 1024
		}
		if policy.Rounds < 1 || policy.Rounds > 20 || policy.StepSeconds <= 0 || policy.TimeoutSeconds <= 0 || policy.MaxTokens < 1 {
			return fmt.Errorf("invalid_validation_policy")
		}
		g.refs++
		g.counts.Probes++
		result.Status = "running"
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			report, e := validateControlledRuntime(c.ctx, g.g, ManagementConfig{WarmupRounds: policy.Rounds, WarmupStepSeconds: policy.StepSeconds, WarmupTimeoutSeconds: policy.TimeoutSeconds, WarmupMaxTokens: policy.MaxTokens})
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
		select {
		case c.retentionWake <- struct{}{}:
		default:
		}
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
	case "test-acquire", "test-release", "test-status":
		return c.testCommandLocked(cmd, result)
	default:
		return fmt.Errorf("unsupported_command")
	}
	return nil
}
func (c *ControlledGateway) testCommandLocked(cmd RuntimeCommand, result *RuntimeResult) error {
	var p struct {
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
	c.test = &runtimeTest{TaskID: p.TaskID, Model: p.Model, RouterID: p.RouterID, BindingRevision: p.BindingRevision, hash: hash}
	return nil
}

func (c *ControlledGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	c.mu.Lock()
	g := c.active
	if g == nil || !c.admission || c.retired {
		c.mu.Unlock()
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
	meta := &runtimeRequest{revision: g.snapshot.BindingRevision}
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
	w.Header().Set("X-Aigate-Backend", g.snapshot.RouterID)
	w.Header().Set("X-Aigate-Route-Version", strconv.FormatUint(g.snapshot.BindingRevision, 10))
	g.g.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), runtimeRequestKey{}, meta)))
}

// Reuses the proven protocol probes with an isolated in-memory accounting object.
// It never invokes newManagement, its file lock, state reader, or state writer.
func validateControlledRuntime(ctx context.Context, g *Gateway, policy ManagementConfig) (*WarmupReport, error) {
	cfg := *g.config
	cfg.Management = policy
	disabled := false
	cfg.Management.AutoRollback = &disabled
	copyGateway := &Gateway{config: &cfg, pool: g.pool, client: g.client, transport: g.transport, audit: g.audit, now: g.now}
	name := cfg.Routing.Backend
	m := &Management{g: copyGateway, ctx: ctx, counts: map[string]*BackendCounters{name: {}}, state: ManagedState{ActiveBackend: name, Backends: map[string]*BackendState{name: {}}, Registry: registryFromConfig(&cfg)}}
	m.persist = func(ManagedState) error { return nil }
	return m.warmup(ctx, name, false, func(*WarmupReport) error { return nil })
}
