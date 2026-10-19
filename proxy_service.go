package aigate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	headerRoute          = "X-Aigate-Route-Version"
	headerTask           = "X-Aigate-Test-Task"
	headerExpectedModel  = "X-Aigate-Expected-Model"
	headerExpectedConfig = "X-Aigate-Expected-Config"
	headerRouter         = "X-Aigate-Expected-Router"
)

// ProxyConfig is local, immutable for the lifetime of a proxy process.
// Listener addresses and disk paths belong to deployment flags.
type ProxyConfig struct {
	ServiceID string          `json:"service_id"`
	RouterID  string          `json:"router_id"`
	Backend   *Backend        `json:"backend"`
	Settings  json.RawMessage `json:"settings"`
}

type ProxyGateway struct {
	g                                  *Gateway
	id, router, boot, digest, dataRoot string
	settings                           RuntimeSettings
	backend                            Backend
	connections                        *connectionDiagnostics
	mu                                 sync.Mutex
	counts                             RuntimeCounts
	admission                          bool
	stop                               chan struct{}
	done                               chan struct{}
	once                               sync.Once
	closeDone                          chan struct{}
	markKeys                           [][]byte // trusted replay mark keys; empty disables labelling
}

func readProxyConfig(path string) (ProxyConfig, error) {
	var cfg ProxyConfig
	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return cfg, err
	}
	if len(data) > 1<<20 {
		return cfg, fmt.Errorf("proxy config exceeds 1 MiB")
	}
	if err = strictDecode(data, &cfg); err != nil {
		return cfg, fmt.Errorf("invalid proxy config: %w", err)
	}
	return cfg, nil
}

func newProxyGateway(cfg ProxyConfig, dataRoot string, audit io.Writer) (*ProxyGateway, error) {
	return prepareProxyGateway(cfg, dataRoot, audit, false)
}

func prepareProxyGateway(cfg ProxyConfig, dataRoot string, audit io.Writer, checkOnly bool) (*ProxyGateway, error) {
	if !validOperationID(cfg.ServiceID) || !validOperationID(cfg.RouterID) || cfg.Backend == nil {
		return nil, fmt.Errorf("service_id, router_id and backend are required")
	}
	if len(cfg.Settings) == 0 {
		cfg.Settings = json.RawMessage(`{}`)
	}
	var raw map[string]json.RawMessage
	if strictDecode(cfg.Settings, &raw) != nil || raw == nil {
		return nil, fmt.Errorf("settings must be an object")
	}
	// Reject retired credentials and deployment paths, including empty values.
	for section, fields := range map[string][]string{"profile": {"api_key", "api_key_env", "directory", "listen"}, "slow_requests": {"directory"}} {
		if body, ok := raw[section]; ok {
			var members map[string]json.RawMessage
			if strictDecode(body, &members) != nil || members == nil {
				return nil, fmt.Errorf("invalid %s", section)
			}
			for _, key := range fields {
				if _, ok := members[key]; ok {
					return nil, fmt.Errorf("%s.%s is not a proxy setting", section, key)
				}
			}
		}
	}
	settings, err := parseRuntimeSettings(cfg.Settings)
	if err != nil {
		return nil, err
	}
	settings.Profile.noAuth = true
	settings.Profile.APIKeyEnv = ""
	settings.Profile.APIKey = ""
	settings.Profile.Directory = ""
	settings.Profile.Listen = "127.0.0.1:18082"
	settings.SlowRequests.Directory = ""
	// Hash the complete normalized local policy, before deployment paths are added.
	policy := settings
	settings.Profile.Directory = filepath.Join(dataRoot, "request-profile", runtimeDigest(policy.Profile)[:16])
	settings.SlowRequests.Directory = filepath.Join(dataRoot, "slow-requests", runtimeDigest(policy.SlowRequests)[:16])
	runtime := &RuntimeConfig{RuntimeSettings: settings, RouterID: cfg.RouterID, Backend: cfg.Backend}
	if err := runtime.validate(); err != nil {
		return nil, err
	}
	p := &ProxyGateway{id: cfg.ServiceID, router: cfg.RouterID, boot: randomID(), dataRoot: dataRoot, counts: RuntimeCounts{RouterID: cfg.RouterID},
		settings: policy, backend: *runtime.Backend, admission: true, stop: make(chan struct{}), done: make(chan struct{}), closeDone: make(chan struct{})}
	// Resolved upstream headers participate in identity without ever being exposed.
	p.digest = runtimeDigest(map[string]any{"service_id": p.id, "router_id": p.router, "settings": policy, "backend": p.backend, "resolved_headers": p.backend.headers})
	if checkOnly {
		p.boot = ""
		p.admission = false
		return p, nil
	}
	g, err := newGateway(runtime, audit)
	if err != nil {
		return nil, err
	}
	p.g = g
	p.connections = newConnectionDiagnostics(audit, p.boot)
	if settings.Profile.Enabled {
		g.incidents, err = newIncidentStore(filepath.Join(dataRoot, "stream-incidents"), p.id, p.boot, audit)
		if err != nil {
			g.audit.Print(`{"event":"stream_incident_storage_unavailable"}`)
		}
		// Detail capture has its own queue, lock and disk budget. Failure must not
		// disable the existing summary store or refuse business admission.
		g.captures, err = newStreamCaptureStore(filepath.Join(dataRoot, "stream-incidents", "requests"), p.id, p.router, p.boot, g.audit, settings.StreamCaptureMaxDiskBytes)
		if err != nil {
			g.audit.Print(`{"event":"stream_capture_storage_unavailable"}`)
		}
	}
	go p.retain()
	return p, nil
}

func (p *ProxyGateway) retain() {
	defer close(p.done)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var lastSlow time.Time
	for {
		protected := map[string]bool{}
		if p.g.profile != nil {
			protected[p.g.profile.c.Directory] = true
		}
		if p.g.slow != nil {
			protected[p.g.slow.c.Directory] = true
		}
		for _, item := range []struct {
			dir   string
			slow  bool
			hours int
			bytes int64
		}{
			{"request-profile", false, p.settings.Profile.RetentionHours, p.settings.Profile.MaxDiskBytes},
		} {
			if err := cleanObservationRoot(filepath.Join(p.dataRoot, item.dir), item.slow, time.Now(), item.hours, item.bytes, protected); err != nil {
				p.g.audit.Print(string(encodeJSON(map[string]any{"event": "observation_retention_error", "error": err.Error()})))
			}
		}
		if p.g.slow == nil && time.Since(lastSlow) >= archiveInterval {
			if err := maintainSlowArchives(filepath.Join(p.dataRoot, "slow-requests"), time.Now(), p.settings.SlowRequests.MaxDiskBytes); err != nil {
				p.g.audit.Printf("slow_archive_failed: %v", err)
			}
			lastSlow = time.Now()
		}
		select {
		case <-p.stop:
			return
		case <-ticker.C:
		}
	}
}

func (p *ProxyGateway) quiesce() { p.mu.Lock(); p.admission = false; p.mu.Unlock() }
func (p *ProxyGateway) Close() {
	p.once.Do(func() {
		p.quiesce()
		close(p.stop)
		// Fixed service-level workers, not per-request goroutines. Start every
		// close before joining so a stuck disk cannot prevent others from stopping.
		go func() {
			var group sync.WaitGroup
			for _, closeFn := range []func(){func() { <-p.done }, p.g.incidents.close, p.g.captures.close, p.g.Close, p.connections.close} {
				group.Add(1)
				go func(f func()) { defer group.Done(); f() }(closeFn)
			}
			group.Wait()
			close(p.closeDone)
		}()
	})
	select {
	case <-p.closeDone:
	case <-time.After(5 * time.Second):
		// Workers retain their file handles and directory locks until they really
		// stop (or the process exits). Never wait on the same stalled disk here.
	}
}

func (p *ProxyGateway) status() map[string]any {
	// Build a detached, redacted view; never expose headers or environment names.
	var settings, backend map[string]any
	_ = json.Unmarshal(encodeJSON(p.settings), &settings)
	_ = json.Unmarshal(encodeJSON(p.backend), &backend)
	for _, key := range []string{"headers", "headers_env", "header_prefixes"} {
		delete(backend, key)
	}
	for _, key := range []string{"url", "models_url"} {
		if raw, ok := backend[key].(string); ok {
			if u, err := url.Parse(raw); err == nil {
				u.RawQuery = ""
				u.ForceQuery = false
				backend[key] = u.String()
			}
		}
	}
	if profile, ok := settings["profile"].(map[string]any); ok {
		for _, key := range []string{"api_key", "api_key_env", "directory", "listen"} {
			delete(profile, key)
		}
	}
	if slow, ok := settings["slow_requests"].(map[string]any); ok {
		delete(slow, "directory")
	}
	p.mu.Lock()
	counts, admission := p.counts, p.admission
	p.mu.Unlock()
	return map[string]any{"service": "aigate-proxy", "service_id": p.id, "router_id": p.router, "boot_id": p.boot,
		"config_digest": p.digest, "settings": settings, "backend": backend, "counts": counts, "admission": admission}
}

func (p *ProxyGateway) ObservationHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The same API is available on the standalone business listener, so it
		// carries its own short I/O bounds rather than relying on server settings.
		controller := http.NewResponseController(w)
		_ = controller.SetReadDeadline(time.Now().Add(10 * time.Second))
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		defer controller.SetWriteDeadline(time.Time{})
		defer controller.SetReadDeadline(time.Time{})
		if r.Method != http.MethodPost || r.URL.Path != "/profile/v1/analyses" {
			abandonRequestBody(w, r)
		}
		if r.URL.Path == "/health" && r.Method == http.MethodGet {
			runtimeResponse(w, 200, map[string]string{"status": "ok", "service": "aigate-proxy"})
			return
		}
		if r.URL.Path == "/runtime/v1/status" && r.Method == http.MethodGet {
			runtimeResponse(w, 200, p.status())
			return
		}
		if p.g.profile == nil {
			runtimeError(w, 503, "profile_unavailable")
			return
		}
		if strings.HasPrefix(r.URL.Path, "/profile/v1/stream-incidents/requests/") {
			if p.g.captures == nil {
				runtimeError(w, 503, "capture_unavailable")
				return
			}
			p.g.captures.serveEvidence(w, r)
			return
		}
		if r.URL.Path == "/profile/v1/stream-incidents" {
			if p.g.incidents == nil {
				runtimeError(w, 503, "incident_storage_unavailable")
				return
			}
			p.g.incidents.serveHTTP(w, r)
			return
		}
		p.g.profile.handler().ServeHTTP(w, r)
		if r.URL.Path == "/metrics" && r.Method == http.MethodGet {
			fmt.Fprintf(w, "aigate_connection_diagnostic_dropped_total %d\naigate_connection_diagnostic_write_errors_total %d\n", p.connections.dropped.Load(), p.connections.writeErrors.Load())
			if p.g.incidents != nil || p.g.captures != nil {
				var dropped, failures uint64
				if p.g.incidents != nil {
					dropped += p.g.incidents.dropped.Load()
					failures += p.g.incidents.failures.Load()
				}
				if p.g.captures != nil {
					dropped += p.g.captures.dropped.Load()
					failures += p.g.captures.failures.Load()
				}
				fmt.Fprintf(w, "aigate_stream_incident_dropped_total %d\naigate_stream_incident_write_errors_total %d\n", dropped, failures)
			}
		}
	})
}

func (p *ProxyGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/runtime/v1/status" || r.URL.Path == "/metrics" || strings.HasPrefix(r.URL.Path, "/profile/") {
		p.ObservationHandler().ServeHTTP(w, r)
		return
	}
	if r.URL.Path != "/v1/chat/completions" && r.URL.Path != "/v1/models" {
		p.g.ServeHTTP(w, r)
		return
	}
	// These are per-request assertions, not credentials or a route-writing API.
	if expected := r.Header.Get(headerExpectedConfig); expected != "" && expected != p.digest {
		abandonRequestBody(w, r)
		runtimeError(w, 409, "test_config_changed")
		return
	}
	if expected := r.Header.Get(headerRouter); expected != "" && expected != p.router {
		abandonRequestBody(w, r)
		runtimeError(w, 409, "test_target_changed")
		return
	}
	p.mu.Lock()
	if !p.admission {
		p.mu.Unlock()
		abandonRequestBody(w, r)
		w.Header().Set("Retry-After", "1")
		runtimeError(w, 503, "gateway_not_accepting")
		return
	}
	isModel := r.URL.Path == "/v1/models"
	if isModel {
		p.counts.Probes++
	} else {
		p.counts.Business++
	}
	stream := false
	meta := &runtimeRequest{task: diagnosticClientID(r.Header.Get(headerTask)), model: r.Header.Get(headerExpectedModel)}
	meta.revision, _ = strconv.ParseUint(r.Header.Get(headerRoute), 10, 64)
	// A verified replay mark only labels observations; forwarding is unchanged.
	if verifyReplayMark(p.markKeys, r.Header.Get(headerReplay)) {
		meta.traffic = "replay"
	}
	meta.stream = func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if !stream {
			stream = true
			p.counts.Streams++
		}
	}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if isModel {
			p.counts.Probes--
		} else {
			p.counts.Business--
		}
		if stream {
			p.counts.Streams--
		}
	}()
	w.Header().Set("X-Aigate-Backend", p.router)
	w.Header().Set(headerRoute, strconv.FormatUint(meta.revision, 10))
	w.Header().Set("X-Aigate-Config-Digest", p.digest)
	p.g.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), runtimeRequestKey{}, meta)))
}
