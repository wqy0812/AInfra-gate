package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type GatewayError struct {
	Status         int
	Code, Message  string
	UpstreamStatus int
}

func (e *GatewayError) Error() string { return e.Message }
func fault(status int, code, message string) *GatewayError {
	return &GatewayError{Status: status, Code: code, Message: message}
}

type modelCache struct {
	mu       sync.Mutex
	model    string
	expires  time.Time
	inflight chan struct{}
}

type BackendPool struct {
	config       *Config
	client       *http.Client
	registryMu   sync.RWMutex
	entries      map[string]*backendRuntime
	policyMu     sync.Mutex
	policyInfo   os.FileInfo
	cachedPolicy Policy
}

func newPool(c *Config, client *http.Client) *BackendPool {
	p := &BackendPool{config: c, client: client, entries: map[string]*backendRuntime{}}
	for _, name := range c.names {
		p.entries[name] = &backendRuntime{backend: c.Backends[name], cache: &modelCache{}}
	}
	return p
}

func (p *BackendPool) policy() (Policy, *GatewayError) {
	if p.config.RoutingFile == "" {
		return p.config.Routing, nil
	}
	p.policyMu.Lock()
	defer p.policyMu.Unlock()
	bad := fault(503, "invalid_routing_config", "路由配置不可用，请检查 routing_file")
	info, err := os.Stat(p.config.RoutingFile)
	if err != nil {
		p.policyInfo = nil
		return Policy{}, bad
	}
	if old := p.policyInfo; old != nil && os.SameFile(old, info) && old.Size() == info.Size() && old.ModTime() == info.ModTime() {
		return p.cachedPolicy, nil
	}
	p.policyInfo = nil
	f, err := os.Open(p.config.RoutingFile)
	if err != nil {
		return Policy{}, bad
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return Policy{}, bad
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return Policy{}, bad
	}
	after, err := f.Stat()
	if err != nil {
		return Policy{}, bad
	}
	var policy Policy
	if before.Size() != after.Size() || before.ModTime() != after.ModTime() || decodeJSON(data, &policy) != nil || policy.validate(p.config) != nil {
		return Policy{}, bad
	}
	p.cachedPolicy, p.policyInfo = policy, after
	return policy, nil
}

func (p *BackendPool) discover(ctx context.Context, name string) (string, *GatewayError) {
	return p.discoverRuntime(ctx, p.snapshot(name))
}

func (p *BackendPool) discoverRuntime(ctx context.Context, runtime *backendRuntime) (string, *GatewayError) {
	if runtime == nil {
		return "", fault(503, "unknown_backend", "后端不存在")
	}
	cached := runtime.cache
	failed := fault(503, "model_discovery_failed", "下游未返回可用模型，未发送生成请求")
	for {
		if ctx.Err() != nil {
			return "", failed
		}
		cached.mu.Lock()
		if time.Now().Before(cached.expires) {
			model := cached.model
			cached.mu.Unlock()
			if model == "" {
				return "", failed
			}
			return model, nil
		}
		if ready := cached.inflight; ready != nil {
			cached.mu.Unlock()
			select {
			case <-ready:
				continue
			case <-ctx.Done():
				return "", failed
			}
		}
		ready := make(chan struct{})
		cached.inflight = ready
		cached.mu.Unlock()
		model, err := p.fetchModel(ctx, runtime.backend)
		cached.mu.Lock()
		cached.model = model
		if ctx.Err() == nil {
			ttl := p.config.ModelCacheSeconds
			if err != nil {
				ttl = p.config.DiscoveryRetrySeconds
			}
			cached.expires = time.Now().Add(seconds(ttl))
		} else {
			cached.expires = time.Time{}
		}
		cached.inflight = nil
		close(ready)
		cached.mu.Unlock()
		if err != nil {
			return "", failed
		}
		return model, nil
	}
}

func (p *BackendPool) fetchModel(parent context.Context, b *Backend) (string, error) {
	ctx, cancel := context.WithTimeout(parent, seconds(p.config.DiscoveryTimeoutSeconds))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.ModelsURL, nil)
	if err != nil {
		return "", err
	}
	req.Header = b.headers.Clone()
	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("discovery status")
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var value any
	if err := decodeJSON(data, &value); err != nil {
		return "", err
	}
	value, ok := lookup(value, b.ModelsListPath)
	if !ok {
		return "", fmt.Errorf("model list missing")
	}
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return "", fmt.Errorf("model list empty")
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		value, ok := lookup(item, b.ModelIDPath)
		if !ok {
			return "", fmt.Errorf("model id missing")
		}
		id, ok := value.(string)
		if !ok || strings.TrimSpace(id) == "" {
			return "", fmt.Errorf("invalid model id")
		}
		ids = append(ids, id)
	}
	if b.PreferredModel != nil {
		for _, id := range ids {
			if id == *b.PreferredModel {
				return id, nil
			}
		}
		return "", fmt.Errorf("preferred model unavailable")
	}
	return ids[0], nil
}

func (p *BackendPool) choose(ctx context.Context) (string, *Backend, string, *GatewayError) {
	policy, err := p.policy()
	if err != nil {
		return "", nil, "", err
	}
	runtime := p.snapshot(policy.Backend)
	profileFrom(ctx).liveBackend(policy.Backend)
	model, discoveryErr := p.discoverRuntime(ctx, runtime)
	if discoveryErr != nil {
		return "", nil, "", discoveryErr
	}
	return policy.Backend, runtime.backend, model, nil
}

func (p *BackendPool) models(ctx context.Context) ([]string, *GatewayError) {
	_, _, model, err := p.choose(ctx)
	if err != nil {
		return nil, err
	}
	return []string{model}, nil
}

// A runtime is immutable except for its private, synchronized model cache.
// Callers retain it for their whole request, including discovery and SSE.
type backendRuntime struct {
	backend *Backend
	cache   *modelCache
}

func (p *BackendPool) snapshot(name string) *backendRuntime {
	p.registryMu.RLock()
	defer p.registryMu.RUnlock()
	return p.entries[name]
}

func (p *BackendPool) publish(name string, backend *Backend) {
	p.registryMu.Lock()
	defer p.registryMu.Unlock()
	p.entries[name] = &backendRuntime{backend: backend, cache: &modelCache{}}
}
