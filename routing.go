package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
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
	config  *RuntimeConfig
	client  *http.Client
	runtime *backendRuntime
}

func newPool(c *RuntimeConfig, client *http.Client) *BackendPool {
	return &BackendPool{config: c, client: client, runtime: &backendRuntime{backend: c.Backend, cache: &modelCache{}}}
}

func (p *BackendPool) discover(ctx context.Context) (string, *GatewayError) {
	return p.discoverRuntime(ctx, p.runtime)
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
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("discovery status")
	}
	if err := decodeUpstreamBody(resp); err != nil {
		return "", err
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
	runtime := p.runtime
	profileFrom(ctx).liveBackend(p.config.RouterID)
	model, discoveryErr := p.discoverRuntime(ctx, runtime)
	if discoveryErr != nil {
		return "", nil, "", discoveryErr
	}
	return p.config.RouterID, runtime.backend, model, nil
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
