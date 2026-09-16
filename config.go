package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	TimeoutSeconds          float64             `json:"timeout_seconds"`
	DiscoveryTimeoutSeconds float64             `json:"discovery_timeout_seconds"`
	ModelCacheSeconds       float64             `json:"model_cache_seconds"`
	DiscoveryRetrySeconds   float64             `json:"discovery_retry_seconds"`
	PublicModelName         string              `json:"public_model_name"`
	GatewayAPIKeyEnv        string              `json:"gateway_api_key_env"`
	RoutingFile             string              `json:"routing_file"`
	Routing                 Policy              `json:"routing"`
	Backends                map[string]*Backend `json:"backends"`
	HTTPPool                PoolConfig          `json:"http_pool"`
	Profile                 ProfileConfig       `json:"profile"`
	SlowRequests            SlowRequestConfig   `json:"slow_requests"`
	Management              ManagementConfig    `json:"management"`
	names                   []string
}

type PoolConfig struct {
	MaxIdleConns        int     `json:"max_idle_connections"`
	MaxIdleConnsPerHost int     `json:"max_idle_connections_per_host"`
	MaxConnsPerHost     int     `json:"max_connections_per_host"`
	IdleTimeoutSeconds  float64 `json:"idle_timeout_seconds"`
}

type Backend struct {
	Adapter             string                     `json:"adapter"`
	URL                 string                     `json:"url"`
	ModelsURL           string                     `json:"models_url"`
	Headers             map[string]string          `json:"headers"`
	HeadersEnv          map[string]string          `json:"headers_env"`
	HeaderPrefixes      map[string]string          `json:"header_prefixes"`
	Overrides           map[string]json.RawMessage `json:"overrides"`
	Compatibility       Compatibility              `json:"compatibility"`
	RequestTemplate     json.RawMessage            `json:"request_template"`
	ResponseContentPath string                     `json:"response_content_path"`
	ModelsListPath      string                     `json:"models_list_path"`
	ModelIDPath         string                     `json:"model_id_path"`
	PreferredModel      *string                    `json:"preferred_model"`
	UpstreamModel       json.RawMessage            `json:"upstream_model,omitempty"`
	headers             http.Header
}

type Compatibility struct {
	MetadataValues string   `json:"metadata_values"`
	DropFields     []string `json:"drop_fields"`
}

type Policy struct {
	Mode    string `json:"mode"`
	Backend string `json:"backend,omitempty"`
}

func (p *Policy) UnmarshalJSON(data []byte) error {
	type plain Policy
	var next plain
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&next); err != nil {
		return err
	}
	*p = Policy(next)
	return nil
}

func seconds(n float64) time.Duration { return time.Duration(n * float64(time.Second)) }

// decodeJSON accepts exactly one JSON value and preserves integer precision.
func decodeJSON(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}

func parseConfig(data []byte, path string) (*Config, error) {
	c := &Config{TimeoutSeconds: 120, DiscoveryTimeoutSeconds: 10, ModelCacheSeconds: 60,
		DiscoveryRetrySeconds: 5, Routing: Policy{Mode: "single"},
		HTTPPool: PoolConfig{MaxIdleConns: 256, MaxIdleConnsPerHost: 128, IdleTimeoutSeconds: 90}}
	if err := decodeJSON(data, c); err != nil {
		return nil, fmt.Errorf("配置 JSON 无效: %w", err)
	}
	// Preserve declaration order for backend status and reject duplicate names.
	var root map[string]json.RawMessage
	if err := decodeJSON(data, &root); err != nil {
		return nil, err
	}
	if raw, ok := root["public_model_name"]; ok {
		if _, valid := rawString(raw); !valid {
			return nil, fmt.Errorf("public_model_name 必须为字符串")
		}
	}
	d := json.NewDecoder(bytes.NewReader(root["backends"]))
	if token, err := d.Token(); err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("配置中必须包含 backends")
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return nil, err
		}
		name := token.(string)
		if seen[name] {
			return nil, fmt.Errorf("重复的 backend: %s", name)
		}
		seen[name] = true
		c.names = append(c.names, name)
		var ignored json.RawMessage
		if err := d.Decode(&ignored); err != nil {
			return nil, err
		}
	}
	if _, explicit := root["routing"]; !explicit && c.RoutingFile == "" && len(c.names) == 1 {
		c.Routing.Backend = c.names[0]
	}
	if c.RoutingFile != "" && !filepath.IsAbs(c.RoutingFile) {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		c.RoutingFile = filepath.Join(filepath.Dir(absolute), c.RoutingFile)
	}
	c.Management.defaults(c, path)
	c.Profile.defaults(path)
	c.SlowRequests.defaults(path)
	return c, nil
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseConfig(data, path)
}

func (p Policy) validate(c *Config) error {
	if p.Mode != "single" {
		return fmt.Errorf("routing.mode 仅支持 single，请指定 routing.backend；不再支持负载均衡")
	}
	if c.Backends[p.Backend] == nil {
		return fmt.Errorf("routing.backend 必须指向已配置的下游")
	}
	return nil
}

func (c *Config) validate() error {
	if err := c.SlowRequests.validate(); err != nil {
		return err
	}
	if err := c.Profile.validate(); err != nil {
		return err
	}
	if len(c.Backends) == 0 || len(c.names) != len(c.Backends) {
		return fmt.Errorf("配置中必须包含 backends")
	}
	for name, n := range map[string]float64{"timeout_seconds": c.TimeoutSeconds, "discovery_timeout_seconds": c.DiscoveryTimeoutSeconds,
		"model_cache_seconds": c.ModelCacheSeconds, "discovery_retry_seconds": c.DiscoveryRetrySeconds, "http_pool.idle_timeout_seconds": c.HTTPPool.IdleTimeoutSeconds} {
		if n <= 0 || n > float64((1<<63-1)/int64(time.Second)) || seconds(n) <= 0 {
			return fmt.Errorf("%s 必须为有效的正秒数", name)
		}
	}
	if c.HTTPPool.MaxIdleConns <= 0 || c.HTTPPool.MaxIdleConnsPerHost <= 0 || c.HTTPPool.MaxConnsPerHost < 0 {
		return fmt.Errorf("连接池 idle 上限必须为正，连接上限不得为负")
	}
	// With a routing file, an omitted inline target is valid; the file is validated by the pool.
	if c.RoutingFile == "" || c.Routing.Backend != "" || c.Routing.Mode != "single" {
		if err := c.Routing.validate(c); err != nil {
			return err
		}
	}
	if strings.TrimSpace(c.PublicModelName) != c.PublicModelName {
		return fmt.Errorf("public_model_name 不能为纯空白或含首尾空白")
	}
	if c.GatewayAPIKeyEnv != "" && os.Getenv(c.GatewayAPIKeyEnv) == "" {
		return fmt.Errorf("缺少环境变量 %s", c.GatewayAPIKeyEnv)
	}
	if err := c.Management.validate(c); err != nil {
		return err
	}
	for name, b := range c.Backends {
		if b == nil {
			return fmt.Errorf("backend %s 必须为对象", name)
		}
		for _, raw := range []string{b.URL, b.ModelsURL} {
			u, err := url.Parse(raw)
			if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
				return fmt.Errorf("backend %s 的 URL 必须为无内嵌凭据的 HTTP(S) 地址", name)
			}
		}
		if b.UpstreamModel != nil {
			return fmt.Errorf("请删除 upstream_model；现在自动发现模型")
		}
		if b.Adapter != "openai" && b.Adapter != "template" {
			return fmt.Errorf("adapter 必须是 openai 或 template")
		}
		if b.ModelsListPath == "" {
			b.ModelsListPath = "data"
		}
		if b.ModelIDPath == "" {
			b.ModelIDPath = "id"
		}
		if b.Compatibility.MetadataValues != "" && b.Compatibility.MetadataValues != "preserve" && b.Compatibility.MetadataValues != "json_string" && b.Compatibility.MetadataValues != "workspace_object_json_string" {
			return fmt.Errorf("metadata_values 必须是 preserve、json_string 或 workspace_object_json_string")
		}
		for _, field := range b.Compatibility.DropFields {
			if field == "model" || field == "messages" || field == "stream" {
				return fmt.Errorf("不能删除 model、messages、stream")
			}
		}
		if b.Adapter == "template" {
			var template map[string]json.RawMessage
			if decodeJSON(b.RequestTemplate, &template) != nil || template == nil || b.ResponseContentPath == "" {
				return fmt.Errorf("template 适配器需要 request_template 对象和 response_content_path")
			}
		}
		b.headers = make(http.Header)
		for key, value := range b.Headers {
			b.headers.Set(key, value)
		}
		for key, env := range b.HeadersEnv {
			value := os.Getenv(env)
			if value == "" {
				return fmt.Errorf("缺少环境变量 %s", env)
			}
			b.headers.Set(key, b.HeaderPrefixes[key]+value)
		}
		for key, values := range b.headers {
			if !validHeaderName(key) {
				return fmt.Errorf("backend %s 的 header 无效", name)
			}
			for _, value := range values {
				if !validHeaderValue(value) {
					return fmt.Errorf("backend %s 的 header 无效", name)
				}
			}
		}
	}
	return nil
}

// Public naming never changes downstream discovery or audit attribution.
func (c *Config) publicModel(actual string) string {
	if c.PublicModelName != "" {
		return c.PublicModelName
	}
	return actual
}
