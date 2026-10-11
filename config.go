package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

// RuntimeSettings is the seven-field settings object accepted by control v2.
// Deployment paths and the Router snapshot are supplied separately.
type RuntimeSettings struct {
	TimeoutSeconds          float64           `json:"timeout_seconds"`
	DiscoveryTimeoutSeconds float64           `json:"discovery_timeout_seconds"`
	ModelCacheSeconds       float64           `json:"model_cache_seconds"`
	DiscoveryRetrySeconds   float64           `json:"discovery_retry_seconds"`
	HTTPPool                PoolConfig        `json:"http_pool"`
	Profile                 ProfileConfig     `json:"profile"`
	SlowRequests            SlowRequestConfig `json:"slow_requests"`
}

// RuntimeConfig belongs to one immutable Router generation. Switching is owned
// by ControlledGateway; a generation never reads routing or management files.
type RuntimeConfig struct {
	RuntimeSettings
	RouterID string
	Backend  *Backend
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

func defaultRuntimeSettings() RuntimeSettings {
	return RuntimeSettings{TimeoutSeconds: 120, DiscoveryTimeoutSeconds: 10, ModelCacheSeconds: 60,
		DiscoveryRetrySeconds: 5,
		HTTPPool:              PoolConfig{MaxIdleConns: 256, MaxIdleConnsPerHost: 128, IdleTimeoutSeconds: 90}}
}

func parseRuntimeSettings(data []byte) (RuntimeSettings, error) {
	settings := defaultRuntimeSettings()
	if err := strictDecode(data, &settings); err != nil {
		return RuntimeSettings{}, err
	}
	settings.Profile.defaults("/runtime/config.json")
	settings.SlowRequests.defaults("/runtime/config.json")
	return settings, nil
}

func (c *RuntimeConfig) validate() error {
	if err := c.SlowRequests.validate(); err != nil {
		return err
	}
	if err := c.Profile.validate(); err != nil {
		return err
	}
	if !validOperationID(c.RouterID) || c.Backend == nil {
		return fmt.Errorf("运行配置必须包含有效 Router ID 和后端")
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
	name, b := c.RouterID, c.Backend
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
	return nil
}
