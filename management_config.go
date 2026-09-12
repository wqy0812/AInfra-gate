package main

import (
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"time"
)

type ManagementConfig struct {
	Enabled              bool    `json:"enabled"`
	Listen               string  `json:"listen"`
	APIKeyEnv            string  `json:"api_key_env"`
	StateFile            string  `json:"state_file"`
	WarmupRounds         int     `json:"warmup_rounds"`
	WarmupStepSeconds    float64 `json:"warmup_step_seconds"`
	WarmupTimeoutSeconds float64 `json:"warmup_timeout_seconds"`
	WarmupMaxTokens      int     `json:"warmup_max_tokens"`
	DrainTimeoutSeconds  float64 `json:"drain_timeout_seconds"`
	AutoRollback         *bool   `json:"auto_rollback,omitempty"`
	FailureThreshold     int     `json:"failure_threshold"`
	FailureWindowSeconds float64 `json:"failure_window_seconds"`
	RollbackRetrySeconds float64 `json:"rollback_retry_seconds"`
}

func (m *ManagementConfig) defaults(c *Config, path string) {
	if !m.Enabled {
		return
	}
	if m.Listen == "" {
		m.Listen = "127.0.0.1:8081"
	}
	if m.APIKeyEnv == "" {
		m.APIKeyEnv = "AIGATE_ADMIN_KEY"
	}
	if m.StateFile == "" {
		m.StateFile = "management-state.json"
	}
	if !filepath.IsAbs(m.StateFile) {
		absolute, _ := filepath.Abs(path)
		m.StateFile = filepath.Join(filepath.Dir(absolute), m.StateFile)
	}
	if m.WarmupRounds == 0 {
		m.WarmupRounds = 2
	}
	if m.WarmupStepSeconds == 0 {
		m.WarmupStepSeconds = 120
	}
	if m.WarmupTimeoutSeconds == 0 {
		m.WarmupTimeoutSeconds = 900
	}
	if m.WarmupMaxTokens == 0 {
		m.WarmupMaxTokens = 1024
	}
	if m.DrainTimeoutSeconds == 0 {
		m.DrainTimeoutSeconds = math.Max(600, c.TimeoutSeconds+c.DiscoveryTimeoutSeconds+30)
	}
	if m.FailureThreshold == 0 {
		m.FailureThreshold = 3
	}
	if m.FailureWindowSeconds == 0 {
		m.FailureWindowSeconds = 60
	}
	if m.RollbackRetrySeconds == 0 {
		m.RollbackRetrySeconds = 60
	}
}

func (m ManagementConfig) rollbackEnabled() bool { return m.AutoRollback == nil || *m.AutoRollback }

func (m ManagementConfig) validate(c *Config) error {
	if !m.Enabled {
		return nil
	}
	if c.RoutingFile != "" || c.Routing.Mode != "single" {
		return fmt.Errorf("management 模式要求 single 路由且不能设置 routing_file")
	}
	if _, _, err := net.SplitHostPort(m.Listen); err != nil {
		return fmt.Errorf("management.listen 必须为 host:port")
	}
	if m.StateFile == "" || m.APIKeyEnv == "" || os.Getenv(m.APIKeyEnv) == "" {
		return fmt.Errorf("management 需要 state_file 和有效的管理密钥环境变量 %s", m.APIKeyEnv)
	}
	if m.WarmupRounds < 1 || m.WarmupRounds > 20 || m.WarmupMaxTokens < 1 || m.FailureThreshold < 1 || m.FailureThreshold > 100 {
		return fmt.Errorf("management 轮数、token 预算或故障阈值无效")
	}
	for _, n := range []float64{m.WarmupStepSeconds, m.WarmupTimeoutSeconds, m.DrainTimeoutSeconds, m.FailureWindowSeconds, m.RollbackRetrySeconds} {
		if math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 || n >= float64((1<<63-1)/int64(time.Second)) || seconds(n) <= 0 {
			return fmt.Errorf("management 时间参数必须为有效的正秒数")
		}
	}
	for name, b := range c.Backends {
		if !validOperationID(name) {
			return fmt.Errorf("management 后端名称须为 1–128 位字母、数字、连字符或下划线")
		}
		if b == nil || b.Adapter != "openai" {
			return fmt.Errorf("management 第一版仅支持 openai 适配器")
		}
	}
	return nil
}
