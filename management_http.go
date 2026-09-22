package main

import (
	"crypto/subtle"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path"
	"strings"
)

// Dedicated, opt-in engine control listener. Never mounted on the business port.
type ManagementHTTPConfig struct {
	Listen       string   `json:"listen,omitempty"`
	KeyEnv       string   `json:"key_env,omitempty"`
	AllowedCIDRs []string `json:"allowed_cidrs,omitempty"`
	GatewayURLs  []string `json:"gateway_urls,omitempty"`
}

func (c ManagementHTTPConfig) validate(enabled bool) error {
	if c.Listen == "" {
		return nil
	}
	if !enabled {
		return fmt.Errorf("管理 HTTP 需要启用 management")
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("管理 HTTP listen 无效")
	}
	if len(os.Getenv(c.KeyEnv)) < 32 {
		return fmt.Errorf("管理 HTTP 密钥环境变量缺失或不足 32 字节")
	}
	if len(c.AllowedCIDRs) == 0 {
		return fmt.Errorf("管理 HTTP 必须设置 allowed_cidrs")
	}
	for _, s := range c.AllowedCIDRs {
		if _, err := netip.ParsePrefix(s); err != nil {
			return fmt.Errorf("管理 HTTP CIDR 无效")
		}
	}
	if len(c.GatewayURLs) == 0 {
		return fmt.Errorf("管理 HTTP 必须登记全部网关地址及别名")
	}
	for _, s := range c.GatewayURLs {
		if _, err := controlBase(s); err != nil {
			return err
		}
	}
	return nil
}

func controlBase(s string) (string, error) {
	u, err := url.Parse(s)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("Router 地址无效")
	}
	p := strings.TrimRight(path.Clean("/"+u.Path), "/")
	for _, suffix := range []string{"/chat/completions", "/models"} {
		p = strings.TrimSuffix(p, suffix)
	}
	p = strings.TrimSuffix(p, "/v1")
	host := strings.TrimRight(strings.ToLower(u.Hostname()), ".")
	port := u.Port()
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port != "" && !(u.Scheme == "http" && port == "80") && !(u.Scheme == "https" && port == "443") {
		host += ":" + port
	}
	return strings.ToLower(u.Scheme) + "://" + host + p, nil
}

func (c ManagementHTTPConfig) checkRouter(b *Backend) error {
	if c.Listen == "" {
		return nil
	}
	for _, value := range []string{b.URL, b.ModelsURL} {
		base, err := controlBase(value)
		if err != nil {
			return err
		}
		for _, gateway := range c.GatewayURLs {
			forbidden, _ := controlBase(gateway)
			if base == forbidden {
				return fmt.Errorf("网关只能指向 Router，禁止网关串联")
			}
		}
	}
	return nil
}

func (m *Management) httpHandler() http.Handler {
	c := m.g.config.Management.HTTP
	key := os.Getenv(c.KeyEnv)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		ip, ipErr := netip.ParseAddr(host)
		allowed := false
		if err == nil && ipErr == nil {
			for _, s := range c.AllowedCIDRs {
				prefix, _ := netip.ParsePrefix(s)
				allowed = allowed || prefix.Contains(ip.Unmap())
			}
		}
		if !allowed || key == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+key)) != 1 {
			http.Error(w, `{"error":"unauthorized"}`, 403)
			return
		}
		if r.URL.Path != "/control/v1" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxControlRequest))
		var req controlRequest
		if err != nil || strictDecode(data, &req) != nil {
			http.Error(w, `{"error":"invalid_request"}`, 400)
			return
		}
		switch req.Command {
		case "status", "backends-list", "operation", "backend-create", "backend-edit", "warmup", "switch", "test-acquire", "test-release", "test-status", "gateway-addresses":
		default:
			http.Error(w, `{"error":"unsupported_command"}`, 400)
			return
		}
		result, problem := m.control(req)
		response := controlResponse{Error: problem}
		if problem != nil {
			w.WriteHeader(problem.Status)
		} else {
			response.Result = encodeJSON(result)
		}
		_, _ = w.Write(encodeJSON(response))
	})
}

// Registered gateway aliases are monotonic: removing an alias must never turn a
// previously forbidden gateway endpoint into an eligible Router.
func (m *Management) registerGatewayAddresses(urls []string) (any, *ManagementError) {
	m.mu.Lock()
	defer m.mu.Unlock()
	known := map[string]bool{}
	for _, u := range m.state.GatewayAddresses {
		known[u] = true
	}
	s := m.cloneLocked()
	for _, u := range urls {
		base, err := controlBase(u)
		if err != nil {
			return nil, &ManagementError{Status: 400, Code: "invalid_gateway_address", Message: "网关地址无效"}
		}
		if !known[base] {
			s.GatewayAddresses = append(s.GatewayAddresses, base)
			known[base] = true
		}
	}
	if len(s.GatewayAddresses) != len(m.state.GatewayAddresses) {
		if m.commitLocked(s) != nil {
			return nil, &ManagementError{Status: 500, Code: "state_write_failed", Message: "地址登记未保存"}
		}
	}
	return map[string]any{"gateway_addresses": m.state.GatewayAddresses}, nil
}
func (m *Management) checkRouterLocked(b *Backend) error {
	c := m.g.config.Management.HTTP
	c.Listen = "registered"
	c.GatewayURLs = append(append([]string{}, c.GatewayURLs...), m.state.GatewayAddresses...)
	return c.checkRouter(b)
}
