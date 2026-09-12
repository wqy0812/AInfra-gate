package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Registry is private persisted configuration, never a management response DTO.
type BackendRegistryState struct {
	Version uint64              `json:"version"`
	Order   []string            `json:"order"`
	Configs map[string]*Backend `json:"configs"`
}

func registryFromConfig(c *Config) *BackendRegistryState {
	var configs map[string]*Backend
	_ = decodeJSON(encodeJSON(c.Backends), &configs)
	return &BackendRegistryState{Version: 1, Order: append([]string(nil), c.names...), Configs: configs}
}

func configFromRegistry(c *Config, registry *BackendRegistryState, active string) (*Config, error) {
	if registry == nil || registry.Version == 0 || len(registry.Order) == 0 || len(registry.Order) != len(registry.Configs) {
		return nil, fmt.Errorf("后端注册表无效；拒绝覆盖现有状态")
	}
	seen := map[string]bool{}
	for _, name := range registry.Order {
		if !validOperationID(name) || seen[name] || registry.Configs[name] == nil {
			return nil, fmt.Errorf("后端注册表名称或顺序无效")
		}
		seen[name] = true
	}
	next := *c
	next.Backends = nil
	next.names = append([]string(nil), registry.Order...)
	if err := decodeJSON(encodeJSON(registry.Configs), &next.Backends); err != nil {
		return nil, err
	}
	next.Routing = Policy{Mode: "single", Backend: active}
	return &next, nil
}

// Overlay persisted backends before validating credentials or the bootstrap route.
func effectiveManagedConfig(c *Config) (*Config, error) {
	if !c.Management.Enabled {
		return c, nil
	}
	data, err := os.ReadFile(c.Management.StateFile)
	if os.IsNotExist(err) {
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	var state ManagedState
	if decodeJSON(data, &state) != nil {
		return nil, fmt.Errorf("管理状态文件无效")
	}
	if state.SchemaVersion == 1 {
		return c, nil
	}
	if state.SchemaVersion != 2 {
		return nil, fmt.Errorf("不支持的管理状态版本")
	}
	return configFromRegistry(c, state.Registry, state.ActiveBackend)
}

func backupLegacyState(path string) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	// Never replace an earlier backup, including one from a failed migration.
	f, err := os.OpenFile(path+".v1-backup-"+randomID(), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

type HeaderChange struct {
	Action string  `json:"action"`
	Value  *string `json:"value,omitempty"`
	Env    string  `json:"env,omitempty"`
	Prefix string  `json:"prefix,omitempty"`
}

type BackendWriteRequest struct {
	OperationID      string                     `json:"operation_id"`
	ExpectedRevision uint64                     `json:"expected_revision"`
	Target           string                     `json:"target,omitempty"`
	Backend          map[string]json.RawMessage `json:"backend"`
	Credentials      map[string]HeaderChange    `json:"credentials,omitempty"`
}

type HeaderView struct {
	Source string `json:"source"`
	Env    string `json:"env,omitempty"`
	Prefix string `json:"prefix,omitempty"`
}

type BackendView struct {
	Backend     *Backend              `json:"backend"`
	Credentials map[string]HeaderView `json:"credentials"`
}

type BackendList struct {
	Revision      uint64                 `json:"revision"`
	ConfigVersion uint64                 `json:"config_version"`
	Order         []string               `json:"order"`
	Backends      map[string]BackendView `json:"backends"`
}

func (m *Management) backendList() BackendList {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.state.Registry
	result := BackendList{Revision: m.state.Revision, ConfigVersion: r.Version, Order: append([]string(nil), r.Order...), Backends: map[string]BackendView{}}
	for _, name := range r.Order {
		var b Backend
		_ = decodeJSON(encodeJSON(r.Configs[name]), &b)
		view := BackendView{Backend: &b, Credentials: map[string]HeaderView{}}
		for key := range b.Headers {
			view.Credentials[http.CanonicalHeaderKey(key)] = HeaderView{Source: "value"}
		}
		for key, env := range b.HeadersEnv {
			view.Credentials[http.CanonicalHeaderKey(key)] = HeaderView{Source: "env", Env: env, Prefix: b.HeaderPrefixes[key]}
		}
		b.Headers, b.HeadersEnv, b.HeaderPrefixes = nil, nil, nil
		result.Backends[name] = view
	}
	return result
}

func strictDecode(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("需要单个 JSON 对象")
	}
	return nil
}

func validHeaderName(key string) bool {
	if key == "" {
		return false
	}
	for _, c := range key {
		if !strings.ContainsRune("!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", c) {
			return false
		}
	}
	return true
}

func editedBackend(old *Backend, req BackendWriteRequest) (*Backend, error) {
	b := Backend{Adapter: "openai"}
	if old != nil {
		_ = decodeJSON(encodeJSON(old), &b)
	}
	// Headers have explicit keep/replace/delete semantics and are never patched as
	// ordinary fields. Other top-level fields replace their whole value.
	fields := map[string]bool{"adapter": true, "url": true, "models_url": true, "preferred_model": true, "models_list_path": true, "model_id_path": true, "overrides": true, "compatibility": true}
	var merged map[string]json.RawMessage
	_ = decodeJSON(encodeJSON(b), &merged)
	for key, value := range req.Backend {
		if !fields[key] {
			return nil, fmt.Errorf("不支持修改字段 %s", key)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) && key != "preferred_model" {
			return nil, fmt.Errorf("字段 %s 不能为 null", key)
		}
		merged[key] = value
	}
	if strictDecode(encodeJSON(merged), &b) != nil {
		return nil, fmt.Errorf("后端字段格式无效")
	}
	if b.Adapter != "openai" {
		return nil, fmt.Errorf("管理模式仅支持 openai 适配器")
	}
	// Normalize old header spelling before applying changes.
	headers, envs, prefixes := map[string]string{}, map[string]string{}, map[string]string{}
	for key, value := range b.Headers {
		headers[http.CanonicalHeaderKey(key)] = value
	}
	for key, value := range b.HeadersEnv {
		envs[http.CanonicalHeaderKey(key)] = value
		prefixes[http.CanonicalHeaderKey(key)] = b.HeaderPrefixes[key]
	}
	seen := map[string]bool{}
	for key, change := range req.Credentials {
		if !validHeaderName(key) {
			return nil, fmt.Errorf("请求头名称无效")
		}
		key = http.CanonicalHeaderKey(key)
		if seen[key] {
			return nil, fmt.Errorf("请求头名称重复")
		}
		seen[key] = true
		switch change.Action {
		case "keep":
			if change.Value != nil || change.Env != "" || change.Prefix != "" {
				return nil, fmt.Errorf("保留凭据时不能提供新值")
			}
		case "delete":
			if change.Value != nil || change.Env != "" || change.Prefix != "" {
				return nil, fmt.Errorf("删除凭据时不能提供新值")
			}
			delete(headers, key)
			delete(envs, key)
			delete(prefixes, key)
		case "replace":
			if (change.Value == nil) == (change.Env == "") || (change.Value != nil && change.Prefix != "") {
				return nil, fmt.Errorf("替换凭据需要且仅需要 value 或 env")
			}
			delete(headers, key)
			delete(envs, key)
			delete(prefixes, key)
			if change.Value != nil {
				headers[key] = *change.Value
			} else {
				envs[key] = change.Env
				prefixes[key] = change.Prefix
			}
		default:
			return nil, fmt.Errorf("凭据操作必须为 keep、replace 或 delete")
		}
	}
	b.Headers, b.HeadersEnv, b.HeaderPrefixes = headers, envs, prefixes
	return &b, nil
}

func publicOperation(op *Operation) *Operation {
	result := *op
	result.Fingerprint = ""
	return &result
}

func (m *Management) writeBackend(create bool, req BackendWriteRequest) (*Operation, *ManagementError) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bad := func(status int, code, message string) (*Operation, *ManagementError) {
		return nil, &ManagementError{status, code, message, m.busy}
	}
	if req.Backend == nil {
		return bad(400, "invalid_request", "backend 必须为 JSON 对象")
	}
	if !validOperationID(req.OperationID) || !validOperationID(req.Target) {
		return bad(400, "invalid_request", "后端名称与 operation_id 须为 1–128 位字母、数字、连字符或下划线")
	}
	kind := "backend-update"
	if create {
		kind = "backend-create"
	}
	var canonical any
	_ = decodeJSON(encodeJSON(req), &canonical)
	digest := sha256.Sum256(encodeJSON(canonical))
	fingerprint := hex.EncodeToString(digest[:])
	if old := operationByID(&m.state, req.OperationID); old != nil {
		if old.Kind != kind || old.Fingerprint != fingerprint {
			return bad(409, "operation_id_conflict", "operation_id 已用于不同请求")
		}
		return publicOperation(old), nil
	}
	if m.closed {
		return bad(503, "gateway_stopping", "网关正在退出")
	}
	if req.ExpectedRevision != m.state.Revision {
		return bad(409, "stale_revision", "状态已变化，请刷新后重试")
	}
	if m.busy != "" {
		return bad(409, "operation_in_progress", "已有管理操作正在进行")
	}
	old := m.state.Registry.Configs[req.Target]
	if create && old != nil {
		return bad(409, "backend_exists", "后端名称已存在")
	}
	if !create && old == nil {
		return bad(404, "unknown_backend", "后端不存在")
	}
	if !create {
		b, c := m.state.Backends[req.Target], m.counts[req.Target]
		if m.state.ActiveBackend == req.Target || b.DrainUnknown || c.Business+c.Probes != 0 {
			return bad(409, "backend_not_drained", "请先切走并确认业务及探测均已排空")
		}
	}
	b, err := editedBackend(old, req)
	if err != nil {
		return bad(400, "invalid_backend", err.Error())
	}
	// Validate an isolated candidate. Do not normalize or mutate a live config.
	candidate := *m.g.config
	candidate.Backends = map[string]*Backend{req.Target: b}
	candidate.names = []string{req.Target}
	candidate.Routing = Policy{Mode: "single", Backend: req.Target}
	if err := candidate.validate(); err != nil {
		return bad(400, "invalid_backend", err.Error())
	}
	s := m.cloneLocked()
	s.Registry.Configs[req.Target] = b
	s.Registry.Version++
	if create {
		s.Registry.Order = append(s.Registry.Order, req.Target)
		s.Backends[req.Target] = &BackendState{}
	} else {
		s.Backends[req.Target].LastWarmup = nil
	}
	if s.RollbackCandidate == req.Target {
		s.RollbackCandidate = ""
		s.RollbackArmed = false
	}
	op := &Operation{ID: req.OperationID, Kind: kind, Source: s.ActiveBackend, Target: req.Target, ExpectedRevision: req.ExpectedRevision, Fingerprint: fingerprint, Phase: "completed", Outcome: "completed", StartedAt: time.Now(), UpdatedAt: time.Now()}
	s.Operations = append(s.Operations, op)
	if len(s.Operations) > 100 {
		s.Operations = s.Operations[len(s.Operations)-100:]
	}
	if err := m.commitLocked(s); err != nil {
		return bad(500, "state_write_failed", err.Error())
	}
	m.g.pool.publish(req.Target, b)
	if create {
		m.counts[req.Target] = &BackendCounters{}
	} else {
		m.counts[req.Target].Model = ""
	}
	m.event(op)
	return publicOperation(op), nil
}

func validHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < 32 && c != '\t') || c == 127 {
			return false
		}
	}
	return true
}
