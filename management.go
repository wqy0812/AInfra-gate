package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type BackendState struct {
	Maintenance  bool          `json:"maintenance"`
	DrainUnknown bool          `json:"drain_unknown"`
	LastWarmup   *WarmupReport `json:"last_warmup,omitempty"`
}

type Operation struct {
	ID               string        `json:"id"`
	Kind             string        `json:"kind"`
	Source           string        `json:"source"`
	Target           string        `json:"target"`
	ExpectedRevision uint64        `json:"expected_revision"`
	Fingerprint      string        `json:"fingerprint,omitempty"`
	Phase            string        `json:"phase"`
	Outcome          string        `json:"outcome"`
	Error            string        `json:"error,omitempty"`
	RouteCommitted   bool          `json:"route_committed"`
	StartedAt        time.Time     `json:"started_at"`
	UpdatedAt        time.Time     `json:"updated_at"`
	Report           *WarmupReport `json:"report,omitempty"`
}

type ManagedState struct {
	ReleasedTestTokens map[string]bool          `json:"released_test_tokens,omitempty"`
	GatewayAddresses   []string                 `json:"gateway_addresses,omitempty"`
	TestOccupancy      *TestOccupancy           `json:"test_occupancy,omitempty"`
	Registry           *BackendRegistryState    `json:"registry,omitempty"`
	SchemaVersion      int                      `json:"schema_version"`
	Revision           uint64                   `json:"revision"`
	RouteVersion       uint64                   `json:"route_version"`
	ActiveBackend      string                   `json:"active_backend"`
	RollbackCandidate  string                   `json:"rollback_candidate,omitempty"`
	RollbackArmed      bool                     `json:"rollback_armed"`
	CleanShutdown      bool                     `json:"clean_shutdown"`
	Backends           map[string]*BackendState `json:"backends"`
	Operations         []*Operation             `json:"operations"`
}

type BackendCounters struct {
	Tests     int    `json:"tests"`
	Model     string `json:"model,omitempty"`
	Business  int    `json:"business"`
	Streams   int    `json:"streams"`
	Probes    int    `json:"probes"`
	Abnormal  uint64 `json:"abnormal"`
	LastError string `json:"last_error,omitempty"`
}

func (m *Management) noteModel(name, model string) {
	if model == "" {
		return
	}
	m.mu.Lock()
	m.counts[name].Model = model
	m.mu.Unlock()
}

type ManagementStatus struct {
	ManagedState
	TestControlVersion int                        `json:"test_control_version"`
	Counts             map[string]BackendCounters `json:"counts"`
	Order              []string                   `json:"order"`
	Busy               string                     `json:"busy,omitempty"`
	Alert              string                     `json:"alert,omitempty"`
	Failures           int                        `json:"consecutive_failures"`
}

type Management struct {
	mu           sync.Mutex
	g            *Gateway
	state        ManagedState
	counts       map[string]*BackendCounters
	busy         string
	alert        string
	closed       bool
	failures     []time.Time
	nextRollback time.Time
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	lock         *os.File
	// Replaced in fault-injection tests; called only with mu held.
	persist func(ManagedState) error
}

func readManagedState(c *Config) (ManagedState, error) {
	data, err := os.ReadFile(c.Management.StateFile)
	if os.IsNotExist(err) {
		s := ManagedState{SchemaVersion: 1, Revision: 1, RouteVersion: 1, ActiveBackend: c.Routing.Backend, CleanShutdown: true, Backends: map[string]*BackendState{}, Operations: []*Operation{}}
		for _, name := range c.names {
			s.Backends[name] = &BackendState{}
		}
		return s, nil
	}
	if err != nil {
		return ManagedState{}, err
	}
	var s ManagedState
	if decodeJSON(data, &s) == nil && s.SchemaVersion == 2 {
		var err error
		c, err = configFromRegistry(c, s.Registry, s.ActiveBackend)
		if err != nil {
			return s, err
		}
	}
	if decodeJSON(data, &s) != nil || (s.SchemaVersion != 1 && s.SchemaVersion != 2) || s.Revision == 0 || s.RouteVersion == 0 || c.Backends[s.ActiveBackend] == nil || len(s.Backends) != len(c.Backends) || len(s.Operations) > 100 {
		return s, fmt.Errorf("管理状态文件无效；拒绝覆盖现有状态")
	}
	for _, name := range c.names {
		if s.Backends[name] == nil {
			return s, fmt.Errorf("状态文件与后端配置不匹配")
		}
	}
	if s.Backends[s.ActiveBackend].Maintenance || (s.RollbackCandidate != "" && c.Backends[s.RollbackCandidate] == nil) || (s.RollbackArmed && (s.RollbackCandidate == "" || s.RollbackCandidate == s.ActiveBackend || s.Backends[s.RollbackCandidate].Maintenance)) {
		return s, fmt.Errorf("管理状态中的路由或维护锁冲突")
	}
	ids := map[string]bool{}
	for _, op := range s.Operations {
		if op == nil || op.ID == "" || ids[op.ID] || s.Backends[op.Target] == nil || s.Backends[op.Source] == nil {
			return s, fmt.Errorf("操作历史无效")
		}
		ids[op.ID] = true
	}
	return s, nil
}

func writeManagedState(path string, s ManagedState) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".management-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(append(encodeJSON(s), '\n')); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func newManagement(g *Gateway) (*Management, error) {
	f, err := os.OpenFile(g.config.Management.StateFile+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = lockState(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("管理状态文件已被其他进程占用: %w", err)
	}
	s, err := readManagedState(g.config)
	if err != nil {
		unlockState(f)
		f.Close()
		return nil, err
	}
	if s.SchemaVersion == 2 {
		next, configErr := configFromRegistry(g.config, s.Registry, s.ActiveBackend)
		if configErr == nil {
			configErr = next.validate()
		}
		if configErr != nil {
			unlockState(f)
			f.Close()
			return nil, configErr
		}
		g.config = next
		g.pool = newPool(next, g.client)
	}
	if s.SchemaVersion == 1 {
		if err := backupLegacyState(g.config.Management.StateFile); err != nil {
			unlockState(f)
			f.Close()
			return nil, err
		}
		s.SchemaVersion = 2
		s.Registry = registryFromConfig(g.config)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Management{g: g, state: s, counts: map[string]*BackendCounters{}, ctx: ctx, cancel: cancel, lock: f}
	m.persist = func(s ManagedState) error { return writeManagedState(g.config.Management.StateFile, s) }
	for name, b := range s.Backends {
		m.counts[name] = &BackendCounters{}
		// A failed completion write can leave durable test inflight even after
		// an otherwise clean shutdown. It still requires explicit confirmation.
		unknownTest := s.TestOccupancy != nil && s.TestOccupancy.Backend == name && s.TestOccupancy.Inflight > 0
		if (!s.CleanShutdown || unknownTest) && !b.Maintenance {
			b.DrainUnknown = true
		}
		if b.LastWarmup != nil {
			b.LastWarmup.Fresh = false
		}
	}
	for _, op := range s.Operations {
		if op.Outcome == "running" || op.Outcome == "drain_timeout" {
			op.Outcome, op.Phase, op.Error = "interrupted", "interrupted", "网关重启；核对下游旧请求后确认排空"
			s.Backends[op.Source].DrainUnknown = true
		}
	}
	if !s.CleanShutdown {
		s.RollbackArmed = false
	}
	s.CleanShutdown = false
	s.Revision++
	m.state = s
	if err := m.persist(s); err != nil {
		cancel()
		unlockState(f)
		f.Close()
		return nil, err
	}
	return m, nil
}

func (m *Management) cloneLocked() ManagedState {
	var s ManagedState
	_ = decodeJSON(encodeJSON(m.state), &s)
	return s
}

func (m *Management) commitLocked(s ManagedState) error {
	s.Revision = m.state.Revision + 1
	if err := m.persist(s); err != nil {
		m.alert = "管理状态写入失败；未提交变更"
		return fmt.Errorf("管理状态写入失败")
	}
	m.state = s
	return nil
}

func (m *Management) event(op *Operation) {
	m.g.audit.Print(string(encodeJSON(map[string]any{"kind": "management", "operation_id": op.ID, "source": op.Source, "target": op.Target, "phase": op.Phase, "outcome": op.Outcome, "error": op.Error, "route_version": m.state.RouteVersion, "duration_ms": time.Since(op.StartedAt).Milliseconds()})))
}

func (m *Management) status() ManagementStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneFailuresLocked()
	s := ManagementStatus{TestControlVersion: 1, ManagedState: m.cloneLocked(), Counts: map[string]BackendCounters{}, Order: append([]string(nil), m.state.Registry.Order...), Busy: m.busy, Alert: m.alert, Failures: len(m.failures)}
	if s.TestOccupancy != nil {
		s.TestOccupancy.TokenHash = ""
	}
	s.ReleasedTestTokens = nil
	s.Registry = nil // Persisted credentials must never enter public status responses.
	for _, op := range s.Operations {
		op.Fingerprint = ""
	}
	for name, count := range m.counts {
		s.Counts[name] = *count
	}
	return s
}

func (m *Management) stop() { m.mu.Lock(); m.closed = true; m.cancel(); m.mu.Unlock() }

func (m *Management) close() {
	m.stop()
	m.wg.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.cloneLocked()
	s.CleanShutdown = true
	if m.busy != "" {
		s.CleanShutdown = false
	}
	for _, c := range m.counts {
		if c.Business+c.Probes > 0 {
			s.CleanShutdown = false
		}
	}
	_ = m.commitLocked(s)
	unlockState(m.lock)
	_ = m.lock.Close()
}

type backendLease struct {
	m             *Management
	name          string
	runtime       *backendRuntime
	version       uint64
	probe, stream bool
	test          bool
	once          sync.Once
}

func (m *Management) acquire(name string, probe, stream, restore bool) (*backendLease, *GatewayError) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acquireLocked(name, probe, stream, restore)
}

func (m *Management) acquireLocked(name string, probe, stream, restore bool) (*backendLease, *GatewayError) {
	if m.closed {
		return nil, fault(503, "gateway_stopping", "网关正在退出")
	}
	if name == "" {
		name = m.state.ActiveBackend
	}
	b := m.state.Backends[name]
	if b == nil || (b.Maintenance && !restore) {
		return nil, fault(503, "backend_in_maintenance", "后端处于维护锁定状态")
	}
	if err := m.checkRouterLocked(m.state.Registry.Configs[name]); err != nil {
		return nil, fault(503, "gateway_target_forbidden", "目标为网关，须迁移为 Router")
	}
	c := m.counts[name]
	if probe {
		c.Probes++
	} else {
		c.Business++
		if stream {
			c.Streams++
		}
	}
	return &backendLease{m: m, name: name, runtime: m.g.pool.snapshot(name), version: m.state.RouteVersion, probe: probe, stream: stream}, nil
}

func (l *backendLease) release(outcome string) {
	l.once.Do(func() {
		m := l.m
		m.mu.Lock()
		defer m.mu.Unlock()
		c := m.counts[l.name]
		if l.test {
			c.Tests--
			s := m.cloneLocked()
			if s.TestOccupancy != nil {
				s.TestOccupancy.Inflight--
				_ = m.persistTestInflightLocked(s)
			}
		}
		if l.probe {
			c.Probes--
		} else {
			c.Business--
			if l.stream {
				c.Streams--
			}
			if outcome != "success" && outcome != "ignored" {
				c.Abnormal++
				c.LastError = outcome
			}
			m.observeLocked(l.name, l.version, outcome)
		}
		m.finishDrainedLocked()
	})
}

func operationByID(s *ManagedState, id string) *Operation {
	for _, op := range s.Operations {
		if op.ID == id {
			return op
		}
	}
	return nil
}

func (m *Management) finishDrainedLocked() {
	if m.closed {
		return
	}
	for _, op := range m.state.Operations {
		if op.Outcome == "drain_timeout" && op.Source != m.state.ActiveBackend && !m.state.Backends[op.Source].DrainUnknown && m.counts[op.Source].Business+m.counts[op.Source].Probes == 0 {
			s := m.cloneLocked()
			next := operationByID(&s, op.ID)
			next.Outcome, next.Phase, next.Error, next.UpdatedAt = "completed", "completed", "", time.Now()
			if m.commitLocked(s) == nil {
				m.event(next)
			}
			return
		}
	}
}
