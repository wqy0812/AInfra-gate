package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"time"
)

// Occupancy never expires on loss of contact. Persistent inflight requests make
// an unclean restart fail closed until their termination can be established.
type TestOccupancy struct {
	TaskID        string    `json:"task_id"`
	Backend       string    `json:"backend"`
	Model         string    `json:"model"`
	RouteVersion  uint64    `json:"route_version"`
	ConfigVersion uint64    `json:"config_version"`
	TokenHash     string    `json:"token_hash,omitempty"`
	Inflight      int       `json:"inflight"`
	Releasing     bool      `json:"releasing"`
	StartedAt     time.Time `json:"started_at"`
}
type TestOccupancyRequest struct {
	TaskID        string `json:"task_id"`
	Backend       string `json:"backend,omitempty"`
	Model         string `json:"model,omitempty"`
	RouteVersion  uint64 `json:"route_version,omitempty"`
	ConfigVersion uint64 `json:"config_version,omitempty"`
	Token         string `json:"token"`
}

func testHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}
func sameTest(o *TestOccupancy, r TestOccupancyRequest) bool {
	return o.TaskID == r.TaskID && subtle.ConstantTimeCompare([]byte(o.TokenHash), []byte(testHash(r.Token))) == 1
}
func publicTest(o *TestOccupancy) *TestOccupancy {
	if o == nil {
		return nil
	}
	c := *o
	c.TokenHash = ""
	return &c
}
func (m *Management) testControl(command string, r TestOccupancyRequest) (any, *ManagementError) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bad := func(code string) (any, *ManagementError) {
		return nil, &ManagementError{Status: 409, Code: code, Message: "测试占用校验未通过：" + code}
	}
	if !validOperationID(r.TaskID) || len(r.Token) < 32 || len(r.Token) > 256 {
		return bad("invalid_test_identity")
	}
	o := m.state.TestOccupancy
	if command == "test-status" {
		if o == nil {
			return map[string]bool{"released": true}, nil
		}
		if !sameTest(o, r) {
			return bad("test_occupied")
		}
		return publicTest(o), nil
	}
	if command == "test-release" {
		if o == nil || !sameTest(o, r) {
			// Persist a tombstone even if acquisition has not arrived yet. A
			// delayed/retried acquire must never recreate a released lease.
			s := m.cloneLocked()
			if s.ReleasedTestTokens == nil {
				s.ReleasedTestTokens = map[string]bool{}
			}
			hash := testHash(r.Token)
			if !s.ReleasedTestTokens[hash] {
				s.ReleasedTestTokens[hash] = true
				if m.commitLocked(s) != nil {
					return bad("state_write_failed")
				}
			}
			return map[string]bool{"released": true}, nil
		}
		if !sameTest(o, r) {
			return bad("test_occupied")
		}
		s := m.cloneLocked()
		s.TestOccupancy.Releasing = true
		if o.Inflight == 0 {
			if s.ReleasedTestTokens == nil {
				s.ReleasedTestTokens = map[string]bool{}
			}
			s.ReleasedTestTokens[testHash(r.Token)] = true
			s.TestOccupancy = nil
		}
		if m.commitLocked(s) != nil {
			return bad("state_write_failed")
		}
		if s.TestOccupancy != nil {
			return publicTest(s.TestOccupancy), nil
		}
		m.maybeRollbackLocked()
		return map[string]bool{"released": true}, nil
	}
	if m.state.ReleasedTestTokens[testHash(r.Token)] {
		return bad("test_already_released")
	}
	if o != nil {
		if sameTest(o, r) && o.Backend == r.Backend && o.Model == r.Model && o.RouteVersion == r.RouteVersion && o.ConfigVersion == r.ConfigVersion && !o.Releasing {
			return publicTest(o), nil
		}
		return bad("test_occupied")
	}
	if m.closed || m.busy != "" {
		return bad("operation_in_progress")
	}
	if r.Backend != m.state.ActiveBackend || r.RouteVersion != m.state.RouteVersion || r.ConfigVersion != m.state.Registry.Version {
		return bad("test_target_changed")
	}
	if m.state.Backends[r.Backend].DrainUnknown {
		return bad("drain_unknown")
	}
	if err := m.checkRouterLocked(m.state.Registry.Configs[r.Backend]); err != nil {
		return bad("gateway_target_forbidden")
	}
	// Discovery is performed by engine before acquisition; pin its exact model.
	if r.Model == "" {
		return bad("invalid_test_model")
	}
	s := m.cloneLocked()
	s.TestOccupancy = &TestOccupancy{TaskID: r.TaskID, Backend: r.Backend, Model: r.Model, RouteVersion: r.RouteVersion, ConfigVersion: r.ConfigVersion, TokenHash: testHash(r.Token), StartedAt: time.Now()}
	if m.commitLocked(s) != nil {
		return bad("state_write_failed")
	}
	return publicTest(s.TestOccupancy), nil
}
func (m *Management) acquireRequest(token string, stream bool) (*backendLease, *GatewayError) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if token == "" {
		return m.acquireLocked("", false, stream, false)
	}
	o := m.state.TestOccupancy
	if o == nil || o.Releasing || m.state.Backends[m.state.ActiveBackend].DrainUnknown || subtle.ConstantTimeCompare([]byte(o.TokenHash), []byte(testHash(token))) != 1 || o.Backend != m.state.ActiveBackend || o.RouteVersion != m.state.RouteVersion || o.ConfigVersion != m.state.Registry.Version {
		return nil, fault(409, "test_target_changed", "测试占用或目标版本已变化")
	}
	lease, err := m.acquireLocked("", false, stream, false)
	if err != nil {
		return nil, err
	}
	s := m.cloneLocked()
	s.TestOccupancy.Inflight++
	if m.persistTestInflightLocked(s) != nil {
		c := m.counts[lease.name]
		c.Business--
		if stream {
			c.Streams--
		}
		return nil, fault(503, "state_write_failed", "无法持久化测试请求占用")
	}
	lease.test = true
	m.counts[lease.name].Tests++
	return lease, nil
}

// Inflight counters are durable but do not alter routing/configuration revision.
func (m *Management) persistTestInflightLocked(s ManagedState) error {
	if err := m.persist(s); err != nil {
		m.alert = "测试请求占用持久化失败，保留占用等待核对"
		return err
	}
	m.state = s
	return nil
}
