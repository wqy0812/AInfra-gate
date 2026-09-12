package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type OperationRequest struct {
	OperationID      string `json:"operation_id"`
	Target           string `json:"target"`
	ExpectedRevision uint64 `json:"expected_revision"`
}

type ManagementError struct {
	Status      int    `json:"-"`
	Code        string `json:"code"`
	Message     string `json:"message"`
	OperationID string `json:"operation_id,omitempty"`
}

func (e *ManagementError) Error() string { return e.Message }

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func validOperationID(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for _, r := range s {
		if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_", r) {
			return false
		}
	}
	return true
}

func (m *Management) submit(kind string, req OperationRequest) (*Operation, *ManagementError) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bad := func(status int, code, msg string) (*Operation, *ManagementError) {
		return nil, &ManagementError{status, code, msg, m.busy}
	}
	if !validOperationID(req.OperationID) {
		return bad(400, "invalid_operation_id", "operation_id 须为 1–128 位字母、数字、连字符或下划线")
	}
	if old := operationByID(&m.state, req.OperationID); old != nil {
		if old.Kind != kind || old.Target != req.Target || old.ExpectedRevision != req.ExpectedRevision {
			return bad(409, "operation_id_conflict", "operation_id 已用于不同请求")
		}
		copyState := m.cloneLocked()
		return operationByID(&copyState, old.ID), nil
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
	b := m.state.Backends[req.Target]
	if b == nil {
		return bad(400, "unknown_backend", "后端不存在")
	}
	c := m.counts[req.Target]
	switch kind {
	case "switch", "warmup":
		if b.Maintenance || b.DrainUnknown {
			return bad(409, "backend_locked", "请先恢复维护端或确认旧请求已排空")
		}
		if kind == "switch" && req.Target == m.state.ActiveBackend {
			return bad(409, "already_active", "该后端已经接收业务流量")
		}
	case "maintenance":
		if req.Target == m.state.ActiveBackend || c.Business+c.Probes != 0 || b.DrainUnknown {
			return bad(409, "backend_not_drained", "后端仍在服务、存在占用或排空尚未确认")
		}
	case "restore":
		if !b.Maintenance {
			return bad(409, "not_in_maintenance", "该后端未进入维护")
		}
	case "confirm-drain":
		if req.Target == m.state.ActiveBackend || c.Business+c.Probes != 0 || !b.DrainUnknown {
			return bad(409, "drain_confirmation_unavailable", "只有非活动、无占用且排空待确认的后端可确认")
		}
	default:
		return bad(400, "invalid_operation", "操作不支持")
	}
	op := &Operation{ID: req.OperationID, Kind: kind, Source: m.state.ActiveBackend, Target: req.Target, ExpectedRevision: req.ExpectedRevision, Phase: "accepted", Outcome: "running", StartedAt: time.Now(), UpdatedAt: time.Now()}
	s := m.cloneLocked()
	s.Operations = append(s.Operations, op)
	if len(s.Operations) > 100 {
		s.Operations = s.Operations[len(s.Operations)-100:]
	}
	if kind == "maintenance" || kind == "confirm-drain" {
		if kind == "maintenance" {
			s.Backends[req.Target].Maintenance = true
			if s.RollbackCandidate == req.Target {
				s.RollbackArmed = false
			}
		} else {
			s.Backends[req.Target].DrainUnknown = false
		}
		op.Phase, op.Outcome = "completed", "completed"
	}
	if err := m.commitLocked(s); err != nil {
		return bad(500, "state_write_failed", err.Error())
	}
	m.event(op)
	copyOp := *op
	if kind == "confirm-drain" {
		m.finishDrainedLocked()
	}
	if op.Outcome == "running" {
		m.busy = op.ID
		m.wg.Add(1)
		go m.runOperation(op.ID)
	}
	return &copyOp, nil
}

func (m *Management) updateOperation(id, phase string, report *WarmupReport) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.cloneLocked()
	op := operationByID(&s, id)
	if op == nil || m.busy != id {
		return fmt.Errorf("管理操作已失效")
	}
	op.Phase, op.UpdatedAt = phase, time.Now()
	if report != nil {
		var snapshot WarmupReport
		_ = decodeJSON(encodeJSON(report), &snapshot)
		op.Report = &snapshot
	}
	if err := m.commitLocked(s); err != nil {
		return err
	}
	m.event(op)
	return nil
}

func (m *Management) finishOperation(id, outcome, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.cloneLocked()
	op := operationByID(&s, id)
	if op == nil {
		return
	}
	op.Phase, op.Outcome, op.Error, op.UpdatedAt = outcome, outcome, message, time.Now()
	if err := m.commitLocked(s); err != nil {
		// Do not declare completion or release the operation barrier on a failed commit.
		m.alert = "操作结果写入失败，已停止管理操作；修复存储后重启并核对下游"
		return
	}
	m.event(op)
	if message != "" {
		m.alert = message
	}
	if m.busy == id {
		m.busy = ""
	}
	if !m.closed && op.Kind != "rollback" {
		m.maybeRollbackLocked()
	}
}

func (m *Management) runOperation(id string) {
	defer m.wg.Done()
	m.mu.Lock()
	s := m.cloneLocked()
	op := operationByID(&s, id)
	m.mu.Unlock()
	if op == nil {
		return
	}
	report, err := m.warmup(m.ctx, op.Target, op.Kind == "restore", func(r *WarmupReport) error { return m.updateOperation(id, "warming", r) })
	if err != nil {
		_ = m.updateOperation(id, "warming", report)
		_ = m.saveWarmup(op.Target, report)
		m.finishOperation(id, "failed", err.Error())
		return
	}
	if err = m.saveWarmup(op.Target, report); err != nil {
		m.finishOperation(id, "failed", err.Error())
		return
	}
	if err = m.updateOperation(id, "verified", report); err != nil {
		m.finishOperation(id, "failed", err.Error())
		return
	}
	if m.ctx.Err() != nil {
		m.finishOperation(id, "interrupted", "网关退出，操作中止")
		return
	}
	if op.Kind == "warmup" {
		m.finishOperation(id, "completed", "")
		return
	}
	if op.Kind == "restore" {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			m.finishOperation(id, "interrupted", "网关退出，恢复验证中止")
			return
		}
		next := m.cloneLocked()
		next.Backends[op.Target].Maintenance = false
		next.Backends[op.Target].DrainUnknown = false
		next.RollbackCandidate, next.RollbackArmed = op.Target, m.g.config.Management.rollbackEnabled()
		err = m.commitLocked(next)
		m.mu.Unlock()
		if err != nil {
			m.finishOperation(id, "failed", err.Error())
		} else {
			m.finishOperation(id, "completed", "")
		}
		return
	}
	m.mu.Lock()
	if m.closed || m.busy != id || m.state.ActiveBackend != op.Source || m.state.RouteVersion != s.RouteVersion || m.state.Backends[op.Target].Maintenance || m.state.Backends[op.Target].DrainUnknown || (op.Kind == "rollback" && !m.rollbackNeededLocked()) {
		m.mu.Unlock()
		m.finishOperation(id, "cancelled", "切流条件已经变化，未提交路由")
		return
	}
	next := m.cloneLocked()
	next.ActiveBackend = op.Target
	next.RouteVersion++
	next.RollbackCandidate = op.Source
	next.RollbackArmed = op.Kind == "switch" && m.g.config.Management.rollbackEnabled() && !next.Backends[op.Source].DrainUnknown
	nextOp := operationByID(&next, id)
	nextOp.RouteCommitted = true
	nextOp.Phase = "draining"
	nextOp.UpdatedAt = time.Now()
	err = m.commitLocked(next)
	if err == nil {
		m.failures = nil
		m.alert = ""
		m.event(nextOp)
	}
	m.mu.Unlock()
	if err != nil {
		m.finishOperation(id, "failed", err.Error())
		return
	}
	deadline := time.NewTimer(seconds(m.g.config.Management.DrainTimeoutSeconds))
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		m.mu.Lock()
		count := m.counts[op.Source]
		unknown := m.state.Backends[op.Source].DrainUnknown
		drained := count.Business+count.Probes == 0 && !unknown
		rollback := op.Kind == "switch" && m.rollbackNeededLocked()
		m.mu.Unlock()
		if rollback {
			m.finishOperation(id, "rollback_requested", "目标端连续故障，启动自动回切")
			return
		}
		if drained {
			m.finishOperation(id, "completed", "")
			return
		}
		if unknown {
			m.finishOperation(id, "drain_timeout", "新请求已切流，原端旧请求需人工确认排空")
			return
		}
		select {
		case <-m.ctx.Done():
			m.finishOperation(id, "interrupted", "网关退出，排空未完成")
			return
		case <-deadline.C:
			m.finishOperation(id, "drain_timeout", "新请求已切流，原端仍未排空")
			return
		case <-tick.C:
		}
	}
}

func (m *Management) saveWarmup(name string, report *WarmupReport) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.cloneLocked()
	s.Backends[name].LastWarmup = report
	return m.commitLocked(s)
}

func (m *Management) pruneFailuresLocked() {
	cutoff := time.Now().Add(-seconds(m.g.config.Management.FailureWindowSeconds))
	for len(m.failures) > 0 && m.failures[0].Before(cutoff) {
		m.failures = m.failures[1:]
	}
}

func (m *Management) rollbackNeededLocked() bool {
	m.pruneFailuresLocked()
	b := m.state.Backends[m.state.RollbackCandidate]
	return !m.closed && m.g.config.Management.rollbackEnabled() && m.state.RollbackArmed && b != nil && !b.Maintenance && !b.DrainUnknown && len(m.failures) >= m.g.config.Management.FailureThreshold
}

func (m *Management) observeLocked(name string, version uint64, outcome string) {
	if version != m.state.RouteVersion || name != m.state.ActiveBackend {
		return
	}
	if outcome == "success" {
		m.failures = nil
		return
	}
	if outcome != "backend_failure" {
		if outcome == "protocol_unknown" || outcome == "upstream_rejected" {
			m.alert = "下游响应异常，请检查后端详情"
		}
		return
	}
	m.pruneFailuresLocked()
	m.failures = append(m.failures, time.Now())
	if len(m.failures) > m.g.config.Management.FailureThreshold {
		m.failures = m.failures[len(m.failures)-m.g.config.Management.FailureThreshold:]
	}
	m.alert = "当前后端出现故障"
	m.maybeRollbackLocked()
}

func (m *Management) maybeRollbackLocked() {
	if m.busy != "" || !m.rollbackNeededLocked() || time.Now().Before(m.nextRollback) {
		return
	}
	s := m.cloneLocked()
	op := &Operation{ID: randomID(), Kind: "rollback", Source: s.ActiveBackend, Target: s.RollbackCandidate, ExpectedRevision: s.Revision, Phase: "accepted", Outcome: "running", StartedAt: time.Now(), UpdatedAt: time.Now()}
	s.Operations = append(s.Operations, op)
	if len(s.Operations) > 100 {
		s.Operations = s.Operations[len(s.Operations)-100:]
	}
	if m.commitLocked(s) != nil {
		return
	}
	m.busy = op.ID
	m.nextRollback = time.Now().Add(seconds(m.g.config.Management.RollbackRetrySeconds))
	m.event(op)
	m.wg.Add(1)
	go m.runOperation(op.ID)
}
