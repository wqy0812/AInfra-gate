package main

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestOccupancyPinsAllSwitchesAndDrains(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	g, _, _ := serveTest(t, managedConfig(t, one, two))
	m := g.management
	s := m.status()
	r := TestOccupancyRequest{TaskID: "task-a", Backend: s.ActiveBackend, Model: "model", RouteVersion: s.RouteVersion, ConfigVersion: m.state.Registry.Version, Token: strings.Repeat("x", 40)}
	if _, err := m.testControl("test-acquire", r); err != nil {
		t.Fatal(err)
	}
	if _, err := m.testControl("test-acquire", r); err != nil {
		t.Fatal("idempotency", err)
	}
	if strings.Contains(string(encodeJSON(m.status())), testHash(r.Token)) {
		t.Fatal("token hash leaked")
	}
	_, err := m.control(controlRequest{Command: "switch", Payload: encodeJSON(OperationRequest{OperationID: "blocked", Target: "api2", ExpectedRevision: m.status().Revision})})
	if err == nil || err.Code != "test_occupied" {
		t.Fatal("switch accepted", err)
	}
	if m.rollbackNeededLocked() {
		t.Fatal("rollback during test")
	}
	testLease, e := m.acquireRequest(r.Token, true)
	if e != nil {
		t.Fatal(e)
	}
	business, e := m.acquireRequest("", false)
	if e != nil {
		t.Fatal(e)
	}
	counts := m.status().Counts[s.ActiveBackend]
	if counts.Business != 2 || counts.Tests != 1 {
		t.Fatal(counts)
	}
	if _, e = m.acquireRequest("wrong", false); e == nil {
		t.Fatal("invalid token admitted")
	}
	if _, err = m.testControl("test-release", r); err != nil {
		t.Fatal(err)
	}
	if m.status().TestOccupancy == nil {
		t.Fatal("released inflight test")
	}
	if _, e = m.acquireRequest(r.Token, false); e == nil {
		t.Fatal("admitted after release began")
	}
	testLease.release("success")
	if _, err = m.testControl("test-release", r); err != nil {
		t.Fatal(err)
	}
	if m.status().TestOccupancy != nil {
		t.Fatal("test not released")
	}
	if m.status().Counts[s.ActiveBackend].Business != 1 {
		t.Fatal("business drain incorrectly released")
	}
	if _, e := m.testControl("test-acquire", r); e == nil || e.Code != "test_already_released" {
		t.Fatal("delayed acquire revived released lease", e)
	}
	late := r
	late.TaskID = "late"
	late.Token = strings.Repeat("q", 40)
	if _, e := m.testControl("test-release", late); e != nil {
		t.Fatal(e)
	}
	if _, e := m.testControl("test-acquire", late); e == nil {
		t.Fatal("out of order acquire accepted")
	}
	business.release("success")
}

func TestOccupancyDurableAndTargetChecks(t *testing.T) {
	one := newProtocolBackend(t)
	c := managedConfig(t, one)
	g, _, _ := serveTest(t, c)
	m := g.management
	r := TestOccupancyRequest{TaskID: "persist", Backend: "api1", Model: "m", RouteVersion: m.status().RouteVersion, ConfigVersion: m.state.Registry.Version, Token: strings.Repeat("z", 40)}
	wrong := r
	wrong.RouteVersion++
	if _, err := m.testControl("test-acquire", wrong); err == nil {
		t.Fatal("changed target accepted")
	}
	if _, err := m.testControl("test-acquire", r); err != nil {
		t.Fatal(err)
	}
	lease, err := m.acquireRequest(r.Token, false)
	if err != nil {
		t.Fatal(err)
	}
	saved, e := readManagedState(c)
	if e != nil {
		t.Fatal(e)
	}
	if saved.TestOccupancy == nil || saved.TestOccupancy.Inflight != 1 || saved.TestOccupancy.TokenHash != testHash(r.Token) {
		t.Fatal("occupancy not durable")
	}
	m.mu.Lock()
	m.state.Backends["api1"].DrainUnknown = true
	m.mu.Unlock()
	if _, e := m.acquireRequest(r.Token, false); e == nil {
		t.Fatal("unknown old traffic admitted")
	}
	lease.release("success")
}

func TestOccupancyRestartConfirmedDrain(t *testing.T) {
	for _, releasing := range []bool{false, true} {
		t.Run(fmt.Sprint("releasing=", releasing), func(t *testing.T) {
			c := managedConfig(t, newProtocolBackend(t), newProtocolBackend(t))
			g, err := newGateway(c, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			m := g.management
			r := TestOccupancyRequest{TaskID: "restart", Backend: "api1", Model: "m", RouteVersion: m.state.RouteVersion, ConfigVersion: m.state.Registry.Version, Token: strings.Repeat("r", 40)}
			if _, e := m.testControl("test-acquire", r); e != nil {
				t.Fatal(e)
			}
			if _, e := m.submit("confirm-drain", OperationRequest{"live-test", "api1", m.status().Revision}); e == nil {
				t.Fatal("a live occupancy without unknown drain must not be cleared")
			}
			if _, e := m.acquireRequest(r.Token, false); e != nil {
				t.Fatal(e)
			}
			if releasing {
				if _, e := m.testControl("test-release", r); e != nil {
					t.Fatal(e)
				}
			}
			// Leave a durable request without its completion callback, as after
			// process loss. close persists clean_shutdown=false with inflight work.
			g.Close()
			g, err = newGateway(c, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			m = g.management
			if m.status().TestOccupancy.Inflight != 1 || !m.status().Backends["api1"].DrainUnknown {
				t.Fatal("restart must preserve unknown inflight")
			}
			if _, e := m.testControl("test-release", r); e != nil {
				t.Fatal(e)
			}
			if m.status().TestOccupancy == nil {
				t.Fatal("release guessed old requests had ended")
			}
			for _, probe := range []bool{false, true} {
				lease, e := m.acquire("api1", probe, false, false)
				if e != nil {
					t.Fatal(e)
				}
				if _, e := m.submit("confirm-drain", OperationRequest{randomID(), "api1", m.status().Revision}); e == nil {
					t.Fatal("confirmation accepted with a current request")
				}
				lease.release("success")
			}
			if _, e := m.submit("confirm-drain", OperationRequest{"stale", "api1", m.status().Revision - 1}); e == nil || e.Code != "stale_revision" {
				t.Fatal("stale recovery accepted", e)
			}
			m.busy = "other-operation"
			if _, e := m.submit("confirm-drain", OperationRequest{"busy", "api1", m.status().Revision}); e == nil || e.Code != "operation_in_progress" {
				t.Fatal("recovery during operation", e)
			}
			m.busy = ""
			persist := m.persist
			m.persist = func(ManagedState) error { return fmt.Errorf("injected write failure") }
			req := OperationRequest{"recover", "api1", m.status().Revision}
			if _, e := m.submit("confirm-drain", req); e == nil {
				t.Fatal("write failure ignored")
			}
			m.persist = persist
			if m.status().TestOccupancy == nil || !m.status().Backends["api1"].DrainUnknown {
				t.Fatal("failed persist partially unlocked state")
			}
			if _, e := m.submit("confirm-drain", req); e != nil {
				t.Fatal(e)
			}
			if _, e := m.submit("confirm-drain", req); e != nil {
				t.Fatal("idempotent retry", e)
			}
			s := m.status()
			if s.TestOccupancy != nil || s.Backends["api1"].DrainUnknown || !s.Backends["api2"].DrainUnknown || s.RouteVersion != r.RouteVersion {
				t.Fatal("recovery must clear only the confirmed backend and keep routing")
			}
			if _, e := m.acquireRequest(r.Token, false); e == nil {
				t.Fatal("old token admitted")
			}
			if _, e := m.testControl("test-acquire", r); e == nil || e.Code != "test_already_released" {
				t.Fatal("old lease revived", e)
			}
			saved, e := readManagedState(c)
			if e != nil || saved.TestOccupancy != nil || !saved.ReleasedTestTokens[testHash(r.Token)] {
				t.Fatal("recovery/tombstone not durable", e)
			}
			// Existing non-active confirmation then switch must work again.
			submitOp(t, m, "confirm-drain", "api2")
			submitOp(t, m, "switch", "api2")
			waitState(t, m, func(s ManagementStatus) bool { return s.ActiveBackend == "api2" && s.Busy == "" })
		})
	}
}

func TestOccupancyFailedCompletionWriteSurvivesCleanRestart(t *testing.T) {
	c := managedConfig(t, newProtocolBackend(t))
	g, err := newGateway(c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	m := g.management
	r := TestOccupancyRequest{TaskID: "failed-completion", Backend: "api1", Model: "m", RouteVersion: m.state.RouteVersion, ConfigVersion: m.state.Registry.Version, Token: strings.Repeat("f", 40)}
	if _, e := m.testControl("test-acquire", r); e != nil {
		t.Fatal(e)
	}
	lease, e := m.acquireRequest(r.Token, false)
	if e != nil {
		t.Fatal(e)
	}
	persist := m.persist
	m.persist = func(ManagedState) error { return fmt.Errorf("injected completion write failure") }
	lease.release("success")
	m.persist = persist
	g.Close()
	saved, err := readManagedState(c)
	if err != nil || !saved.CleanShutdown || saved.TestOccupancy.Inflight != 1 {
		t.Fatal("fixture must retain stale inflight after clean close", err)
	}
	g, err = newGateway(c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	m = g.management
	if !m.status().Backends["api1"].DrainUnknown {
		t.Fatal("stale durable test inflight must remain unknown")
	}
	submitOp(t, m, "confirm-drain", "api1")
	if m.status().TestOccupancy != nil {
		t.Fatal("stale test occupancy not recovered")
	}
}
