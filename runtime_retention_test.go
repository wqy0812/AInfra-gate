package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func retentionFile(t *testing.T, dir, name string, size int, at time.Time) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(strings.Repeat("x", size)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRuntimeRetentionAfterRotationAndRestart(t *testing.T) {
	c := controlledFixture(t)
	p := newProtocolBackend(t)
	s := snapshotV2("r1", p.server.URL, 1)
	s.Settings = json.RawMessage(`{"profile":{"enabled":true,"api_key":"old","retention_hours":72}}`)
	activateV2(t, c, "initial", s)
	old := c.active.g.profile
	now := time.Now()
	path := retentionFile(t, old.c.Directory, "events-v2-1000000000000-review.jsonl", 100, now)
	s.BindingRevision = 2
	s.Settings = json.RawMessage(`{"profile":{"enabled":true,"api_key":"new","retention_hours":1}}`)
	activateV2(t, c, "rotate", s)
	c.waitClosingObservers() // drained observers close asynchronously, outside c.mu
	if !old.closed {
		t.Fatal("old writer still running")
	}
	// Age the files only after their writer closes, then simulate an Engine-
	// controlled gateway restart. No in-memory list of old directories survives.
	if err := os.Chtimes(path, now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	c.Close()
	restarted := controlledFixture(t)
	restarted.dataRoot = c.dataRoot
	activateV2(t, restarted, "restore", s)
	if err := restarted.cleanupObservations(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("old expired segment was retained", err)
	}
	if restarted.active.g.profile.closed {
		t.Fatal("active writer was closed")
	}
}

func TestObservationBudgetIncludesLiveAndAllAbandonedPolicies(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	live := filepath.Join(root, "1111111111111111")
	active := retentionFile(t, live, "events-v2-live.jsonl", 60, now)
	old := retentionFile(t, filepath.Join(root, "2222222222222222"), "events-v2-old.jsonl", 30, now.Add(-time.Minute))
	newer := retentionFile(t, filepath.Join(root, "3333333333333333"), "events-v2-new.jsonl", 30, now)
	if err := cleanObservationRoot(root, false, now, 72, 100, map[string]bool{live: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("global quota did not evict oldest abandoned segment")
	}
	for _, path := range []string{active, newer} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRetentionProtectsDrainingGenerationUntilRelease(t *testing.T) {
	c := controlledFixture(t)
	p := newProtocolBackend(t)
	s := snapshotV2("r1", p.server.URL, 1)
	s.Settings = json.RawMessage(`{"profile":{"enabled":true,"api_key":"old"}}`)
	activateV2(t, c, "initial", s)
	c.mu.Lock()
	old := c.active
	old.refs++
	c.mu.Unlock()
	s.BindingRevision = 2
	s.Settings = json.RawMessage(`{"profile":{"enabled":true,"api_key":"new","retention_hours":1}}`)
	activateV2(t, c, "rotate", s)
	// Using a future cleanup clock avoids the live writer's own timer pruning
	// this file, while making it expired for the cross-generation collector.
	now := time.Now()
	path := retentionFile(t, old.g.profile.c.Directory, "events-v2-draining.jsonl", 100, now)
	if err := c.cleanupObservations(now.Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("draining writer data removed", err)
	}
	c.mu.Lock()
	old.refs--
	c.cleanupLocked()
	c.mu.Unlock()
	if err := c.cleanupObservations(now.Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("closed generation escaped retention")
	}
}

func TestSlowRetentionAcrossPoliciesAndAfterCaptureDisabled(t *testing.T) {
	c := controlledFixture(t)
	p := newProtocolBackend(t)
	s := snapshotV2("r1", p.server.URL, 1)
	s.Settings = json.RawMessage(`{"slow_requests":{"enabled":true,"retention_hours":72}}`)
	activateV2(t, c, "initial", s)
	store := c.active.g.slow
	now := time.Now()
	job := slowTestJob(1, now.Add(-2*time.Hour), "body")
	if err := store.writeRecord(job); err != nil {
		t.Fatal(err)
	}
	s.BindingRevision = 2
	s.Settings = json.RawMessage(`{"slow_requests":{"enabled":false,"retention_hours":1}}`)
	activateV2(t, c, "disable", s)
	if err := c.cleanupObservations(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(store.c.Directory, "request-"+job.meta.RequestID)); !os.IsNotExist(err) {
		t.Fatal("disabled capture leaked old records")
	}
}

func TestRetentionDoesNotFollowSymlinksOrDeleteForeignFiles(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	external := t.TempDir()
	outside := retentionFile(t, external, "events-v2-outside.jsonl", 100, now.Add(-3*time.Hour))
	if err := os.Symlink(external, filepath.Join(root, "1111111111111111")); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "2222222222222222")
	foreign := retentionFile(t, dir, "keep.txt", 100, now)
	if err := os.Symlink(outside, filepath.Join(dir, "events-v2-link.jsonl")); err != nil {
		t.Fatal(err)
	}
	if err := cleanObservationRoot(root, false, now, 1, 1, nil); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{outside, foreign} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRetentionRespectsOtherProcessDirectoryLock(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	dir := filepath.Join(root, "1111111111111111")
	path := retentionFile(t, dir, "events-v2-locked.jsonl", 100, now.Add(-3*time.Hour))
	f, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = lockState(f); err != nil {
		t.Fatal(err)
	}
	if err = cleanObservationRoot(root, false, now, 1, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("locked directory modified", err)
	}
	unlockState(f)
	if err = cleanObservationRoot(root, false, now, 1, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("unlocked directory not collected")
	}
}
