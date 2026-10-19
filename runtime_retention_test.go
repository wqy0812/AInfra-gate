package aigate

import (
	"encoding/json"
	"io"
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

func retentionProxy(t *testing.T, root, settings string) *ProxyGateway {
	t.Helper()
	p, err := newProxyGateway(ProxyConfig{ServiceID: "retention-proxy", RouterID: "r1",
		Backend:  &Backend{Adapter: "openai", URL: "http://127.0.0.1:1/v1/chat/completions", ModelsURL: "http://127.0.0.1:1/v1/models"},
		Settings: json.RawMessage(settings)}, root, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func awaitRetention(t *testing.T, path string) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); ; {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("expired observation retained", path)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestProxyRetentionAfterPolicyChangeAndRestart(t *testing.T) {
	root := t.TempDir()
	old := retentionProxy(t, root, `{"profile":{"enabled":true,"retention_hours":72}}`)
	now := time.Now()
	path := retentionFile(t, old.g.profile.c.Directory, "events-v2-1000000000000-review.jsonl", 100, now)
	old.Close()
	if !old.g.profile.closed {
		t.Fatal("old proxy writer still running")
	}
	if err := os.Chtimes(path, now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	restarted := retentionProxy(t, root, `{"profile":{"enabled":true,"retention_hours":1}}`)
	awaitRetention(t, path)
	if restarted.g.profile.closed {
		t.Fatal("active proxy writer closed")
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

func TestRetentionProtectsLiveProxyWriterUntilClose(t *testing.T) {
	root := t.TempDir()
	p := retentionProxy(t, root, `{"profile":{"enabled":true}}`)
	now := time.Now()
	path := retentionFile(t, p.g.profile.c.Directory, "events-v2-live-proxy.jsonl", 100, now)
	clean := func() {
		if err := cleanObservationRoot(filepath.Join(root, "request-profile"), false, now.Add(2*time.Hour), 1, 1<<20, nil); err != nil {
			t.Fatal(err)
		}
	}
	clean()
	if _, err := os.Stat(path); err != nil {
		t.Fatal("live proxy writer data removed", err)
	}
	p.Close()
	clean()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("closed proxy policy escaped retention")
	}
}

func TestSlowRetentionAfterProxyRestartWithCaptureDisabled(t *testing.T) {
	root := t.TempDir()
	p := retentionProxy(t, root, `{"slow_requests":{"enabled":true,"retention_hours":72}}`)
	store := p.g.slow
	now := time.Now()
	job := slowTestJob(1, now.Add(-74*time.Hour), "body")
	if err := store.writeRecord(job); err != nil {
		t.Fatal(err)
	}
	p.Close()
	inactive := retentionProxy(t, root, `{"slow_requests":{"enabled":false,"retention_hours":1}}`)
	if _, err := diagnosticReference(filepath.Join(store.c.Directory, "request-"+job.meta.RequestID)); !os.IsNotExist(err) {
		t.Fatal("startup performed maintenance")
	}
	inactive.maintainInactiveStorage(time.Now())
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := diagnosticReference(filepath.Join(store.c.Directory, "request-"+job.meta.RequestID)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("disabled capture historical record not archived")
		}
		time.Sleep(time.Millisecond)
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
