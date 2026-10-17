package aigate

import (
	"bytes"
	"io"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openRecoveredReplay(t *testing.T, recordRoot, dataRoot string) *replayService {
	t.Helper()
	svc, err := newReplayService(strings.Repeat("r", 32), []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}, recordRoot, dataRoot,
		map[string]string{"r1": "http://127.0.0.1:1"}, testMarkKey, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestReplayRecoveryRetainsSyncedVerification(t *testing.T) {
	root, req := syntheticReplay(t, 2, 30, `{}`)
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"usage":{"completion_tokens":800}}`)
	})
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	f.svc.start(req)
	t.Cleanup(func() { f.svc.stop(req.RunID) })
	deadline := time.Now().Add(5 * time.Second)
	for {
		live, _ := f.svc.loadRun(req.RunID)
		if live.Progress.Completed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first request did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Preserve the exact files visible during a live run, without the final
	// state write performed by a graceful shutdown. Reopen only this snapshot.
	restartRoot := t.TempDir()
	if err := os.CopyFS(restartRoot, os.DirFS(f.svc.dataRoot)); err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(restartRoot, "runs", req.RunID, "results.jsonl")
	before, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	restored := openRecoveredReplay(t, root, restartRoot)
	view, ok := restored.loadRun(req.RunID)
	restored.Close()
	if !ok || view.State != "end_unknown" || view.EndReason != "service_restarted" ||
		view.Output.Matched != 0 || view.Output.ActualTokens != 800 || !view.Output.Incomplete || view.Output.ResultsFailed {
		t.Fatalf("lost synced verification: %s", encodeJSON(view))
	}
	if view.Progress.Sent != 1 || view.Progress.Completed != 1 || view.Progress.Unsent != 0 || view.Progress.Unconfirmed != 1 {
		t.Fatalf("invented dispatch outcome: %+v", view.Progress)
	}
	after, err := os.ReadFile(resultPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("recovery changed result evidence", err)
	}
	// A second restart preserves the recovered terminal snapshot exactly.
	reopened := openRecoveredReplay(t, root, restartRoot)
	defer reopened.Close()
	again, _ := reopened.loadRun(req.RunID)
	if !bytes.Equal(encodeJSON(view), encodeJSON(again)) || len(reopened.active) != 0 {
		t.Fatal("recovery was not idempotent")
	}
}

func TestReplayRecoveryUsesOnlyCompleteUniqueResults(t *testing.T) {
	actual, sent := uint64(800), 0.1
	matched := replayResult{Sequence: 2, RequestID: "new-2", OriginalRequestID: "old-2", SendOffset: &sent, HTTPStatus: 200,
		Verification: "matched", ActualTokens: &actual}
	line := string(append(encodeJSON(matched), '\n'))
	for _, test := range []struct {
		name, suffix                string
		missing, unreadable, failed bool
	}{
		{name: "clean"},
		{name: "partial_json", suffix: `{"sequence":1`, failed: true},
		{name: "no_newline", suffix: string(encodeJSON(matched)), failed: true},
		{name: "malformed", suffix: "invalid\n", failed: true},
		{name: "duplicate", suffix: line, failed: true},
		{name: "oversized", suffix: strings.Repeat("x", recordingSmallFile), failed: true},
		{name: "missing", missing: true},
		{name: "unreadable", unreadable: true, failed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := t.TempDir()
			dir := filepath.Join(data, "runs", "old")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			view := replayRunView{RunID: "old", State: "running", CreatedAt: time.Now(), Request: &replayRequest{},
				Progress: replayProgress{Planned: 3}}
			if _, err := recordingAtomicJSON(dir, "state.json", view); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "results.jsonl")
			content := line + test.suffix
			if test.unreadable {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			} else if !test.missing {
				if err := os.WriteFile(path, []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			svc := openRecoveredReplay(t, t.TempDir(), data)
			defer svc.Close()
			got, _ := svc.loadRun("old")
			known := uint64(1)
			if test.missing || test.unreadable {
				known = 0
			}
			if got.Output.Matched != known || got.Output.ActualTokens != known*800 ||
				got.Output.ResultsFailed != test.failed || !got.Output.Incomplete ||
				got.Progress.Unconfirmed != 3-int64(known) || got.Progress.Unsent != 0 {
				t.Fatalf("%s", encodeJSON(got))
			}
			if !test.missing && !test.unreadable {
				if raw, err := os.ReadFile(path); err != nil || string(raw) != content {
					t.Fatal("recovery modified the log", err)
				}
			}
		})
	}
}

func TestReplayRecoveryRestoresAllResultClasses(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "runs", "old")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	view := replayRunView{RunID: "old", State: "stopping", CreatedAt: time.Now(), Request: &replayRequest{}, Progress: replayProgress{Planned: 4}}
	if _, err := recordingAtomicJSON(dir, "state.json", view); err != nil {
		t.Fatal(err)
	}
	sent, short, long := 0.1, uint64(100), uint64(900)
	results := []replayResult{
		{Sequence: 3, RequestID: "new3", OriginalRequestID: "old3", SendOffset: &sent, Error: "http_400", Verification: "unverifiable"},
		{Sequence: 1, RequestID: "new1", OriginalRequestID: "old1", SendOffset: &sent, HTTPStatus: 200, Verification: "short", ActualTokens: &short},
		{Sequence: 4, RequestID: "new4", OriginalRequestID: "old4", Error: "not_sent", Verification: "unverifiable"},
		{Sequence: 2, RequestID: "new2", OriginalRequestID: "old2", SendOffset: &sent, HTTPStatus: 200, Verification: "long", ActualTokens: &long},
	}
	var log bytes.Buffer
	for _, result := range results {
		log.Write(encodeJSON(result))
		log.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "results.jsonl"), log.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	svc := openRecoveredReplay(t, t.TempDir(), data)
	defer svc.Close()
	got, _ := svc.loadRun("old")
	want := replayOutputSummary{Short: 1, Long: 1, Unverifiable: 2, ActualTokens: 1000, Unknown: 2}
	if got.Output != want || got.Progress.Sent != 3 || got.Progress.Completed != 2 || got.Progress.Failed != 1 ||
		got.Progress.Unsent != 1 || got.Progress.Unconfirmed != 0 || got.State != "end_unknown" {
		t.Fatalf("%s", encodeJSON(got))
	}
	if !bytes.Contains(encodeJSON(got.Progress), []byte(`"unconfirmed":0`)) {
		t.Fatal("known zero must not look like an unavailable recovery count")
	}
}
