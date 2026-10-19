package aigate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDiagnosticCleanupBeyondExportEntryLimit(t *testing.T) {
	root := t.TempDir()
	// Even non-candidate entries used to disable cleanup before it could delete
	// one completed record. They consume no body bytes in this fixture.
	for i := 0; i <= archiveManifestMaxFiles; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("other-%05d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-time.Hour)
	record := captureSeedRecord(t, root, "request-"+randomID(), streamCaptureMeta{Schema: 1, EndedAt: &old, CaptureState: "complete"}, map[string][]byte{"request.json": []byte("raw")})
	if err := maintainDiagnosticStorage(root, time.Now(), 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(record); !os.IsNotExist(err) {
		t.Fatal("completed record was not reclaimed", err)
	}
	// Inactive slow-request maintenance must also traverse the large directory
	// without reintroducing the export limit during its recovery pre-pass.
	parent := t.TempDir()
	policy := filepath.Join(parent, "0123456789abcdef")
	if err := os.Rename(root, policy); err != nil {
		t.Fatal(err)
	}
	job := slowTestJob(1, old, "raw")
	record = filepath.Join(policy, "request-"+job.meta.RequestID)
	if err := os.Mkdir(record, 0700); err != nil {
		t.Fatal(err)
	}
	meta, err := json.Marshal(job.meta)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"meta.json": meta, "request.json": job.body} {
		if err := os.WriteFile(filepath.Join(record, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := maintainInactiveDiagnostics(parent, time.Now(), 1, func() error {
		return recoverSlowDirectories(parent, true)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(record); !os.IsNotExist(err) {
		t.Fatal("inactive policy record was not reclaimed", err)
	}
}

func TestDiagnosticCleanupAcrossCandidateBatches(t *testing.T) {
	for _, protected := range []bool{false, true} {
		t.Run(fmt.Sprintf("protected=%v", protected), func(t *testing.T) {
			root := t.TempDir()
			policy := filepath.Join(root, "0123456789abcdef")
			if err := os.Mkdir(policy, 0700); err != nil {
				t.Fatal(err)
			}
			old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			newer := old.Add(time.Hour)
			// Create the newer record first; directory iteration order must not
			// determine which half-hour bucket is reclaimed.
			keep := captureSeedRecord(t, root, "request-newer", streamCaptureMeta{EndedAt: &newer, CaptureState: "complete"}, nil)
			paths := make([]string, diagnosticCandidateBatch+1)
			for i := range paths {
				dir := root
				if i%2 == 0 {
					dir = policy
				}
				paths[i] = captureSeedRecord(t, dir, fmt.Sprintf("request-%05d", i), streamCaptureMeta{EndedAt: &old, CaptureState: "complete"}, nil)
				if protected {
					if err := os.WriteFile(filepath.Join(paths[i], "pinned"), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			candidates, _, err := diagnosticCandidates([]string{root, policy}, nil, nil)
			if err != nil || len(candidates) != diagnosticCandidateBatch {
				t.Fatalf("candidate memory bound: count=%d err=%v", len(candidates), err)
			}
			for _, item := range candidates {
				if !item.at.Equal(old) {
					t.Fatal("newer bucket selected before the oldest batch")
				}
			}
			total, err := recordingTreeBytes(root)
			if err != nil {
				t.Fatal(err)
			}
			budget := total - 1
			if protected {
				budget = 1
			}
			if err := maintainDiagnosticStorage(root, time.Now(), budget, nil); err != nil {
				t.Fatal(err)
			}
			for _, path := range paths {
				_, err := os.Stat(path)
				if protected && err != nil {
					t.Fatal("protected candidate deleted", path, err)
				}
				if !protected && !os.IsNotExist(err) {
					t.Fatal("half-hour bucket stopped at a batch boundary", path, err)
				}
			}
			_, err = os.Stat(keep)
			if protected && !os.IsNotExist(err) {
				t.Fatal("protected first batch starved later deletable records", err)
			}
			if !protected && err != nil {
				t.Fatal("newer bucket deleted after reaching the budget", err)
			}
		})
	}
}

func TestDiagnosticCleanupCountsPendingReaders(t *testing.T) {
	root := t.TempDir()
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := old.Add(time.Hour)
	first := captureSeedRecord(t, root, "request-old", streamCaptureMeta{EndedAt: &old, CaptureState: "complete"}, nil)
	keep := captureSeedRecord(t, root, "request-newer", streamCaptureMeta{EndedAt: &newer, CaptureState: "complete"}, nil)
	lease, err := acquireStorageRead(first)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if lease != nil {
			lease.Close()
		}
	}()
	budget, err := recordingTreeBytes(keep)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := maintainDiagnosticStorage(root, time.Now(), budget, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(first); err != nil {
			t.Fatal("reader lost its record", err)
		}
		if _, err := os.Stat(keep); err != nil {
			t.Fatal("pending deletion caused extra eviction", err)
		}
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	lease = nil
	if _, err := os.Stat(first); !os.IsNotExist(err) {
		t.Fatal("reader close did not finish deletion", err)
	}
}
