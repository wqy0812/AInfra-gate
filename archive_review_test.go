package aigate

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestArchiveManifestRejectsOversizedMetadata(t *testing.T) {
	dir := t.TempDir()
	payload := `{"version":1,"bytes":1,"sha256":"` + strings.Repeat("a", 64) + `","files":[{"name":"` + strings.Repeat("x", 17<<20) + `","bytes":0,"sha256":"` + strings.Repeat("a", 64) + `"}]}`
	if err := os.WriteFile(filepath.Join(dir, archiveManifestName), []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSegmentArchive(dir); err == nil {
		t.Fatal("accepted unbounded manifest metadata")
	}
}

func TestArchiveMetadataBudgetKeepsSourcesOnFailure(t *testing.T) {
	for _, limits := range []archiveLimits{
		{files: 1, blocks: 10, metadata: 1 << 20},
		{files: 10, blocks: 10, metadata: 1024},
		{files: 10, blocks: 1, metadata: 1 << 20},
	} {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "one"), bytes.Repeat([]byte("x"), 2<<20), 0600)
		os.WriteFile(filepath.Join(dir, "two"), []byte("second"), 0600)
		if _, err := prepareSegmentArchiveLimits(dir, limits); err == nil {
			t.Fatal("metadata budget was ignored")
		}
		for _, name := range []string{"one", "two"} {
			if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
				t.Fatal("source lost", err)
			}
		}
		for _, name := range []string{archivePayload, archiveManifestName, ".archive.pending"} {
			if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
				t.Fatal("partial archive published", name, err)
			}
		}
	}
}

func TestSlowDailyMaintenancePreservesLivePending(t *testing.T) {
	c := slowTestConfig(t)
	s, _ := slowTestStore(t, c)
	pending := filepath.Join(c.Directory, ".pending-11111111111111111111111111111111")
	if err := os.Mkdir(pending, 0700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(pending, "request.json"), []byte("writer has not committed yet"), 0600)
	if err := s.cleanup(time.Now(), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(pending); err != nil {
		t.Fatal("removed a live writer's pending directory", err)
	}
}

func TestDisabledSlowMaintenanceRecoversPending(t *testing.T) {
	root := t.TempDir()
	pending := filepath.Join(root, "slow-requests", ".pending-11111111111111111111111111111111")
	os.MkdirAll(pending, 0700)
	os.WriteFile(filepath.Join(pending, "request.json"), []byte("abandoned"), 0600)
	p := retentionProxy(t, root, `{"slow_requests":{"enabled":false}}`)
	p.maintainInactiveStorage(time.Now())
	if _, err := os.Stat(pending); !os.IsNotExist(err) {
		t.Fatal("abandoned pending survives maintenance", err)
	}
}

func TestArchivedRangeRejectsCorruptBlockBeforeReturningBytes(t *testing.T) {
	root := t.TempDir()
	id := "11111111111111111111111111111111"
	dir := filepath.Join(root, "request-"+id)
	os.Mkdir(dir, 0700)
	meta, _ := json.Marshal(map[string]any{"ended_at": time.Now().Add(-96 * time.Hour), "request_id": id})
	os.WriteFile(filepath.Join(dir, "meta.json"), meta, 0600)
	os.WriteFile(filepath.Join(dir, "response.sse"), bytes.Repeat([]byte("0123456789abcdef"), 200000), 0600)
	if err := maintainDiagnosticArchives(root, time.Now(), 10<<30, nil); err != nil {
		t.Fatal(err)
	}
	ref, _ := diagnosticReference(dir)
	archive := filepath.Join(root, ref.Archive)
	manifest, err := readSegmentArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(archive, archivePayload), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the first gzip member's CRC, beyond the requested 16 output bytes.
	at := manifest.Blocks[1].Compressed - 8
	b := make([]byte, 1)
	f.ReadAt(b, at)
	b[0] ^= 0xff
	f.WriteAt(b, at)
	f.Close()
	r, _, _, err := archivedDiagnosticRange(context.Background(), dir, "response.sse", 0)
	if err == nil {
		defer r.Close()
		_, err = io.ReadFull(r, make([]byte, 16))
	}
	if err == nil {
		t.Fatal("returned bytes from a corrupted compressed member")
	}
}
