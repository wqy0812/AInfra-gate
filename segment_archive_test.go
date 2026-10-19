package aigate

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRawExportPreservesFilesAndRejectsLinks(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "segment.json"), []byte("metadata"), 0600)
	os.Mkdir(filepath.Join(dir, "r-1"), 0700)
	os.WriteFile(filepath.Join(dir, "r-1", "request.json"), []byte("original body"), 0600)
	m, e := prepareSegmentArchive(dir)
	if e != nil {
		t.Fatal(e)
	}
	var buf bytes.Buffer
	if e = writeSegmentTar(&buf, dir, m); e != nil {
		t.Fatal(e)
	}
	reader := tar.NewReader(&buf)
	for _, want := range m.Files {
		header, e := reader.Next()
		if e != nil || header.Name != want.Name {
			t.Fatal(e, header)
		}
		data, _ := io.ReadAll(reader)
		original, _ := os.ReadFile(filepath.Join(dir, want.Name))
		if !bytes.Equal(data, original) {
			t.Fatal("content changed")
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		t.Fatal("export created persistent archive")
	}
	os.Symlink("/etc/passwd", filepath.Join(dir, "link"))
	if _, e = prepareSegmentArchive(dir); e == nil {
		t.Fatal("link exported")
	}
}
func TestDeletionDeniesNewReadsAndWaitsForReader(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "segment")
	os.Mkdir(dir, 0700)
	os.WriteFile(filepath.Join(dir, "body"), []byte("body"), 0600)
	lease, e := acquireStorageRead(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = markStorageDelete(dir); e != nil {
		t.Fatal(e)
	}
	if other, e := acquireStorageRead(dir); e == nil {
		other.Close()
		t.Fatal("read after deletion intent")
	}
	if e = finishStorageDelete(dir); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(dir, "body")); e != nil {
		t.Fatal("active reader lost body")
	}
	lease.Close()
	if _, e = os.Stat(dir); !os.IsNotExist(e) {
		t.Fatal("not reclaimed after reader")
	}
}
func TestDeletionRestartRecoveryAndPinProtection(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "segment")
	os.Mkdir(dir, 0700)
	os.WriteFile(filepath.Join(dir, "pinned"), nil, 0600)
	if e := markStorageDelete(dir); e == nil {
		t.Fatal("deleted pin")
	}
	os.Remove(filepath.Join(dir, "pinned"))
	if e := markStorageDelete(dir); e != nil {
		t.Fatal(e)
	}
	if e := recoverStorageDeletes(root); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(dir); !os.IsNotExist(e) {
		t.Fatal(e)
	}
}
func TestDiagnosticMaintenanceIsCapacityOnly(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-1000 * time.Hour)
	dir := captureSeedRecord(t, root, "request-"+randomID(), streamCaptureMeta{Schema: 1, EndedAt: &old, CaptureState: "complete"}, map[string][]byte{"request.json": []byte("raw")})
	if e := maintainDiagnosticStorage(root, time.Now(), 10<<30, nil); e != nil {
		t.Fatal(e)
	}
	if b, e := os.ReadFile(filepath.Join(dir, "request.json")); e != nil || string(b) != "raw" {
		t.Fatal("age changed original", e)
	}
	if e := maintainDiagnosticStorage(root, time.Now(), 1, func(string) bool { return true }); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(dir); e != nil {
		t.Fatal("active record deleted")
	}
	if e := maintainDiagnosticStorage(root, time.Now(), 1, nil); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(dir); !os.IsNotExist(e) {
		t.Fatal("quota not enforced", e)
	}
}
func TestDeletionRecoversTrashAfterMarkerWasRemoved(t *testing.T) {
	root := t.TempDir()
	trash := filepath.Join(root, ".trash-segment-interrupted")
	os.Mkdir(trash, 0700)
	os.WriteFile(filepath.Join(trash, "body"), []byte("remaining body"), 0600)
	if e := recoverStorageDeletes(root); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(trash); !os.IsNotExist(e) {
		t.Fatal("interrupted physical delete did not finish", e)
	}
}
func TestRecordingRawExportAuthenticated(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "export", 2)
	prefix := "/control/v3/recordings/export/segments/" + summary.Segments[0].Segment
	key := rec.c.key
	if w := runtimeHTTP(t, rec.c, "GET", prefix+"/manifest", "", "127.0.0.1:1", ""); w.Code != 401 && w.Code != 403 {
		t.Fatal(w.Code)
	}
	w := runtimeHTTP(t, rec.c, "GET", prefix+"/manifest", "", "127.0.0.1:1", key)
	var response struct {
		Archive segmentArchive `json:"archive"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &response); e != nil || w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if response.Archive.Format != "tar" {
		t.Fatal(response)
	}
	manifestBytes := append([]byte(nil), w.Body.Bytes()...)
	w = runtimeHTTP(t, rec.c, "GET", prefix+"/archive", "", "127.0.0.1:1", key)
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/x-tar" {
		t.Fatal(w.Code)
	}
	if out := os.Getenv("AIGATE_ARCHIVE_FIXTURE_DIR"); out != "" {
		if e := os.MkdirAll(out, 0700); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(filepath.Join(out, "manifest.json"), manifestBytes, 0600); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(filepath.Join(out, "segment.tar"), w.Body.Bytes(), 0600); e != nil {
			t.Fatal(e)
		}
	}
	reader := tar.NewReader(w.Body)
	for range response.Archive.Files {
		if _, e := reader.Next(); e != nil {
			t.Fatal(e)
		}
	}
}
