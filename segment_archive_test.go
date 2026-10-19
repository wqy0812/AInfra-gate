package aigate

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSegmentArchiveIntegrityAndRecovery(t *testing.T) {
	dir := t.TempDir()
	os.Mkdir(filepath.Join(dir, "r-one"), 0700)
	body := bytes.Repeat([]byte("original SSE\n"), 10000)
	os.WriteFile(filepath.Join(dir, "r-one", "response.body"), body, 0600)
	os.WriteFile(filepath.Join(dir, "segment.json"), []byte(`{"state":"sealed"}`), 0600)
	m, e := prepareSegmentArchive(dir)
	if e != nil {
		t.Fatal(e)
	}
	if m.Bytes >= int64(len(body)) {
		t.Fatal("not compressed")
	}
	if e = compactSegmentArchive(dir, map[string]bool{"segment.json": true}); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(dir, "r-one")); !os.IsNotExist(e) {
		t.Fatal(e)
	}
	if e = compactSegmentArchive(dir, map[string]bool{"segment.json": true}); e != nil {
		t.Fatal(e)
	}
	if e = verifySegmentArchive(filepath.Join(dir, archivePayload), m); e != nil {
		t.Fatal(e)
	}
	f, _ := os.OpenFile(filepath.Join(dir, archivePayload), os.O_WRONLY, 0)
	f.WriteAt([]byte("bad"), 0)
	f.Close()
	if e = compactSegmentArchive(dir, nil); e == nil {
		t.Fatal("corrupt archive accepted")
	}
	if _, e = os.Stat(filepath.Join(dir, "segment.json")); e != nil {
		t.Fatal("removed source after corruption")
	}
}
func TestArchiveRefusesSymlink(t *testing.T) {
	dir := t.TempDir()
	os.Symlink("/etc/passwd", filepath.Join(dir, "link"))
	if _, e := prepareSegmentArchive(dir); e == nil {
		t.Fatal("followed link")
	}
}
func TestDiagnosticHalfHourArchiveAndEviction(t *testing.T) {
	root := t.TempDir()
	at := time.Now().UTC().Add(-74 * time.Hour).Truncate(recordingSegment)
	ids := []string{"11111111111111111111111111111111", "22222222222222222222222222222222"}
	for _, id := range ids {
		dir := filepath.Join(root, "request-"+id)
		os.Mkdir(dir, 0700)
		b, _ := json.Marshal(map[string]any{"ended_at": at, "request_id": id, "capture_state": "complete"})
		os.WriteFile(filepath.Join(dir, "meta.json"), b, 0600)
		os.WriteFile(filepath.Join(dir, "request.json"), bytes.Repeat([]byte(id), 1000), 0600)
	}
	if e := maintainDiagnosticArchives(root, time.Now(), 10<<30, nil); e != nil {
		t.Fatal(e)
	}
	a, e := diagnosticReference(filepath.Join(root, "request-"+ids[0]))
	if e != nil {
		t.Fatal(e)
	}
	b, e := diagnosticReference(filepath.Join(root, "request-"+ids[1]))
	if e != nil || a != b {
		t.Fatal("not grouped", e)
	}
	r, _, _, e := archivedDiagnosticFile(filepath.Join(root, "request-"+ids[0]), "request.json")
	if e != nil {
		t.Fatal(e)
	}
	raw, e := io.ReadAll(r)
	r.Close()
	if e != nil || !bytes.Equal(raw, bytes.Repeat([]byte(ids[0]), 1000)) {
		t.Fatal("bad archive body")
	}
	if e = maintainDiagnosticArchives(root, time.Now(), 10<<30, nil); e != nil {
		t.Fatal(e)
	}
	if e = maintainDiagnosticArchives(root, time.Now(), 1, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(root, "request-"+ids[0])); !os.IsNotExist(e) {
		t.Fatal("not evicted")
	}
}
func TestDiagnosticAgeAndActiveProtection(t *testing.T) {
	root := t.TempDir()
	at := time.Now().UTC()
	id := "11111111111111111111111111111111"
	dir := filepath.Join(root, "request-"+id)
	os.Mkdir(dir, 0700)
	data, _ := json.Marshal(map[string]any{"ended_at": at, "request_id": id})
	os.WriteFile(filepath.Join(dir, "meta.json"), data, 0600)
	os.WriteFile(filepath.Join(dir, "request.json"), []byte("body"), 0600)
	if e := maintainDiagnosticArchives(root, at.Add(72*time.Hour), 10000, nil); e != nil {
		t.Fatal(e)
	}
	if _, e := diagnosticReference(dir); !os.IsNotExist(e) {
		t.Fatal("early compression")
	}
	if e := maintainDiagnosticArchives(root, at.Add(80*time.Hour), 1, func(string) bool { return true }); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(dir); e != nil {
		t.Fatal("active removed")
	}
}

func TestRecordingArchiveExportAuthenticationAndIntegrity(t *testing.T) {
	rec := newRecordingFixture(t, recordingDefaultMaxBytes)
	summary := recordSample(t, rec, "export", 2)
	prefix := "/control/v3/recordings/export/segments/" + summary.Segments[0].Segment
	if w := runtimeHTTP(t, rec.c, "GET", prefix+"/manifest", "", "127.0.0.1:1", "wrong"); w.Code != 403 {
		t.Fatal(w.Code)
	}
	key := string(bytes.Repeat([]byte("k"), 32))
	w := runtimeHTTP(t, rec.c, "GET", prefix+"/manifest", "", "127.0.0.1:1", key)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	manifestBytes := append([]byte(nil), w.Body.Bytes()...)
	var manifest struct {
		Archive segmentArchive `json:"archive"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &manifest); e != nil {
		t.Fatal(e)
	}
	w = runtimeHTTP(t, rec.c, "GET", prefix+"/archive", "", "127.0.0.1:1", key)
	if w.Code != 200 || w.Header().Get("X-Archive-SHA256") != manifest.Archive.SHA256 {
		t.Fatal(w.Code)
	}
	if out := os.Getenv("AIGATE_ARCHIVE_FIXTURE_DIR"); out != "" {
		if e := os.MkdirAll(out, 0700); e != nil {
			t.Fatal(e)
		}
		for name, data := range map[string][]byte{"segment.tar.gz": w.Body.Bytes(), "manifest.json": manifestBytes} {
			if e := os.WriteFile(filepath.Join(out, name), data, 0600); e != nil {
				t.Fatal(e)
			}
		}
		session, e := os.ReadFile(filepath.Join(rec.root, "g1", "export", "session.json"))
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(out, "session.json"), session, 0600); e != nil {
			t.Fatal(e)
		}
	}
	path := filepath.Join(t.TempDir(), "export.tar.gz")
	os.WriteFile(path, w.Body.Bytes(), 0600)
	if e := verifySegmentArchive(path, manifest.Archive); e != nil {
		t.Fatal(e)
	}
	if w := runtimeHTTP(t, rec.c, "GET", "/control/v3/recordings", "", "127.0.0.1:1", key); w.Code != 200 {
		t.Fatal(w.Code)
	}
	dir := filepath.Join(rec.root, "g1", "export", summary.Segments[0].Segment)
	lock, e := os.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Close()
	if e = lockState(lock); e != nil {
		t.Fatal(e)
	}
	defer unlockState(lock)
	if w := runtimeHTTP(t, rec.c, "GET", prefix+"/archive", "", "127.0.0.1:1", key); w.Code != 409 {
		t.Fatal("export ignored lease", w.Code)
	}
}
