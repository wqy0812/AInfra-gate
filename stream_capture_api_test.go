package aigate

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func evidenceSeed(t *testing.T, s *streamCaptureStore, id string, request, response []byte, state string) string {
	t.Helper()
	dir := filepath.Join(s.root, "request-"+id)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	m := streamCaptureMeta{Schema: 1, RequestID: id, ServiceID: s.serviceID, RouterID: s.routerID, StartedAt: time.Now(), Retained: true, FirstIncidentID: "incident", IncidentCount: 1, CaptureState: state, RequestSaved: true, RequestExpectedBytes: int64(len(request)), RequestSavedBytes: int64(len(request)), ResponseSavedBytes: int64(len(response)), ResponseSyncedBytes: int64(len(response))}
	raw, _ := json.Marshal(m)
	for name, data := range map[string][]byte{"request.json": request, "response.sse": response, "meta.json": raw} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
func evidenceQuery(s *streamCaptureStore, id string, q url.Values) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.serveEvidence(w, httptest.NewRequest("GET", "/profile/v1/stream-incidents/requests/"+id+"?"+q.Encode(), nil))
	return w
}
func evidenceSnapshot(t *testing.T, s *streamCaptureStore, id string) string {
	t.Helper()
	w := evidenceQuery(s, id, nil)
	if w.Code != 200 {
		t.Fatalf("meta: %d %s", w.Code, w.Body)
	}
	var result struct {
		Snapshot string `json:"snapshot"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result.Snapshot
}
func TestEvidenceChunksAndFixedSnapshot(t *testing.T) {
	s := captureTestStore(t)
	id := randomID()
	data := []byte(strings.Repeat("问题😀", 40000))
	dir := evidenceSeed(t, s, id, []byte(`{"messages":[]}`), data, "recording")
	snapshot := evidenceSnapshot(t, s, id)
	// Later unsynced bytes are intentionally excluded from the earlier snapshot.
	f, err := os.OpenFile(filepath.Join(dir, "response.sse"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("later"))
	f.Close()
	var got []byte
	for offset := 0; offset < len(data); {
		w := evidenceQuery(s, id, url.Values{"part": {"response"}, "snapshot": {snapshot}, "offset": {strconv.Itoa(offset)}, "limit": {"262144"}})
		if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("chunk: %d %s", w.Code, w.Body)
		}
		var v struct {
			Data  string `json:"data"`
			Next  int    `json:"next_offset"`
			Total int    `json:"total_bytes"`
		}
		json.Unmarshal(w.Body.Bytes(), &v)
		b, err := base64.StdEncoding.DecodeString(v.Data)
		if err != nil || len(b) > captureChunkLimit || v.Next != offset+len(b) || v.Total != len(data) {
			t.Fatal("invalid chunk")
		}
		got = append(got, b...)
		offset = v.Next
	}
	if !bytes.Equal(got, data) {
		t.Fatal("bytes changed")
	}
	for _, q := range []url.Values{
		{"part": {"timeline"}}, {"part": {"response"}, "snapshot": {snapshot}, "offset": {"-1"}, "limit": {"1"}},
		{"part": {"response"}, "snapshot": {snapshot}, "offset": {"0"}, "limit": {"262145"}},
		{"part": {"response"}, "snapshot": {"bad"}, "offset": {"0"}, "limit": {"1"}},
	} {
		if w := evidenceQuery(s, id, q); w.Code != 400 {
			t.Fatalf("invalid query: %d", w.Code)
		}
	}
	os.Remove(filepath.Join(dir, "response.sse"))
	w := evidenceQuery(s, id, url.Values{"part": {"response"}, "snapshot": {snapshot}, "offset": {"0"}, "limit": {"1"}})
	if w.Code != 404 {
		t.Fatalf("removed file: %d", w.Code)
	}
}
func TestEvidenceMissingUnsafeAndPartial(t *testing.T) {
	s := captureTestStore(t)
	for _, state := range []string{"complete", "truncated", "failed", "end_unknown", "recording"} {
		id := randomID()
		dir := evidenceSeed(t, s, id, []byte("partial"), []byte("response"), state)
		if w := evidenceQuery(s, id, nil); w.Code != 200 {
			t.Fatalf("%s: %d", state, w.Code)
		}
		os.Remove(filepath.Join(dir, "request.json"))
		w := evidenceQuery(s, id, nil)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"available":false`) {
			t.Fatalf("missing: %s", w.Body)
		}
	}
	id := randomID()
	dir := evidenceSeed(t, s, id, []byte("safe"), nil, "complete")
	secret := filepath.Join(t.TempDir(), "secret")
	os.WriteFile(secret, []byte("private-data"), 0600)
	os.Remove(filepath.Join(dir, "request.json"))
	os.Symlink(secret, filepath.Join(dir, "request.json"))
	if w := evidenceQuery(s, id, nil); w.Code != 503 || strings.Contains(w.Body.String(), "private-data") {
		t.Fatal("followed file link")
	}
	alias := randomID()
	os.Symlink(dir, filepath.Join(s.root, "request-"+alias))
	if w := evidenceQuery(s, alias, nil); w.Code != 503 {
		t.Fatal("followed directory link")
	}
	for _, bad := range []string{"bad", "../secret", strings.ToUpper(id)} {
		if w := evidenceQuery(s, bad, nil); w.Code != 400 {
			t.Fatalf("bad id: %d", w.Code)
		}
	}
	if w := evidenceQuery(s, randomID(), nil); w.Code != 404 {
		t.Fatal("missing record")
	}
	s.querySlots <- struct{}{}
	s.querySlots <- struct{}{}
	if w := evidenceQuery(s, id, nil); w.Code != 429 {
		t.Fatal("query bound")
	}
	<-s.querySlots
	<-s.querySlots
}

func TestEvidenceRecoveredAndObservationRoute(t *testing.T) {
	p, _ := splitProxyFixture(t, "r1", "http://unused.invalid", `{"profile":{"enabled":true}}`)
	s := p.g.captures
	id := randomID()
	dir := evidenceSeed(t, s, id, []byte(`{}`), []byte("recovered"), "end_unknown")
	m := captureReadMeta(t, s, id)
	m.ResponseSyncedBytes = 0
	m.IncidentCount = 0
	m.FirstIncidentID = ""
	m.FailureReason = "process_end_unobserved"
	raw, _ := json.Marshal(m)
	os.WriteFile(filepath.Join(dir, "meta.json"), raw, 0600)
	for _, handler := range []interface {
		ServeHTTP(http.ResponseWriter, *http.Request)
	}{p, p.ObservationHandler()} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", "/profile/v1/stream-incidents/requests/"+id, nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"bytes":9`) {
			t.Fatalf("route/recovery: %d %s", w.Code, w.Body)
		}
	}
	snapshot := evidenceSnapshot(t, s, id)
	w := evidenceQuery(s, id, url.Values{"part": {"response"}, "snapshot": {snapshot}, "offset": {"0"}, "limit": {"100"}})
	if w.Code != 200 || !strings.Contains(w.Body.String(), base64.StdEncoding.EncodeToString([]byte("recovered"))) {
		t.Fatalf("recovered bytes: %d %s", w.Code, w.Body)
	}
}

func TestEvidenceReplacementExpiryAndCleanup(t *testing.T) {
	s := captureTestStore(t)
	id := randomID()
	dir := evidenceSeed(t, s, id, []byte("request"), []byte("response"), "complete")
	snapshot := evidenceSnapshot(t, s, id)
	q := url.Values{"part": {"response"}, "snapshot": {snapshot}, "offset": {"0"}, "limit": {"1"}}
	// Keep the old inode alive to make replacement deterministic on all filesystems.
	os.Rename(filepath.Join(dir, "response.sse"), filepath.Join(dir, "old"))
	os.WriteFile(filepath.Join(dir, "response.sse"), []byte("different response"), 0600)
	if w := evidenceQuery(s, id, q); w.Code != 409 {
		t.Fatalf("replacement: %d", w.Code)
	}
	os.Remove(filepath.Join(dir, "old"))
	os.Remove(filepath.Join(dir, "response.sse"))
	if err := syscall.Mkfifo(filepath.Join(dir, "response.sse"), 0600); err != nil {
		t.Fatal(err)
	}
	if w := evidenceQuery(s, id, nil); w.Code != 503 {
		t.Fatalf("fifo: %d", w.Code)
	}
	os.Remove(filepath.Join(dir, "response.sse"))
	os.WriteFile(filepath.Join(dir, "response.sse"), []byte("response"), 0600)
	meta := captureReadMeta(t, s, id)
	meta.StartedAt = time.Now().Add(-100 * time.Hour)
	raw, _ := json.Marshal(meta)
	os.WriteFile(filepath.Join(dir, "meta.json"), raw, 0600)
	if w := evidenceQuery(s, id, nil); w.Code != 200 {
		t.Fatalf("old retained evidence unavailable: %d", w.Code)
	}
	now := time.Now()
	meta.TriggeredAt = &now
	raw, _ = json.Marshal(meta)
	os.WriteFile(filepath.Join(dir, "meta.json"), raw, 0600)
	snapshot = evidenceSnapshot(t, s, id)
	q.Set("snapshot", snapshot)
	done := make(chan struct{})
	go func() { defer close(done); os.RemoveAll(dir) }()
	for i := 0; i < 20; i++ {
		w := evidenceQuery(s, id, q)
		if w.Code != 200 && w.Code != 404 && w.Code != 503 {
			t.Fatalf("cleanup: %d", w.Code)
		}
	}
	<-done
}
