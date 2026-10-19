package aigate

// Reads are independent of the capture worker and never hold its mutex across I/O.
import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const captureChunkLimit = 256 << 10

type captureFileSnapshot struct {
	Bytes     int64  `json:"bytes"`
	Inode     uint64 `json:"inode"`
	Available bool   `json:"available"`
}
type captureSnapshot struct {
	RequestID string              `json:"request_id"`
	Request   captureFileSnapshot `json:"request"`
	Response  captureFileSnapshot `json:"response"`
}

// The store root and request directories are private (0700). Check each parent
// and its identity around open; O_NOFOLLOW/O_NONBLOCK also reject links/FIFOs at
// the leaf without following them or waiting on a writer. Cleanup may unlink an
// opened regular file; that descriptor continues to provide the same snapshot.
func (s *streamCaptureStore) openEvidence(id, name string) (*os.File, error) {
	root, err := os.Lstat(s.root)
	if err != nil {
		return nil, err
	}
	original, err := s.directory.Stat()
	if err != nil || !root.IsDir() || !os.SameFile(root, original) {
		return nil, errors.New("invalid capture root")
	}
	dir := filepath.Join(s.root, "request-"+id)
	before, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode().Perm() != 0700 {
		return nil, errors.New("invalid capture directory")
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	after, parentErr := os.Lstat(dir)
	rootAfter, rootErr := os.Lstat(s.root)
	if err != nil || !info.Mode().IsRegular() || parentErr != nil || !os.SameFile(before, after) || rootErr != nil || !os.SameFile(root, rootAfter) {
		f.Close()
		return nil, errors.New("capture changed or unsafe file")
	}
	return f, nil
}

func (s *streamCaptureStore) evidenceMeta(id string) (streamCaptureMeta, error) {
	var meta streamCaptureMeta
	f, err := s.openEvidence(id, "meta.json")
	if err != nil {
		return meta, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, captureMetaLimit+1))
	if err != nil || len(raw) > captureMetaLimit || json.Unmarshal(raw, &meta) != nil || meta.Schema != 1 || meta.RequestID != id || !meta.Retained || meta.ServiceID != s.serviceID || meta.RouterID != s.routerID {
		return meta, errors.New("invalid capture metadata")
	}
	return meta, nil
}

// Recovery hashes the retained file but resets synced bytes to zero: those
// recovered bytes remain readable with end_unknown status. Active snapshots use
// the published sync boundary, excluding bytes currently being appended.
func evidenceResponseBoundary(meta streamCaptureMeta) int64 {
	if meta.CaptureState == "recording" {
		return meta.ResponseSyncedBytes
	}
	return meta.ResponseSavedBytes
}

func (s *streamCaptureStore) fileSnapshot(id, name string, boundary int64) (captureFileSnapshot, error) {
	var v captureFileSnapshot
	f, err := s.openEvidence(id, name)
	if os.IsNotExist(err) {
		return v, nil
	}
	if err != nil {
		return v, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return v, err
	}
	if boundary < 0 || boundary > info.Size() {
		return v, errors.New("capture boundary unavailable")
	}
	v.Available = true
	v.Bytes = boundary
	v.Inode = info.Sys().(*syscall.Stat_t).Ino
	return v, nil
}
func (s *streamCaptureStore) serveEvidence(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	fail := func(status int, code string) { runtimeError(w, status, code) }
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		fail(405, "method_not_allowed")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/profile/v1/stream-incidents/requests/")
	if !slowRequestID(id) {
		fail(400, "invalid_capture_query")
		return
	}
	lease, leaseErr := acquireStorageRead(filepath.Join(s.root, "request-"+id))
	if leaseErr != nil {
		if errors.Is(leaseErr, errStorageDeleting) {
			fail(410, "capture_deleting")
		} else if os.IsNotExist(leaseErr) {
			fail(404, "capture_not_retained")
		} else {
			fail(503, "capture_unavailable")
		}
		return
	}
	defer lease.Close()
	q := r.URL.Query()
	part := q.Get("part")
	if len(q.Get("snapshot")) > 4096 || (part != "" && part != "request" && part != "response") {
		fail(400, "invalid_capture_query")
		return
	}
	select {
	case s.querySlots <- struct{}{}:
		defer func() { <-s.querySlots }()
	default:
		fail(429, "capture_busy")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	meta, err := s.evidenceMeta(id)
	if os.IsNotExist(err) {
		fail(404, "capture_not_retained")
		return
	}
	if err != nil {
		fail(503, "capture_unavailable")
		return
	}
	if part == "" {
		request, e1 := s.fileSnapshot(id, "request.json", meta.RequestSavedBytes)
		response, e2 := s.fileSnapshot(id, "response.sse", evidenceResponseBoundary(meta))
		if e1 != nil || e2 != nil || ctx.Err() != nil {
			fail(503, "capture_unavailable")
			return
		}
		snapshot := captureSnapshot{RequestID: id, Request: request, Response: response}
		raw, _ := json.Marshal(snapshot)
		runtimeResponse(w, 200, map[string]any{"request_id": id, "meta": meta, "files": snapshot, "snapshot": base64.RawURLEncoding.EncodeToString(raw)})
		return
	}
	offset, e1 := strconv.ParseInt(q.Get("offset"), 10, 64)
	limit, e2 := strconv.Atoi(q.Get("limit"))
	raw, e3 := base64.RawURLEncoding.DecodeString(q.Get("snapshot"))
	var snapshot captureSnapshot
	if e1 != nil || e2 != nil || e3 != nil || offset < 0 || limit < 1 || limit > captureChunkLimit || len(raw) > 2048 || json.Unmarshal(raw, &snapshot) != nil || snapshot.RequestID != id {
		fail(400, "invalid_capture_query")
		return
	}
	target := snapshot.Request
	name := "request.json"
	current := meta.RequestSavedBytes
	if part == "response" {
		target = snapshot.Response
		name = "response.sse"
		current = evidenceResponseBoundary(meta)
	}
	if !target.Available {
		fail(404, "capture_not_retained")
		return
	}
	if target.Bytes < 0 || target.Bytes > current || offset > target.Bytes {
		fail(400, "invalid_capture_query")
		return
	}
	f, err := s.openEvidence(id, name)
	if os.IsNotExist(err) {
		fail(404, "capture_not_retained")
		return
	}
	if err != nil {
		fail(503, "capture_unavailable")
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Sys().(*syscall.Stat_t).Ino != target.Inode || info.Size() < target.Bytes {
		fail(409, "capture_snapshot_changed")
		return
	}
	size := min(int64(limit), target.Bytes-offset)
	data := make([]byte, size)
	if _, err = io.ReadFull(io.NewSectionReader(f, offset, size), data); err != nil || ctx.Err() != nil {
		fail(503, "capture_unavailable")
		return
	}
	runtimeResponse(w, 200, map[string]any{"request_id": id, "part": part, "offset": offset, "next_offset": offset + size, "total_bytes": target.Bytes, "data": base64.StdEncoding.EncodeToString(data)})
}
