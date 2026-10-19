package aigate

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (r *trafficRecorder) maintainStorage() {
	now := r.now()
	for _, seg := range r.stored {
		if !seg.sealed || !now.After(seg.start.Add(recordingSegment+archiveAfter)) {
			continue
		}
		dir := filepath.Join(r.sessionDir(seg.recording), seg.name)
		f, e := os.Open(dir)
		if e != nil {
			continue
		}
		if e = lockState(f); e != nil {
			f.Close()
			continue
		}
		e = compactSegmentArchive(dir, map[string]bool{"segment.json": true})
		unlockState(f)
		f.Close()
		if e != nil {
			r.audit.Printf("recording_archive_failed segment=%s error=%v", seg.key, e)
		}
	}
	// Include archives, manifests and temporary files, not logical body lengths.
	for _, seg := range r.stored {
		n, e := recordingTreeBytes(filepath.Join(r.sessionDir(seg.recording), seg.name))
		if e != nil {
			continue
		}
		r.mu.Lock()
		r.diskBytes += n - seg.bytes
		if seg.pinned {
			r.pinnedBytes += n - seg.bytes
		}
		seg.bytes = n
		r.mu.Unlock()
	}
	r.evict()
}

// Export holds a directory lock through the entire transfer. A persistent pin
// additionally protects retries between HTTP requests until NAS commit succeeds.
func (r *trafficRecorder) archiveHTTP(w http.ResponseWriter, req *http.Request) {
	if r == nil {
		runtimeError(w, 409, "recording_disabled")
		return
	}
	if req.Method != "GET" {
		runtimeError(w, 405, "method_not_allowed")
		return
	}
	suffix := strings.TrimPrefix(req.URL.Path, "/control/v3/recordings")
	if suffix == "" || suffix == "/" {
		service := replayService{recordRoot: filepath.Dir(r.root)}
		rows, e := service.listRecordings(r.gatewayID)
		if e != nil {
			runtimeError(w, 500, "recording_list_failed")
			return
		}
		replayJSON(w, 200, map[string]any{"gateway_id": r.gatewayID, "recordings": rows})
		return
	}
	parts := strings.Split(strings.Trim(suffix, "/"), "/")
	if len(parts) != 4 || !validOperationID(parts[0]) || parts[1] != "segments" || (parts[3] != "archive" && parts[3] != "manifest") {
		runtimeError(w, 404, "not_found")
		return
	}
	if _, ok := parseRecordingSegment(parts[2]); !ok {
		runtimeError(w, 400, "invalid_segment")
		return
	}
	dir := filepath.Join(r.root, parts[0], parts[2])
	info, e := os.Lstat(dir)
	if e != nil || !info.IsDir() {
		runtimeError(w, 404, "segment_not_found")
		return
	}
	f, e := os.Open(dir)
	if e != nil {
		runtimeError(w, 404, "segment_not_found")
		return
	}
	defer f.Close()
	if e = lockState(f); e != nil {
		runtimeError(w, 409, "segment_busy")
		return
	}
	defer unlockState(f)
	// Revalidate after locking: eviction may have acquired the inode first.
	if _, e = os.Stat(dir); e != nil {
		runtimeError(w, 404, "segment_not_found")
		return
	}
	var seg recordingSegmentFile
	data, e := recordingReadSmall(filepath.Join(dir, "segment.json"))
	if e != nil || json.Unmarshal(data, &seg) != nil || seg.State != "sealed" {
		runtimeError(w, 409, "segment_not_sealed")
		return
	}
	// Authorized archive generation/transfer may exceed the ordinary control
	// response timeout; keep a finite deadline scoped to this request.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(2 * time.Hour))
	m, e := prepareSegmentArchive(dir)
	if e != nil {
		runtimeError(w, 500, "archive_failed")
		return
	}
	if parts[3] == "manifest" {
		replayJSON(w, 200, map[string]any{"gateway_id": r.gatewayID, "recording_id": parts[0], "segment": parts[2], "archive": m, "metadata": seg})
		return
	}
	payload, e := os.Open(filepath.Join(dir, archivePayload))
	if e != nil {
		runtimeError(w, 500, "archive_missing")
		return
	}
	defer payload.Close()
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("X-Archive-SHA256", m.SHA256)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", parts[2]+".tar.gz"))
	http.ServeContent(w, req, archivePayload, time.Time{}, payload)
}
