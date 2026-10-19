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
	r.maintenanceMu.Lock()
	defer r.maintenanceMu.Unlock()
	r.mu.Lock()
	segments := make([]recordingStoredSegment, 0, len(r.stored))
	for _, seg := range r.stored {
		if seg.sealed {
			segments = append(segments, *seg)
		}
	}
	r.mu.Unlock()
	// Include archives, manifests and temporary files, not logical body lengths.
	for _, snapshot := range segments {
		seg := &snapshot
		n, e := recordingTreeBytes(filepath.Join(r.sessionDir(seg.recording), seg.name))
		if os.IsNotExist(e) {
			r.mu.Lock()
			delete(r.stored, seg.key)
			r.mu.Unlock()
			continue
		}
		if e != nil {
			continue
		}
		r.mu.Lock()
		seg = r.stored[seg.key]
		if seg == nil {
			r.mu.Unlock()
			continue
		}
		r.diskBytes += n - seg.bytes
		if seg.pinned {
			r.pinnedBytes += n - seg.bytes
		}
		seg.bytes = n
		r.mu.Unlock()
	}
	total, e := recordingTreeBytes(r.root)
	if e != nil {
		r.audit.Printf("recording_account_failed: %v", e)
		return
	}
	r.mu.Lock()
	r.diskBytes = total
	r.mu.Unlock()
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
	lease, e := acquireStorageRead(dir)
	if e != nil {
		runtimeError(w, 410, "segment_deleting_or_unavailable")
		return
	}
	defer lease.Close()
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
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", parts[2]+".tar"))
	if e = writeSegmentTar(w, dir, m); e != nil {
		r.audit.Printf("recording_export_failed: %v", e)
	}
}
