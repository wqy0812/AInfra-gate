package aigate

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
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
		rows, e := listStoredRecordings(filepath.Dir(r.root), r.gatewayID)
		if e != nil {
			runtimeError(w, 500, "recording_list_failed")
			return
		}
		newRuntimeResponse(200, map[string]any{"gateway_id": r.gatewayID, "recordings": rows}).write(w)
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
		newRuntimeResponse(200, map[string]any{"gateway_id": r.gatewayID, "recording_id": parts[0], "segment": parts[2], "archive": m, "metadata": seg}).write(w)
		return
	}
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", parts[2]+".tar"))
	if e = writeSegmentTar(w, dir, m); e != nil {
		r.audit.Printf("recording_export_failed: %v", e)
	}
}

type storedRecordingView struct {
	recordingSessionFile
	SegmentStatus []replaySegmentStatus `json:"segment_status"`
}

// listRecordings returns metadata only, never request or response bytes.
func listStoredRecordings(root, gateway string) ([]storedRecordingView, error) {
	dir := filepath.Join(root, gateway)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []storedRecordingView{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := []storedRecordingView{}
	for _, entry := range entries {
		if !entry.IsDir() || !validOperationID(entry.Name()) {
			continue
		}
		session, err := readRecordingSession(root, gateway, entry.Name())
		if err != nil {
			continue
		}
		view := storedRecordingView{recordingSessionFile: session, SegmentStatus: []replaySegmentStatus{}}
		children, _ := os.ReadDir(filepath.Join(dir, entry.Name()))
		for _, child := range children {
			if _, ok := parseRecordingSegment(child.Name()); !ok || !child.IsDir() {
				continue
			}
			if status, err := readRecordingSegment(root, gateway, entry.Name(), child.Name()); err == nil {
				view.SegmentStatus = append(view.SegmentStatus, status)
			}
		}
		sort.Slice(view.SegmentStatus, func(i, j int) bool { return view.SegmentStatus[i].Start.Before(view.SegmentStatus[j].Start) })
		out = append(out, view)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if len(out) > 500 {
		out = out[:500]
	}
	return out, nil
}
