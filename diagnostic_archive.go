package aigate

import (
	"container/heap"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var diagnosticMaintenanceMu sync.Mutex

const diagnosticCandidateBatch = 4096

type diagnosticItem struct {
	path  string
	at    time.Time
	bytes int64
}

// Keep the newest selected item at the root, so a scan retains only the
// oldest fixed-size batch. Path breaks ties within a half-hour bucket and
// provides a cursor even when a protected item cannot be deleted.
func (item diagnosticItem) before(other diagnosticItem) bool {
	a, b := item.at.Truncate(recordingSegment), other.at.Truncate(recordingSegment)
	if !a.Equal(b) {
		return a.Before(b)
	}
	return item.path < other.path
}

type diagnosticHeap []diagnosticItem

func (h diagnosticHeap) Len() int           { return len(h) }
func (h diagnosticHeap) Less(i, j int) bool { return h[j].before(h[i]) }
func (h diagnosticHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *diagnosticHeap) Push(value any)    { *h = append(*h, value.(diagnosticItem)) }
func (h *diagnosticHeap) Pop() any {
	last := len(*h) - 1
	value := (*h)[last]
	*h = (*h)[:last]
	return value
}

func diagnosticFolders(root string) ([]string, error) {
	dirs := []string{root}
	e := visitDiagnosticEntries(root, func(d os.DirEntry) error {
		if d.IsDir() && observationPolicyDir(d.Name()) {
			dirs = append(dirs, filepath.Join(root, d.Name()))
		}
		return nil
	})
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	return dirs, nil
}

func diagnosticCandidates(dirs []string, active func(string) bool, after *diagnosticItem) ([]diagnosticItem, int64, error) {
	candidates := &diagnosticHeap{}
	var pending int64
	for _, dir := range dirs {
		e := visitDiagnosticEntries(dir, func(entry os.DirEntry) error {
			if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "request-") {
				return nil
			}
			path := filepath.Join(dir, entry.Name())
			if active != nil && active(path) {
				return nil
			}
			var meta struct {
				EndedAt         *time.Time `json:"ended_at"`
				StorageClosedAt *time.Time `json:"storage_closed_at"`
				CaptureState    string     `json:"capture_state"`
			}
			data, e := captureReadSmall(filepath.Join(path, "meta.json"), captureMetaLimit)
			if e != nil || json.Unmarshal(data, &meta) != nil || meta.CaptureState == "recording" {
				return nil
			}
			at := meta.EndedAt
			if at == nil {
				at = meta.StorageClosedAt
			}
			if at == nil {
				return nil
			}
			size, e := recordingTreeBytes(path)
			if e != nil {
				if os.IsNotExist(e) {
					return nil
				}
				return e
			}
			if _, e = os.Stat(filepath.Join(path, deleteMarker)); e == nil {
				pending += size
				return nil
			}
			item := diagnosticItem{path, *at, size}
			if after != nil && !after.before(item) {
				return nil
			}
			if candidates.Len() < diagnosticCandidateBatch {
				heap.Push(candidates, item)
			} else if item.before((*candidates)[0]) {
				(*candidates)[0] = item
				heap.Fix(candidates, 0)
			}
			return nil
		})
		if e != nil {
			return nil, 0, e
		}
	}
	items := []diagnosticItem(*candidates)
	sort.Slice(items, func(i, j int) bool { return items[i].before(items[j]) })
	return items, pending, nil
}

func maintainDiagnosticStorage(root string, now time.Time, budget int64, active func(string) bool) error {
	diagnosticMaintenanceMu.Lock()
	defer diagnosticMaintenanceMu.Unlock()
	dirs, e := diagnosticFolders(root)
	if e != nil {
		return e
	}
	for _, dir := range dirs {
		if e = recoverStorageDeletes(dir); e != nil {
			return e
		}
	}
	items, pending, e := diagnosticCandidates(dirs, active, nil)
	if e != nil {
		return e
	}
	total, e := recordingTreeBytes(root)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	projected := total - pending
	if projected <= budget {
		return nil
	}
	var bucket time.Time
	for len(items) > 0 {
		for _, item := range items {
			nextBucket := item.at.Truncate(recordingSegment)
			// Preserve whole-half-hour cleanup even across batch boundaries.
			if projected <= budget && !nextBucket.Equal(bucket) {
				return nil
			}
			bucket = nextBucket
			if e = markStorageDelete(item.path); e != nil {
				continue
			}
			projected -= item.bytes
			if e = finishStorageDelete(item.path); e != nil {
				return e
			}
		}
		if len(items) < diagnosticCandidateBatch {
			return nil
		}
		after := items[len(items)-1]
		items = nil
		items, _, e = diagnosticCandidates(dirs, active, &after)
		if e != nil {
			return e
		}
	}
	return nil
}

// Inactive maintenance must own every policy directory, just as an active writer does.
func maintainInactiveDiagnostics(root string, now time.Time, budget int64, recover ...func() error) error {
	dirs, err := diagnosticFolders(root)
	if err != nil {
		return err
	}
	files := []*os.File{}
	defer func() {
		for _, f := range files {
			unlockState(f)
			f.Close()
		}
	}()
	for _, dir := range dirs {
		f, err := os.Open(dir)
		if err != nil {
			return err
		}
		if err = lockState(f); err != nil {
			f.Close()
			return err
		}
		files = append(files, f)
	}
	if budget <= 0 {
		budget = 10 << 30
	}
	for _, repair := range recover {
		if err := repair(); err != nil {
			return err
		}
	}
	return maintainDiagnosticStorage(root, now, budget, nil)
}

// Directory size never limits cleanup. Only one small read batch is retained.
func visitDiagnosticEntries(path string, visit func(os.DirEntry) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		batch, err := f.ReadDir(128)
		if err != nil && err != io.EOF {
			return err
		}
		for _, entry := range batch {
			if err := visit(entry); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
	}
}
