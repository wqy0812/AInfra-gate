package aigate

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var diagnosticMaintenanceMu sync.Mutex

type diagnosticItem struct {
	path  string
	at    time.Time
	bytes int64
}

func diagnosticFolders(root string) ([]string, error) {
	dirs := []string{root}
	entries, e := boundedDiagnosticEntries(root)
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	for _, d := range entries {
		if d.IsDir() && observationPolicyDir(d.Name()) {
			dirs = append(dirs, filepath.Join(root, d.Name()))
		}
	}
	return dirs, nil
}
func maintainDiagnosticStorage(root string, now time.Time, budget int64, active func(string) bool) error {
	diagnosticMaintenanceMu.Lock()
	defer diagnosticMaintenanceMu.Unlock()
	dirs, e := diagnosticFolders(root)
	if e != nil {
		return e
	}
	groups := map[time.Time][]diagnosticItem{}
	var pending int64
	for _, dir := range dirs {
		if e = recoverStorageDeletes(dir); e != nil {
			return e
		}
		entries, e := boundedDiagnosticEntries(dir)
		if e != nil {
			return e
		}
		for _, entry := range entries {
			if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "request-") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			if active != nil && active(path) {
				continue
			}
			var meta struct {
				EndedAt         *time.Time `json:"ended_at"`
				StorageClosedAt *time.Time `json:"storage_closed_at"`
				CaptureState    string     `json:"capture_state"`
			}
			data, e := captureReadSmall(filepath.Join(path, "meta.json"), captureMetaLimit)
			if e != nil || json.Unmarshal(data, &meta) != nil || meta.CaptureState == "recording" {
				continue
			}
			at := meta.EndedAt
			if at == nil {
				at = meta.StorageClosedAt
			}
			if at == nil {
				continue
			}
			size, e := recordingTreeBytes(path)
			if e != nil {
				return e
			}
			if _, e = os.Stat(filepath.Join(path, deleteMarker)); e == nil {
				pending += size
				continue
			}
			bucket := at.Truncate(recordingSegment)
			groups[bucket] = append(groups[bucket], diagnosticItem{path, *at, size})
		}
	}
	total, e := recordingTreeBytes(root)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	projected := total - pending
	buckets := []time.Time{}
	for b := range groups {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Before(buckets[j]) })
	for _, b := range buckets {
		if projected <= budget {
			break
		}
		for _, item := range groups[b] {
			if e = markStorageDelete(item.path); e != nil {
				continue
			}
			projected -= item.bytes
			if e = finishStorageDelete(item.path); e != nil {
				return e
			}
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

// Explicit candidate budget protects maintenance before it builds a manifest.
// No original data is modified when a directory exceeds this safety budget.
func boundedDiagnosticEntries(path string) ([]os.DirEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	entries := []os.DirEntry{}
	for {
		batch, err := f.ReadDir(128)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if len(entries)+len(batch) > archiveManifestMaxFiles {
			return nil, fmt.Errorf("archive_metadata_limit")
		}
		entries = append(entries, batch...)
		if err == io.EOF {
			return entries, nil
		}
	}
}
