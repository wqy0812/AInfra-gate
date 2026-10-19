package aigate

import (
	"archive/tar"
	"compress/gzip"
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

// Serializes maintenance with diagnostic readers; request forwarding never
// takes this lock. Published request directories are immutable.
var diagnosticArchiveMu sync.RWMutex

const diagnosticRefName = "archive-ref.json"

type diagnosticRef struct {
	Archive string `json:"archive"`
}
type diagnosticItem struct {
	path     string
	at       time.Time
	bytes    int64
	archived bool
}

func diagnosticReference(dir string) (diagnosticRef, error) {
	var ref diagnosticRef
	b, e := os.ReadFile(filepath.Join(dir, diagnosticRefName))
	if e != nil {
		return ref, e
	}
	if len(b) > 1024 || json.Unmarshal(b, &ref) != nil || !strings.HasPrefix(ref.Archive, "archive-") || filepath.Base(ref.Archive) != ref.Archive {
		return ref, fmt.Errorf("invalid diagnostic archive reference")
	}
	return ref, nil
}
func diagnosticFolders(root string) ([]string, error) {
	dirs := []string{root}
	entries, e := os.ReadDir(root)
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
func diagnosticScan(root string, active func(string) bool) ([]diagnosticItem, error) {
	dirs, e := diagnosticFolders(root)
	if e != nil {
		return nil, e
	}
	var items []diagnosticItem
	for _, dir := range dirs {
		entries, e := os.ReadDir(dir)
		if e != nil {
			return nil, e
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			if strings.HasPrefix(entry.Name(), "archive-") {
				m, e := readSegmentArchive(path)
				if e != nil {
					continue
				}
				_ = m
				// The UTC bucket is embedded in the stable archive name.
				parts := strings.Split(entry.Name(), "-")
				if len(parts) != 3 {
					continue
				}
				at, ok := parseRecordingSegment(parts[1])
				if !ok {
					continue
				}
				n, e := recordingTreeBytes(path)
				if e != nil {
					return nil, e
				}
				items = append(items, diagnosticItem{path, at.Add(recordingSegment), n, true})
				continue
			}
			if !strings.HasPrefix(entry.Name(), "request-") || !slowRequestID(strings.TrimPrefix(entry.Name(), "request-")) || (active != nil && active(path)) {
				continue
			}
			if _, e := diagnosticReference(path); e == nil {
				continue
			} else if !os.IsNotExist(e) {
				return nil, e
			}
			if _, _, err := captureRecordFiles(path); err != nil {
				continue
			}
			b, e := captureReadSmall(filepath.Join(path, "meta.json"), captureMetaLimit)
			if e != nil {
				continue
			}
			var m struct {
				EndedAt         *time.Time `json:"ended_at"`
				StorageClosedAt *time.Time `json:"storage_closed_at"`
				CaptureState    string     `json:"capture_state"`
			}
			if json.Unmarshal(b, &m) != nil || m.CaptureState == "recording" {
				continue
			}
			if m.EndedAt == nil {
				m.EndedAt = m.StorageClosedAt
			}
			if m.EndedAt == nil {
				continue
			}
			n, e := recordingTreeBytes(path)
			if e != nil {
				return nil, e
			}
			items = append(items, diagnosticItem{path, *m.EndedAt, n, false})
		}
	}
	return items, nil
}
func archiveDiagnosticGroup(dir, bucket string, items []diagnosticItem) error {
	name := "archive-" + bucket + "-" + randomID()
	dest := filepath.Join(dir, name)
	if e := os.Mkdir(dest, 0700); e != nil {
		return e
	}
	// Hard links avoid a second uncompressed copy, and capture immutable files.
	for _, item := range items {
		base := filepath.Base(item.path)
		if e := os.Mkdir(filepath.Join(dest, base), 0700); e != nil {
			return e
		}
		files, e := os.ReadDir(item.path)
		if e != nil {
			return e
		}
		for _, f := range files {
			info, e := f.Info()
			if e != nil {
				return e
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("unsafe diagnostic member")
			}
			if e = os.Link(filepath.Join(item.path, f.Name()), filepath.Join(dest, base, f.Name())); e != nil {
				return e
			}
		}
	}
	if _, e := prepareSegmentArchive(dest); e != nil {
		return e
	}
	return finalizeDiagnosticArchive(dest)
}
func finalizeDiagnosticArchive(dest string) error {
	m, e := readSegmentArchive(dest)
	if e != nil {
		return e
	}
	// Verify the published copy before discarding any source; also repairs a
	// crash between manifest publication and pointer/source updates.
	digest, n, e := archiveHash(filepath.Join(dest, archivePayload))
	if e != nil {
		return e
	}
	if digest != m.SHA256 || n != m.Bytes {
		return fmt.Errorf("diagnostic archive checksum mismatch")
	}
	records := map[string]bool{}
	for _, f := range m.Files {
		parts := strings.Split(f.Name, "/")
		if len(parts) != 2 || !slowRequestID(strings.TrimPrefix(parts[0], "request-")) || parts[0] != "request-"+strings.TrimPrefix(parts[0], "request-") {
			return fmt.Errorf("invalid diagnostic member")
		}
		records[parts[0]] = true
	}
	for name := range records {
		path := filepath.Join(filepath.Dir(dest), name)
		if _, e = os.Stat(path); os.IsNotExist(e) {
			continue
		} else if e != nil {
			return e
		}
		if ref, err := diagnosticReference(path); err == nil && ref.Archive != filepath.Base(dest) {
			return fmt.Errorf("diagnostic already archived")
		}
		if _, e = recordingAtomicJSON(path, diagnosticRefName, diagnosticRef{filepath.Base(dest)}); e != nil {
			return e
		}
		for _, f := range m.Files {
			if !strings.HasPrefix(f.Name, name+"/") || filepath.Base(f.Name) == "meta.json" {
				continue
			}
			source := filepath.Join(filepath.Dir(dest), filepath.FromSlash(f.Name))
			if e = os.Remove(source); e != nil && !os.IsNotExist(e) {
				return e
			}
		}
		if e = syncDirectory(path); e != nil {
			return e
		}
	}
	return compactSegmentArchive(dest, nil)
}
func removeDiagnosticItem(item diagnosticItem) error {
	if !item.archived {
		return removeDiagnosticRecord(item.path)
	}
	m, e := readSegmentArchive(item.path)
	if e != nil {
		return e
	}
	seen := map[string]bool{}
	for _, f := range m.Files {
		name := strings.Split(f.Name, "/")[0]
		if seen[name] {
			continue
		}
		seen[name] = true
		path := filepath.Join(filepath.Dir(item.path), name)
		ref, e := diagnosticReference(path)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		if ref.Archive != filepath.Base(item.path) {
			return fmt.Errorf("diagnostic reference conflict")
		}
		if e = removeDiagnosticRecord(path); e != nil {
			return e
		}
	}
	entries, e := os.ReadDir(item.path)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		if entry.Name() != archivePayload && entry.Name() != archiveManifestName {
			return fmt.Errorf("unexpected archive content")
		}
	}
	for _, entry := range entries {
		if e = os.Remove(filepath.Join(item.path, entry.Name())); e != nil {
			return e
		}
	}
	return os.Remove(item.path)
}
func removeDiagnosticRecord(path string) error {
	entries, _, e := captureRecordFiles(path)
	if e != nil {
		return e
	}
	for _, f := range entries {
		if e = os.Remove(filepath.Join(path, f.Name())); e != nil {
			return e
		}
	}
	return os.Remove(path)
}
func maintainDiagnosticArchives(root string, now time.Time, budget int64, active func(string) bool) error {
	diagnosticArchiveMu.Lock()
	defer diagnosticArchiveMu.Unlock()
	dirs, e := diagnosticFolders(root)
	if e != nil {
		return e
	}
	for _, dir := range dirs {
		entries, e := os.ReadDir(dir)
		if e != nil {
			return e
		}
		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), "archive-") {
				dest := filepath.Join(dir, entry.Name())
				if _, e := readSegmentArchive(dest); e == nil {
					if e = finalizeDiagnosticArchive(dest); e != nil {
						return e
					}
				} else if os.IsNotExist(e) {
					if e = discardUncommittedDiagnostic(dest); e != nil {
						return e
					}
				}
			}
		}
	}
	items, e := diagnosticScan(root, active)
	if e != nil {
		return e
	}
	groups := map[string][]diagnosticItem{}
	for _, item := range items {
		if !item.archived && now.After(item.at.Truncate(recordingSegment).Add(recordingSegment+archiveAfter)) {
			key := filepath.Join(filepath.Dir(item.path), recordingSegmentName(item.at.Truncate(recordingSegment)))
			groups[key] = append(groups[key], item)
		}
	}
	for key, items := range groups {
		if e = archiveDiagnosticGroup(filepath.Dir(key), filepath.Base(key), items); e != nil {
			return e
		}
	}
	items, e = diagnosticScan(root, active)
	if e != nil {
		return e
	}
	sort.Slice(items, func(i, j int) bool { return items[i].at.Before(items[j].at) })
	total, e := recordingTreeBytes(root)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	for _, item := range items {
		if total <= budget {
			break
		}
		if e = removeDiagnosticItem(item); e != nil {
			return e
		}
		total, e = recordingTreeBytes(root)
		if e != nil {
			return e
		}
	}
	return nil
}

// The caller holds the diagnostic read lock. No extraction or unbounded body
// buffering is needed to read an archived evidence range.
func archivedDiagnosticFile(dir, name string) (io.ReadCloser, archiveEntry, string, error) {
	ref, e := diagnosticReference(dir)
	if e != nil {
		return nil, archiveEntry{}, "", e
	}
	path := filepath.Join(filepath.Dir(dir), ref.Archive)
	m, e := readSegmentArchive(path)
	if e != nil {
		return nil, archiveEntry{}, "", e
	}
	wanted := filepath.Base(dir) + "/" + name
	var entry archiveEntry
	for _, f := range m.Files {
		if f.Name == wanted {
			entry = f
			break
		}
	}
	if entry.Name == "" {
		return nil, entry, "", os.ErrNotExist
	}
	f, e := os.Open(filepath.Join(path, archivePayload))
	if e != nil {
		return nil, entry, "", e
	}
	gz, e := gzip.NewReader(f)
	if e != nil {
		f.Close()
		return nil, entry, "", e
	}
	tr := tar.NewReader(gz)
	for {
		h, e := tr.Next()
		if e != nil {
			gz.Close()
			f.Close()
			return nil, entry, "", e
		}
		if h.Name == wanted {
			if h.Size != entry.Bytes {
				gz.Close()
				f.Close()
				return nil, entry, "", fmt.Errorf("archive size mismatch")
			}
			return &diagnosticArchiveReader{Reader: tr, gz: gz, file: f}, entry, m.SHA256, nil
		}
	}
}

type diagnosticArchiveReader struct {
	io.Reader
	gz   *gzip.Reader
	file *os.File
}

func (r *diagnosticArchiveReader) Close() error { r.gz.Close(); return r.file.Close() }

// Before manifest publication, original records are untouched. Discard a
// partial hard-link staging tree only when every source inode is still present.
func discardUncommittedDiagnostic(dest string) error {
	if _, e := os.Lstat(filepath.Join(dest, archiveManifestName)); !os.IsNotExist(e) {
		return e
	}
	e := filepath.WalkDir(dest, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if path == dest {
			return nil
		}
		rel, e := filepath.Rel(dest, path)
		if e != nil {
			return e
		}
		parts := strings.Split(rel, string(os.PathSeparator))
		if len(parts) == 1 && !d.IsDir() {
			if rel == archivePayload || rel == ".archive.pending" || rel == archiveManifestName+".tmp" {
				info, e := d.Info()
				if e == nil && info.Mode().IsRegular() {
					return nil
				}
			}
			return fmt.Errorf("unknown archive preparation file")
		}
		if !strings.HasPrefix(parts[0], "request-") || !slowRequestID(strings.TrimPrefix(parts[0], "request-")) {
			return fmt.Errorf("unknown archive preparation directory")
		}
		if d.IsDir() {
			if len(parts) != 1 {
				return fmt.Errorf("nested archive preparation")
			}
			return nil
		}
		if len(parts) != 2 {
			return fmt.Errorf("invalid archive preparation member")
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		source, e := os.Lstat(filepath.Join(filepath.Dir(dest), rel))
		if e != nil || !info.Mode().IsRegular() || !source.Mode().IsRegular() || !os.SameFile(info, source) {
			return fmt.Errorf("archive preparation source unavailable")
		}
		return nil
	})
	if e != nil {
		return e
	}
	return os.RemoveAll(dest)
}
