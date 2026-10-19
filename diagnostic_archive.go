package aigate

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
var diagnosticMaintenanceMu sync.Mutex

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
func diagnosticScan(root string, active func(string) bool) ([]diagnosticItem, error) {
	dirs, e := diagnosticFolders(root)
	if e != nil {
		return nil, e
	}
	var items []diagnosticItem
	for _, dir := range dirs {
		entries, e := boundedDiagnosticEntries(dir)
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
				if len(items) >= archiveManifestMaxFiles {
					return nil, fmt.Errorf("archive_metadata_limit")
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
			if len(items) >= archiveManifestMaxFiles {
				return nil, fmt.Errorf("archive_metadata_limit")
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
	records := map[string][]archiveEntry{}
	for _, f := range m.Files {
		parts := strings.Split(f.Name, "/")
		if len(parts) != 2 || !slowRequestID(strings.TrimPrefix(parts[0], "request-")) || parts[0] != "request-"+strings.TrimPrefix(parts[0], "request-") {
			return fmt.Errorf("invalid diagnostic member")
		}
		records[parts[0]] = append(records[parts[0]], f)
	}
	if e = func() error {
		diagnosticArchiveMu.Lock()
		defer diagnosticArchiveMu.Unlock()
		for name, files := range records {
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
			for _, f := range files {
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
		return nil
	}(); e != nil {
		return e
	}
	return compactSegmentArchive(dest, nil)
}
func removeDiagnosticItem(item diagnosticItem) error {
	diagnosticArchiveMu.Lock()
	defer diagnosticArchiveMu.Unlock()
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
	diagnosticMaintenanceMu.Lock()
	defer diagnosticMaintenanceMu.Unlock()
	dirs, e := diagnosticFolders(root)
	if e != nil {
		return e
	}
	for _, dir := range dirs {
		entries, e := boundedDiagnosticEntries(dir)
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
	return archivedDiagnosticRange(context.Background(), dir, name, 0)
}
func archivedDiagnosticRange(ctx context.Context, dir, name string, offset int64) (io.ReadCloser, archiveEntry, string, error) {
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
	if offset < 0 || offset > entry.Bytes || len(m.Blocks) == 0 {
		f.Close()
		return nil, entry, "", fmt.Errorf("invalid archive range index")
	}
	reader := &verifiedArchiveReader{ctx: ctx, file: f, blocks: m.Blocks, position: entry.Offset + offset, end: entry.Offset + entry.Bytes}
	// Validate the first complete block before exposing any requested bytes.
	if reader.position < reader.end {
		if e = reader.load(); e != nil {
			f.Close()
			return nil, entry, "", e
		}
	}
	return reader, entry, m.SHA256, nil
}

type archiveContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *archiveContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

// Each compressed member is bounded, SHA-256 verified and fully decompressed
// (including gzip CRC/trailer) before its bytes are made available to callers.
// At most one decompressed block is retained across Read calls.
type verifiedArchiveReader struct {
	ctx           context.Context
	file          *os.File
	blocks        []archiveBlock
	position, end int64
	buffer        []byte
	bufferOffset  int64
}

func (r *verifiedArchiveReader) load() error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	index := sort.Search(len(r.blocks), func(i int) bool { return r.blocks[i].Offset > r.position }) - 1
	if index < 0 {
		return fmt.Errorf("invalid archive range index")
	}
	block := r.blocks[index]
	if r.position >= block.Offset+block.RawBytes {
		return fmt.Errorf("invalid archive range index")
	}
	data := make([]byte, block.Bytes)
	if _, err := io.ReadFull(&archiveContextReader{r.ctx, io.NewSectionReader(r.file, block.Compressed, block.Bytes)}, data); err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != block.SHA256 {
		return fmt.Errorf("archive block checksum mismatch")
	}
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	gz.Multistream(false)
	plain, err := io.ReadAll(io.LimitReader(&archiveContextReader{r.ctx, gz}, archiveBlockBytes+1))
	gz.Close()
	if err != nil {
		return err
	}
	if int64(len(plain)) != block.RawBytes {
		return fmt.Errorf("archive block size mismatch")
	}
	r.buffer, r.bufferOffset = plain, block.Offset
	return nil
}
func (r *verifiedArchiveReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.position >= r.end {
		return 0, io.EOF
	}
	if r.buffer == nil || r.position >= r.bufferOffset+int64(len(r.buffer)) {
		r.buffer = nil
		if err := r.load(); err != nil {
			return 0, err
		}
	}
	start := r.position - r.bufferOffset
	n := copy(p, r.buffer[start:min(int64(len(r.buffer)), start+r.end-r.position)])
	r.position += int64(n)
	return n, nil
}
func (r *verifiedArchiveReader) Close() error { r.buffer = nil; return r.file.Close() }

// Before manifest publication, original records are untouched. Discard a
// partial hard-link staging tree only when every source inode is still present.
func discardUncommittedDiagnostic(dest string) error {
	if _, e := os.Lstat(filepath.Join(dest, archiveManifestName)); !os.IsNotExist(e) {
		return e
	}
	e := walkArchiveFiles(dest, func(path string, d os.DirEntry, e error) error {
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
	return maintainDiagnosticArchives(root, now, budget, nil)
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
