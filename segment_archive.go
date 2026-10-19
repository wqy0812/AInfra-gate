package aigate

// Archives are published before any source is removed. The manifest is the
// commit record; an archive without a manifest is an interrupted preparation.
import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const archiveAfter = 72 * time.Hour
const archivePayload = "segment.tar.gz"
const archiveManifestName = "archive.json"
const archiveManifestMaxBytes = int64(16 << 20)
const archiveManifestMaxFiles = 65536
const archiveManifestMaxBlocks = 131072

type archiveLimits struct {
	files, blocks int
	metadata      int64
}

var defaultArchiveLimits = archiveLimits{archiveManifestMaxFiles, archiveManifestMaxBlocks, archiveManifestMaxBytes}

type archiveMetadataBudget struct {
	limits archiveLimits
	bytes  int64
}

func (b *archiveMetadataBudget) reserve(n int64) error {
	if n < 0 || b.bytes > b.limits.metadata-n {
		return fmt.Errorf("archive_metadata_limit")
	}
	b.bytes += n
	return nil
}

type archiveEntry struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
	Offset int64  `json:"offset"`
}
type segmentArchive struct {
	Version int            `json:"version"`
	Bytes   int64          `json:"bytes"`
	SHA256  string         `json:"sha256"`
	Files   []archiveEntry `json:"files"`
	Blocks  []archiveBlock `json:"blocks"`
}

func archiveHash(path string) (string, int64, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", 0, e
	}
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, e
}
func readSegmentArchive(dir string) (segmentArchive, error) {
	var m segmentArchive
	f, e := os.Open(filepath.Join(dir, archiveManifestName))
	if e != nil {
		return m, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return m, e
	}
	if info.Size() > archiveManifestMaxBytes {
		return m, fmt.Errorf("archive_metadata_limit")
	}
	// Decode arrays incrementally: a small JSON document can still contain an
	// excessive number of tiny objects. Reject before growing either slice.
	dec := json.NewDecoder(io.LimitReader(f, archiveManifestMaxBytes+1))
	token, e := dec.Token()
	if e != nil || token != json.Delim('{') {
		return m, fmt.Errorf("invalid archive manifest")
	}
	seen := map[string]bool{}
	for dec.More() {
		token, e = dec.Token()
		if e != nil {
			return m, e
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return m, fmt.Errorf("invalid archive manifest field")
		}
		seen[key] = true
		switch key {
		case "version":
			e = dec.Decode(&m.Version)
		case "bytes":
			e = dec.Decode(&m.Bytes)
		case "sha256":
			e = dec.Decode(&m.SHA256)
		case "files", "blocks":
			token, e = dec.Token()
			if e != nil || token != json.Delim('[') {
				return m, fmt.Errorf("invalid archive manifest array")
			}
			for dec.More() {
				if key == "files" {
					if len(m.Files) >= archiveManifestMaxFiles {
						return m, fmt.Errorf("archive_metadata_limit")
					}
					var entry archiveEntry
					if e = dec.Decode(&entry); e != nil {
						return m, e
					}
					if !fs.ValidPath(entry.Name) || strings.Contains(entry.Name, "\\") || len(entry.Name) > 4096 || !validDigest(entry.SHA256) || entry.Offset < 0 || entry.Bytes < 0 {
						return m, fmt.Errorf("invalid archive entry")
					}
					m.Files = append(m.Files, entry)
				} else {
					if len(m.Blocks) >= archiveManifestMaxBlocks {
						return m, fmt.Errorf("archive_metadata_limit")
					}
					var block archiveBlock
					if e = dec.Decode(&block); e != nil {
						return m, e
					}
					m.Blocks = append(m.Blocks, block)
				}
			}
			_, e = dec.Token()
		default:
			return m, fmt.Errorf("invalid archive manifest field")
		}
		if e != nil {
			return m, e
		}
	}
	if _, e = dec.Token(); e != nil {
		return m, e
	}
	if _, e = dec.Token(); e != io.EOF {
		return m, fmt.Errorf("trailing archive metadata")
	}
	if m.Version != 1 || !validDigest(m.SHA256) || m.Bytes <= 0 || len(m.Blocks) == 0 {
		return m, fmt.Errorf("invalid archive manifest")
	}
	var raw, compressed int64
	for _, block := range m.Blocks {
		if block.Offset != raw || block.Compressed != compressed || block.Bytes <= 0 || block.Bytes > 2*archiveBlockBytes || block.RawBytes <= 0 || block.RawBytes > archiveBlockBytes || !validDigest(block.SHA256) {
			return m, fmt.Errorf("invalid archive block index")
		}
		raw += block.RawBytes
		compressed += block.Bytes
	}
	if compressed != m.Bytes {
		return m, fmt.Errorf("archive block size mismatch")
	}
	for _, entry := range m.Files {
		if entry.Offset > raw || entry.Bytes > raw-entry.Offset {
			return m, fmt.Errorf("invalid archive entry bounds")
		}
	}
	return m, nil
}

func syncDirectory(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}

// Caller owns the directory lock and guarantees all included files are sealed.
func prepareSegmentArchive(dir string) (segmentArchive, error) {
	return prepareSegmentArchiveLimits(dir, defaultArchiveLimits)
}
func prepareSegmentArchiveLimits(dir string, limits archiveLimits) (segmentArchive, error) {
	if m, e := readSegmentArchive(dir); e == nil {
		digest, n, e := archiveHash(filepath.Join(dir, archivePayload))
		if e != nil {
			return m, e
		}
		if digest != m.SHA256 || n != m.Bytes {
			return m, fmt.Errorf("archive checksum mismatch")
		}
		return m, nil
	} else if !os.IsNotExist(e) {
		return segmentArchive{}, e
	}
	m := segmentArchive{Version: 1, Files: []archiveEntry{}}
	tmp := filepath.Join(dir, ".archive.pending")
	f, e := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return m, e
	}
	defer os.Remove(tmp)
	budget := &archiveMetadataBudget{limits: limits, bytes: 1024}
	gz := &indexedGzipWriter{file: f, budget: budget}
	tw := tar.NewWriter(gz)
	e = walkArchiveFiles(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == dir {
			return nil
		}
		name, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if name == archivePayload || name == archiveManifestName || name == ".archive.pending" || name == "pinned" || name == archiveManifestName+".tmp" {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("archive refuses non-regular file: %s", name)
		}
		if len(m.Files) >= limits.files || len(name) > 4096 {
			return fmt.Errorf("archive_metadata_limit")
		}
		quoted, _ := json.Marshal(filepath.ToSlash(name))
		if err = budget.reserve(int64(len(quoted)) + 200); err != nil {
			return err
		}
		h := &tar.Header{Name: filepath.ToSlash(name), Mode: 0600, Size: info.Size(), Typeflag: tar.TypeReg}
		if err = tw.WriteHeader(h); err != nil {
			return err
		}
		offset := gz.position
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		sum := sha256.New()
		n, err := io.Copy(io.MultiWriter(tw, sum), src)
		src.Close()
		if err != nil {
			return err
		}
		if n != info.Size() {
			return fmt.Errorf("archive source changed")
		}
		m.Files = append(m.Files, archiveEntry{Name: filepath.ToSlash(name), Bytes: n, SHA256: hex.EncodeToString(sum.Sum(nil)), Offset: offset})
		return nil
	})
	for _, close := range []func() error{tw.Close, gz.Close, f.Sync, f.Close} {
		if err := close(); e == nil {
			e = err
		}
	}
	if e != nil {
		return m, e
	}
	m.Blocks = gz.blocks
	m.SHA256, m.Bytes, e = archiveHash(tmp)
	if e != nil {
		return m, e
	}
	if e = verifySegmentArchive(tmp, m); e != nil {
		return m, e
	}
	if e = os.Rename(tmp, filepath.Join(dir, archivePayload)); e != nil {
		return m, e
	}
	if e = syncDirectory(dir); e != nil {
		return m, e
	}
	if _, e = recordingAtomicJSON(dir, archiveManifestName, m); e != nil {
		return m, e
	}
	return m, syncDirectory(dir)
}
func verifySegmentArchive(path string, m segmentArchive) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	gz, e := gzip.NewReader(f)
	if e != nil {
		return e
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for _, want := range m.Files {
		h, e := tr.Next()
		if e != nil {
			return e
		}
		if h.Name != want.Name || h.Size != want.Bytes || h.Typeflag != tar.TypeReg {
			return fmt.Errorf("archive entry mismatch")
		}
		sum := sha256.New()
		n, e := io.Copy(sum, tr)
		if e != nil {
			return e
		}
		if n != want.Bytes || hex.EncodeToString(sum.Sum(nil)) != want.SHA256 {
			return fmt.Errorf("archive entry checksum mismatch")
		}
	}
	if _, e = tr.Next(); e != io.EOF {
		return fmt.Errorf("unexpected archive entries")
	}
	_, e = io.Copy(io.Discard, gz)
	return e
}

// Safe to repeat after a crash. Only unchanged manifest members are removed.
func compactSegmentArchive(dir string, keep map[string]bool) error {
	m, e := prepareSegmentArchive(dir)
	if e != nil {
		return e
	}
	for _, item := range m.Files {
		if keep[item.Name] {
			continue
		}
		if !fs.ValidPath(item.Name) || strings.Contains(item.Name, "\\") {
			return fmt.Errorf("invalid archive member")
		}
		path := filepath.Join(dir, filepath.FromSlash(item.Name))
		digest, n, e := archiveHash(path)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		if digest != item.SHA256 || n != item.Bytes {
			return fmt.Errorf("archive source changed: %s", item.Name)
		}
		if e = os.Remove(path); e != nil {
			return e
		}
		for parent := filepath.Dir(path); parent != dir && parent != "."; parent = filepath.Dir(parent) {
			if os.Remove(parent) != nil {
				break
			}
		}
	}
	return syncDirectory(dir)
}

const archiveBlockBytes = int64(1 << 20)

type archiveBlock struct {
	Offset     int64  `json:"offset"`
	Compressed int64  `json:"compressed"`
	Bytes      int64  `json:"bytes"`
	RawBytes   int64  `json:"raw_bytes"`
	SHA256     string `json:"sha256"`
}
type indexedGzipWriter struct {
	file     *os.File
	writer   *gzip.Writer
	position int64
	blocks   []archiveBlock
	sum      hash.Hash
	budget   *archiveMetadataBudget
}

func (w *indexedGzipWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		if w.writer == nil {
			offset, err := w.file.Seek(0, io.SeekCurrent)
			if err != nil {
				return total, err
			}
			if len(w.blocks) >= w.budget.limits.blocks {
				return total, fmt.Errorf("archive_metadata_limit")
			}
			if err := w.budget.reserve(200); err != nil {
				return total, err
			}
			w.blocks = append(w.blocks, archiveBlock{Offset: w.position, Compressed: offset})
			w.sum = sha256.New()
			w.writer, _ = gzip.NewWriterLevel(io.MultiWriter(w.file, w.sum), gzip.BestSpeed)
		}
		size := min(int64(len(p)), archiveBlockBytes-w.position%archiveBlockBytes)
		n, err := w.writer.Write(p[:size])
		w.position += int64(n)
		total += n
		p = p[n:]
		if err != nil {
			return total, err
		}
		if w.position%archiveBlockBytes == 0 {
			if err := w.closeBlock(); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}
func (w *indexedGzipWriter) closeBlock() error {
	if w.writer == nil {
		return nil
	}
	err := w.writer.Close()
	w.writer = nil
	if err != nil {
		return err
	}
	offset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	block := &w.blocks[len(w.blocks)-1]
	block.Bytes = offset - block.Compressed
	block.RawBytes = w.position - block.Offset
	block.SHA256 = hex.EncodeToString(w.sum.Sum(nil))
	return nil
}
func (w *indexedGzipWriter) Close() error { return w.closeBlock() }

// WalkDir reads and sorts an entire directory. Read bounded batches instead so
// even millions of small request directories cannot allocate an unbounded list.
func walkArchiveFiles(root string, visit fs.WalkDirFunc) error {
	var walk func(string, int) error
	walk = func(path string, depth int) error {
		if depth > 32 {
			return fmt.Errorf("archive_directory_depth_limit")
		}
		dir, err := os.Open(path)
		if err != nil {
			return err
		}
		defer dir.Close()
		for {
			entries, err := dir.ReadDir(128)
			if err != nil && err != io.EOF {
				return err
			}
			for _, entry := range entries {
				child := filepath.Join(path, entry.Name())
				if e := visit(child, entry, nil); e != nil {
					return e
				}
				if entry.IsDir() {
					if e := walk(child, depth+1); e != nil {
						return e
					}
				}
			}
			if err == io.EOF {
				return nil
			}
		}
	}
	return walk(root, 0)
}
