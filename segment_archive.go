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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const archiveAfter = 72 * time.Hour
const archiveInterval = 5 * time.Minute
const archivePayload = "segment.tar.gz"
const archiveManifestName = "archive.json"

type archiveEntry struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type segmentArchive struct {
	Version int            `json:"version"`
	Bytes   int64          `json:"bytes"`
	SHA256  string         `json:"sha256"`
	Files   []archiveEntry `json:"files"`
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
	e = json.NewDecoder(io.LimitReader(f, 64<<20)).Decode(&m)
	if e == nil && (m.Version != 1 || !validDigest(m.SHA256) || m.Bytes <= 0) {
		e = fmt.Errorf("invalid archive manifest")
	}
	return m, e
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
	gz, _ := gzip.NewWriterLevel(f, gzip.BestSpeed)
	tw := tar.NewWriter(gz)
	e = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
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
		h := &tar.Header{Name: filepath.ToSlash(name), Mode: 0600, Size: info.Size(), Typeflag: tar.TypeReg}
		if err = tw.WriteHeader(h); err != nil {
			return err
		}
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
		m.Files = append(m.Files, archiveEntry{filepath.ToSlash(name), n, hex.EncodeToString(sum.Sum(nil))})
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
