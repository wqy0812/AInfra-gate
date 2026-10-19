package aigate

// Raw streaming export. No persistent archive, compression, or range index.
import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

const archiveManifestMaxBytes = 16 << 20
const archiveManifestMaxFiles = 65536
const exportMaxBytes = int64(100 << 30)

type archiveEntry struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type segmentArchive struct {
	Version int            `json:"version"`
	Format  string         `json:"format"`
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
func syncDirectory(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func prepareSegmentArchive(dir string) (segmentArchive, error) {
	m := segmentArchive{Version: 2, Format: "tar", Files: []archiveEntry{}}
	var total int64
	var metadataBytes int64
	e := walkArchiveFiles(dir, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		name, e := filepath.Rel(dir, path)
		if e != nil {
			return e
		}
		if name == "pinned" || name == deleteMarker || name == deleteMarker+".tmp" {
			return nil
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("export refuses nonregular member")
		}
		if len(m.Files) >= archiveManifestMaxFiles || len(name) > 4096 {
			return fmt.Errorf("archive_metadata_limit")
		}
		metadataBytes += int64(len(name)*6 + 200)
		if metadataBytes > archiveManifestMaxBytes {
			return fmt.Errorf("archive_metadata_limit")
		}
		total += info.Size()
		if total > exportMaxBytes {
			return fmt.Errorf("archive_expansion_limit")
		}
		hash, n, e := archiveHash(path)
		if e != nil {
			return e
		}
		m.Files = append(m.Files, archiveEntry{filepath.ToSlash(name), n, hash})
		return nil
	})
	if e != nil {
		return m, e
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Name < m.Files[j].Name })
	raw, e := json.Marshal(m)
	if len(raw) > archiveManifestMaxBytes {
		return m, fmt.Errorf("archive_metadata_limit")
	}
	return m, e
}
func writeSegmentTar(w io.Writer, dir string, m segmentArchive) error {
	tw := tar.NewWriter(w)
	for _, entry := range m.Files {
		if e := tw.WriteHeader(&tar.Header{Name: entry.Name, Size: entry.Bytes, Mode: 0600, Typeflag: tar.TypeReg}); e != nil {
			return e
		}
		f, e := os.Open(filepath.Join(dir, filepath.FromSlash(entry.Name)))
		if e != nil {
			return e
		}
		h := sha256.New()
		n, e := io.Copy(io.MultiWriter(tw, h), f)
		f.Close()
		if e != nil {
			return e
		}
		if n != entry.Bytes || hex.EncodeToString(h.Sum(nil)) != entry.SHA256 {
			return fmt.Errorf("export source changed")
		}
	}
	return tw.Close()
}

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
