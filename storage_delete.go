package aigate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

const deleteMarker = ".deleting"

var storageLeaseMu sync.Mutex
var errStorageDeleting = errors.New("segment_deleting")

type storageReadLease struct {
	file *os.File
	path string
}

func acquireStorageRead(path string) (*storageReadLease, error) {
	storageLeaseMu.Lock()
	defer storageLeaseMu.Unlock()
	info, e := os.Lstat(path)
	if e != nil {
		return nil, e
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("invalid storage directory")
	}
	if _, e := os.Stat(filepath.Join(path, deleteMarker)); e == nil {
		return nil, errStorageDeleting
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, e
	}
	return &storageReadLease{f, path}, nil
}
func (l *storageReadLease) Close() error {
	unlockState(l.file)
	e := l.file.Close()
	_ = finishStorageDelete(l.path)
	return e
}
func markStorageDelete(path string) error {
	storageLeaseMu.Lock()
	defer storageLeaseMu.Unlock()
	if _, e := os.Stat(filepath.Join(path, "pinned")); !os.IsNotExist(e) {
		return fmt.Errorf("segment_protected")
	}
	if _, e := os.Stat(filepath.Join(path, deleteMarker)); e == nil {
		return nil
	}
	_, e := recordingAtomicJSON(path, deleteMarker, map[string]bool{"deleting": true})
	if e != nil {
		return e
	}
	return syncDirectory(path)
}
func finishStorageDelete(path string) error {
	storageLeaseMu.Lock()
	trashCommitted := strings.HasPrefix(filepath.Base(path), ".trash-")
	if _, e := os.Stat(filepath.Join(path, deleteMarker)); e != nil && !trashCommitted {
		storageLeaseMu.Unlock()
		if os.IsNotExist(e) {
			return nil
		}
		return e
	}
	f, e := os.Open(path)
	if e != nil {
		storageLeaseMu.Unlock()
		return e
	}
	if e = lockState(f); e != nil {
		f.Close()
		storageLeaseMu.Unlock()
		return nil
	}
	trash := filepath.Join(filepath.Dir(path), ".trash-"+filepath.Base(path)+"-"+randomID())
	if strings.HasPrefix(filepath.Base(path), ".trash-") {
		trash = path
	} else {
		e = os.Rename(path, trash)
	}
	if e == nil {
		e = syncDirectory(filepath.Dir(path))
	}
	storageLeaseMu.Unlock()
	defer f.Close()
	defer unlockState(f)
	if e != nil {
		return e
	}
	if e = os.RemoveAll(trash); e != nil {
		return e
	}
	return syncDirectory(filepath.Dir(trash))
}
func recoverStorageDeletes(root string) error {
	e := visitDiagnosticEntries(root, func(entry os.DirEntry) error {
		if !entry.IsDir() {
			return nil
		}
		path := filepath.Join(root, entry.Name())
		if _, e := os.Stat(filepath.Join(path, deleteMarker)); e == nil || strings.HasPrefix(entry.Name(), ".trash-") {
			return finishStorageDelete(path)
		} else if !os.IsNotExist(e) {
			return e
		}
		return nil
	})
	if os.IsNotExist(e) {
		return nil
	}
	return e
}
