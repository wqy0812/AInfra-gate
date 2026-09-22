package main

// A policy change stops the old observer, but must not stop retention of its
// files. Only inactive directories are collected here; live writers retain
// their own files and count against the same budget. Draining generations may
// temporarily exceed it until their last request releases the writer.
import (
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type observationRecord struct {
	path  string
	files []string
	at    time.Time
	bytes int64
	slow  bool
}

func (c *ControlledGateway) retainObservations() {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-c.retentionStop:
			return
		case <-ticker.C:
		case <-c.retentionWake:
		}
		if err := c.cleanupObservations(time.Now()); err != nil {
			fmt.Fprintln(c.audit, string(encodeJSON(map[string]any{"event": "observation_retention_error", "error": err.Error()})))
		}
	}
}

func (c *ControlledGateway) cleanupObservations(now time.Time) error {
	// Prevent prepare from reopening a directory between the liveness check and
	// deletion. No observer acquires c.mu, so closing/locking it cannot invert locks.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		return nil
	} // a draft is not a retention policy
	protected := map[string]bool{}
	for _, g := range c.generations {
		if g.closed {
			continue
		}
		if g.g.profile != nil {
			protected[g.g.profile.c.Directory] = true
		}
		if g.g.slow != nil {
			protected[g.g.slow.c.Directory] = true
		}
	}
	cfg := c.active.g.config
	profileErr := cleanObservationRoot(filepath.Join(c.dataRoot, "request-profile"), false, now, cfg.Profile.RetentionHours, cfg.Profile.MaxDiskBytes, protected)
	slowErr := cleanObservationRoot(filepath.Join(c.dataRoot, "slow-requests"), true, now, cfg.SlowRequests.RetentionHours, cfg.SlowRequests.MaxDiskBytes, protected)
	if profileErr != nil {
		return profileErr
	}
	return slowErr
}

func observationPolicyDir(name string) bool {
	if len(name) != 16 {
		return false
	}
	for _, r := range name {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

func observationBytes(path string) (int64, error) {
	var size int64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, e := d.Info()
			if os.IsNotExist(e) {
				return nil
			}
			if e != nil {
				return e
			}
			size += info.Size()
		}
		return nil // WalkDir does not follow symlinks
	})
	return size, err
}

func abandonedProfileRecords(dir string) ([]observationRecord, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var records []observationRecord
	for _, entry := range entries {
		name := entry.Name()
		segment := strings.HasPrefix(name, "events-v2-") && strings.HasSuffix(name, ".jsonl")
		counter := name == "counters-metric-v2.json" || name == "counters-metric-v2.json.tmp"
		orphan := false
		for _, suffix := range []string{".meta.json", ".meta.tmp"} {
			if strings.HasPrefix(name, "events-v2-") && strings.HasSuffix(name, ".jsonl"+suffix) {
				_, e := os.Lstat(filepath.Join(dir, strings.TrimSuffix(name, suffix)))
				orphan = os.IsNotExist(e)
			}
		}
		if !segment && !counter && !orphan {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		path := filepath.Join(dir, name)
		r := observationRecord{path: path, files: []string{path}, at: info.ModTime(), bytes: info.Size()}
		if segment {
			for _, suffix := range []string{".meta.json", ".meta.tmp"} {
				meta, e := os.Lstat(path + suffix)
				if os.IsNotExist(e) {
					continue
				}
				if e != nil {
					return nil, e
				}
				if !meta.Mode().IsRegular() {
					return nil, fmt.Errorf("non-regular profile metadata: %s", path+suffix)
				}
				r.files = append(r.files, path+suffix)
				r.bytes += meta.Size()
			}
		}
		records = append(records, r)
	}
	return records, nil
}

func cleanObservationRoot(root string, slow bool, now time.Time, hours int, budget int64, protected map[string]bool) error {
	if hours <= 0 || int64(hours) > math.MaxInt64/int64(time.Hour) || budget <= 0 {
		return fmt.Errorf("invalid observation retention limits")
	}
	info, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("observation root must be a real directory: %s", root)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	var locks []*os.File
	defer func() {
		for _, f := range locks {
			unlockState(f)
			f.Close()
		}
	}()
	var records []observationRecord
	var dirs []string
	var total int64
	for _, entry := range entries {
		if !entry.IsDir() || !observationPolicyDir(entry.Name()) {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		if !protected[dir] {
			f, e := os.Open(dir)
			if e != nil {
				return e
			}
			if lockState(f) == nil {
				locks = append(locks, f)
				dirs = append(dirs, dir)
				if slow {
					stored, e := slowStoredRecords(dir)
					if e != nil {
						return e
					}
					for _, r := range stored {
						records = append(records, observationRecord{path: r.path, at: r.ended, bytes: r.bytes, slow: true})
					}
				} else {
					stored, e := abandonedProfileRecords(dir)
					if e != nil {
						return e
					}
					records = append(records, stored...)
				}
			} else {
				f.Close()
			} // another process still owns this directory
		}
		size, e := observationBytes(dir)
		if e != nil {
			return e
		}
		total += size
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].at.Equal(records[j].at) {
			return records[i].path < records[j].path
		}
		return records[i].at.Before(records[j].at)
	})
	cutoff := now.Add(-time.Duration(hours) * time.Hour)
	for _, r := range records {
		if r.at.After(cutoff) && total <= budget {
			continue
		}
		if r.slow {
			tmp := filepath.Join(filepath.Dir(r.path), ".pending-"+strings.TrimPrefix(filepath.Base(r.path), "request-"))
			if err := os.Rename(r.path, tmp); err != nil {
				return err
			}
			if err := slowRemoveFiles(tmp); err != nil {
				return err
			}
		} else {
			for _, path := range r.files {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
		}
		total -= r.bytes
	}
	for _, dir := range dirs {
		// Remove only empty directories. Never recursively delete unknown content.
		children, e := os.ReadDir(dir)
		if e != nil {
			return e
		}
		if len(children) == 0 {
			if e = os.Remove(dir); e != nil {
				return e
			}
		}
	}
	return nil
}
