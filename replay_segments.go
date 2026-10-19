package aigate

import (
	"fmt"
	"os"
	"path/filepath"
)

// The service admits only one batch. Its target runs share one filesystem lock
// per segment, so eviction stays excluded until the last reader has drained.
type replaySegmentLease struct {
	file *os.File
	refs int
}

func (s *replayService) acquireSegment(path string) (*os.File, error) {
	if s.taskMode {
		relative, err := filepath.Rel(s.recordRoot, path)
		if err != nil || !s.taskInputs[filepath.ToSlash(relative)] {
			return nil, fmt.Errorf("segment_not_saved")
		}
		return nil, nil // Immutable NAS inputs are protected by platform references.
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if lease := s.segments[path]; lease != nil {
		lease.refs++
		return lease.file, nil
	}
	var f *os.File
	var err error
	f, err = os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("segment_not_found")
	}
	if err := lockState(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("segment_busy")
	}
	s.segments[path] = &replaySegmentLease{file: f, refs: 1}
	return f, nil
}

func (s *replayService) releaseSegmentLocked(f *os.File) {
	if f == nil {
		return
	}
	lease := s.segments[f.Name()]
	lease.refs--
	if lease.refs == 0 {
		unlockState(f)
		f.Close()
		delete(s.segments, f.Name())
	}
}
