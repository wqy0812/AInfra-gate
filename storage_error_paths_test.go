package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProfileConfigurationResourceBounds(t *testing.T) {
	t.Setenv("AIGATE_PROFILE_BOUNDARY_KEY", "")
	c := ProfileConfig{Enabled: true, APIKey: "key"}
	c.defaults(filepath.Join(t.TempDir(), "config.json"))
	for _, change := range []func(*ProfileConfig){
		func(v *ProfileConfig) { v.Listen = "bad" }, func(v *ProfileConfig) { v.APIKey = ""; v.APIKeyEnv = "AIGATE_PROFILE_BOUNDARY_KEY" },
		func(v *ProfileConfig) { v.Directory = "" }, func(v *ProfileConfig) { v.RetentionHours = 0 }, func(v *ProfileConfig) { v.RetentionHours = 721 },
		func(v *ProfileConfig) { v.MaxDiskBytes = (1 << 20) - 1 }, func(v *ProfileConfig) { v.QueueEvents = 0 }, func(v *ProfileConfig) { v.QueueEvents = 8193 },
	} {
		v := c
		change(&v)
		if v.validate() == nil {
			t.Fatal("invalid profile config accepted", v)
		}
	}
	if c.validate() != nil {
		t.Fatal("valid config rejected")
	}
}

func TestObservationStoresRejectFilesSymlinksAndSharedDirectories(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	if e := os.WriteFile(file, []byte("preserve"), 0600); e != nil {
		t.Fatal(e)
	}
	shared := filepath.Join(root, "shared")
	if e := os.Mkdir(shared, 0755); e != nil {
		t.Fatal(e)
	}
	link := filepath.Join(root, "link")
	if e := os.Symlink(shared, link); e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{file, link, shared} {
		if path != shared {
			if p, e := newRequestProfiler(ProfileConfig{Directory: path}); e == nil {
				p.close()
				t.Fatal("profile adopted foreign path", path)
			}
		}
		cfg := SlowRequestConfig{Enabled: true, Directory: path}
		cfg.defaults(filepath.Join(root, "config.json"))
		if s, e := newSlowRequestStore(cfg, log.New(io.Discard, "", 0)); e == nil {
			s.close()
			t.Fatal("slow store adopted foreign path", path)
		}
		if s, e := newIncidentStore(path, "g", "boot", nil); e == nil {
			s.close()
			t.Fatal("incident store adopted foreign path", path)
		}
	}
	data, _ := os.ReadFile(file)
	if string(data) != "preserve" {
		t.Fatal("foreign file changed")
	}
	if e := slowWriteFile(file, []byte("replace")); e == nil {
		t.Fatal("existing slow record overwritten")
	}
	if _, _, e := slowRecordFiles(filepath.Join(root, "missing")); e == nil {
		t.Fatal("missing record accepted")
	}
	if _, e := incidentDirs(filepath.Join(root, "missing")); e == nil {
		t.Fatal("missing incidents accepted")
	}
}

type shortDiagnosticWriter struct{}

func (shortDiagnosticWriter) Write(p []byte) (int, error) {
	return len(p) - 1, errors.New("write failed")
}

func TestConnectionDiagnosticsBoundsAndWriteFailure(t *testing.T) {
	var absent *connectionDiagnostics
	absent.emit(nil)
	absent.close()
	d := &connectionDiagnostics{queue: make(chan []byte, 1)}
	d.emit(map[string]string{"valid": "record"})
	d.emit("overflow")
	d.emit(make(chan int))
	d.emit(strings.Repeat("x", 16385))
	if d.dropped.Load() != 3 || len(d.queue) != 1 {
		t.Fatal(d.dropped.Load(), len(d.queue))
	}
	d.closed = true
	d.emit("closed")
	if d.dropped.Load() != 4 {
		t.Fatal("closed logger accepted an event")
	}
	live := newConnectionDiagnostics(shortDiagnosticWriter{}, "boot")
	live.emit("event")
	live.close()
	live.close()
	if live.writeErrors.Load() != 1 {
		t.Fatal("writer failure was not recorded")
	}
	nilWriter := newConnectionDiagnostics(nil, "boot")
	nilWriter.emit("event")
	nilWriter.close()
}

func TestProfileInvalidCheckpointsDoNotRestorePartialCounters(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*profileCounterState)
	}{
		{"version", func(v *profileCounterState) { v.Version = 2 }},
		{"epoch", func(v *profileCounterState) { v.Started = 0 }},
		{"missing values", func(v *profileCounterState) { v.Values = nil }},
		{"missing types", func(v *profileCounterState) { v.Types = nil }},
		{"missing groups", func(v *profileCounterState) { v.Groups = nil }},
		{"foreign metric", func(v *profileCounterState) { v.Values["foreign"] = 1 }},
		{"long metric", func(v *profileCounterState) { v.Values["aigate_"+strings.Repeat("x", 2049)] = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := profileAPIFixture()
			p.c.Directory = t.TempDir()
			p.values = map[string]float64{"aigate_keep": 9}
			state := profileCounterState{Version: 1, Started: 1, Values: map[string]float64{}, Types: map[string]string{}, Groups: map[string]bool{}}
			tc.change(&state)
			data, _ := json.Marshal(state)
			path := filepath.Join(p.c.Directory, "counters-metric-v2.json")
			if e := os.WriteFile(path, data, 0600); e != nil {
				t.Fatal(e)
			}
			p.restoreCounters()
			if p.writes.Load() != 1 || p.values["aigate_keep"] != 9 {
				t.Fatal("invalid checkpoint partly applied")
			}
		})
	}
	p := profileAPIFixture()
	p.c.Directory = filepath.Join(t.TempDir(), "missing")
	p.saveCounters()
	if p.writes.Load() != 1 {
		t.Fatal("failed checkpoint write hidden")
	}
}

func TestIncidentPersistenceNeverReplacesForeignPaths(t *testing.T) {
	now := time.Now()
	root := t.TempDir()
	s := &incidentStore{root: root, retention: time.Hour}
	old := streamIncident{ID: "old", TriggeredAt: now.Add(-2 * time.Hour)}
	if e := s.save(old); e != nil {
		t.Fatal(e)
	}
	if entries, _ := os.ReadDir(root); len(entries) != 0 {
		t.Fatal("expired incident persisted")
	}
	v := streamIncident{ID: strings.Repeat("x", 17000), TriggeredAt: now}
	if e := s.save(v); e == nil {
		t.Fatal("oversized incident accepted")
	}
	v = streamIncident{ID: "record", TriggeredAt: now}
	path := s.recordPath(v)
	if e := os.MkdirAll(path, 0700); e != nil {
		t.Fatal(e)
	}
	if e := s.save(v); e == nil {
		t.Fatal("foreign directory replaced")
	}
	if _, e := os.Stat(path + ".tmp"); !os.IsNotExist(e) {
		t.Fatal("failed rename left temporary incident")
	}
}
