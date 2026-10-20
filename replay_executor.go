package aigate

// The executor is owned by one immutable task container. It exposes no HTTP
// service, leases, batch coordination, run retention, or restart dispatch.
import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type replayExecutor struct {
	checkpointTicker     func(time.Duration) (<-chan time.Time, func())
	taskInputs           map[string]bool
	recordRoot, dataRoot string
	targets              map[string]string
	markKey              []byte
	boot                 string
	audit                *log.Logger
	client               *http.Client
	bodyMemory           *replayByteBudget
	openResults          func(string) (replayResultFile, error)
	writeJSON            func(string, string, any) (int64, error)
	directory            *os.File
	ctx                  context.Context
	cancel               context.CancelFunc
	mu                   sync.Mutex
	active               map[string]*replayRun
	runs                 map[string]*replayRun
	wg                   sync.WaitGroup
}

func newReplayExecutor(recordRoot, dataRoot string, targets map[string]string, markKey []byte, audit io.Writer) (*replayExecutor, error) {
	if len(markKey) < replayMarkMinBytes || recordRoot == "" || dataRoot == "" || len(targets) == 0 {
		return nil, fmt.Errorf("replay mark key, input root, result root and target are required")
	}
	if audit == nil {
		audit = io.Discard
	}
	if err := os.MkdirAll(filepath.Join(dataRoot, "runs"), 0700); err != nil {
		return nil, err
	}
	directory, err := os.OpenFile(filepath.Join(dataRoot, ".replay.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = lockState(directory); err != nil {
		directory.Close()
		return nil, fmt.Errorf("task result root is in use: %w", err)
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext, MaxIdleConns: 4096, MaxIdleConnsPerHost: 4096, IdleConnTimeout: 90 * time.Second, DisableCompression: true}
	ctx, cancel := context.WithCancel(context.Background())
	return &replayExecutor{checkpointTicker: func(d time.Duration) (<-chan time.Time, func()) { t := time.NewTicker(d); return t.C, t.Stop }, recordRoot: recordRoot, dataRoot: dataRoot, targets: targets, markKey: markKey, boot: randomID(), audit: log.New(audit, "", 0), bodyMemory: newReplayByteBudget(), writeJSON: recordingAtomicJSON, openResults: func(path string) (replayResultFile, error) {
		return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	}, directory: directory, ctx: ctx, cancel: cancel, runs: map[string]*replayRun{}, active: map[string]*replayRun{}, client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (s *replayExecutor) Close() {
	s.cancel()
	s.mu.Lock()
	active := make([]*replayRun, 0, len(s.active))
	for _, r := range s.active {
		active = append(active, r)
	}
	s.mu.Unlock()
	for _, r := range active {
		r.stopped.Store(true)
		r.cancel()
		<-r.done
	}
	s.wg.Wait()
	unlockState(s.directory)
	s.directory.Close()
}

func (s *replayExecutor) checkTarget(ctx context.Context, name string) (replayTargetState, error) {
	state := replayTargetState{Name: name}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.targets[name]+"/runtime/v1/status", nil)
	if err != nil {
		return state, fmt.Errorf("target_unavailable")
	}
	response, err := s.client.Do(req)
	if err != nil {
		return state, fmt.Errorf("target_unavailable")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	var status struct {
		Service      string `json:"service"`
		ServiceID    string `json:"service_id"`
		RouterID     string `json:"router_id"`
		ConfigDigest string `json:"config_digest"`
	}
	// Only a functional proxy is a valid target: a switch business entry,
	// including the one being recorded, has no /runtime/v1/status.
	if err != nil || len(data) > 1<<20 || response.StatusCode != 200 || json.Unmarshal(data, &status) != nil || status.Service != "aigate-proxy" || !validDigest(status.ConfigDigest) {
		return state, fmt.Errorf("target_not_proxy")
	}
	// Target names are Router IDs: the platform locks the Router by this name.
	if status.RouterID != name {
		return state, fmt.Errorf("target_router_mismatch")
	}
	state.ServiceID, state.RouterID, state.ConfigDigest = status.ServiceID, status.RouterID, status.ConfigDigest
	return state, nil
}

func (s *replayExecutor) runDir(id string) string { return filepath.Join(s.dataRoot, "runs", id) }

func (s *replayExecutor) loadRun(id string) (replayRunView, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadRunLocked(id)
}

func (s *replayExecutor) loadRunLocked(id string) (replayRunView, bool) {
	r := s.runs[id]
	if r != nil {
		return r.view(), true
	}
	var view replayRunView
	data, err := recordingReadSmall(filepath.Join(s.runDir(id), "state.json"))
	if err != nil || json.Unmarshal(data, &view) != nil {
		return view, false
	}
	return view, true
}

func (s *replayExecutor) start(req replayRequest) (replayRunView, int, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	digest := runtimeDigest(req)
	if view, ok := s.loadRunLocked(req.RunID); ok {
		// A durable stop-before-start record is a barrier for every delayed
		// delivery of this ID, regardless of its request body.
		if view.StartCancelled {
			return view, 200, ""
		}
		// Idempotent by run_id: the same request returns the existing run.
		if runtimeDigest(view.Request) != digest {
			return view, 409, "run_conflict"
		}
		return view, 200, ""
	}
	if s.ctx.Err() != nil {
		return replayRunView{}, 409, "service_stopping"
	}
	if len(s.active) != 0 {
		return replayRunView{}, 409, "run_active"
	}
	dir := s.runDir(req.RunID)
	if err := os.Mkdir(dir, 0700); err != nil {
		return replayRunView{}, 409, "run_conflict"
	}
	ctx, cancel := context.WithCancel(s.ctx)
	r := &replayRun{svc: s, req: req, digest: digest, dir: dir, cancel: cancel, done: make(chan struct{}), state: "planning", created: time.Now().UTC()}
	if _, err := recordingAtomicJSON(dir, "request.json", req); err != nil {
		cancel()
		_ = os.RemoveAll(dir)
		return replayRunView{}, 500, "storage_failed"
	}
	if err := r.persist(); err != nil {
		cancel()
		_ = os.RemoveAll(dir)
		return replayRunView{}, 500, "storage_failed"
	}
	s.active[req.RunID] = r
	s.runs[req.RunID] = r
	if len(s.runs) > 32 {
		for id, old := range s.runs {
			if old != r && s.active[id] == nil && old.view().EndedAt != nil {
				delete(s.runs, id)
			}
		}
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		r.execute(ctx)
	}()
	return r.view(), 200, ""
}

func (s *replayExecutor) stop(id string) (replayRunView, int, string) {
	s.mu.Lock()
	r := s.runs[id]
	if r == nil {
		defer s.mu.Unlock()
		return s.stopAbsentLocked(id)
	}
	s.mu.Unlock()
	r.mu.Lock()
	changed := r.ended == nil && r.state != "stopping"
	if changed {
		r.state = "stopping"
		r.stopped.Store(true)
		r.cancel()
	}
	r.mu.Unlock()
	if changed {
		if err := r.persist(); err != nil {
			r.abort("storage_failed")
			return r.view(), 500, "storage_failed"
		}
	}
	return r.view(), 200, ""
}

func (s *replayExecutor) stopAbsentLocked(id string) (replayRunView, int, string) {
	if view, ok := s.loadRunLocked(id); ok {
		return view, 200, ""
	}
	// A durable stop prevents a delayed task start from dispatching.
	dir := s.runDir(id)
	if err := os.Mkdir(dir, 0700); err != nil {
		return replayRunView{}, 500, "storage_failed"
	}
	now := time.Now().UTC()
	view := replayRunView{SampledAt: &now, RunID: id, State: "stopped", CreatedAt: now, EndedAt: &now, StartCancelled: true}
	if _, err := recordingAtomicJSON(dir, "state.json", view); err != nil {
		_ = os.RemoveAll(dir)
		return replayRunView{}, 500, "storage_failed"
	}
	return view, 200, ""
}
