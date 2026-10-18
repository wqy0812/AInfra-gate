package aigate

// aigate-replay serve: the only replay entry. It is called by the platform
// engine through an authenticated control listener and sends only to targets
// named in its local configuration file.
import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	replayControlKeyEnv      = "AIGATE_REPLAY_CONTROL_KEY"
	replayRunsKept           = 200
	replayRecordingList      = 500
	replayProtocolCapability = "recording-original-request-v1"
)

type replayTargetsFile struct {
	Targets map[string]string `json:"targets"`
}

func readReplayTargets(path string) (map[string]string, error) {
	data, err := recordingReadSmall(path)
	if err != nil {
		return nil, err
	}
	var file replayTargetsFile
	if err = strictDecode(data, &file); err != nil || len(file.Targets) == 0 {
		return nil, fmt.Errorf("invalid targets file: a non-empty {\"targets\":{name:url}} object is required")
	}
	out := map[string]string{}
	for name, raw := range file.Targets {
		base, err := controlBase(raw)
		if !validOperationID(name) || err != nil {
			return nil, fmt.Errorf("invalid replay target %q", name)
		}
		out[name] = base
	}
	return out, nil
}

type replayService struct {
	key         string
	cidrs       []netip.Prefix
	recordRoot  string
	dataRoot    string
	targets     map[string]string
	markKey     []byte
	boot        string
	audit       *log.Logger
	client      *http.Client
	bodyMemory  *replayByteBudget
	openResults func(string) (replayResultFile, error)
	writeJSON   func(string, string, any) (int64, error)
	directory   *os.File
	ctx         context.Context
	cancel      context.CancelFunc

	mu        sync.Mutex
	active    map[string]*replayRun
	reporting map[string]int
	segments  map[string]*replaySegmentLease
	runs      map[string]*replayRun
	wg        sync.WaitGroup
}

func newReplayService(key string, cidrs []netip.Prefix, recordRoot, dataRoot string, targets map[string]string, markKey []byte, audit io.Writer) (*replayService, error) {
	if len(key) < 32 || len(cidrs) == 0 || len(markKey) < replayMarkMinBytes || recordRoot == "" || dataRoot == "" || len(targets) == 0 {
		return nil, fmt.Errorf("control key (32+ bytes), replay mark key (32+ bytes), allowed CIDRs, record root, data root and targets are required")
	}
	if audit == nil {
		audit = io.Discard
	}
	runs := filepath.Join(dataRoot, "runs")
	if err := os.MkdirAll(runs, 0700); err != nil {
		return nil, err
	}
	directory, err := os.Open(dataRoot)
	if err != nil {
		return nil, err
	}
	if err = lockState(directory); err != nil {
		directory.Close()
		return nil, fmt.Errorf("replay data root is in use: %w", err)
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns: 4096, MaxIdleConnsPerHost: 4096, IdleConnTimeout: 90 * time.Second, DisableCompression: true}
	ctx, cancel := context.WithCancel(context.Background())
	s := &replayService{key: key, cidrs: cidrs, recordRoot: recordRoot, dataRoot: dataRoot, targets: targets, markKey: markKey, boot: randomID(),
		audit: log.New(audit, "", 0), bodyMemory: newReplayByteBudget(), writeJSON: recordingAtomicJSON, openResults: func(path string) (replayResultFile, error) {
			return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		}, directory: directory, ctx: ctx, cancel: cancel, runs: map[string]*replayRun{}, active: map[string]*replayRun{}, reporting: map[string]int{}, segments: map[string]*replaySegmentLease{},
		client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if err = s.recover(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// recover marks runs interrupted by an earlier process as end_unknown and
// keeps at most replayRunsKept run directories.
func (s *replayService) recover() error {
	root := filepath.Join(s.dataRoot, "runs")
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !validOperationID(entry.Name()) {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		var view replayRunView
		data, err := recordingReadSmall(filepath.Join(dir, "state.json"))
		if err != nil || json.Unmarshal(data, &view) != nil {
			continue
		}
		if view.State == "prepared" || view.State == "scheduled" || view.State == "planning" || view.State == "running" || view.State == "stopping" {
			if err := recoverReplayResults(dir, &view); err != nil {
				s.audit.Print(string(encodeJSON(map[string]any{"event": "replay_results_recovery_incomplete", "run_id": view.RunID, "error": err.Error()})))
			}
			now := time.Now().UTC()
			view.State, view.Error, view.EndedAt = "end_unknown", "service_restarted", &now
			view.EndReason = "service_restarted"
			for i := range view.Sources {
				view.Sources[i].State = "end_unknown"
			}
			if _, err := recordingAtomicJSON(dir, "state.json", view); err != nil {
				return err
			}
		}
	}
	return s.pruneRuns(replayRunsKept)
}

// pruneRuns removes the oldest run directories until at most keep remain.
// Callers hold s.mu or run before the service starts. Never remove the active
// run when recording a stop that arrived before its start.
func (s *replayService) pruneRuns(keep int) error {
	root := filepath.Join(s.dataRoot, "runs")
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	type kept struct {
		name string
		at   time.Time
	}
	var all []kept
	activeBatches := map[string]bool{}
	for _, run := range s.active {
		if run.req.BatchID != "" {
			activeBatches[run.req.BatchID] = true
		}
	}
	for _, entry := range entries {
		if !entry.IsDir() || !validOperationID(entry.Name()) {
			continue
		}
		if s.active[entry.Name()] != nil || s.reporting[entry.Name()] > 0 {
			keep--
			continue
		}
		var view replayRunView
		data, err := recordingReadSmall(filepath.Join(root, entry.Name(), "state.json"))
		if err != nil || json.Unmarshal(data, &view) != nil {
			all = append(all, kept{entry.Name(), time.Time{}})
			continue
		}
		if view.Request != nil && activeBatches[view.Request.BatchID] {
			keep--
			continue
		}
		all = append(all, kept{entry.Name(), view.CreatedAt})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	for len(all) > max(keep, 0) {
		_ = os.RemoveAll(filepath.Join(root, all[0].name))
		delete(s.runs, all[0].name)
		all = all[1:]
	}
	return nil
}

func (s *replayService) Close() {
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

func (s *replayService) checkTarget(ctx context.Context, name string) (replayTargetState, error) {
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

func (s *replayService) runDir(id string) string { return filepath.Join(s.dataRoot, "runs", id) }

func (s *replayService) loadRun(id string) (replayRunView, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadRunLocked(id)
}

func (s *replayService) loadRunLocked(id string) (replayRunView, bool) {
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

func replayJSON(w http.ResponseWriter, status int, value any) {
	newRuntimeResponse(status, value).write(w)
}

func (s *replayService) start(req replayRequest) (replayRunView, int, string) {
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
	for _, active := range s.active {
		if req.BatchID == "" || active.req.BatchID != req.BatchID || active.req.Target == req.Target {
			return replayRunView{}, 409, "run_active"
		}
	}
	_ = s.pruneRuns(replayRunsKept - 1)
	dir := s.runDir(req.RunID)
	if err := os.Mkdir(dir, 0700); err != nil {
		return replayRunView{}, 409, "run_conflict"
	}
	ctx, cancel := context.WithCancel(s.ctx)
	r := &replayRun{svc: s, req: req, digest: digest, dir: dir, cancel: cancel, done: make(chan struct{}), state: "planning", created: time.Now().UTC(), startSignal: make(chan struct{}), leaseUntil: time.Now().Add(15 * time.Second)}
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
	if req.PrepareOnly {
		s.wg.Add(1)
		go func() { defer s.wg.Done(); r.watchLease(ctx) }()
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		r.execute(ctx)
	}()
	return r.view(), 200, ""
}

func (s *replayService) stop(id string) (replayRunView, int, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.runs[id]
	if r == nil {
		if view, ok := s.loadRunLocked(id); ok {
			return view, 200, ""
		}
		// 404 cannot prove a concurrent POST /runs will never arrive. Persist
		// the stop under the start lock before acknowledging it to the platform.
		if err := s.pruneRuns(replayRunsKept - 1); err != nil {
			return replayRunView{}, 500, "storage_failed"
		}
		dir := s.runDir(id)
		if err := os.Mkdir(dir, 0700); err != nil {
			return replayRunView{}, 500, "storage_failed"
		}
		now := time.Now().UTC()
		view := replayRunView{RunID: id, State: "stopped", CreatedAt: now, EndedAt: &now, StartCancelled: true}
		if _, err := recordingAtomicJSON(dir, "state.json", view); err != nil {
			_ = os.RemoveAll(dir)
			return replayRunView{}, 500, "storage_failed"
		}
		return view, 200, ""
	}
	r.mu.Lock()
	if r.ended == nil && r.state != "stopping" {
		r.state = "stopping"
		r.stopped.Store(true)
		r.cancel()
	}
	r.mu.Unlock()
	return r.view(), 200, ""
}

type replayRecordingView struct {
	recordingSessionFile
	SegmentStatus []replaySegmentStatus `json:"segment_status"`
}

// listRecordings returns metadata only, never request or response bytes.
func (s *replayService) listRecordings(gateway string) ([]replayRecordingView, error) {
	dir := filepath.Join(s.recordRoot, gateway)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []replayRecordingView{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := []replayRecordingView{}
	for _, entry := range entries {
		if !entry.IsDir() || !validOperationID(entry.Name()) {
			continue
		}
		session, err := readRecordingSession(s.recordRoot, gateway, entry.Name())
		if err != nil {
			continue
		}
		view := replayRecordingView{recordingSessionFile: session, SegmentStatus: []replaySegmentStatus{}}
		children, _ := os.ReadDir(filepath.Join(dir, entry.Name()))
		for _, child := range children {
			if _, ok := parseRecordingSegment(child.Name()); !ok || !child.IsDir() {
				continue
			}
			if status, err := readRecordingSegment(s.recordRoot, gateway, entry.Name(), child.Name()); err == nil {
				view.SegmentStatus = append(view.SegmentStatus, status)
			}
		}
		sort.Slice(view.SegmentStatus, func(i, j int) bool { return view.SegmentStatus[i].Start.Before(view.SegmentStatus[j].Start) })
		out = append(out, view)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if len(out) > replayRecordingList {
		out = out[:replayRecordingList]
	}
	return out, nil
}

func (s *replayService) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !controlAuthorized(r, s.cidrs, s.key) {
			runtimeError(w, 403, "unauthorized")
			return
		}
		const prefix = "/control/replay/v1/"
		path := strings.TrimPrefix(r.URL.Path, prefix)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == prefix+"clock":
			received := time.Now().UTC()
			replayJSON(w, 200, map[string]any{"boot_id": s.boot, "received_at": received, "sent_at": time.Now().UTC()})
		case r.Method == http.MethodGet && r.URL.Path == prefix+"status":
			s.mu.Lock()
			var active any
			ids := make([]string, 0, len(s.active))
			batch := ""
			for id, run := range s.active {
				ids = append(ids, id)
				batch = run.req.BatchID
			}
			sort.Strings(ids)
			if len(ids) > 0 {
				active = ids[0]
			}
			s.mu.Unlock()
			names := []string{}
			for name := range s.targets {
				names = append(names, name)
			}
			sort.Strings(names)
			replayJSON(w, 200, map[string]any{"service": "aigate-replay", "boot_id": s.boot, "targets": names, "active_run": active, "active_runs": ids, "active_batch": batch, "capabilities": []string{replayProtocolCapability, replayOverlayCapability, replayMultiTargetCapability, replayPerformanceCapability, replaySourceConcurrencyCapability}, "recording_schema": recordingSchema})
		case r.Method == http.MethodGet && r.URL.Path == prefix+"recordings":
			gateway := r.URL.Query().Get("gateway_id")
			if !validOperationID(gateway) {
				runtimeError(w, 400, "invalid_gateway")
				return
			}
			list, err := s.listRecordings(gateway)
			if err != nil {
				runtimeError(w, 500, "recordings_unreadable")
				return
			}
			replayJSON(w, 200, map[string]any{"gateway_id": gateway, "recordings": list})
		case r.Method == http.MethodPost && r.URL.Path == prefix+"runs":
			data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
			var req replayRequest
			if err != nil || strictDecode(data, &req) != nil {
				runtimeError(w, 400, "invalid_request")
				return
			}
			var fields map[string]json.RawMessage
			_ = json.Unmarshal(data, &fields)
			_, single := fields["recording"]
			_, multiple := fields["recordings"]
			if single && multiple {
				runtimeError(w, 400, "invalid_recordings")
				return
			}
			if err := req.normalize(); err != nil {
				runtimeError(w, 400, err.Error())
				return
			}
			if _, ok := s.targets[req.Target]; !ok {
				runtimeError(w, 400, "unknown_target")
				return
			}
			view, status, code := s.start(req)
			if code != "" {
				runtimeError(w, status, code)
				return
			}
			replayJSON(w, 200, view)
		case strings.HasPrefix(path, "runs/"):
			rest := strings.TrimPrefix(path, "runs/")
			id, action, _ := strings.Cut(rest, "/")
			if !validOperationID(id) {
				runtimeError(w, 404, "run_not_found")
				return
			}
			if r.Method == http.MethodPost && action == "truncate" {
				s.mu.Lock()
				run := s.runs[id]
				s.mu.Unlock()
				if run == nil {
					runtimeError(w, 404, "run_not_found")
					return
				}
				view, status, code := run.truncateTail(true)
				if code != "" {
					runtimeError(w, status, code)
				} else {
					replayJSON(w, status, view)
				}
				return
			}
			if r.Method == http.MethodGet && action == "report" {
				if r.URL.Query().Has("version") {
					runtimeError(w, 400, "report_version_selection_removed")
					return
				}
				if report, ok := s.loadReport(id); ok {
					replayJSON(w, 200, report)
				} else {
					runtimeError(w, 404, "run_not_found")
				}
				return
			}
			if r.Method == http.MethodGet && action == "" {
				if view, ok := s.loadRun(id); ok {
					replayJSON(w, 200, view)
				} else {
					runtimeError(w, 404, "run_not_found")
				}
				return
			}
			if r.Method == http.MethodPost && (action == "start" || action == "lease") {
				var command replaySyncCommand
				data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
				if err != nil || strictDecode(data, &command) != nil {
					runtimeError(w, 400, "invalid_request")
					return
				}
				view, status, code := s.syncRun(id, action, command)
				if code != "" {
					runtimeError(w, status, code)
					return
				}
				replayJSON(w, 200, view)
				return
			}
			if r.Method == http.MethodPost && action == "stop" {
				view, status, code := s.stop(id)
				if code != "" {
					runtimeError(w, status, code)
					return
				}
				replayJSON(w, 200, view)
				return
			}
			runtimeError(w, 404, "not_found")
		default:
			runtimeError(w, 404, "not_found")
		}
	})
}

// RunReplay starts the replay control service. There are no other subcommands.
func RunReplay(args []string, out, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "version" {
		_, err := fmt.Fprintln(out, "aigate-replay control/replay/v1")
		return err
	}
	if len(args) == 0 || args[0] != "serve" {
		return fmt.Errorf("usage: aigate-replay serve [flags]; replay is controlled only through the platform")
	}
	f := flag.NewFlagSet("aigate-replay serve", flag.ContinueOnError)
	f.SetOutput(stderr)
	listen := f.String("listen", "127.0.0.1:18085", "authenticated platform control listener")
	allowed := f.String("allowed-cidrs", "127.0.0.0/8,::1/128", "control source CIDRs")
	recordRoot := f.String("record-root", "", "switch recording directory (read only)")
	dataRoot := f.String("data-root", "/var/lib/aigate-replay", "run plans, state and progress")
	targetsPath := f.String("targets", "", "local JSON file naming the allowed replay targets")
	shutdown := f.Duration("shutdown-timeout", 30*time.Second, "graceful shutdown deadline")
	if err := f.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if f.NArg() != 0 || *recordRoot == "" || *targetsPath == "" || *shutdown <= 0 {
		return fmt.Errorf("--record-root and --targets are required")
	}
	targets, err := readReplayTargets(*targetsPath)
	if err != nil {
		return err
	}
	cidrs, err := parseControlCIDRs(strings.Split(*allowed, ","))
	if err != nil {
		return err
	}
	markKey, err := replayMarkKeyFromEnv()
	if err != nil {
		return err
	}
	s, err := newReplayService(os.Getenv(replayControlKeyEnv), cidrs, *recordRoot, *dataRoot, targets, markKey, stderr)
	if err != nil {
		return err
	}
	defer s.Close()
	server := &http.Server{Addr: *listen, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		return err
	case <-ctx.Done():
	}
	closing, cancel := context.WithTimeout(context.Background(), *shutdown)
	defer cancel()
	return server.Shutdown(closing)
}
