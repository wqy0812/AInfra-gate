package aigate

// Request profiling is observational. No profile failure changes a business response.
import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var profileLengthBuckets = []float64{256, 1024, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576}
var profileTimeBuckets = []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}

type ProfileConfig struct {
	// noAuth is only set by the standalone proxy bootstrap, never by JSON.
	noAuth         bool
	Enabled        bool   `json:"enabled"`
	Listen         string `json:"listen"`
	APIKeyEnv      string `json:"api_key_env"`
	APIKey         string `json:"api_key,omitempty"`
	Directory      string `json:"directory"`
	RetentionHours int    `json:"retention_hours"`
	MaxDiskBytes   int64  `json:"max_disk_bytes"`
	QueueEvents    int    `json:"queue_events"`
	// Deprecated: accepted for configuration compatibility; no prefix index or payload analysis remains.
	MaxIndexEntries int `json:"max_index_entries"`
	MaxPayloadBytes int `json:"max_payload_bytes"`
}

func (c *ProfileConfig) defaults(path string) {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:18082"
	}
	if c.APIKeyEnv == "" {
		c.APIKeyEnv = "AIGATE_PROFILE_KEY"
	}
	if c.Directory == "" {
		c.Directory = "request-profile"
	}
	if !filepath.IsAbs(c.Directory) {
		p, _ := filepath.Abs(path)
		c.Directory = filepath.Join(filepath.Dir(p), c.Directory)
	}
	if c.RetentionHours == 0 {
		c.RetentionHours = 72
	}
	if c.MaxDiskBytes == 0 {
		c.MaxDiskBytes = 10 << 30
	}
	if c.QueueEvents == 0 {
		c.QueueEvents = 2048
	}
}
func (c ProfileConfig) validate() error {
	if !c.Enabled {
		return nil
	}
	if _, _, e := net.SplitHostPort(c.Listen); e != nil {
		return fmt.Errorf("profile.listen: %w", e)
	}
	if !c.noAuth && c.APIKey == "" && os.Getenv(c.APIKeyEnv) == "" {
		return fmt.Errorf("profile requires %s", c.APIKeyEnv)
	}
	if c.Directory == "" || c.RetentionHours < 1 || c.RetentionHours > 720 || c.MaxDiskBytes < 1<<20 || c.QueueEvents < 1 || c.QueueEvents > 8192 {
		return fmt.Errorf("invalid profile resource limits")
	}
	return nil
}

type profileSegmentMeta struct {
	Min   float64 `json:"min_arrival"`
	Max   float64 `json:"max_arrival"`
	Bytes int64   `json:"bytes"`
}

type profileEvent struct {
	Kind             string          `json:"kind"`
	ID               string          `json:"request_id"`
	Boot             string          `json:"boot"`
	Seq              uint64          `json:"sequence"`
	At               float64         `json:"at"`
	Arrived          float64         `json:"arrived"`
	TestTask         string          `json:"test_task,omitempty"`
	RouteVersion     uint64          `json:"route_version,omitempty"`
	Traffic          string          `json:"traffic,omitempty"`
	Backend          string          `json:"backend,omitempty"`
	Model            string          `json:"model,omitempty"`
	Stream           bool            `json:"stream,omitempty"`
	Bytes            int             `json:"bytes,omitempty"`
	Outcome          string          `json:"outcome,omitempty"`
	Usage            json.RawMessage `json:"usage,omitempty"`
	First            *float64        `json:"first_seconds,omitempty"`
	Duration         float64         `json:"duration_seconds,omitempty"`
	Quality          string          `json:"quality,omitempty"`
	UpstreamStatus   int             `json:"upstream_status,omitempty"`
	ErrorClass       string          `json:"error_class,omitempty"`
	GenerationResult string          `json:"generation_result,omitempty"`
}
type profileRequest struct {
	profiler *requestProfiler
	e        profileEvent
	start    time.Time
	tags     string
	usage    json.RawMessage
	first    *float64
	quality  string
	outcome  string
}
type profileContextKey struct{}
type profileJob struct {
	ID      string       `json:"id"`
	State   string       `json:"state"`
	Created float64      `json:"created_at"`
	Error   string       `json:"error,omitempty"`
	Result  any          `json:"result,omitempty"`
	Query   profileQuery `json:"query"`
}
type profileQuery struct {
	Start   float64 `json:"start"`
	End     float64 `json:"end"`
	Backend string  `json:"backend,omitempty"`
	Model   string  `json:"model,omitempty"`
}
type requestProfiler struct {
	directory      *os.File // exclude other writers and cross-generation cleanup
	c              ProfileConfig
	boot           string
	counterStarted float64
	started        time.Time
	emitMu         sync.RWMutex
	closed         bool
	mu             sync.Mutex
	active         map[uint64]profileLiveRequest
	liveBackends   map[string]bool
	values         map[string]float64
	types          map[string]string
	groups         map[string]bool
	seq            atomic.Uint64
	dropped        atomic.Uint64
	writes         atomic.Uint64
	queue          chan []byte
	done           chan struct{}
	closeOnce      sync.Once
	diskMu         sync.Mutex
	files          []string
	oldest         float64
	storedBytes    int64
	lastWrite      float64
	jobsMu         sync.Mutex
	jobs           map[string]*profileJob
	work           chan string
	cancel         context.CancelFunc
	workers        sync.WaitGroup
}

func newRequestProfiler(c ProfileConfig) (*requestProfiler, error) {
	if e := os.MkdirAll(c.Directory, 0700); e != nil {
		return nil, e
	}
	info, err := os.Lstat(c.Directory)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("profile directory must be a real directory")
	}
	directory, err := os.Open(c.Directory)
	if err != nil {
		return nil, err
	}
	if err = lockState(directory); err != nil {
		directory.Close()
		return nil, fmt.Errorf("profile directory lock: %w", err)
	}
	now := time.Now()
	p := &requestProfiler{c: c, boot: randomID(), started: now, values: map[string]float64{}, types: map[string]string{}, groups: map[string]bool{}, queue: make(chan []byte, c.QueueEvents), done: make(chan struct{}), jobs: map[string]*profileJob{}, work: make(chan string, 4)}
	p.counterStarted = profileNow()
	p.directory = directory
	p.restoreCounters()
	p.add("nonstream_requests_total", "", 0)
	if p.values["aigate_error_metrics_start_time_seconds"] == 0 {
		p.values["aigate_error_metrics_start_time_seconds"] = profileNow()
	}
	p.types["aigate_error_metrics_start_time_seconds"] = "gauge"
	p.liveBackends = map[string]bool{"unrouted": true}
	p.add("requests_started_total", "", 0)
	p.add("requests_inflight", "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.refreshFiles()
	p.workers.Add(2)
	go func() { defer p.workers.Done(); p.writer() }()
	go func() { defer p.workers.Done(); p.analyzer(ctx) }()
	return p, nil
}
func (p *requestProfiler) close() {
	p.closeOnce.Do(func() {
		p.cancel()
		p.emitMu.Lock()
		p.closed = true
		close(p.queue)
		p.emitMu.Unlock()
		p.workers.Wait()
		p.saveCounters()
		unlockState(p.directory)
		p.directory.Close()
	})
}
func profileNow() float64 { return float64(time.Now().UnixNano()) / 1e9 }
func (p *requestProfiler) add(name, tags string, n float64) {
	p.values["aigate_"+name+tags] += n
	kind := "counter"
	if name == "requests_inflight" {
		kind = "gauge"
	}
	p.types["aigate_"+name] = kind
}
func (p *requestProfiler) hist(name, tags string, n float64, buckets []float64) {
	if n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
		return
	}
	p.types["aigate_"+name] = "histogram"
	p.values["aigate_"+name+"_sum"+tags] += n
	p.values["aigate_"+name+"_count"+tags]++
	for _, b := range buckets {
		key := "aigate_" + name + "_bucket" + profileExtra(tags, "le", strconv.FormatFloat(b, 'g', -1, 64))
		if n <= b {
			p.values[key]++
		} else {
			p.values[key] += 0
		}
	}
	p.values["aigate_"+name+"_bucket"+profileExtra(tags, "le", "+Inf")]++
}
func profileExtra(tags, key, value string) string {
	if tags == "" {
		return "{" + key + "=" + strconv.Quote(value) + "}"
	}
	return strings.TrimSuffix(tags, "}") + "," + key + "=" + strconv.Quote(value) + "}"
}
func (p *requestProfiler) tags(backend, model string) string {
	t := "{backend=" + strconv.Quote(backend) + ",model=" + strconv.Quote(model) + "}"
	if !p.groups[t] && len(p.groups) >= 64 {
		t = `{backend="overflow",model="overflow"}`
	}
	if !p.groups[t] {
		p.values["aigate_profile_group_start_time_seconds"+t] = profileNow()
		p.types["aigate_profile_group_start_time_seconds"] = "gauge"
		for _, name := range []string{"prompt_tokens_total", "completion_tokens_total", "cached_tokens_total", "cache_prompt_tokens_total", "cache_usage_known_total"} {
			p.add(name, t, 0)
		}
		for _, field := range []string{"prompt_tokens", "completion_tokens"} {
			p.add("usage_known_total", profileExtra(t, "field", field), 0)
			p.add("usage_missing_total", profileExtra(t, "field", field), 0)
		}
	}
	p.groups[t] = true
	return t
}
func (p *requestProfiler) emit(e profileEvent) {
	p.emitMu.RLock()
	defer p.emitMu.RUnlock()
	if p.closed {
		return
	}
	b := encodeJSON(e)
	if len(b) > 16<<10 {
		p.dropped.Add(1)
		return
	}
	select {
	case p.queue <- b:
	default:
		p.dropped.Add(1)
	}
}
func profileTrafficTags(traffic string) string {
	if traffic == "" {
		return ""
	}
	return profileExtra("", "traffic", traffic)
}

func (p *requestProfiler) begin(id string, arrived ...time.Time) *profileRequest {
	return p.beginRequest(id, true, "", arrived...)
}
func (p *requestProfiler) beginRequest(id string, stream bool, traffic string, arrived ...time.Time) *profileRequest {
	r := &profileRequest{profiler: p, start: time.Now(), e: profileEvent{Kind: "start", Stream: stream, ID: id, Traffic: traffic, Boot: p.boot, Seq: p.seq.Add(1), At: profileNow()}}
	if len(arrived) > 0 {
		r.start = arrived[0]
		r.e.At = float64(r.start.UnixNano()) / 1e9
	}
	r.e.Arrived = r.e.At
	p.mu.Lock()
	if p.active == nil {
		p.active = make(map[uint64]profileLiveRequest)
	}
	if p.liveBackends == nil {
		p.liveBackends = map[string]bool{"unrouted": true}
	}
	p.active[r.e.Seq] = profileLiveRequest{start: r.start, backend: "unrouted", stage: "routing_discovery", nonstream: !stream, traffic: traffic}
	if !stream {
		p.add("nonstream_requests_total", profileTrafficTags(traffic), 1)
	}
	p.add("requests_started_total", profileTrafficTags(traffic), 1)
	p.add("requests_inflight", profileTrafficTags(traffic), 1)
	p.mu.Unlock()
	p.emit(r.e)
	return r
}
func (p *requestProfiler) input(r *profileRequest, payload []byte, b *Backend, backend, model string) {
	r.e.Backend = backend
	r.e.Model = model
	p.mu.Lock()
	r.tags = p.tags(backend, model)
	if r.e.Traffic != "" {
		// Replay series stay separable so monitoring can exclude them.
		r.tags = profileExtra(r.tags, "traffic", r.e.Traffic)
	}
	p.add("requests_routed_total", r.tags, 1)
	p.hist("request_bytes", r.tags, float64(len(payload)), profileLengthBuckets)
	p.mu.Unlock()
	r.e.Kind = "input"
	r.e.At = profileNow()
	r.e.Bytes = len(payload)
	p.emit(r.e)
}

func profileUsage(raw json.RawMessage) map[string]uint64 {
	out := map[string]uint64{}
	var f map[string]json.RawMessage
	_ = json.Unmarshal(usageCounters(raw), &f)
	for _, k := range []string{"prompt_tokens", "completion_tokens", "total_tokens"} {
		var n uint64
		if v, ok := f[k]; ok && json.Unmarshal(v, &n) == nil {
			out[k] = n
		}
	}
	var d map[string]json.RawMessage
	_ = json.Unmarshal(f["prompt_tokens_details"], &d)
	var n uint64
	if v, ok := d["cached_tokens"]; ok && json.Unmarshal(v, &n) == nil {
		out["cached_tokens"] = n
	}
	return out
}
func profileBucket(n uint64) string {
	for _, b := range profileLengthBuckets {
		if float64(n) <= b {
			return strconv.FormatFloat(b, 'g', -1, 64)
		}
	}
	return "+Inf"
}
func (p *requestProfiler) end(r *profileRequest, entry *auditEntry, cancelled bool) {
	e := r.e
	e.Kind = "end"
	e.At = profileNow()
	e.Outcome = entry.Outcome
	if r.outcome != "" {
		e.Outcome = r.outcome
	}
	if cancelled {
		e.Outcome = "client_cancelled"
	}
	if s := entry.generation; s != nil && s.started {
		e.UpstreamStatus, e.ErrorClass, e.GenerationResult = s.status, s.errorClass, s.result
	}
	e.Usage = usageCounters(r.usage)
	e.First = r.first
	if !e.Stream {
		e.First = nil
	}
	e.Duration = time.Since(r.start).Seconds()
	e.Quality = r.quality
	p.mu.Lock()
	delete(p.active, r.e.Seq)
	if r.tags == "" {
		r.tags = p.tags("unrouted", "unknown")
		if r.e.Traffic != "" {
			r.tags = profileExtra(r.tags, "traffic", r.e.Traffic)
		}
	}
	p.add("requests_inflight", profileTrafficTags(r.e.Traffic), -1)
	p.add("requests_ended_total", profileExtra(r.tags, "outcome", e.Outcome), 1)
	p.generationEnd(entry.generation, r.tags)
	p.hist("request_duration_seconds", r.tags, e.Duration, profileTimeBuckets)
	if e.Stream && e.First != nil {
		p.hist("first_increment_seconds", r.tags, *e.First, profileTimeBuckets)
	}
	u := profileUsage(e.Usage)
	in, iok := u["prompt_tokens"]
	out, ook := u["completion_tokens"]
	for _, k := range []string{"prompt_tokens", "completion_tokens"} {
		if n, ok := u[k]; ok {
			p.add(k+"_total", r.tags, float64(n))
			p.hist(k, r.tags, float64(n), profileLengthBuckets)
			p.add("usage_known_total", profileExtra(r.tags, "field", k), 1)
		} else {
			p.add("usage_missing_total", profileExtra(r.tags, "field", k), 1)
		}
	}
	if iok && ook {
		tags := profileExtra(profileExtra(r.tags, "input_le", profileBucket(in)), "output_le", profileBucket(out))
		p.add("token_pairs_total", tags, 1)
	}
	if n, ok := u["cached_tokens"]; ok && iok && n <= in {
		p.add("cached_tokens_total", r.tags, float64(n))
		p.add("cache_prompt_tokens_total", r.tags, float64(in))
		p.add("cache_usage_known_total", r.tags, 1)
	}
	if r.quality != "" {
		p.add("profile_parse_failures_total", profileExtra(r.tags, "reason", r.quality), 1)
	}
	p.mu.Unlock()
	p.emit(e)
}
func (p *requestProfiler) refreshFiles() {
	p.diskMu.Lock()
	defer p.diskMu.Unlock()
	p.refreshFilesLocked()
}
func (p *requestProfiler) refreshFilesLocked() {
	files, _ := filepath.Glob(filepath.Join(p.c.Directory, "events-v2-*.jsonl"))
	sort.Strings(files)
	var total int64
	kept := []string{}
	cutoff := time.Now().Add(-time.Duration(p.c.RetentionHours) * time.Hour)
	for _, path := range files {
		s, e := os.Stat(path)
		if e != nil {
			continue
		}
		if s.ModTime().Before(cutoff) {
			if os.Remove(path) != nil {
				p.writes.Add(1)
			}
			_ = os.Remove(path + ".meta.json")
			_ = os.Remove(path + ".meta.tmp")
			continue
		}
		kept = append(kept, path)
		total += s.Size()
		if ts := float64(s.ModTime().UnixNano()) / 1e9; s.Size() > 0 && ts > p.lastWrite {
			p.lastWrite = ts
		}
	}
	for total > p.c.MaxDiskBytes && len(kept) > 1 {
		s, _ := os.Stat(kept[0])
		if os.Remove(kept[0]) != nil {
			p.writes.Add(1)
			break
		}
		_ = os.Remove(kept[0] + ".meta.json")
		_ = os.Remove(kept[0] + ".meta.tmp")
		total -= s.Size()
		kept = kept[1:]
	}
	p.files = kept
	p.storedBytes = total
	p.oldest = profileNow()
	if len(kept) > 0 {
		base := strings.TrimPrefix(filepath.Base(kept[0]), "events-v2-")
		if n, e := strconv.ParseInt(strings.Split(base, "-")[0], 10, 64); e == nil {
			p.oldest = float64(n) / 1000
		}
		if f, err := os.Open(kept[0]); err == nil {
			line, _, _ := bufio.NewReader(f).ReadLine()
			var event profileEvent
			if json.Unmarshal(line, &event) == nil && event.Arrived > 0 {
				p.oldest = event.Arrived
			}
			f.Close()
		}
	}
}
func (p *requestProfiler) writer() {
	defer close(p.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var f *os.File
	var opened time.Time
	var size int64
	var path string
	var meta profileSegmentMeta
	flushMeta := func() {
		if f == nil {
			return
		}
		tmp := path + ".meta.tmp"
		if err := os.WriteFile(tmp, encodeJSON(meta), 0600); err != nil {
			p.writes.Add(1)
			return
		}
		if os.Rename(tmp, path+".meta.json") != nil {
			p.writes.Add(1)
		}
	}
	closeFile := func() {
		if f != nil {
			if f.Sync() != nil {
				p.writes.Add(1)
			}
			flushMeta()
			f.Close()
			f = nil
		}
	}
	defer closeFile()
	for {
		select {
		case b, ok := <-p.queue:
			if !ok {
				return
			}
			if f == nil || size >= min(p.c.MaxDiskBytes/2, 4<<20) || time.Since(opened) >= time.Minute {
				closeFile()
				opened = time.Now()
				path = filepath.Join(p.c.Directory, fmt.Sprintf("events-v2-%013d-%s.jsonl", opened.UnixMilli(), randomID()))
				var err error
				f, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				size = 0
				meta = profileSegmentMeta{}
				if err != nil {
					p.writes.Add(1)
					continue
				}
				p.refreshFiles()
			}
			n, err := f.Write(append(b, '\n'))
			if err != nil {
				p.writes.Add(1)
				closeFile()
				continue
			}
			size += int64(n)
			meta.Bytes = size
			var event profileEvent
			if json.Unmarshal(b, &event) == nil {
				if meta.Min == 0 || event.Arrived < meta.Min {
					meta.Min = event.Arrived
				}
				if event.Arrived > meta.Max {
					meta.Max = event.Arrived
				}
			}
			p.diskMu.Lock()
			p.storedBytes += int64(n)
			p.lastWrite = profileNow()
			p.diskMu.Unlock()
		case <-ticker.C:
			if f != nil && f.Sync() != nil {
				p.writes.Add(1)
			}
			flushMeta()
			p.refreshFiles()
		}
	}
}
func (p *requestProfiler) status() any {
	p.diskMu.Lock()
	defer p.diskMu.Unlock()
	return map[string]any{"schema": "gateway-profile-v1", "request_scope": "all", "scope_policy": "metric-v2", "started_at": float64(p.started.UnixNano()) / 1e9, "retained_start": p.oldest, "retained_end": p.lastWrite, "queue_events": len(p.queue), "dropped_events": p.dropped.Load(), "write_errors": p.writes.Load(), "stored_bytes": p.storedBytes, "max_analysis_seconds": 86400}
}
func (p *requestProfiler) metrics(w io.Writer) {
	p.mu.Lock()
	now := time.Now()
	active := make(map[uint64]profileLiveRequest, len(p.active))
	for seq, live := range p.active {
		active[seq] = live
	}
	backends := make(map[string]bool, len(p.liveBackends))
	for backend := range p.liveBackends {
		backends[backend] = true
	}
	values := make(map[string]float64, len(p.values))
	for k, v := range p.values {
		values[k] = v
	}
	metricTypes := make(map[string]string, len(p.types))
	for k, v := range p.types {
		metricTypes[k] = v
	}
	p.mu.Unlock()
	profileLiveMetrics(now, active, backends, values, metricTypes)
	names := make([]string, 0, len(values))
	for k := range values {
		names = append(names, k)
	}
	sort.Strings(names)
	types := make([]string, 0, len(metricTypes))
	for k := range metricTypes {
		types = append(types, k)
	}
	sort.Strings(types)
	for _, k := range types {
		fmt.Fprintf(w, "# TYPE %s %s\n", k, metricTypes[k])
	}
	for _, k := range names {
		name, tags := k, ""
		if i := strings.IndexByte(k, '{'); i >= 0 {
			name, tags = k[:i], k[i:]
		}
		scope := profileMetricScope(name)
		fmt.Fprintf(w, "%s%s %g\n", name, profileExtra(tags, "request_scope", scope), values[k])
	}
	fmt.Fprintf(w, "aigate_profile_dropped_events_total{request_scope=\"all\"} %d\naigate_profile_write_errors_total{request_scope=\"all\"} %d\naigate_profile_queue_events{request_scope=\"all\"} %d\naigate_profile_start_time_seconds{request_scope=\"all\"} %g\n", p.dropped.Load(), p.writes.Load(), len(p.queue), float64(p.started.Unix()))
	fmt.Fprintf(w, "aigate_profile_counter_start_time_seconds{request_scope=\"all\"} %g\n", p.counterStarted)
	p.diskMu.Lock()
	fmt.Fprintf(w, "aigate_profile_stored_bytes{request_scope=\"all\"} %d\naigate_profile_last_write_seconds{request_scope=\"all\"} %g\n", p.storedBytes, p.lastWrite)
	p.diskMu.Unlock()
}

// Scope follows the observation semantics, independently of the request population.
func profileMetricScope(name string) string {
	if name == "aigate_nonstream_requests_total" {
		return "nonstreaming"
	}
	if strings.HasPrefix(name, "aigate_first_increment_seconds_") || strings.HasPrefix(name, "aigate_stream_") || name == "aigate_streams_waiting_first_output" {
		return "streaming"
	}
	return "all"
}

func profileBoolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
func (p *requestProfiler) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := p.c.APIKey
		if key == "" {
			key = os.Getenv(p.c.APIKeyEnv)
		}
		if !p.c.noAuth && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+key)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.Method == "GET" && r.URL.Path == "/metrics" {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			p.metrics(w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method == "GET" && r.URL.Path == "/profile/v1/status" {
			json.NewEncoder(w).Encode(p.status())
			return
		}
		if r.Method == "POST" && r.URL.Path == "/profile/v1/analyses" {
			var q profileQuery
			d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
			d.DisallowUnknownFields()
			err := d.Decode(&q)
			var tail any
			if err != nil || d.Decode(&tail) != io.EOF || math.IsNaN(q.Start) || math.IsNaN(q.End) || q.Start <= 0 || q.Start >= q.End || q.End-q.Start > 86400 || q.End > profileNow()+1 || len(q.Backend) > 128 || len(q.Model) > 256 {
				http.Error(w, "invalid window or filters", 400)
				return
			}
			p.diskMu.Lock()
			oldest, last := p.oldest, p.lastWrite
			p.diskMu.Unlock()
			if q.Start < oldest || last == 0 {
				http.Error(w, "outside retained event range", 410)
				return
			}
			p.jobsMu.Lock()
			defer p.jobsMu.Unlock()
			for id, j := range p.jobs {
				if profileNow()-j.Created > 600 && j.State != "running" && j.State != "queued" {
					delete(p.jobs, id)
				}
			}
			if len(p.jobs) >= 16 {
				http.Error(w, "analysis cache full", 429)
				return
			}
			id := randomID()
			j := &profileJob{ID: id, State: "queued", Created: profileNow(), Query: q}
			p.jobs[id] = j
			select {
			case p.work <- id:
				w.WriteHeader(202)
				json.NewEncoder(w).Encode(j)
			default:
				delete(p.jobs, id)
				http.Error(w, "analysis queue full", 429)
			}
			return
		}
		if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/profile/v1/analyses/") {
			id := strings.TrimPrefix(r.URL.Path, "/profile/v1/analyses/")
			p.jobsMu.Lock()
			defer p.jobsMu.Unlock()
			j := p.jobs[id]
			if j == nil || profileNow()-j.Created > 600 {
				http.Error(w, "analysis expired or missing", 404)
				return
			}
			json.NewEncoder(w).Encode(j)
			return
		}
		http.NotFound(w, r)
	})
}

// This is a single bounded operational checkpoint, not a historical metric/report database.
// Graceful restarts preserve counters even when no scrape can observe a reset to zero.
type profileCounterState struct {
	Version int                `json:"version"`
	Started float64            `json:"started"`
	Values  map[string]float64 `json:"values"`
	Types   map[string]string  `json:"types"`
	Groups  map[string]bool    `json:"groups"`
}

func (p *requestProfiler) restoreCounters() {
	path := filepath.Join(p.c.Directory, "counters-metric-v2.json")
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil || info.Size() > 16<<20 {
		p.writes.Add(1)
		return
	}
	data, err := os.ReadFile(path)
	var saved profileCounterState
	if err != nil || json.Unmarshal(data, &saved) != nil || saved.Version != 1 || saved.Started <= 0 || saved.Values == nil || saved.Types == nil || saved.Groups == nil || len(saved.Values) > 100000 || len(saved.Groups) > 65 {
		p.writes.Add(1)
		return
	}
	for key, value := range saved.Values {
		if !strings.HasPrefix(key, "aigate_") || len(key) > 2048 || math.IsNaN(value) || math.IsInf(value, 0) {
			p.writes.Add(1)
			return
		}
	}
	// Retired observations must not reappear after restoring an older checkpoint.
	for name := range saved.Values {
		if retiredProfileMetric(name) {
			delete(saved.Values, name)
		}
	}
	for name := range saved.Types {
		if retiredProfileMetric(name) {
			delete(saved.Types, name)
		}
	}
	p.values = saved.Values
	p.types = saved.Types
	p.groups = saved.Groups
	p.counterStarted = saved.Started
	for key := range p.values {
		if key == "aigate_requests_inflight" || strings.HasPrefix(key, "aigate_requests_inflight{") {
			p.values[key] = 0
		}
	}
	// Consume the checkpoint: an unclean exit must not silently reuse an older graceful snapshot.
	if os.Remove(path) != nil {
		p.writes.Add(1)
	}
}
func (p *requestProfiler) saveCounters() {
	p.mu.Lock()
	data := encodeJSON(profileCounterState{1, p.counterStarted, p.values, p.types, p.groups})
	p.mu.Unlock()
	if len(data) > 16<<20 {
		p.writes.Add(1)
		return
	}
	path := filepath.Join(p.c.Directory, "counters-metric-v2.json")
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		p.writes.Add(1)
		return
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	if err == nil {
		err = os.Rename(path+".tmp", path)
	}
	if err != nil {
		p.writes.Add(1)
	}
}

func retiredProfileMetric(name string) bool {
	for _, prefix := range []string{"aigate_prefix_", "aigate_message_count", "aigate_tool_count", "aigate_profile_index_"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
