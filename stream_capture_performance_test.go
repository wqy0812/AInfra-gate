//go:build darwin || linux

package aigate

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// This opt-in experiment uses real loopback HTTP, actual asynchronous files and
// an identical profiler/summary configuration on both sides. The synthetic
// Router has a declared 20 ms first-token delay and emits 24 ~4 KiB events at
// 10 ms intervals. This is a paced forwarding comparison, not a model benchmark.
// CPU and allocation figures cover the proxy, synthetic Router and HTTP clients
// in this one process; the capture memory high-water mark covers only its budget.
//
// AIGATE_CAPTURE_PERFORMANCE=1 AIGATE_CAPTURE_PERFORMANCE_OUTPUT=/absolute/file.json \
//
//	GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go test -mod=vendor \
//	  -run '^TestStreamCapturePerformance$' -count=1 -v .
type capturePerformanceSample struct {
	Group             int     `json:"group"`
	Concurrency       int     `json:"concurrency"`
	Capture           bool    `json:"capture"`
	Requests          int     `json:"requests"`
	ElapsedSeconds    float64 `json:"elapsed_seconds"`
	RequestsPerSecond float64 `json:"requests_per_second"`
	BytesPerSecond    float64 `json:"response_bytes_per_second"`
	FirstMeanMS       float64 `json:"first_event_mean_ms"`
	FirstP95MS        float64 `json:"first_event_p95_ms"`
	FirstRoundP95MS   float64 `json:"first_client_round_p95_ms"`
	LaterRoundsP95MS  float64 `json:"later_client_rounds_p95_ms"`
	IntervalP95MS     float64 `json:"event_interval_p95_ms"`
	CPUSeconds        float64 `json:"process_cpu_seconds"`
	CPUPerRequestMS   float64 `json:"process_cpu_ms_per_request"`
	AllocatedBytes    uint64  `json:"process_allocated_bytes"`
	Allocations       uint64  `json:"process_allocations"`
	PeakCaptureBytes  int64   `json:"peak_capture_budget_bytes"`
	CaptureDropped    uint64  `json:"capture_dropped"`
	CaptureFailures   uint64  `json:"capture_failures"`
}

type capturePerformanceComparison struct {
	Group           int     `json:"group"`
	Concurrency     int     `json:"concurrency"`
	ThroughputLoss  float64 `json:"throughput_loss_percent"`
	FirstGrowth     float64 `json:"first_event_p95_growth_percent"`
	IntervalGrowth  float64 `json:"event_interval_p95_growth_percent"`
	WithinTolerance bool    `json:"within_5_percent"`
}

func capturePerformancePercentile(values []float64, fraction float64) float64 {
	copyOfValues := append([]float64(nil), values...)
	sort.Float64s(copyOfValues)
	return copyOfValues[max(0, min(len(copyOfValues)-1, int(math.Ceil(float64(len(copyOfValues))*fraction))-1))]
}

func capturePerformanceCPU(t *testing.T) float64 {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func capturePerformanceDrain(t *testing.T, store *streamCaptureStore) {
	t.Helper()
	if store == nil {
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		store.mu.Lock()
		idle := len(store.active) == 0 && store.queued == 0 && store.usedBytes == 0
		store.mu.Unlock()
		if idle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("capture did not finish background cleanup within 5 seconds")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func capturePerformanceOne(t *testing.T, upstream string, group, concurrency, requests int, enabled bool, expected []byte) capturePerformanceSample {
	t.Helper()
	proxy, err := newProxyGateway(ProxyConfig{ServiceID: "performance-proxy", RouterID: "performance-router",
		Backend:  &Backend{Adapter: "openai", URL: upstream + "/v1/chat/completions", ModelsURL: upstream + "/v1/models"},
		Settings: json.RawMessage(`{"profile":{"enabled":true}}`)}, t.TempDir(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	store := proxy.g.captures
	if store == nil {
		t.Fatal("capture store unavailable")
	}
	if !enabled {
		store.close()
		proxy.g.captures = nil
		store = nil
	}
	server := httptest.NewServer(proxy)
	defer func() { server.Close(); proxy.Close() }()
	transport := &http.Transport{MaxIdleConns: concurrency * 2, MaxIdleConnsPerHost: concurrency * 2, MaxConnsPerHost: concurrency}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	body := []byte(`{"messages":[{"role":"user","content":"<user_query>capture performance synthetic request</user_query>"}],"stream":true}`)
	type observed struct {
		first     float64
		round     int
		intervals []float64
		size      int
		err       error
	}
	request := func() observed {
		started := time.Now()
		response, err := client.Post(server.URL+"/v1/chat/completions", "application/json", bytes.NewReader(body))
		if err != nil {
			return observed{err: err}
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			return observed{err: fmt.Errorf("HTTP %d", response.StatusCode)}
		}
		var result observed
		var received bytes.Buffer
		reader := bufio.NewReader(response.Body)
		var previous time.Time
		for {
			line, err := reader.ReadBytes('\n')
			received.Write(line)
			if bytes.HasPrefix(line, []byte("data: {")) && bytes.Contains(line, []byte(`"content"`)) {
				now := time.Now()
				if previous.IsZero() {
					result.first = float64(now.Sub(started)) / float64(time.Millisecond)
				} else {
					result.intervals = append(result.intervals, float64(now.Sub(previous))/float64(time.Millisecond))
				}
				previous = now
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return observed{err: err}
			}
		}
		if !bytes.Equal(received.Bytes(), expected) || result.first == 0 || len(result.intervals) != 23 {
			return observed{err: fmt.Errorf("response bytes/events mismatch: bytes=%d intervals=%d", received.Len(), len(result.intervals))}
		}
		result.size = received.Len()
		return result
	}
	// Warm up discovery, HTTP pools, profiler groups and the capture worker before
	// measuring. Both configurations execute the same warm-up traffic.
	for i := 0; i < 2; i++ {
		if got := request(); got.err != nil {
			t.Fatal(got.err)
		}
	}
	capturePerformanceDrain(t, store)
	var dropped, failures uint64
	if store != nil {
		store.peakBytes.Store(0)
		dropped = store.dropped.Load()
		failures = store.failures.Load()
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuBefore := capturePerformanceCPU(t)
	started := time.Now()
	results := make(chan observed, concurrency*requests)
	var workers sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < requests; j++ {
				result := request()
				result.round = j
				results <- result
			}
		}()
	}
	workers.Wait()
	close(results)
	elapsed := time.Since(started).Seconds()
	// Include trailing asynchronous cleanup in CPU/allocation cost, but not in
	// request throughput: business responses never wait for capture completion.
	capturePerformanceDrain(t, store)
	cpu := capturePerformanceCPU(t) - cpuBefore
	runtime.ReadMemStats(&after)
	sample := capturePerformanceSample{Group: group, Concurrency: concurrency, Capture: enabled, ElapsedSeconds: elapsed,
		CPUSeconds: cpu, AllocatedBytes: after.TotalAlloc - before.TotalAlloc, Allocations: after.Mallocs - before.Mallocs}
	var first, initialRound, laterRounds, intervals []float64
	var totalBytes int
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		first = append(first, result.first)
		if result.round == 0 {
			initialRound = append(initialRound, result.first)
		} else {
			laterRounds = append(laterRounds, result.first)
		}
		intervals = append(intervals, result.intervals...)
		totalBytes += result.size
		sample.FirstMeanMS += result.first
		sample.Requests++
	}
	sample.RequestsPerSecond = float64(sample.Requests) / elapsed
	sample.BytesPerSecond = float64(totalBytes) / elapsed
	sample.FirstMeanMS /= float64(sample.Requests)
	sample.FirstP95MS = capturePerformancePercentile(first, .95)
	sample.FirstRoundP95MS = capturePerformancePercentile(initialRound, .95)
	if len(laterRounds) > 0 {
		sample.LaterRoundsP95MS = capturePerformancePercentile(laterRounds, .95)
	}
	sample.IntervalP95MS = capturePerformancePercentile(intervals, .95)
	sample.CPUPerRequestMS = cpu * 1000 / float64(sample.Requests)
	if store != nil {
		sample.PeakCaptureBytes = store.peakBytes.Load()
		sample.CaptureDropped = store.dropped.Load() - dropped
		sample.CaptureFailures = store.failures.Load() - failures
	}
	return sample
}

func TestStreamCapturePerformance(t *testing.T) {
	if os.Getenv("AIGATE_CAPTURE_PERFORMANCE") != "1" {
		t.Skip("opt-in local HTTP/SSE capture comparison")
	}
	groups, requests := 3, 12
	for name, destination := range map[string]*int{"AIGATE_CAPTURE_PERFORMANCE_GROUPS": &groups, "AIGATE_CAPTURE_PERFORMANCE_REQUESTS": &requests} {
		if raw := os.Getenv(name); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value < 1 {
				t.Fatalf("invalid %s", name)
			}
			*destination = value
		}
	}
	if groups < 3 {
		t.Fatal("performance evidence requires at least three alternating pairs")
	}
	frame := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"" + strings.Repeat("x", 4096) + "\"}}]}\n\n"
	ending := "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
	expected := []byte(strings.Repeat(frame, 24) + ending)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"data":[{"id":"synthetic-performance-model"}]}`)
			return
		}
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		w.Header().Set("Content-Type", "text/event-stream")
		wait := func(delay time.Duration) bool {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
				return true
			case <-r.Context().Done():
				return false
			}
		}
		if !wait(20 * time.Millisecond) {
			return
		}
		for i := 0; i < 24; i++ {
			if i > 0 && !wait(10*time.Millisecond) {
				return
			}
			if _, err := io.WriteString(w, frame); err != nil {
				return
			}
			if http.NewResponseController(w).Flush() != nil {
				return
			}
		}
		io.WriteString(w, ending)
		http.NewResponseController(w).Flush()
	}))
	defer upstream.Close()
	report := struct {
		Schema      int                            `json:"schema_version"`
		GeneratedAt time.Time                      `json:"generated_at"`
		GoVersion   string                         `json:"go_version"`
		OS          string                         `json:"os"`
		Arch        string                         `json:"arch"`
		GOMAXPROCS  int                            `json:"gomaxprocs"`
		Workload    string                         `json:"workload"`
		CPUScope    string                         `json:"cpu_allocation_scope"`
		Samples     []capturePerformanceSample     `json:"samples"`
		Comparisons []capturePerformanceComparison `json:"comparisons"`
	}{Schema: 1, GeneratedAt: time.Now().UTC(), GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0),
		Workload: "real local HTTP; 20ms first output, 24 x ~4KiB SSE content events at 10ms intervals; identical profile+summary enabled; normal requests, no incident trigger",
		CPUScope: "process CPU/allocation includes synthetic Router, proxy, HTTP clients and final asynchronous capture cleanup; throughput/latency stop at business completion"}
	within := true
	complete := true
	for _, concurrency := range []int{1, 32} {
		for group := 1; group <= groups; group++ {
			var off, on capturePerformanceSample
			order := []bool{false, true}
			if group%2 == 0 {
				order = []bool{true, false}
			}
			for _, enabled := range order {
				sample := capturePerformanceOne(t, upstream.URL, group, concurrency, requests, enabled, expected)
				report.Samples = append(report.Samples, sample)
				encoded, _ := json.Marshal(sample)
				t.Log(string(encoded))
				if enabled {
					on = sample
					complete = complete && sample.CaptureDropped == 0 && sample.CaptureFailures == 0
				} else {
					off = sample
				}
			}
			comparison := capturePerformanceComparison{Group: group, Concurrency: concurrency, ThroughputLoss: (1 - on.RequestsPerSecond/off.RequestsPerSecond) * 100,
				FirstGrowth: (on.FirstP95MS/off.FirstP95MS - 1) * 100, IntervalGrowth: (on.IntervalP95MS/off.IntervalP95MS - 1) * 100}
			comparison.WithinTolerance = comparison.ThroughputLoss <= 5 && comparison.FirstGrowth <= 5 && comparison.IntervalGrowth <= 5
			within = within && comparison.WithinTolerance
			report.Comparisons = append(report.Comparisons, comparison)
			encoded, _ := json.Marshal(comparison)
			t.Log(string(encoded))
		}
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if output := os.Getenv("AIGATE_CAPTURE_PERFORMANCE_OUTPUT"); output != "" {
		if !filepath.IsAbs(output) {
			t.Fatal("performance output must be absolute")
		}
		if err := os.MkdirAll(filepath.Dir(output), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(output, append(encoded, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if !within {
		t.Errorf("one or more paired runs exceed the 5%% tolerance; preserve raw evidence and investigate workload/noise before acceptance")
	}
	if !complete {
		t.Errorf("detailed capture dropped or failed in this workload; latency results alone cannot establish sustainable detailed capture performance")
	}
}
