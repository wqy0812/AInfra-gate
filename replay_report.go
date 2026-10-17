package aigate

// Reports use bounded histograms and at most two timing events per planned
// request (the existing plan limit is 200,000). No response bodies are retained.
import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

const replayReportVersion = 2

type replayHistogram struct {
	Count int         `json:"count"`
	Bins  map[int]int `json:"bins"`
	P50   *float64    `json:"p50_seconds"`
	P95   *float64    `json:"p95_seconds"`
	P99   *float64    `json:"p99_seconds"`
}

func (h *replayHistogram) add(v float64) {
	if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	if h.Bins == nil {
		h.Bins = map[int]int{}
	}
	i := 0
	if v > 0 {
		i = max(1, min(2047, int(math.Ceil(math.Log(max(v, 1e-6)/1e-6)/math.Log(1.02)))+1))
	}
	h.Bins[i]++
	h.Count++
}
func histogramValue(i int) float64 {
	if i == 0 {
		return 0
	}
	return 1e-6 * math.Pow(1.02, float64(i-1))
}
func (h *replayHistogram) finish() {
	if h.Count == 0 {
		return
	}
	keys := make([]int, 0, len(h.Bins))
	for k := range h.Bins {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	quantile := func(q float64) *float64 {
		n := 0
		for _, k := range keys {
			n += h.Bins[k]
			if n >= int(math.Ceil(q*float64(h.Count))) {
				v := histogramValue(k)
				return &v
			}
		}
		return nil
	}
	h.P50, h.P95, h.P99 = quantile(.5), quantile(.95), quantile(.99)
}

type replayMetrics struct {
	Sent         int             `json:"sent"`
	Success      int             `json:"success"`
	Failed       int             `json:"failed"`
	Truncated    int             `json:"truncated"`
	UsageKnown   int             `json:"usage_known"`
	OutputTokens uint64          `json:"output_tokens"`
	E2E          replayHistogram `json:"e2e"`
	TTFT         replayHistogram `json:"ttft"`
	TPOT         replayHistogram `json:"tpot"`
}
type replayCurve struct {
	Cohort       replayMetrics   `json:"cohort"`
	E2EStream    replayHistogram `json:"e2e_stream"`
	E2ENonstream replayHistogram `json:"e2e_nonstream"`
	TTFT         replayHistogram `json:"ttft"`
	TPOT         replayHistogram `json:"tpot"`
	UsageKnown   int             `json:"usage_known"`
	OutputTokens uint64          `json:"output_tokens"`
	Offset       float64         `json:"offset_seconds"`
	Width        float64         `json:"width_seconds"`
	InflightMean float64         `json:"inflight_mean"`
	InflightPeak int             `json:"inflight_peak"`
	Success      int             `json:"success"`
}
type replayReport struct {
	BucketSeconds          int               `json:"bucket_seconds"`
	LatencyBasis           string            `json:"latency_basis"`
	InputDigest            string            `json:"input_digest"`
	FilterPolicy           string            `json:"filter_policy"`
	FilterEvidence         string            `json:"filter_evidence"`
	Truncation             *replayTruncation `json:"truncation,omitempty"`
	Version                int               `json:"version"`
	State                  string            `json:"state"`
	Reason                 string            `json:"reason,omitempty"`
	RunID                  string            `json:"run_id"`
	RunState               string            `json:"run_state"`
	StartedAt              any               `json:"started_at"`
	Target                 replayTargetState `json:"target"`
	Request                *replayRequest    `json:"request"`
	Progress               replayProgress    `json:"progress"`
	Metrics                replayMetrics     `json:"metrics"`
	Stream                 replayMetrics     `json:"stream"`
	Nonstream              replayMetrics     `json:"nonstream"`
	Errors                 map[string]int    `json:"errors"`
	SendDelay              replayHistogram   `json:"send_delay"`
	MaxSendDelay           float64           `json:"max_send_delay_seconds"`
	FirstSend              *float64          `json:"first_send_seconds"`
	LastSend               *float64          `json:"last_send_seconds"`
	LastEnd                *float64          `json:"last_end_seconds"`
	Window                 float64           `json:"window_seconds"`
	Drain                  *float64          `json:"drain_seconds"`
	RequestRate            *float64          `json:"requests_per_second"`
	TokenRate              *float64          `json:"tokens_per_second"`
	InflightMean           float64           `json:"inflight_mean"`
	InflightPeak           int               `json:"inflight_peak"`
	Curve                  []replayCurve     `json:"curve"`
	CurveTruncated         bool              `json:"curve_truncated,omitempty"`
	PlannedDuration        float64           `json:"planned_duration_seconds"`
	HistogramRelativeError float64           `json:"histogram_relative_error"`
}
type replayTimingEvent struct {
	at      float64
	delta   int
	success bool
}

func (m *replayMetrics) add(v replayResult) {
	m.Sent++
	if v.Error == "tail_truncated" {
		m.Truncated++
		return
	}
	if v.Error != "" || v.HTTPStatus < 200 || v.HTTPStatus >= 300 {
		m.Failed++
		return
	}
	m.Success++
	if v.ActualTokens != nil {
		m.UsageKnown++
		m.OutputTokens += *v.ActualTokens
	}
	m.E2E.add(*v.EndOffset - *v.SendOffset)
	if !v.Stream || v.Observation == nil || !v.Observation.ProtocolComplete {
		return
	}
	if v.FirstOutput != nil {
		m.TTFT.add(*v.FirstOutput - *v.SendOffset)
	}
	if !v.MultipleChoices && v.ActualTokens != nil && *v.ActualTokens > 1 && v.FirstOutput != nil && v.LastOutput != nil && *v.LastOutput > *v.FirstOutput {
		m.TPOT.add((*v.LastOutput - *v.FirstOutput) / float64(*v.ActualTokens-1))
	}
}
func (m *replayMetrics) finish() { m.E2E.finish(); m.TTFT.finish(); m.TPOT.finish() }
func buildReplayReport(dir string, view replayRunView) replayReport {
	return buildReplayReportVersion(dir, view, 2)
}
func buildReplayReportVersion(dir string, view replayRunView, version int) replayReport {
	width, limit := 30.0, 20160
	basis := "planned_send"
	if view.Request != nil && view.Request.Mode == "concurrency" {
		basis = "actual_send"
	}
	if version == 1 {
		width, limit, basis = 60, 10080, "completion"
	}
	r := replayReport{Version: version, BucketSeconds: int(width), LatencyBasis: basis, Truncation: view.Truncation, State: "complete", RunID: view.RunID, RunState: view.State, StartedAt: view.StartedAt, Target: view.Target, Request: view.Request, Progress: view.Progress, Errors: map[string]int{}, Curve: []replayCurve{}, HistogramRelativeError: .02}
	if view.Plan != nil {
		r.PlannedDuration = view.Plan.DurationSeconds
		r.InputDigest = view.Plan.InputDigest
		r.FilterPolicy = view.Plan.FilterPolicy
	}
	if view.State != "completed" || view.Output.ResultsFailed || view.Output.Incomplete {
		r.State = "partial"
		r.Reason = "run_incomplete"
	}
	f, err := os.Open(filepath.Join(dir, "results.jsonl"))
	if err != nil {
		r.State = "unavailable"
		r.Reason = "no_timing_results"
		return r
	}
	defer f.Close()
	if version == 2 {
		r.InputDigest, r.FilterPolicy, r.FilterEvidence = replayReportInput(dir, view)
	}
	if view.State == "truncated" {
		r.Reason = "tail_truncated"
	}
	events := make([]replayTimingEvent, 0)
	minutes := map[int]*replayCurve{}
	seen := map[int]bool{}
	reader := bufio.NewReaderSize(f, recordingSmallFile)
	legacy := false
	for {
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, io.EOF) && len(line) == 0 {
			break
		}
		if err != nil {
			r.State = "partial"
			r.Reason = "results_incomplete"
			break
		}
		var v replayResult
		if json.Unmarshal(line, &v) != nil || seen[v.Sequence] || len(seen) >= replayMaxPlanItems {
			r.State = "partial"
			r.Reason = "invalid_results"
			break
		}
		seen[v.Sequence] = true
		if v.TimingVersion != 1 {
			legacy = true
			continue
		}
		if v.SendOffset == nil {
			continue
		}
		if v.EndOffset == nil || *v.SendOffset < 0 || *v.EndOffset < *v.SendOffset {
			r.State = "partial"
			r.Reason = "missing_timing"
			continue
		}
		if r.FirstSend == nil || *v.SendOffset < *r.FirstSend {
			x := *v.SendOffset
			r.FirstSend = &x
		}
		if r.LastSend == nil || *v.SendOffset > *r.LastSend {
			x := *v.SendOffset
			r.LastSend = &x
		}
		if r.LastEnd == nil || *v.EndOffset > *r.LastEnd {
			x := *v.EndOffset
			r.LastEnd = &x
		}
		r.Metrics.add(v)

		completion := max(0, int(math.Ceil(*v.EndOffset/width))-1)
		cohortAt := v.PlannedOffset
		if basis == "actual_send" {
			cohortAt = *v.SendOffset
		}
		cohort := max(0, int(math.Floor(cohortAt/width)))
		if version == 1 {
			cohort = completion
		}
		pointAt := func(index int) *replayCurve {
			if index >= limit {
				return nil
			}
			if minutes[index] == nil {
				minutes[index] = &replayCurve{}
			}
			return minutes[index]
		}
		var m replayMetrics
		m.add(v)
		if point := pointAt(completion); point != nil {
			point.Success += m.Success
			point.UsageKnown += m.UsageKnown
			point.OutputTokens += m.OutputTokens
		}
		if point := pointAt(cohort); point != nil {
			point.Cohort.add(v)
			if v.Stream {
				mergeReplayHistogram(&point.E2EStream, m.E2E)
			} else {
				mergeReplayHistogram(&point.E2ENonstream, m.E2E)
			}
			mergeReplayHistogram(&point.TTFT, m.TTFT)
			mergeReplayHistogram(&point.TPOT, m.TPOT)
		}

		if v.Stream || (v.RequestStream != nil && *v.RequestStream) {
			r.Stream.add(v)
		} else {
			r.Nonstream.add(v)
		}
		delay := max(0, *v.SendOffset-v.PlannedOffset)
		r.SendDelay.add(delay)
		r.MaxSendDelay = max(r.MaxSendDelay, delay)
		if v.Error != "" && v.Error != "tail_truncated" {
			r.Errors[v.Error]++
		}
		events = append(events, replayTimingEvent{*v.SendOffset, 1, false}, replayTimingEvent{*v.EndOffset, -1, v.Error == "" && v.HTTPStatus >= 200 && v.HTTPStatus < 300})
	}
	if legacy {
		r.State = "unavailable"
		r.Reason = "legacy_without_timing"
		return r
	}
	if int64(r.Metrics.Sent) != view.Progress.Sent || view.Progress.Unconfirmed > 0 {
		r.State = "partial"
		r.Reason = "unconfirmed_requests"
	}
	// Metrics only cover usable result rows. Preserve the dispatch count and
	// expose missing rows in addition to requests already unknown after recovery.
	// This is report-local; it must not rewrite the run's execution progress.
	r.Progress.Unconfirmed += max(0, view.Progress.Sent-int64(r.Metrics.Sent))
	r.Metrics.finish()
	r.Stream.finish()
	r.Nonstream.finish()
	r.SendDelay.finish()
	if r.FirstSend == nil {
		return r
	}
	r.Window = *r.LastEnd - *r.FirstSend
	drain := *r.LastEnd - *r.LastSend
	r.Drain = &drain
	if r.Window > 0 {
		rate := float64(r.Metrics.Success) / r.Window
		r.RequestRate = &rate
		if r.Metrics.UsageKnown > 0 {
			rate := float64(r.Metrics.OutputTokens) / r.Window
			r.TokenRate = &rate
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].at == events[j].at {
			return events[i].delta > events[j].delta
		}
		return events[i].at < events[j].at
	})
	horizon := *r.LastEnd
	if version == 2 && basis == "planned_send" {
		horizon = max(horizon, r.PlannedDuration)
	}
	count := max(1, min(limit, int(math.Ceil(horizon/width))))
	r.CurveTruncated = horizon > float64(limit)*width
	r.Curve = make([]replayCurve, count)
	for i := range r.Curve {
		if point := minutes[i]; point != nil {
			r.Curve[i] = *point
		}
		r.Curve[i].Offset = float64(i) * width
		r.Curve[i].Width = math.Max(1e-9, math.Min(width, horizon-float64(i)*width))
		r.Curve[i].Cohort.finish()
		r.Curve[i].E2EStream.finish()
		r.Curve[i].E2ENonstream.finish()
		r.Curve[i].TTFT.finish()
		r.Curve[i].TPOT.finish()
	}
	current := 0
	prev := 0.
	area := 0.
	for _, e := range events {
		area += (e.at - prev) * float64(current)
		start := prev
		for start < e.at && start < float64(count)*width {
			i := min(count-1, int(start/width))
			end := math.Min(e.at, float64(i+1)*width)
			if end <= start {
				break
			}
			a := (end - start) * float64(current)
			r.Curve[i].InflightMean += a / r.Curve[i].Width
			r.Curve[i].InflightPeak = max(r.Curve[i].InflightPeak, current)
			start = end
		}
		current += e.delta
		r.InflightPeak = max(r.InflightPeak, current)
		if e.at < float64(count)*width {
			i := min(count-1, int(e.at/width))
			r.Curve[i].InflightPeak = max(r.Curve[i].InflightPeak, current)
		}
		prev = e.at
	}
	if r.Window > 0 {
		r.InflightMean = area / r.Window
	}
	return r
}
func (s *replayService) saveReport(view replayRunView) {
	report := buildReplayReport(s.runDir(view.RunID), view)
	if _, err := recordingAtomicJSON(s.runDir(view.RunID), "report.json", report); err != nil {
		s.audit.Print("replay_report_write_failed run=" + view.RunID)
	}
}
func (s *replayService) loadReport(id string, versions ...string) (any, bool) {
	version := 2
	if len(versions) > 0 && versions[0] == "1" {
		version = 1
	}
	s.mu.Lock()
	view, ok := s.loadRunLocked(id)
	if ok {
		s.reporting[id]++
	}
	s.mu.Unlock()
	if ok {
		defer func() {
			s.mu.Lock()
			s.reporting[id]--
			if s.reporting[id] == 0 {
				delete(s.reporting, id)
			}
			s.mu.Unlock()
		}()
	}
	if !ok {
		return nil, false
	}
	if view.EndedAt == nil {
		return map[string]any{"version": version, "state": "generating", "run_id": id}, true
	}
	data, err := os.ReadFile(filepath.Join(s.runDir(id), "report.json"))
	var header struct {
		Version int `json:"version"`
	}
	if err == nil && json.Unmarshal(data, &header) == nil && header.Version == version {
		return json.RawMessage(data), true
	}
	// Read-only reconstruction allows retry after a report write failure. Legacy
	// data is explicitly unavailable; reading a report never sends requests.
	return buildReplayReportVersion(s.runDir(id), view, version), true
}

func mergeReplayHistogram(to *replayHistogram, from replayHistogram) {
	if to.Bins == nil {
		to.Bins = map[int]int{}
	}
	for key, count := range from.Bins {
		to.Bins[key] += count
		to.Count += count
	}
}

// The fingerprint excludes local paths and run/target identity. Historical
// stream eligibility is only certified from hash-verified original requests.
func replayReportInput(dir string, view replayRunView) (string, string, string) {
	file, err := os.Open(filepath.Join(dir, "plan.jsonl"))
	if err != nil {
		return "", "legacy_unverified", "missing_plan"
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 65536), 1<<20)
	type identity struct {
		Gateway   string
		Recording string
		Segment   string
		Request   string
		SHA       string
		Offset    float64
	}
	items := []identity{}
	verified := true
	for scanner.Scan() {
		var item replayPlanItem
		if json.Unmarshal(scanner.Bytes(), &item) != nil || len(items) >= replayMaxPlanItems {
			return "", "legacy_unverified", "invalid_plan"
		}
		items = append(items, identity{item.GatewayID, item.RecordingID, item.Segment, item.RequestID, item.RequestSHA256, item.OffsetSeconds})
		if view.Plan == nil || view.Plan.FilterPolicy != "stream_only_v1" {
			body, err := readReplayBody(item)
			if err != nil || replayStreamExclusion(body) != "" {
				verified = false
			}
		}
	}
	if scanner.Err() != nil || len(items) == 0 {
		return "", "legacy_unverified", "invalid_plan"
	}
	if view.Plan != nil && view.Plan.FilterPolicy == "stream_only_v1" {
		return runtimeDigest(items), "stream_only_v1", "execution_plan"
	}
	if verified {
		return runtimeDigest(items), "stream_only_v1", "historical_request_bodies"
	}
	return runtimeDigest(items), "legacy_unverified", "requests_unavailable_or_mixed"
}
