package aigate

import (
	"math"
	"sort"
	"strconv"
	"time"
)

type replayPercentiles struct {
	Count int      `json:"count"`
	P50   *float64 `json:"p50"`
	P95   *float64 `json:"p95"`
	P99   *float64 `json:"p99"`
	Max   *float64 `json:"max"`
}

func percentiles(values []float64) replayPercentiles {
	out := replayPercentiles{Count: len(values)}
	if len(values) == 0 {
		return out
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	rank := func(p float64) *float64 {
		i := int(math.Ceil(p*float64(len(sorted)))) - 1
		if i < 0 {
			i = 0
		}
		v := sorted[i]
		return &v
	}
	out.P50, out.P95, out.P99, out.Max = rank(0.50), rank(0.95), rank(0.99), rank(1)
	return out
}

type replayStats struct {
	Segment               string            `json:"segment,omitempty"`
	Planned               int               `json:"planned"`
	Sent                  int               `json:"sent"`
	Completed             int               `json:"completed"`
	Outcomes              map[string]int    `json:"outcomes"`
	Statuses              map[string]int    `json:"statuses"`
	SuccessRate           *float64          `json:"success_rate"`
	TimeoutRate           *float64          `json:"timeout_rate"`
	TTFT                  replayPercentiles `json:"ttft_seconds"`
	E2E                   replayPercentiles `json:"e2e_seconds"`
	SendDelay             replayPercentiles `json:"send_delay_seconds"`
	MaxInflight           int               `json:"max_inflight"`
	PromptTokens          *uint64           `json:"prompt_tokens"`
	CompletionTokens      *uint64           `json:"completion_tokens"`
	CachedTokens          *uint64           `json:"cached_tokens"`
	CachedRatio           *float64          `json:"cached_ratio"`
	UsageKnown            int               `json:"usage_known"`
	OutputTokensPerSecond *float64          `json:"output_tokens_per_second"`
	ExpectedArrivalRate   *float64          `json:"expected_arrival_rate"`
	ActualArrivalRate     *float64          `json:"actual_arrival_rate"`
	DurationSeconds       float64           `json:"duration_seconds"`
	FixOutputDeviations   int               `json:"fix_output_deviation_over_1pct"`
}

// replayBaseline uses the same metrics computed from the recording itself.
// Recorded times include the client→switch hop and the first write is the
// first response byte (the recording keeps only the response tail).
type replayBaseline struct {
	Segment          string            `json:"segment,omitempty"`
	Requests         int               `json:"requests"`
	ArrivalRate      *float64          `json:"arrival_rate"`
	E2E              replayPercentiles `json:"e2e_seconds"`
	FirstWrite       replayPercentiles `json:"first_write_seconds"`
	PromptTokens     *uint64           `json:"prompt_tokens"`
	CompletionTokens *uint64           `json:"completion_tokens"`
	CachedTokens     *uint64           `json:"cached_tokens"`
	CachedRatio      *float64          `json:"cached_ratio"`
	CoverageSeconds  float64           `json:"coverage_seconds"`
}

type replayReport struct {
	Schema      int               `json:"schema_version"`
	RunID       string            `json:"run_id"`
	GatewayID   string            `json:"gateway_id"`
	RecordingID string            `json:"recording_id"`
	Mode        string            `json:"mode"`
	Target      replayTargetState `json:"target"`
	StartedAt   time.Time         `json:"started_at"`
	EndedAt     time.Time         `json:"ended_at"`
	Rewritten   []string          `json:"rewritten"`
	Original    bool              `json:"original_bytes"`
	Measurement map[string]string `json:"measurement"`
	Valid       bool              `json:"valid"`
	Invalid     []string          `json:"invalid_reasons"`
	Excluded    map[string]int    `json:"excluded"`
	WarmupSent  int               `json:"warmup_sent"`
	Overall     replayStats       `json:"overall"`
	Segments    []replayStats     `json:"segments"`
	Baseline    replayBaseline    `json:"baseline"`
	BaselineBy  []replayBaseline  `json:"baseline_segments"`
}

func floatPtr(v float64) *float64 { return &v }

func ratio(a, b float64) *float64 {
	if b <= 0 {
		return nil
	}
	return floatPtr(a / b)
}

func buildReplayStats(segment string, planned int, coverage float64, results []replayResult) replayStats {
	s := replayStats{Segment: segment, Planned: planned, Outcomes: map[string]int{}, Statuses: map[string]int{}}
	var ttft, e2e, delay []float64
	var prompt, completion, cached, promptForCache uint64
	var promptKnown, completionKnown, cachedKnown bool
	first, last, lastSent := math.Inf(1), math.Inf(-1), math.Inf(-1)
	timeouts := 0
	for _, r := range results {
		s.Outcomes[r.Outcome]++
		if r.Outcome == "not_sent" {
			continue
		}
		s.Sent++
		if r.Status != 0 {
			s.Statuses[strconv.Itoa(r.Status)]++
		}
		if r.Outcome == "completed" {
			s.Completed++
		}
		if r.Outcome == "timeout" {
			timeouts++
		}
		delay = append(delay, r.SendDelaySeconds)
		if r.InflightAtSend > s.MaxInflight {
			s.MaxInflight = r.InflightAtSend
		}
		first = math.Min(first, r.SentSeconds)
		last = math.Max(last, r.SentSeconds+r.E2ESeconds)
		lastSent = math.Max(lastSent, r.SentSeconds)
		if r.Outcome != "completed" {
			continue
		}
		e2e = append(e2e, r.E2ESeconds)
		if r.TTFTSeconds != nil {
			ttft = append(ttft, *r.TTFTSeconds)
		}
		if len(r.Usage) > 0 {
			s.UsageKnown++
		}
		if n, ok := r.Usage["prompt_tokens"]; ok {
			prompt += n
			promptKnown = true
			if c, ok := r.Usage["cached_tokens"]; ok && c <= n {
				cached += c
				promptForCache += n
				cachedKnown = true
			}
		}
		if n, ok := r.Usage["completion_tokens"]; ok {
			completion += n
			completionKnown = true
			if r.TargetTokens > 0 && math.Abs(float64(n)-float64(r.TargetTokens)) > 0.01*float64(r.TargetTokens) {
				s.FixOutputDeviations++
			}
		}
	}
	s.TTFT, s.E2E, s.SendDelay = percentiles(ttft), percentiles(e2e), percentiles(delay)
	if promptKnown {
		s.PromptTokens = &prompt
	}
	if completionKnown {
		s.CompletionTokens = &completion
	}
	if cachedKnown {
		s.CachedTokens = &cached
		s.CachedRatio = ratio(float64(cached), float64(promptForCache))
	}
	if s.Sent > 0 && last > first {
		s.DurationSeconds = last - first
		if completionKnown {
			s.OutputTokensPerSecond = ratio(float64(completion), s.DurationSeconds)
		}
	}
	// Arrivals are measured over the send interval, not until the last response ends.
	if s.Sent > 0 && lastSent > first {
		s.ActualArrivalRate = ratio(float64(s.Sent), lastSent-first)
	}
	s.ExpectedArrivalRate = ratio(float64(planned), coverage)
	s.SuccessRate = ratio(float64(s.Completed), float64(s.Sent))
	s.TimeoutRate = ratio(float64(timeouts), float64(s.Sent))
	return s
}

func buildReplayBaseline(segment string, coverage float64, items []replayPlanItem) replayBaseline {
	b := replayBaseline{Segment: segment, Requests: len(items), CoverageSeconds: coverage, ArrivalRate: ratio(float64(len(items)), coverage)}
	var e2e, first []float64
	var prompt, completion, cached, promptForCache uint64
	var promptKnown, completionKnown, cachedKnown bool
	for _, item := range items {
		e2e = append(e2e, item.RecordedSeconds)
		if item.RecordedFirst != nil {
			first = append(first, *item.RecordedFirst)
		}
		if n, ok := item.RecordedUsage["prompt_tokens"]; ok {
			prompt += n
			promptKnown = true
			if c, ok := item.RecordedUsage["cached_tokens"]; ok && c <= n {
				cached += c
				promptForCache += n
				cachedKnown = true
			}
		}
		if n, ok := item.RecordedUsage["completion_tokens"]; ok {
			completion += n
			completionKnown = true
		}
	}
	b.E2E, b.FirstWrite = percentiles(e2e), percentiles(first)
	if promptKnown {
		b.PromptTokens = &prompt
	}
	if completionKnown {
		b.CompletionTokens = &completion
	}
	if cachedKnown {
		b.CachedTokens = &cached
		b.CachedRatio = ratio(float64(cached), float64(promptForCache))
	}
	return b
}

// buildReplayReport covers measured segments only; warmup requests are sent
// but excluded. A sender that cannot keep the schedule invalidates the run.
func buildReplayReport(req replayRequest, plan *replayPlan, target replayTargetState, results []replayResult, started, ended time.Time) *replayReport {
	report := &replayReport{Schema: 1, RunID: req.RunID, GatewayID: plan.GatewayID, RecordingID: plan.RecordingID, Mode: req.Mode, Target: target,
		StartedAt: started, EndedAt: ended, Rewritten: plan.Rewritten, Original: plan.OriginalBytes, Excluded: plan.Excluded, Invalid: []string{}, Segments: []replayStats{},
		Measurement: map[string]string{"recording": "client to aigate-switch, first byte from the first response write", "replay": "aigate-replay to target proxy, no switch hop; TTFT is the first useful generated output"}}
	bySegment := map[string][]replayResult{}
	var measured []replayResult
	for _, r := range results {
		if r.Warmup {
			report.WarmupSent++
			continue
		}
		measured = append(measured, r)
		bySegment[r.Segment] = append(bySegment[r.Segment], r)
	}
	itemsBy := map[string][]replayPlanItem{}
	var measuredItems []replayPlanItem
	for _, item := range plan.items {
		if !item.Warmup {
			itemsBy[item.Segment] = append(itemsBy[item.Segment], item)
			measuredItems = append(measuredItems, item)
		}
	}
	var coverage float64
	planned := 0
	dropped := false
	for _, seg := range plan.Segments {
		if seg.Warmup {
			continue
		}
		dropped = dropped || seg.Dropped > 0
		c := seg.CoverageEnd.Sub(seg.CoverageStart).Seconds()
		coverage += c
		planned += seg.Planned
		report.Segments = append(report.Segments, buildReplayStats(seg.Segment, seg.Planned, c, bySegment[seg.Segment]))
		report.BaselineBy = append(report.BaselineBy, buildReplayBaseline(seg.Segment, c, itemsBy[seg.Segment]))
	}
	report.Overall = buildReplayStats("", planned, coverage, measured)
	report.Baseline = buildReplayBaseline("", coverage, measuredItems)
	if dropped {
		report.Invalid = append(report.Invalid, "source_requests_dropped")
	}
	if req.Mode == "timed" && report.Overall.SendDelay.P99 != nil && *report.Overall.SendDelay.P99 > 1 {
		report.Invalid = append(report.Invalid, "sender_delay_p99_over_1s")
	}
	if report.Overall.Sent < planned {
		report.Invalid = append(report.Invalid, "not_all_requests_sent")
	}
	report.Valid = len(report.Invalid) == 0
	return report
}
