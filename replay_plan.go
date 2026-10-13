package aigate

// Replay planning reads sealed recording segments and fixes the request
// sequence before anything is sent. Only metadata, response tails and the
// first timeline write are read here; request bodies are read at send time.
import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	replayMaxSegments    = 96
	replayMaxPlanItems   = 200_000
	replayTimelineProbe  = 64 << 10
	replayHeaderCopyable = "Content-Type,Content-Encoding,Accept,User-Agent"
)

type replayRecordingRef struct {
	GatewayID   string   `json:"gateway_id"`
	RecordingID string   `json:"recording_id"`
	Segments    []string `json:"segments"`
}

type replayFilter struct {
	Status string `json:"status,omitempty"`
	Path   string `json:"path,omitempty"`
}

// replayRequest is the platform's run request. JSON names follow the design's
// --option names with hyphens replaced by underscores.
type replayRequest struct {
	RunID                 string             `json:"run_id"`
	Recording             replayRecordingRef `json:"recording"`
	WarmupSegments        []string           `json:"warmup_segments,omitempty"`
	Filter                replayFilter       `json:"filter"`
	Target                string             `json:"target"`
	Mode                  string             `json:"mode"`
	Concurrency           int                `json:"concurrency,omitempty"`
	MaxInflight           int                `json:"max_inflight,omitempty"`
	FixOutput             bool               `json:"fix_output,omitempty"`
	SetModel              string             `json:"set_model,omitempty"`
	SetMaxTokens          int                `json:"set_max_tokens,omitempty"`
	ForceStream           bool               `json:"force_stream,omitempty"`
	RequestTimeoutSeconds int                `json:"request_timeout_seconds,omitempty"`
}

func (r *replayRequest) normalize() error {
	if !validOperationID(r.RunID) || !validOperationID(r.Recording.GatewayID) || !validOperationID(r.Recording.RecordingID) {
		return fmt.Errorf("invalid_identity")
	}
	seen := map[string]bool{}
	all := append(append([]string{}, r.WarmupSegments...), r.Recording.Segments...)
	if len(r.Recording.Segments) == 0 || len(all) > replayMaxSegments {
		return fmt.Errorf("invalid_segments")
	}
	for _, name := range all {
		if _, ok := parseRecordingSegment(name); !ok || seen[name] {
			return fmt.Errorf("invalid_segments")
		}
		seen[name] = true
	}
	if r.Filter.Status == "" {
		r.Filter.Status = "2xx"
	}
	if r.Filter.Status != "2xx" && r.Filter.Status != "all" {
		return fmt.Errorf("invalid_filter")
	}
	if r.Filter.Path == "" {
		r.Filter.Path = "/v1/chat/completions"
	}
	if r.Filter.Path != "/v1/chat/completions" {
		return fmt.Errorf("invalid_filter")
	}
	switch r.Mode {
	case "timed":
		if r.Concurrency != 0 {
			return fmt.Errorf("invalid_concurrency")
		}
		if r.MaxInflight == 0 {
			r.MaxInflight = 512
		}
		if r.MaxInflight < 1 || r.MaxInflight > 4096 {
			return fmt.Errorf("invalid_max_inflight")
		}
	case "concurrency":
		if r.Concurrency < 1 || r.Concurrency > 1024 || r.MaxInflight != 0 {
			return fmt.Errorf("invalid_concurrency")
		}
	default:
		return fmt.Errorf("invalid_mode")
	}
	if len(r.SetModel) > 256 || r.SetMaxTokens < 0 || r.SetMaxTokens > 1_000_000 || (r.FixOutput && r.SetMaxTokens > 0) {
		return fmt.Errorf("invalid_rewrite")
	}
	if r.RequestTimeoutSeconds == 0 {
		r.RequestTimeoutSeconds = 3720
	}
	if r.RequestTimeoutSeconds < 1 || r.RequestTimeoutSeconds > 86400 {
		return fmt.Errorf("invalid_timeout")
	}
	return nil
}

func (r replayRequest) rewritten() []string {
	out := []string{}
	if r.SetModel != "" {
		out = append(out, "set_model")
	}
	if r.SetMaxTokens > 0 {
		out = append(out, "set_max_tokens")
	}
	if r.ForceStream {
		out = append(out, "force_stream")
	}
	if r.FixOutput {
		out = append(out, "fix_output")
	}
	return out
}

type replayPlanItem struct {
	Sequence        int               `json:"sequence"`
	Segment         string            `json:"segment"`
	Warmup          bool              `json:"warmup,omitempty"`
	OffsetSeconds   float64           `json:"offset_seconds"`
	Directory       string            `json:"directory"`
	RequestID       string            `json:"request_id"`
	Path            string            `json:"path"`
	Headers         map[string]string `json:"headers"`
	RequestSHA256   string            `json:"request_sha256"`
	RequestBytes    int64             `json:"request_bytes"`
	Stream          bool              `json:"stream"`
	TargetTokens    uint64            `json:"target_tokens,omitempty"`
	RecordedStatus  int               `json:"recorded_status"`
	RecordedSeconds float64           `json:"recorded_seconds"`
	RecordedFirst   *float64          `json:"recorded_first_write_seconds,omitempty"`
	RecordedUsage   map[string]uint64 `json:"recorded_usage,omitempty"`
}

type replayPlanSegment struct {
	Segment       string    `json:"segment"`
	Warmup        bool      `json:"warmup,omitempty"`
	CoverageStart time.Time `json:"coverage_start"`
	CoverageEnd   time.Time `json:"coverage_end"`
	Partial       bool      `json:"partial"`
	BaseSeconds   float64   `json:"base_seconds"`
	Recorded      uint64    `json:"recorded"`
	Dropped       uint64    `json:"dropped"`
	Truncated     uint64    `json:"truncated"`
	Planned       int       `json:"planned"`
}

type replayPlan struct {
	Schema          int                 `json:"schema_version"`
	RunID           string              `json:"run_id"`
	GatewayID       string              `json:"gateway_id"`
	RecordingID     string              `json:"recording_id"`
	Segments        []replayPlanSegment `json:"segments"`
	Planned         int                 `json:"planned"`
	Excluded        map[string]int      `json:"excluded"`
	Rewritten       []string            `json:"rewritten"`
	OriginalBytes   bool                `json:"original_bytes"`
	FixOutputMissed int                 `json:"fix_output_unknown"`
	DurationSeconds float64             `json:"duration_seconds"`
	items           []replayPlanItem
}

// replaySegmentStatus is the metadata the platform lists for each segment.
type replaySegmentStatus struct {
	recordingSegmentFile
	Pinned bool `json:"pinned"`
}

func readRecordingSession(root, gateway, recording string) (recordingSessionFile, error) {
	var session recordingSessionFile
	data, err := recordingReadSmall(filepath.Join(root, gateway, recording, "session.json"))
	if errors.Is(err, os.ErrNotExist) {
		return session, fmt.Errorf("recording_not_found")
	}
	if err != nil || json.Unmarshal(data, &session) != nil || session.RecordingID != recording || session.GatewayID != gateway {
		return session, fmt.Errorf("recording_invalid")
	}
	return session, nil
}

func readRecordingSegment(root, gateway, recording, name string) (replaySegmentStatus, error) {
	var out replaySegmentStatus
	dir := filepath.Join(root, gateway, recording, name)
	data, err := recordingReadSmall(filepath.Join(dir, "segment.json"))
	if errors.Is(err, os.ErrNotExist) {
		if _, statErr := os.Stat(dir); statErr == nil {
			return out, fmt.Errorf("segment_not_sealed")
		}
		return out, fmt.Errorf("segment_not_found")
	}
	if err != nil || json.Unmarshal(data, &out.recordingSegmentFile) != nil || out.Segment != name {
		return out, fmt.Errorf("segment_invalid")
	}
	_, pinErr := os.Stat(filepath.Join(dir, "pinned"))
	out.Pinned = pinErr == nil
	return out, nil
}

// replayTailUsage returns the last usage object in a response tail. It matches
// the "usage" key itself rather than whole lines: a summary tail of a large
// non-stream body starts mid-object, and a quote inside a JSON string is always
// escaped, so the unescaped key cannot come from content.
func replayTailUsage(tail []byte) map[string]uint64 {
	key := []byte(`"usage"`)
	for end := len(tail); ; {
		i := bytes.LastIndex(tail[:end], key)
		if i < 0 {
			return nil
		}
		end = i
		rest := bytes.TrimLeft(tail[i+len(key):], " \t\r\n")
		if len(rest) == 0 || rest[0] != ':' {
			continue
		}
		var raw json.RawMessage
		if json.NewDecoder(bytes.NewReader(rest[1:])).Decode(&raw) != nil || len(raw) == 0 || raw[0] != '{' {
			continue
		}
		if usage := profileUsage(raw); len(usage) > 0 {
			return usage
		}
	}
}

func replayFirstWrite(path string) *float64 {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	scanner := bufio.NewScanner(io.LimitReader(f, replayTimelineProbe))
	scanner.Buffer(make([]byte, 4096), 64<<10)
	for scanner.Scan() {
		var line recordingTimelineLine
		if json.Unmarshal(scanner.Bytes(), &line) == nil && line.Type == "write" {
			v := line.Seconds
			return &v
		}
	}
	return nil
}

func copyableHeaders(headers map[string]string) map[string]string {
	out := map[string]string{}
	for _, k := range strings.Split(replayHeaderCopyable, ",") {
		if v := headers[k]; v != "" {
			out[k] = v
		}
	}
	return out
}

// buildReplayPlan selects sealed segments and orders requests by arrival.
// Segments are concatenated in time order by their covered durations, so
// contiguous segments keep real time and gaps between distant ones collapse.
func buildReplayPlan(root string, req replayRequest) (*replayPlan, error) {
	if _, err := readRecordingSession(root, req.Recording.GatewayID, req.Recording.RecordingID); err != nil {
		return nil, err
	}
	plan := &replayPlan{Schema: 1, RunID: req.RunID, GatewayID: req.Recording.GatewayID, RecordingID: req.Recording.RecordingID,
		Excluded: map[string]int{}, Rewritten: req.rewritten(), Segments: []replayPlanSegment{}}
	plan.OriginalBytes = len(plan.Rewritten) == 0
	type chosen struct {
		status replaySegmentStatus
		warmup bool
	}
	var segments []chosen
	for _, group := range []struct {
		names  []string
		warmup bool
	}{{req.WarmupSegments, true}, {req.Recording.Segments, false}} {
		var ordered []chosen
		for _, name := range group.names {
			status, err := readRecordingSegment(root, req.Recording.GatewayID, req.Recording.RecordingID, name)
			if err != nil {
				return nil, err
			}
			if status.State != "sealed" {
				return nil, fmt.Errorf("segment_not_sealed")
			}
			ordered = append(ordered, chosen{status, group.warmup})
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].status.Start.Before(ordered[j].status.Start) })
		segments = append(segments, ordered...)
	}
	base := 0.0
	for _, seg := range segments {
		coverage := seg.status.CoverageEnd.Sub(seg.status.CoverageStart).Seconds()
		if coverage < 0 {
			coverage = 0
		}
		entry := replayPlanSegment{Segment: seg.status.Segment, Warmup: seg.warmup, CoverageStart: seg.status.CoverageStart, CoverageEnd: seg.status.CoverageEnd,
			Partial: seg.status.Partial, BaseSeconds: base, Recorded: seg.status.Recorded, Dropped: seg.status.Dropped, Truncated: seg.status.Truncated}
		dir := filepath.Join(root, req.Recording.GatewayID, req.Recording.RecordingID, seg.status.Segment)
		items, err := planSegment(dir, seg.status, seg.warmup, base, req, plan)
		if err != nil {
			return nil, err
		}
		entry.Planned = len(items)
		plan.items = append(plan.items, items...)
		if len(plan.items) > replayMaxPlanItems {
			return nil, fmt.Errorf("plan_too_large")
		}
		plan.Segments = append(plan.Segments, entry)
		base += coverage
	}
	plan.DurationSeconds = base
	for i := range plan.items {
		plan.items[i].Sequence = i + 1
	}
	plan.Planned = len(plan.items)
	if plan.Planned == 0 {
		return nil, fmt.Errorf("plan_empty")
	}
	return plan, nil
}

func planSegment(dir string, seg replaySegmentStatus, warmup bool, base float64, req replayRequest, plan *replayPlan) ([]replayPlanItem, error) {
	f, err := os.Open(filepath.Join(dir, "index.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("segment_invalid")
	}
	defer f.Close()
	shift := seg.CoverageStart.Sub(seg.Start).Seconds()
	var items []replayPlanItem
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var line recordingIndexLine
		if json.Unmarshal(scanner.Bytes(), &line) != nil || !strings.HasPrefix(line.Directory, "r-") || strings.ContainsAny(line.Directory, `/\`) {
			plan.Excluded["invalid_index_line"]++
			continue
		}
		reason := ""
		switch {
		case line.Outcome == "rejected":
			reason = "entry_rejected"
		case line.Method != "POST" || line.Path != req.Filter.Path:
			reason = "path"
		case line.RequestState != "complete":
			reason = "request_" + line.RequestState
		case line.CaptureState != "complete":
			reason = "capture_" + line.CaptureState
		case req.Filter.Status == "2xx" && (line.Status < 200 || line.Status > 299):
			reason = "status"
		}
		if reason != "" {
			plan.Excluded[reason]++
			continue
		}
		recordDir := filepath.Join(dir, line.Directory)
		var meta recordingMeta
		data, err := recordingReadSmall(filepath.Join(recordDir, "meta.json"))
		if err != nil || json.Unmarshal(data, &meta) != nil || meta.RequestID != line.RequestID {
			plan.Excluded["meta_unreadable"]++
			continue
		}
		item := replayPlanItem{Segment: seg.Segment, Warmup: warmup, OffsetSeconds: base + line.OffsetSeconds - shift, Directory: recordDir, RequestID: meta.RequestID,
			Path: meta.Path, Headers: copyableHeaders(meta.Headers), RequestSHA256: meta.RequestSHA256, RequestBytes: meta.RequestBytes,
			Stream: strings.HasPrefix(strings.ToLower(meta.ResponseHeaders["Content-Type"]), "text/event-stream"), RecordedStatus: meta.Status,
			RecordedSeconds: meta.DurationSeconds, RecordedFirst: replayFirstWrite(filepath.Join(recordDir, "timeline.jsonl"))}
		if item.OffsetSeconds < base {
			item.OffsetSeconds = base
		}
		if tail, err := os.ReadFile(filepath.Join(recordDir, meta.ResponseFile)); err == nil && len(tail) <= 64<<20 {
			item.RecordedUsage = replayTailUsage(tail)
		}
		if req.FixOutput {
			if n, ok := item.RecordedUsage["completion_tokens"]; ok && n > 0 {
				item.TargetTokens = n
			} else {
				plan.FixOutputMissed++
			}
		}
		items = append(items, item)
	}
	if scanner.Err() != nil {
		return nil, fmt.Errorf("segment_invalid")
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].OffsetSeconds < items[j].OffsetSeconds })
	return items, nil
}

// rewriteReplayBody applies the only allowed rewrites. Without rewrites the
// recorded bytes are sent unchanged.
func rewriteReplayBody(body []byte, req replayRequest, item replayPlanItem) ([]byte, error) {
	if req.SetModel == "" && req.SetMaxTokens == 0 && !req.ForceStream && item.TargetTokens == 0 {
		return body, nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return nil, fmt.Errorf("body_not_rewritable")
	}
	if req.SetModel != "" {
		fields["model"] = encodeJSON(req.SetModel)
	}
	if req.SetMaxTokens > 0 {
		fields["max_tokens"] = encodeJSON(req.SetMaxTokens)
		delete(fields, "max_completion_tokens")
	}
	if req.ForceStream {
		fields["stream"] = json.RawMessage("true")
	}
	if item.TargetTokens > 0 {
		// Equal work: the recorded output length with end-of-sequence ignored
		// (SGLang/vLLM ignore_eos and min_tokens).
		fields["max_tokens"] = encodeJSON(item.TargetTokens)
		fields["min_tokens"] = encodeJSON(item.TargetTokens)
		fields["ignore_eos"] = json.RawMessage("true")
		delete(fields, "max_completion_tokens")
	}
	return encodeJSON(fields), nil
}
