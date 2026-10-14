package aigate

// Replay planning reads sealed recording segments and fixes the request
// sequence before anything is sent. Equal-output targets come only from schema-2
// metadata. Request integrity is checked locally and rechecked at send time.
import (
	"bufio"
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
	RunID                 string               `json:"run_id"`
	Recording             replayRecordingRef   `json:"recording"`
	Recordings            []replayRecordingRef `json:"recordings,omitempty"`
	PrepareOnly           bool                 `json:"prepare_only,omitempty"`
	Filter                replayFilter         `json:"filter"`
	Target                string               `json:"target"`
	Mode                  string               `json:"mode"`
	Concurrency           int                  `json:"concurrency,omitempty"`
	FixOutput             bool                 `json:"fix_output,omitempty"`
	SetModel              string               `json:"set_model,omitempty"`
	ForceStream           bool                 `json:"force_stream,omitempty"`
	RequestTimeoutSeconds int                  `json:"request_timeout_seconds,omitempty"`
}

// Omit the legacy selector from multi-source state as well as wire requests.
func (r replayRequest) MarshalJSON() ([]byte, error) {
	type plain replayRequest
	var recording *replayRecordingRef
	if len(r.Recordings) == 0 {
		recording = &r.Recording
	}
	return json.Marshal(struct {
		plain
		Recording *replayRecordingRef `json:"recording,omitempty"`
	}{plain: plain(r), Recording: recording})
}

func (r replayRequest) sources() []replayRecordingRef {
	if len(r.Recordings) != 0 {
		return r.Recordings
	}
	return []replayRecordingRef{r.Recording}
}

func (r *replayRequest) normalize() error {
	if len(r.Recordings) > 0 {
		if len(r.Recordings) > 2 || r.Recording.GatewayID != "" || r.Recording.RecordingID != "" || len(r.Recording.Segments) != 0 {
			return fmt.Errorf("invalid_recordings")
		}
		seen := map[string]bool{}
		total := 0
		for _, ref := range r.Recordings {
			key := ref.GatewayID + "/" + ref.RecordingID
			if seen[key] {
				return fmt.Errorf("duplicate_recording")
			}
			seen[key] = true
			one := *r
			one.Recordings = nil
			one.Recording = ref
			if err := one.normalize(); err != nil {
				return err
			}
			total += len(ref.Segments)
		}
		if total > replayMaxSegments {
			return fmt.Errorf("invalid_segments")
		}
		if len(r.Recordings) > 1 && r.Mode != "" && r.Mode != "timed" {
			return fmt.Errorf("overlay_timed_required")
		}
		one := *r
		one.Recordings = nil
		one.Recording = r.Recordings[0]
		if err := one.normalize(); err != nil {
			return err
		}
		r.Filter, r.Mode, r.RequestTimeoutSeconds = one.Filter, one.Mode, one.RequestTimeoutSeconds
		return nil
	}
	if r.PrepareOnly && r.Mode != "" && r.Mode != "timed" {
		return fmt.Errorf("overlay_timed_required")
	}

	if !validOperationID(r.RunID) || !validOperationID(r.Recording.GatewayID) || !validOperationID(r.Recording.RecordingID) {
		return fmt.Errorf("invalid_identity")
	}
	seen := map[string]bool{}
	if len(r.Recording.Segments) == 0 || len(r.Recording.Segments) > replayMaxSegments {
		return fmt.Errorf("invalid_segments")
	}
	for _, name := range r.Recording.Segments {
		if _, ok := parseRecordingSegment(name); !ok || seen[name] {
			return fmt.Errorf("invalid_segments")
		}
		seen[name] = true
	}
	if r.Filter.Status == "" {
		r.Filter.Status = "all"
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
	if r.Mode == "" {
		r.Mode = "timed"
	}
	switch r.Mode {
	case "timed":
		if r.Concurrency != 0 {
			return fmt.Errorf("invalid_concurrency")
		}
	case "concurrency":
		if r.Concurrency < 1 || r.Concurrency > 1024 {
			return fmt.Errorf("invalid_concurrency")
		}
	default:
		return fmt.Errorf("invalid_mode")
	}
	if len(r.SetModel) > 256 {
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
	if r.ForceStream {
		out = append(out, "force_stream")
	}
	if r.FixOutput {
		out = append(out, "fix_output")
	}
	return out
}

type replayPlanItem struct {
	GatewayID     string            `json:"gateway_id"`
	RecordingID   string            `json:"recording_id"`
	Sequence      int               `json:"sequence"`
	Segment       string            `json:"segment"`
	OffsetSeconds float64           `json:"offset_seconds"`
	Directory     string            `json:"directory"`
	RequestID     string            `json:"request_id"`
	Path          string            `json:"path"`
	Headers       map[string]string `json:"headers"`
	RequestSHA256 string            `json:"request_sha256"`
	RequestBytes  int64             `json:"request_bytes"`
	TargetTokens  uint64            `json:"target_tokens,omitempty"`
}

type replayPlanSegment struct {
	GatewayID     string    `json:"gateway_id"`
	RecordingID   string    `json:"recording_id"`
	Segment       string    `json:"segment"`
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
	if len(req.Recordings) > 0 {
		merged := &replayPlan{Schema: recordingSchema, RunID: req.RunID, Excluded: map[string]int{}, Rewritten: req.rewritten(), OriginalBytes: len(req.rewritten()) == 0}
		for _, ref := range req.Recordings {
			one := req
			one.Recordings = nil
			one.Recording = ref
			plan, err := buildReplayPlan(root, one)
			if err != nil {
				return nil, err
			}
			merged.items = append(merged.items, plan.items...)
			merged.Segments = append(merged.Segments, plan.Segments...)
			merged.DurationSeconds = max(merged.DurationSeconds, plan.DurationSeconds)
			for k, v := range plan.Excluded {
				merged.Excluded[k] += v
			}
			if len(merged.items) > replayMaxPlanItems {
				return nil, fmt.Errorf("plan_too_large")
			}
		}
		sort.SliceStable(merged.items, func(i, j int) bool { return merged.items[i].OffsetSeconds < merged.items[j].OffsetSeconds })
		for i := range merged.items {
			merged.items[i].Sequence = i + 1
		}
		merged.Planned = len(merged.items)
		return merged, nil
	}

	session, err := readRecordingSession(root, req.Recording.GatewayID, req.Recording.RecordingID)
	if err != nil {
		return nil, err
	}
	if session.Schema != recordingSchema {
		return nil, fmt.Errorf("recording_schema_unsupported_rerecord")
	}
	plan := &replayPlan{Schema: recordingSchema, RunID: req.RunID, GatewayID: req.Recording.GatewayID, RecordingID: req.Recording.RecordingID,
		Excluded: map[string]int{}, Rewritten: req.rewritten(), Segments: []replayPlanSegment{}}
	plan.OriginalBytes = len(plan.Rewritten) == 0
	segments := []replaySegmentStatus{}
	for _, name := range req.Recording.Segments {
		status, err := readRecordingSegment(root, req.Recording.GatewayID, req.Recording.RecordingID, name)
		if err != nil {
			return nil, err
		}
		if status.Schema != recordingSchema {
			return nil, fmt.Errorf("recording_schema_unsupported_rerecord")
		}
		if status.State != "sealed" {
			return nil, fmt.Errorf("segment_not_sealed")
		}
		segments = append(segments, status)
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].Start.Before(segments[j].Start) })
	base := 0.0
	for _, seg := range segments {
		coverage := seg.CoverageEnd.Sub(seg.CoverageStart).Seconds()
		if coverage < 0 {
			coverage = 0
		}
		entry := replayPlanSegment{GatewayID: req.Recording.GatewayID, RecordingID: req.Recording.RecordingID, Segment: seg.Segment, CoverageStart: seg.CoverageStart, CoverageEnd: seg.CoverageEnd,
			Partial: seg.Partial, BaseSeconds: base, Recorded: seg.Recorded, Dropped: seg.Dropped, Truncated: seg.Truncated}
		dir := filepath.Join(root, req.Recording.GatewayID, req.Recording.RecordingID, seg.Segment)
		items, err := planSegment(dir, seg, base, req, plan)
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

func planSegment(dir string, seg replaySegmentStatus, base float64, req replayRequest, plan *replayPlan) ([]replayPlanItem, error) {
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
		// Decode response metadata separately so an invalid token value has the
		// explicit equal-output error rather than silently excluding the record.
		wire := struct {
			*recordingMeta
			ResponseResult json.RawMessage `json:"response_result"`
		}{recordingMeta: &meta}
		data, err := recordingReadSmall(filepath.Join(recordDir, "meta.json"))
		if err != nil || json.Unmarshal(data, &wire) != nil || meta.RequestID != line.RequestID {
			plan.Excluded["meta_unreadable"]++
			continue
		}
		if meta.Schema != recordingSchema {
			return nil, fmt.Errorf("recording_schema_unsupported_rerecord")
		}
		if err := json.Unmarshal(wire.ResponseResult, &meta.ResponseResult); err != nil && req.FixOutput {
			return nil, fmt.Errorf("fix_output_usage_unknown")
		}
		item := replayPlanItem{GatewayID: req.Recording.GatewayID, RecordingID: req.Recording.RecordingID, Segment: seg.Segment, OffsetSeconds: base + line.OffsetSeconds - shift, Directory: recordDir, RequestID: meta.RequestID,
			Path: meta.Path, Headers: copyableHeaders(meta.Headers), RequestSHA256: meta.RequestSHA256, RequestBytes: meta.RequestBytes}
		if item.OffsetSeconds < base {
			item.OffsetSeconds = base
		}
		body, err := readReplayBody(item)
		if err != nil {
			return nil, err
		}
		if req.FixOutput {
			item.TargetTokens = meta.ResponseResult.Usage["completion_tokens"]
			if !meta.ResponseResult.UsageKnown || item.TargetTokens == 0 {
				return nil, fmt.Errorf("fix_output_usage_unknown")
			}
			if err := checkSingleCandidate(body); err != nil {
				return nil, err
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
	if req.SetModel == "" && !req.ForceStream && !req.FixOutput {
		return body, nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return nil, fmt.Errorf("body_not_rewritable")
	}
	if req.SetModel != "" {
		fields["model"] = encodeJSON(req.SetModel)
	}
	if req.ForceStream {
		fields["stream"] = json.RawMessage("true")
	}
	if req.FixOutput {
		if item.TargetTokens == 0 {
			return nil, fmt.Errorf("fix_output_usage_unknown")
		}
		if err := checkSingleCandidate(body); err != nil {
			return nil, err
		}
		// Equal work: the recorded output length with end-of-sequence ignored
		// (SGLang/vLLM ignore_eos and min_tokens).
		fields["max_tokens"] = encodeJSON(item.TargetTokens)
		fields["min_tokens"] = encodeJSON(item.TargetTokens)
		fields["ignore_eos"] = json.RawMessage("true")
		delete(fields, "max_completion_tokens")
		delete(fields, "stop")
		delete(fields, "stop_token_ids")
		var stream bool
		if raw, ok := fields["stream"]; ok {
			if string(raw) == "null" || json.Unmarshal(raw, &stream) != nil {
				return nil, fmt.Errorf("invalid_stream")
			}
		}
		if stream {
			options := map[string]json.RawMessage{}
			if raw, ok := fields["stream_options"]; ok {
				if json.Unmarshal(raw, &options) != nil || options == nil {
					return nil, fmt.Errorf("invalid_stream_options")
				}
			}
			options["include_usage"] = json.RawMessage("true")
			fields["stream_options"] = encodeJSON(options)
		}
	}
	return encodeJSON(fields), nil
}

// Only metadata from new recordings is supported. Reading one bounded request
// at a time catches corruption and multi-candidate requests before any sends.
func readReplayBody(item replayPlanItem) ([]byte, error) {
	file, err := os.Open(filepath.Join(item.Directory, "request.json"))
	if err != nil {
		return nil, fmt.Errorf("record_unreadable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != item.RequestBytes || info.Size() < 0 || info.Size() > recordingMemory {
		return nil, fmt.Errorf("record_changed")
	}
	body, err := io.ReadAll(io.LimitReader(file, info.Size()+1))
	if err != nil || int64(len(body)) != info.Size() || sha256Hex(body) != item.RequestSHA256 {
		return nil, fmt.Errorf("record_changed")
	}
	return body, nil
}

func checkSingleCandidate(body []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return fmt.Errorf("body_not_rewritable")
	}
	if raw, ok := fields["n"]; ok {
		var n int
		if json.Unmarshal(raw, &n) != nil || n != 1 {
			return fmt.Errorf("fix_output_single_candidate_required")
		}
	}
	return nil
}
