package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)

func (p *requestProfiler) analyzer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-p.work:
			p.jobsMu.Lock()
			j := p.jobs[id]
			j.State = "running"
			q := j.Query
			p.jobsMu.Unlock()
			work, cancel := context.WithTimeout(ctx, 30*time.Second)
			result, err := p.analyze(work, q)
			cancel()
			p.jobsMu.Lock()
			if err != nil {
				j.State = "failed"
				j.Error = err.Error()
			} else {
				j.State = "completed"
				j.Result = result
			}
			p.jobsMu.Unlock()
		}
	}
}

type profileRow struct{ start, input, end *profileEvent }

func profileStats(values []float64) map[string]any {
	if len(values) == 0 {
		return map[string]any{"count": 0, "mean": nil, "p50": nil, "p95": nil, "p99": nil}
	}
	sort.Float64s(values)
	sum := float64(0)
	for _, v := range values {
		sum += v
	}
	return map[string]any{"count": len(values), "mean": sum / float64(len(values)), "p50": values[(len(values)-1)/2], "p95": values[int(float64(len(values)-1)*.95)], "p99": values[int(float64(len(values)-1)*.99)], "max": values[len(values)-1]}
}
func (p *requestProfiler) analyze(ctx context.Context, q profileQuery) (any, error) {
	p.diskMu.Lock()
	files := append([]string{}, p.files...)
	oldest := p.oldest
	// Filesystem mtime may be rounded below the final event timestamp. All events
	// retained at startup precede this boot even if their file has a coarse mtime.
	asof := max(p.lastWrite, float64(p.started.UnixNano())/1e9)
	p.diskMu.Unlock()
	// Only live requests can be pending. Missing end events may have been dropped.
	p.mu.Lock()
	active := make(map[uint64]struct{}, len(p.active))
	for seq := range p.active {
		active[seq] = struct{}{}
	}
	pendingAsOf := profileNow()
	p.mu.Unlock()
	if q.Start < oldest {
		return nil, fmt.Errorf("events expired before analysis")
	}
	rows := map[string]*profileRow{}
	var scanned int64
	invalid := 0
	events := 0
	for _, path := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("event segment no longer available")
		}
		// Fix the read boundary so later writes cannot extend this analysis indefinitely.
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		// Only a complete sidecar may exclude a segment. A crash or active tail is scanned conservatively.
		if data, err := os.ReadFile(path + ".meta.json"); err == nil {
			var meta profileSegmentMeta
			if json.Unmarshal(data, &meta) == nil && meta.Bytes == info.Size() && meta.Min > 0 && meta.Max >= meta.Min && (meta.Max < q.Start || meta.Min >= q.End) {
				f.Close()
				continue
			}
		}
		scanned += info.Size()
		if scanned > 256<<20 {
			f.Close()
			return nil, fmt.Errorf("analysis exceeds 256 MiB scan budget; choose a shorter range")
		}
		scanner := bufio.NewScanner(&profileLimitedReader{file: f, left: info.Size()})
		scanner.Buffer(make([]byte, 16384), 32768)
		for scanner.Scan() {
			events++
			if events%256 == 0 {
				if err := ctx.Err(); err != nil {
					f.Close()
					return nil, err
				}
			}
			var e profileEvent
			if json.Unmarshal(scanner.Bytes(), &e) != nil {
				invalid++
				continue
			}
			if e.At > asof || e.Arrived < q.Start || e.Arrived >= q.End {
				continue
			}
			key := e.Boot + ":" + e.ID
			r := rows[key]
			if r == nil {
				if len(rows) >= 100000 {
					f.Close()
					return nil, fmt.Errorf("analysis exceeds 100000 requests; choose a shorter range")
				}
				r = &profileRow{}
				rows[key] = r
			}
			switch e.Kind {
			case "start":
				r.start = &e
			case "input":
				r.input = &e
			case "end":
				r.end = &e
			}
		}
		err = scanner.Err()
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("event read failed: %w", err)
		}
	}
	selected := []*profileRow{}
	for _, r := range rows {
		e := r.input
		if e == nil {
			e = r.end
		}
		if q.Backend != "" && (e == nil || e.Backend != q.Backend) {
			continue
		}
		if q.Model != "" && (e == nil || e.Model != q.Model) {
			continue
		}
		selected = append(selected, r)
	}
	arrival := func(r *profileRow) *profileEvent {
		if r.start != nil {
			return r.start
		}
		if r.input != nil {
			return r.input
		}
		return r.end
	}
	sort.Slice(selected, func(i, j int) bool {
		a, b := arrival(selected[i]), arrival(selected[j])
		if a.Arrived != b.Arrived {
			return a.Arrived < b.Arrived
		}
		if a.Boot != b.Boot {
			return a.Boot < b.Boot
		}
		return a.Seq < b.Seq
	})
	input, output, durations, shared := []float64{}, []float64{}, []float64{}, []float64{}
	outcomes := map[string]int{}
	pending, unknown, missingStart := 0, 0, 0
	levels := map[string]any{}
	longest := make([]float64, len(selected))
	for _, level := range profileLevels {
		counts := map[string]int{}
		last := map[string]int{}
		tree := make([]int, len(selected)+2)
		times := map[string]float64{}
		gaps, seconds, distinct := []float64{}, []float64{}, []float64{}
		eligible := 0
		add := func(i, delta int) {
			for i++; i < len(tree); i += i & -i {
				tree[i] += delta
			}
		}
		total := func(i int) int {
			n := 0
			for i++; i > 0; i -= i & -i {
				n += tree[i]
			}
			return n
		}
		for i, r := range selected {
			if r.input == nil {
				continue
			}
			for _, mark := range r.input.Marks {
				if mark.Level != level {
					continue
				}
				eligible++
				key := r.input.Domain + ":" + mark.Digest
				counts[key]++
				if j, ok := last[key]; ok {
					gaps = append(gaps, float64(i-j))
					seconds = append(seconds, r.input.Arrived-times[key])
					distinct = append(distinct, float64(total(i-1)-total(j)))
					if float64(mark.Bytes) > longest[i] {
						longest[i] = float64(mark.Bytes)
					}
					add(j, -1)
				}
				add(i, 1)
				last[key] = i
				times[key] = r.input.Arrived
			}
		}
		ranked := []int{}
		for _, n := range counts {
			ranked = append(ranked, n)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(ranked)))
		top := []map[string]any{}
		for i, n := range ranked {
			if i == 10 {
				break
			}
			top = append(top, map[string]any{"group": i + 1, "count": n, "share": float64(n) / float64(eligible)})
		}
		levels[strconv.Itoa(level)] = map[string]any{"eligible": eligible, "families": len(counts), "repeated": len(gaps), "top10": top, "gap_requests": profileStats(gaps), "gap_seconds": profileStats(seconds), "intervening_distinct": profileStats(distinct)}
	}
	for i, r := range selected {
		if r.start == nil {
			missingStart++
		}
		if r.input != nil && len(r.input.Marks) > 0 {
			shared = append(shared, longest[i])
		}
		if r.end == nil {
			e := arrival(r)
			_, live := active[e.Seq]
			if e.Boot == p.boot && live {
				pending++
			} else {
				unknown++
			}
			continue
		}
		outcomes[r.end.Outcome]++
		u := profileUsage(r.end.Usage)
		if n, ok := u["prompt_tokens"]; ok {
			input = append(input, float64(n))
		}
		if n, ok := u["completion_tokens"]; ok {
			output = append(output, float64(n))
		}
		durations = append(durations, r.end.Duration)
	}
	return map[string]any{"schema": "gateway-profile-v1", "source": "gateway", "request_scope": "all", "scope_policy": "metric-v2", "time_basis": "arrival", "prefix_basis": "within_window_messages", "start": q.Start, "end": q.End, "as_of": asof, "pending_as_of": pendingAsOf, "computed_at": profileNow(), "requests": len(selected), "pending": pending, "unknown": unknown, "outcomes": outcomes, "input_tokens": profileStats(input), "output_tokens": profileStats(output), "duration_seconds": profileStats(durations), "observed_shared_prefix_bytes": profileStats(shared), "prefix_levels": levels, "quality": map[string]any{"missing_start": missingStart, "shared_prefix_observed": len(shared), "shared_prefix_missing": len(selected) - len(shared), "invalid_records": invalid, "process_dropped_events": p.dropped.Load(), "process_write_errors": p.writes.Load(), "queue_events": len(p.queue), "approximate_prefix": true, "crash_tail_may_be_missing": true}}, nil
}

type profileLimitedReader struct {
	file *os.File
	left int64
}

func (r *profileLimitedReader) Read(b []byte) (int, error) {
	if int64(len(b)) > r.left {
		b = b[:r.left]
	}
	if r.left == 0 {
		return 0, io.EOF
	}
	n, e := r.file.Read(b)
	r.left -= int64(n)
	return n, e
}
