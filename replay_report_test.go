package aigate

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fp(v float64) *float64 { return &v }
func up(v uint64) *uint64   { return &v }
func TestReplayReportDistributionsAndTimeline(t *testing.T) {
	dir := t.TempDir()
	rows := []replayResult{
		{TimingVersion: 1, Sequence: 1, SendOffset: fp(1), EndOffset: fp(5), FirstOutput: fp(2), LastOutput: fp(4), Stream: true, HTTPStatus: 200, ActualTokens: up(3), Observation: &responseObservation{ProtocolComplete: true}},
		{TimingVersion: 1, Sequence: 2, SendOffset: fp(2), EndOffset: fp(6), HTTPStatus: 200},
		{TimingVersion: 1, Sequence: 3, SendOffset: fp(3), EndOffset: fp(4), HTTPStatus: 500, Error: "http_500"},
	}
	var b strings.Builder
	for _, r := range rows {
		b.Write(encodeJSON(r))
		b.WriteByte('\n')
	}
	os.WriteFile(filepath.Join(dir, "results.jsonl"), []byte(b.String()), 0600)
	report := buildReplayReport(dir, replayRunView{State: "completed", Progress: replayProgress{Sent: 3}})
	if report.State != "complete" || report.Metrics.Success != 2 || report.Metrics.UsageKnown != 1 || report.Metrics.OutputTokens != 3 || report.Stream.TPOT.Count != 1 || report.Nonstream.TTFT.Count != 0 {
		t.Fatalf("%+v", report)
	}
	if report.Window != 5 || *report.RequestRate != .4 || *report.TokenRate != .6 || report.InflightPeak != 3 || math.Abs(report.InflightMean-1.8) > 1e-9 {
		t.Fatalf("%+v", report)
	}
	if *report.Stream.TPOT.P50 < 1 || *report.Stream.TPOT.P50 > 1.02 {
		t.Fatal(report.Stream.TPOT)
	}
	report = buildReplayReport(dir, replayRunView{State: "end_unknown", Progress: replayProgress{Sent: 4, Unconfirmed: 1}})
	if report.State != "partial" {
		t.Fatal(report.State)
	}
	os.WriteFile(filepath.Join(dir, "results.jsonl"), []byte("{\"sequence\":1}\n"), 0600)
	if got := buildReplayReport(dir, replayRunView{}); got.State != "unavailable" {
		t.Fatal(got)
	}
}
func TestReplayOutputObserverContentAndChunking(t *testing.T) {
	for _, split := range []int{1, 7, 10000} {
		o := newResponseObserver(http.Header{"Content-Type": []string{"text/event-stream"}})
		calls := 0
		o.onOutput = func(time.Time) { calls++ }
		events := []string{`{"choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}`, `{"choices":[{"index":0,"delta":{"reasoning_content":"thinking"}}]}`, `{"choices":[{"index":0,"delta":{"tool_calls":[{"function":{"name":"f","arguments":"{}"}}]}}]}`, `{"choices":[{"index":1,"delta":{"content":"answer"}}]}`, `{"usage":{"completion_tokens":20}}`}
		text := ""
		for _, event := range events {
			text += "data: " + event + "\n\n"
		}
		text += "data: [DONE]\n\n"
		for len(text) > 0 {
			n := min(split, len(text))
			o.Write([]byte(text[:n]))
			text = text[n:]
		}
		observed := o.finish("")
		if calls != 3 || !o.multipleChoices || !observed.ProtocolComplete || !observed.UsageKnown {
			t.Fatalf("split=%d calls=%d %+v", split, calls, observed)
		}
	}
}
func TestReplayReportProductionFlow(t *testing.T) {
	for _, mode := range []string{"timed", "concurrency"} {
		t.Run(mode, func(t *testing.T) {
			root, req := syntheticReplay(t, 2, .01, `{"stream":true}`)
			req.Mode = mode
			if mode == "concurrency" {
				req.Concurrency = 2
			}
			target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
				w.(http.Flusher).Flush()
				time.Sleep(15 * time.Millisecond)
				fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n")
				w.(http.Flusher).Flush()
				time.Sleep(15 * time.Millisecond)
				fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"two\"}}]}\n\ndata: {\"usage\":{\"completion_tokens\":3}}\n\ndata: [DONE]\n\n")
			})
			f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
			f.svc.start(req)
			f.wait(t, req.RunID)
			raw, ok := f.svc.loadReport(req.RunID)
			if !ok {
				t.Fatal("missing report")
			}
			data, _ := json.Marshal(raw)
			var r replayReport
			if err := json.Unmarshal(data, &r); err != nil {
				t.Fatal(err)
			}
			if r.State != "complete" || r.Stream.TTFT.Count != 2 || r.Stream.TPOT.Count != 2 || *r.Stream.TTFT.P50 < .01 {
				t.Fatalf("%s", data)
			}
			results := readReplayResults(t, f, req.RunID)
			for _, v := range results {
				if v.EndOffset == nil || v.FirstOutput == nil || *v.EndOffset < *v.LastOutput {
					t.Fatal(v)
				}
			}
			// Report write loss can be reconstructed without re-executing requests.
			os.Remove(filepath.Join(f.svc.runDir(req.RunID), "report.json"))
			raw, ok = f.svc.loadReport(req.RunID)
			if !ok {
				t.Fatal("no reconstruction")
			}
			if raw.(replayReport).Metrics.Success != 2 {
				t.Fatal(raw)
			}
		})
	}
}

func TestReplayMinuteBucketsAndMissingSamples(t *testing.T) {
	dir := t.TempDir()
	rows := []replayResult{
		{TimingVersion: 1, Sequence: 1, SendOffset: fp(0), EndOffset: fp(60), FirstOutput: fp(1), LastOutput: fp(59), HTTPStatus: 200, Stream: true, ActualTokens: up(3), Observation: &responseObservation{ProtocolComplete: true}},
		{TimingVersion: 1, Sequence: 2, SendOffset: fp(61), EndOffset: fp(121), HTTPStatus: 200},
	}
	var b strings.Builder
	for _, r := range rows {
		b.Write(encodeJSON(r))
		b.WriteByte('\n')
	}
	os.WriteFile(filepath.Join(dir, "results.jsonl"), []byte(b.String()), 0600)
	r := buildReplayReport(dir, replayRunView{State: "completed", Progress: replayProgress{Sent: 2}})
	if len(r.Curve) != 3 || r.Curve[0].Width != 60 || r.Curve[1].Width != 60 || r.Curve[2].Width != 1 {
		t.Fatal(r.Curve)
	}
	if r.Curve[0].Success != 1 || r.Curve[0].TTFT.Count != 1 || r.Curve[1].Success != 0 || r.Curve[1].TTFT.P50 != nil || r.Curve[2].E2ENonstream.Count != 1 {
		t.Fatal(r.Curve)
	}
	if r.Curve[0].OutputTokens != 3 || r.Curve[2].UsageKnown != 0 {
		t.Fatal(r.Curve)
	}
	if math.Abs(r.Curve[1].InflightMean-59.0/60) > 1e-9 {
		t.Fatal(r.Curve[1])
	}
}

func TestReplaySameReadDoesNotInventGenerationSpan(t *testing.T) {
	o := newResponseObserver(http.Header{"Content-Type": []string{"text/event-stream"}})
	var times []time.Time
	o.onOutput = func(at time.Time) { times = append(times, at) }
	o.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\ndata: [DONE]\n\n"))
	if len(times) != 2 || !times[0].Equal(times[1]) {
		t.Fatal(times)
	}
}

func TestReplayReportPreservesMissingResultCounts(t *testing.T) {
	for _, existingUnknown := range []int64{0, 2} {
		t.Run(fmt.Sprint(existingUnknown), func(t *testing.T) {
			dir := t.TempDir()
			row := replayResult{TimingVersion: 1, Sequence: 1, SendOffset: fp(0), EndOffset: fp(1), HTTPStatus: 200}
			if err := os.WriteFile(filepath.Join(dir, "results.jsonl"), append(encodeJSON(row), '\n'), 0600); err != nil {
				t.Fatal(err)
			}
			view := replayRunView{State: "failed", Progress: replayProgress{Planned: 4, Sent: 2, Completed: 2, Unconfirmed: existingUnknown}, Output: replayOutputSummary{ResultsFailed: true}}
			report := buildReplayReport(dir, view)
			if report.State != "partial" || report.Metrics.Sent != 1 || report.Metrics.Success != 1 || report.Progress.Sent != 2 || report.Progress.Unconfirmed != existingUnknown+1 {
				t.Fatalf("%+v", report)
			}
			if view.Progress.Unconfirmed != existingUnknown {
				t.Fatal("report changed execution progress")
			}
		})
	}
}
