package aigate

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReplayIndependentRecordingAllowances(t *testing.T) {
	for _, count := range []int{2, 3, 4} {
		t.Run(fmt.Sprint(count), func(t *testing.T) { testRecordingAllowances(t, count) })
	}
}

func testRecordingAllowances(t *testing.T, count int) {
	root, req := recordingsFixture(t, 24, count)
	req.Mode, req.Concurrency = "concurrency", 8
	active, peak, sent := make([]atomic.Int64, count), make([]atomic.Int64, count), make([]atomic.Int64, count)
	gates, release := make([]chan struct{}, count), make([]sync.Once, count)
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	defer func() {
		for i := range gates {
			release[i].Do(func() { close(gates[i]) })
		}
	}()
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
		seq, _ := strconv.Atoi(strings.Split(r.Header.Get(headerReplay), "/")[1])
		i := (seq - 1) % count // Equal offsets interleave all recorded sources.
		n := active[i].Add(1)
		defer active[i].Add(-1)
		for old := peak[i].Load(); n > old && !peak[i].CompareAndSwap(old, n); old = peak[i].Load() {
		}
		sent[i].Add(1)
		select {
		case <-gates[i]:
		case <-r.Context().Done():
		}
		io.WriteString(w, `{}`)
	})
	f := newReplayFixture(t, root, map[string]string{"r1": target.URL})
	if _, status, reason := f.svc.start(req); status != 200 {
		t.Fatal(status, reason)
	}
	deadline := time.Now().Add(5 * time.Second)
	for i := range gates {
		for active[i].Load() != 8 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if active[i].Load() != 8 {
			t.Fatalf("source %d allowance not reached: %d", i, active[i].Load())
		}
	}
	for i := 1; i < count; i++ {
		release[i].Do(func() { close(gates[i]) })
		for sent[i].Load() != 24 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if sent[i].Load() != 24 || sent[0].Load() != 8 {
			t.Fatalf("source %d blocked or slow source borrowed its allowance: %d/%d", i, sent[i].Load(), sent[0].Load())
		}
	}
	release[0].Do(func() { close(gates[0]) })
	view := f.wait(t, req.RunID)
	if view.State != "completed" || view.Progress.Sent != int64(24*count) || len(view.Sources) != count {
		t.Fatal(view)
	}
	for i := range peak {
		if peak[i].Load() > 8 {
			t.Fatal("source exceeded allowance", i, peak[i].Load())
		}
	}
	report := buildReplayReport(f.svc.runDir(req.RunID), view)
	if len(report.SourceInputs) != count {
		t.Fatal(report.SourceInputs)
	}
	for _, source := range req.Recordings {
		one := req
		one.Recordings, one.Recording = nil, source
		plan, err := buildReplayPlan(root, one, savedTestInputs(root))
		if err != nil {
			t.Fatal(err)
		}
		plan.FilterPolicy = "stream_only_v1"
		dir := t.TempDir()
		if err := writeReplayPlan(dir, plan); err != nil {
			t.Fatal(err)
		}
		digest, _, _, _ := replayReportInput(dir, replayRunView{Plan: plan})
		for _, input := range report.SourceInputs {
			if input.GatewayID == source.GatewayID && input.RecordingID == source.RecordingID && input.InputDigest != digest {
				t.Fatal("source digest depends on sender grouping", input, digest)
			}
		}
	}
}
