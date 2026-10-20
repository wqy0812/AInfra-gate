package aigate

import (
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
	root, req := overlayFixtureCount(t, 24)
	req.Mode, req.Concurrency = "concurrency", 8
	var active, peak, sent [2]atomic.Int64
	gates := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var release [2]sync.Once
	defer func() {
		for i := range gates {
			release[i].Do(func() { close(gates[i]) })
		}
	}()
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) {
		seq, _ := strconv.Atoi(strings.Split(r.Header.Get(headerReplay), "/")[1])
		i := (seq - 1) % 2 // Equal offsets interleave the two recorded sources.
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
	for (active[0].Load() != 8 || active[1].Load() != 8) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active[0].Load() != 8 || active[1].Load() != 8 {
		t.Fatalf("source allowances not reached: %d/%d", active[0].Load(), active[1].Load())
	}
	release[1].Do(func() { close(gates[1]) })
	for sent[1].Load() != 24 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if sent[1].Load() != 24 || sent[0].Load() != 8 {
		t.Fatalf("slow source blocked fast source or borrowed its allowance: %d/%d", sent[0].Load(), sent[1].Load())
	}
	release[0].Do(func() { close(gates[0]) })
	view := f.wait(t, req.RunID)
	if view.State != "completed" || view.Progress.Sent != 48 || peak[0].Load() > 8 || peak[1].Load() > 8 {
		t.Fatal(view, peak[0].Load(), peak[1].Load())
	}
	report := buildReplayReport(f.svc.runDir(req.RunID), view)
	if len(report.SourceInputs) != 2 {
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
			if input.GatewayID == source.GatewayID && input.InputDigest != digest {
				t.Fatal("source digest depends on sender grouping", input, digest)
			}
		}
	}
}
