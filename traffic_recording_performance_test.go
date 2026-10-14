//go:build darwin || linux

package aigate

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Opt-in comparison of switch SSE forwarding with recording off and on, over
// real loopback HTTP (switch → proxy → synthetic Router) and real files. The
// Router emits a role chunk, waits 20 ms, then 64 one-token chunks.
//
//	AIGATE_RECORDING_PERFORMANCE=1 GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local \
//	  go test -mod=vendor -run '^TestRecordingPerformance$' -count=1 -v .
func TestRecordingPerformance(t *testing.T) {
	if os.Getenv("AIGATE_RECORDING_PERFORMANCE") != "1" {
		t.Skip("set AIGATE_RECORDING_PERFORMANCE=1")
	}
	const concurrency, perWorker = 32, 20
	body := `{"messages":[{"role":"user","content":"` + strings.Repeat("p", 8192) + `"}],"stream":true,"max_tokens":64}`
	for _, enabled := range []bool{false, true, false, true} {
		f := newRecordingFixture(t, recordingDefaultMaxBytes)
		f.router.delay = 20 * time.Millisecond
		if enabled {
			now := time.Now()
			startRecording(t, f.c, "perf", halfHour(now), halfHour(now).Add(time.Hour), nil)
		}
		var mu sync.Mutex
		var first []float64
		var bytes int64
		start := time.Now()
		var wg sync.WaitGroup
		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perWorker; i++ {
					sent := time.Now()
					response, err := http.Post(f.switch_.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
					if err != nil {
						t.Error(err)
						return
					}
					reader := bufio.NewReader(response.Body)
					var firstContent float64
					var n int64
					for {
						line, err := reader.ReadString('\n')
						n += int64(len(line))
						if firstContent == 0 && strings.Contains(line, `"content":"x"`) {
							firstContent = time.Since(sent).Seconds()
						}
						if err == io.EOF {
							break
						}
						if err != nil {
							t.Error(err)
							break
						}
					}
					response.Body.Close()
					mu.Lock()
					first = append(first, firstContent)
					bytes += n
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start).Seconds()
		sort.Float64s(first)
		percentile := func(p float64) float64 { return first[int(p*float64(len(first)-1))] * 1000 }
		recorded := uint64(0)
		if enabled {
			recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": "perf"})
			recorded = waitRecordingEnded(t, f.c, "perf").Recorded
		}
		fmt.Printf("recording=%v requests=%d req/s=%.1f bytes/s=%.0f first_content_p50_ms=%.2f p95_ms=%.2f p99_ms=%.2f recorded=%d\n",
			enabled, len(first), float64(len(first))/elapsed, float64(bytes)/elapsed, percentile(0.5), percentile(0.95), percentile(0.99), recorded)
		f.switch_.Close()
	}
}
