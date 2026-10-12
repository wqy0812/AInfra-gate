package aigate

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIncidentQuestionBody(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"  plain  ", "  plain  "}, {"<USER_QUERY>x</USER_QUERY>", "<USER_QUERY>x</USER_QUERY>"},
		{"prefix<user_query> 中文🙂 </user_query>suffix", "中文🙂"},
		{"<user_query>first</user_query><user_query>last</user_query>", "last"},
		{"<user_query>first</user_query><user_query>unfinished", "first"},
		{"<user_query>outer<user_query>inner</user_query>end</user_query>", "outer<user_query>inner</user_query>end"},
		{"<user_query> \n\t </user_query>", ""}, {"<user_query>x", "<user_query>x"},
		{"<user_query attr='x'>x</user_query>", "<user_query attr='x'>x</user_query>"},
	} {
		if got := incidentQuestionText(tc.in); got != tc.want {
			t.Fatalf("%q: got %q want %q", tc.in, got, tc.want)
		}
	}
	body, err := parseChat([]byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"<user_"},{"type":"image_url","image_url":{"url":"image"}},{"type":"text","text":"query>问题🙂</user_query>"}]}],"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	before := body.question
	if incidentQuestionText(before) != "问题🙂" || body.question != before {
		t.Fatal("multimodal extraction changed adapter question")
	}
}

func TestCaptureBlockedDiskDoesNotBackpressureHTTP(t *testing.T) {
	for _, queueLimit := range []int{1024, 2} {
		t.Run(fmt.Sprintf("queue_%d", queueLimit), func(t *testing.T) {
			first := completionEvent(0, map[string]any{"content": "before"}, nil)
			last := completionEvent(0, map[string]any{"content": "after"}, "stop") + "data: [DONE]\n\n"
			upstreamGate, diskGate, entered := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var upstreamOnce, diskOnce, enteredOnce sync.Once
			releaseUpstream := func() { upstreamOnce.Do(func() { close(upstreamGate) }) }
			releaseDisk := func() { diskOnce.Do(func() { close(diskGate) }) }
			defer releaseUpstream()
			defer releaseDisk()
			u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, first)
				http.NewResponseController(w).Flush()
				select {
				case <-upstreamGate:
					io.WriteString(w, last)
				case <-r.Context().Done():
				}
			})
			p, server := splitProxyFixture(t, "blocked", u.URL, `{"profile":{"enabled":true}}`)
			p.g.captures.maxQueue = queueLimit
			p.g.captures.writePayload = func(f *os.File, data []byte) (int, error) {
				enteredOnce.Do(func() { close(entered) })
				<-diskGate
				return f.Write(data)
			}
			response, err := http.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user","content":"test"}],"stream":true}`))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			prefix := make([]byte, len(first))
			if _, err = io.ReadFull(response.Body, prefix); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("disk writer never blocked")
			}
			// Health/status remain callable while the capture worker is frozen.
			w := httptest.NewRecorder()
			p.ServeHTTP(w, httptest.NewRequest("GET", "/runtime/v1/status", nil))
			if w.Code != 200 {
				t.Fatal(w.Code)
			}
			releaseUpstream()
			done := make(chan []byte, 1)
			go func() { tail, _ := io.ReadAll(response.Body); done <- tail }()
			select {
			case tail := <-done:
				if string(prefix)+string(tail) != first+last {
					t.Fatal("stream bytes changed")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("business waited for capture writer")
			}
			if queueLimit == 2 && p.g.captures.dropped.Load() == 0 {
				t.Fatal("small queue did not exercise capture truncation")
			}
			releaseDisk()
			p.Close()
		})
	}
}

func TestCaptureSSEPositionsAcrossChunks(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n", "\r"} {
		text := strings.ReplaceAll(": heartbeat\n\n"+completionEvent(0, map[string]any{"content": "中🙂"}, nil)+"data: {\"usage\":{\"completion_tokens\":1},\"choices\":[]}\n\n"+completionEvent(0, map[string]any{"reasoning": "again"}, "stop")+"data: [DONE]\n\ndata: unfinished", "\n", newline)
		for _, step := range []int{1, 2, 7, len(text)} {
			o := newSSEObserver(false)
			at := time.Now()
			o.observedAt = at
			var events []sseEventPosition
			o.onEvent = func(p sseEventPosition) { events = append(events, p) }
			for start := 0; start < len(text); start += step {
				o.Feed([]byte(text[start:min(start+step, len(text))]))
			}
			if len(events) != 4 || !o.Done || o.positionOffset != int64(len(text)) {
				t.Fatalf("newline=%q step=%d events=%+v offset=%d", newline, step, events, o.positionOffset)
			}
			for i, p := range events {
				if p.Index != uint64(i+1) || p.Start < 0 || p.End > int64(len(text)) || p.End <= p.Start || !p.Known || !p.At.Equal(at) || !strings.Contains(text[p.Start:p.End], "data:") {
					t.Fatalf("invalid position %+v", p)
				}
			}
			if o.LastUsefulEvent.Index != 3 || events[1].Useful || events[3].Useful {
				t.Fatal("heartbeat/usage/done advanced useful output")
			}
		}
	}
}

// This uses real HTTP and forces only the observer's elapsed clock. No model or
// 60-second wall-clock sleep is involved. Both resumptions arrive in one Read.
func TestCaptureHTTPStallPreservesOriginalBytes(t *testing.T) {
	first := completionEvent(0, map[string]any{"content": "before中"}, nil)
	rest := ": heartbeat\r\n\r\n" + "data: {\"usage\":{\"completion_tokens\":1},\"choices\":[]}\n\n" + completionEvent(0, map[string]any{"reasoning": "first recovery"}, nil) + completionEvent(0, map[string]any{"content": "last recovery🙂"}, "stop") + "data: [DONE]\n\n"
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	u := upstreamServer(t, "real", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, first)
		http.NewResponseController(w).Flush()
		select {
		case <-gate:
			io.WriteString(w, rest)
		case <-r.Context().Done():
		}
	})
	p, server := splitProxyFixture(t, "capture", u.URL, `{"profile":{"enabled":true}}`)
	raw := []byte(`{ "messages": [{"role":"user","content":"wrapper<user_query>问题正文🙂</user_query>suffix"}], "stream": true, "tools": [] }`)
	response, err := http.Post(server.URL+"/v1/chat/completions", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	prefix := make([]byte, len(first))
	if _, err = io.ReadFull(reader, prefix); err != nil {
		t.Fatal(err)
	}
	var id string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.g.incidents.mu.Lock()
		for request := range p.g.incidents.active {
			if request.reading && !request.lastUseful.IsZero() {
				request.readStart = time.Now().Add(-61 * time.Second)
				request.lastUseful = time.Now().Add(-62 * time.Second)
				request.check(time.Now())
				id = request.base.RequestID
			}
		}
		p.g.incidents.mu.Unlock()
		if id != "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if id == "" {
		t.Fatal("forwarder never waited for next upstream read")
	}
	release()
	tail, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(prefix)+string(tail) != first+rest {
		t.Fatal("forwarded bytes changed")
	}
	p.Close()
	dir := filepath.Join(p.dataRoot, "stream-incidents", "requests", "request-"+id)
	for name, want := range map[string][]byte{"request.json": raw, "response.sse": []byte(first + rest)} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s differs: %v", name, err)
		}
	}
	timeline, err := os.ReadFile(filepath.Join(dir, "timeline.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(timeline, []byte("output_resumed")) || !bytes.Contains(timeline, []byte("incident_trigger")) {
		t.Fatalf("missing stall or recovery: %s", timeline)
	}
	if output := os.Getenv("AIGATE_CAPTURE_EXAMPLE_DIR"); output != "" {
		if err := os.MkdirAll(output, 0700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"request.json", "response.sse", "timeline.jsonl", "meta.json"} {
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(output, name), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("synthetic incident evidence: %s", output)
	}
}
