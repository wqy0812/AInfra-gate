package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Deterministic tracker tests use a queue-only store, no wall-clock sleeps.
func (r *incidentRequest) endRead(now time.Time, n int, o *sseObserver) {
	r.readReturned(now, n)
	r.observe(now, o)
}

func incidentFixture() (*incidentStore, *incidentRequest, time.Time) {
	s := &incidentStore{active: map[*incidentRequest]bool{}, queue: make(chan streamIncident, 100), gatewayID: "g1", boot: "boot"}
	now := time.Now().Add(-10 * time.Minute)
	base := streamIncident{RequestID: "request", Router: "r1", Model: "model", BindingRevision: 7, RequestStartedAt: now}
	base.QuestionHead, base.QuestionTail, base.QuestionLength = questionEdges("前十个字符用于寻找中间内容不得保存后十个字符作为尾部")
	r := s.begin(base)
	return s, r, now
}
func takeIncident(t *testing.T, s *incidentStore) streamIncident {
	t.Helper()
	select {
	case v := <-s.queue:
		return v
	default:
		t.Fatal("missing incident")
		return streamIncident{}
	}
}
func noIncident(t *testing.T, s *incidentStore) {
	t.Helper()
	if len(s.queue) != 0 {
		t.Fatalf("unexpected %d events", len(s.queue))
	}
}
func TestIncidentQuestionEdges(t *testing.T) {
	for _, text := range []string{"", "短问题🙂", "0123456789", "01234567890", "一二三四五六七八九十中🙂后半段测试文本内容", "0123456789SECRET-MIDDLEabcdefghij"} {
		h, tail, n := questionEdges(text)
		r := []rune(text)
		if n != len(r) || h != string(r[:min(10, len(r))]) || tail != string(r[max(0, len(r)-10):]) {
			t.Fatalf("bad edges %q %q %q", text, h, tail)
		}
	}
}
func TestIncidentOnlyMidstreamAndReadTime(t *testing.T) {
	s, r, start := incidentFixture()
	o := newSSEObserver(false)
	r.startRead(start)
	s.tick(start.Add(2 * time.Minute))
	noIncident(t, s)
	o.Useful = true
	o.LastUsefulAt = start.Add(2 * time.Minute)
	r.endRead(o.LastUsefulAt, 10, o)
	base := o.LastUsefulAt
	r.startRead(base)
	r.endRead(base.Add(30*time.Second), 5, o) // heartbeat
	r.startWrite(base.Add(30*time.Second), 5)
	r.startFlush(base.Add(31*time.Second), 5)
	r.endWrite(base.Add(80*time.Second), nil)
	r.startRead(base.Add(80 * time.Second))
	s.tick(base.Add(110 * time.Second))
	noIncident(t, s) // exactly 60s Read wait
	s.tick(base.Add(110*time.Second + time.Nanosecond))
	v := takeIncident(t, s)
	if v.Kind != "output_stall" || v.ReadWaitSeconds <= 60 || v.OutputGapSeconds < 110 || v.ReceivedBytes != 5 {
		t.Fatalf("bad stall %+v", v)
	}
	s.tick(base.Add(115 * time.Second))
	noIncident(t, s)
	o.LastUsefulAt = base.Add(120 * time.Second)
	r.endRead(o.LastUsefulAt, 20, o)
	v = takeIncident(t, s)
	if v.EndReason != "output_resumed" || v.State != "ended" || v.DurationSeconds != 70 {
		t.Fatalf("bad recovery %+v", v)
	}
	r.startRead(base.Add(120 * time.Second))
	r.endRead(base.Add(181*time.Second), 0, o)
	if takeIncident(t, s).Kind != "output_stall" {
		t.Fatal("missing second stall")
	}
	r.finish(base.Add(182*time.Second), "client_cancelled")
	if takeIncident(t, s).EndReason != "client_cancelled" {
		t.Fatal("bad cancellation")
	}
	s.tick(base.Add(300 * time.Second))
	noIncident(t, s)
}
func TestIncidentWriteBlockAndUnknown(t *testing.T) {
	s, r, start := incidentFixture()
	o := newSSEObserver(false)
	o.Useful = true
	o.LastUsefulAt = start
	r.startRead(start)
	r.endRead(start, 2, o)
	r.startWrite(start, 8192)
	s.tick(start.Add(60 * time.Second))
	noIncident(t, s)
	s.tick(start.Add(61 * time.Second))
	v := takeIncident(t, s)
	if v.Kind != "client_write_stall" || v.Operation != "write" || v.WrittenBytes != nil {
		t.Fatalf("bad write %+v", v)
	}
	r.startFlush(start.Add(62*time.Second), 8192)
	s.tick(start.Add(65 * time.Second))
	noIncident(t, s)
	r.endWrite(start.Add(66*time.Second), nil)
	v = takeIncident(t, s)
	if v.WriteSeconds != 62 || v.FlushSeconds != 4 || *v.WrittenBytes != 8192 {
		t.Fatalf("bad phases %+v", v)
	}
	r.startRead(start.Add(66 * time.Second))
	o.Unknown = true
	r.endRead(start.Add(127*time.Second), 1, o)
	takeIncident(t, s)
	v = takeIncident(t, s)
	if v.Observation != "unknown" || v.EndReason != "observation_unknown" {
		t.Fatal(v)
	}
	r.startRead(start.Add(128 * time.Second))
	s.tick(start.Add(300 * time.Second))
	noIncident(t, s)
	r.startWrite(start.Add(301*time.Second), 1)
	r.wrote(0)
	r.endWrite(start.Add(362*time.Second), io.ErrClosedPipe)
	takeIncident(t, s)
	v = takeIncident(t, s)
	if v.EndReason != "write_error" || *v.WrittenBytes != 0 {
		t.Fatal(v)
	}
}
func TestIncidentCompletionStopsStallButNotFinalWrite(t *testing.T) {
	s, r, start := incidentFixture()
	o := newSSEObserver(false)
	o.Useful = true
	o.LastUsefulAt = start
	o.HasError = true
	r.startRead(start)
	r.endRead(start, 10, o)
	r.startRead(start)
	s.tick(start.Add(120 * time.Second))
	noIncident(t, s)
	r.startWrite(start.Add(120*time.Second), 100)
	r.startFlush(start.Add(120*time.Second), 100)
	r.endWrite(start.Add(181*time.Second), nil)
	if takeIncident(t, s).Kind != "client_write_stall" {
		t.Fatal("no final write incident")
	}
	takeIncident(t, s)
}
func incidentQuery(t *testing.T, s *incidentStore, values url.Values) (int, map[string]json.RawMessage) {
	t.Helper()
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest("GET", "/profile/v1/stream-incidents?"+values.Encode(), nil))
	var data map[string]json.RawMessage
	if w.Code == 200 {
		if err := json.Unmarshal(w.Body.Bytes(), &data); err != nil {
			t.Fatal(err)
		}
	}
	return w.Code, data
}
func TestIncidentPersistenceSearchPaginationAndRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "incidents")
	s, err := newIncidentStore(root, "g1", "boot1", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Minute)
	var first streamIncident
	for i := 0; i < 4; i++ {
		v := streamIncident{ID: fmt.Sprintf("id%d", i), RequestID: "req", BootID: "boot1", Kind: "output_stall", TriggeredAt: now.Add(time.Duration(i) * time.Second), QuestionHead: "0123456789", QuestionTail: "abcdefghij", QuestionLength: 30, State: "ongoing"}
		if i == 0 {
			first = v
		}
		if err = s.save(v); err != nil {
			t.Fatal(err)
		}
	}
	s.close()
	s, err = newIncidentStore(root, "g1", "boot2", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	q := url.Values{"start": {fmt.Sprint(now.Add(-time.Second).Unix())}, "end": {fmt.Sprint(now.Add(4 * time.Second).Unix())}, "limit": {"2"}, "keyword": {"ABC"}}
	status, data := incidentQuery(t, s, q)
	if status != 200 {
		t.Fatal(status)
	}
	var rows []streamIncident
	json.Unmarshal(data["records"], &rows)
	if len(rows) != 2 || rows[0].ID != "id3" || rows[0].State != "unknown" {
		t.Fatal(rows)
	}
	var cursor string
	json.Unmarshal(data["next_cursor"], &cursor)
	if cursor == "" {
		t.Fatal("no cursor")
	}
	q.Set("cursor", cursor)
	_, data = incidentQuery(t, s, q)
	json.Unmarshal(data["records"], &rows)
	if len(rows) != 2 || rows[1].ID != "id0" {
		t.Fatal(rows)
	}
	q.Set("keyword", "changed")
	status, _ = incidentQuery(t, s, q)
	if status != 400 {
		t.Fatal("cursor accepted for other query")
	}
	q.Del("cursor")
	for _, keyword := range []string{"SECRET", "9a"} {
		q.Set("keyword", keyword)
		_, data = incidentQuery(t, s, q)
		json.Unmarshal(data["records"], &rows)
		if len(rows) != 0 {
			t.Fatal("searched absent middle or across edges")
		}
	}
	q.Set("keyword", "")
	q.Set("start", fmt.Sprint(first.TriggeredAt.Unix()))
	q.Set("end", fmt.Sprint(first.TriggeredAt.Add(time.Second).Unix()))
	_, data = incidentQuery(t, s, q)
	json.Unmarshal(data["records"], &rows)
	if len(rows) != 1 || rows[0].ID != "id0" {
		t.Fatal("time boundary", rows)
	}
	if strings.Contains(string(mustRead(t, s.recordPath(first))), "SECRET") {
		t.Fatal("full input retained")
	}
	s.maxDisk = 1
	if err = s.cleanup(time.Now()); err != nil {
		t.Fatal(err)
	}
	_, data = incidentQuery(t, s, q)
	json.Unmarshal(data["records"], &rows)
	if len(rows) != 0 {
		t.Fatal("retention failed")
	}
}
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestIncidentQueueAndDiskFailures(t *testing.T) {
	s, r, start := incidentFixture()
	s.queue = make(chan streamIncident, 1)
	s.queue <- streamIncident{}
	r.startWrite(start, 1)
	s.tick(start.Add(61 * time.Second))
	if s.dropped.Load() != 1 {
		t.Fatal("missing dropped count")
	}
	r.endWrite(start.Add(62*time.Second), nil)
	if s.dropped.Load() != 2 {
		t.Fatal("completion must not block")
	}
	store, err := newIncidentStore(filepath.Join(t.TempDir(), "incidents"), "g", "b", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err = os.WriteFile(filepath.Join(store.root, now.UTC().Format("2006010215")), []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.enqueue(streamIncident{ID: "bad", TriggeredAt: now})
	store.mu.Unlock()
	store.close()
	if store.failures.Load() != 1 {
		t.Fatal("disk failure not counted")
	}
}
func TestIncidentLiveQueryableBeforeWriter(t *testing.T) {
	s, r, start := incidentFixture()
	s.root = t.TempDir()
	s.retention = incidentRetention
	s.querySlots = make(chan struct{}, 2)
	r.startWrite(start, 1)
	s.tick(start.Add(61 * time.Second))
	q := url.Values{"start": {fmt.Sprint(start.Unix())}, "end": {fmt.Sprint(time.Now().Unix())}}
	status, data := incidentQuery(t, s, q)
	if status != 200 {
		t.Fatal(status)
	}
	var rows []streamIncident
	json.Unmarshal(data["records"], &rows)
	if len(rows) != 1 || rows[0].State != "ongoing" || rows[0].DurationSeconds < 61 {
		t.Fatal(rows)
	}
}
func TestIncidentHTTPValidationAndConcurrency(t *testing.T) {
	s, err := newIncidentStore(filepath.Join(t.TempDir(), "incidents"), "g", "b", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	for _, q := range []string{"start=NaN&end=10", "start=1&end=Inf", "start=1&end=2&limit=0", "start=1&end=2&cursor=bad"} {
		w := httptest.NewRecorder()
		s.serveHTTP(w, httptest.NewRequest("GET", "/?"+q, nil))
		if w.Code != 400 {
			t.Fatal(q, w.Code)
		}
	}
	s.querySlots <- struct{}{}
	s.querySlots <- struct{}{}
	defer func() { <-s.querySlots; <-s.querySlots }()
	status, _ := incidentQuery(t, s, url.Values{"start": {"1"}, "end": {"2"}})
	if status != 429 {
		t.Fatal(status)
	}
}

func TestIncidentExpiredQueryCannotReturnSuccess(t *testing.T) {
	s, err := newIncidentStore(filepath.Join(t.TempDir(), "incidents"), "g", "b", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/?start=1&end=2", nil).WithContext(ctx)
	s.serveHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expired query returned %d: %s", w.Code, w.Body.String())
	}
	if len(s.querySlots) != 0 {
		t.Fatal("expired query leaked its slot")
	}
}

// The real forwarding loop must preserve bytes while registering/cleaning the tracker.
func TestStreamIncidentForwardingIntegration(t *testing.T) {
	s, e := newIncidentStore(filepath.Join(t.TempDir(), "incidents"), "g", "b", io.Discard)
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	payload := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
	g := &Gateway{incidents: s}
	g.buffers.New = func() any { b := make([]byte, 32768); return &b }
	entry := &auditEntry{RequestID: "id", started: time.Now(), generation: &generationResult{stream: true, observer: newSSEObserver(false)}}
	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	outcome := g.stream(w, r, &http.Response{Body: io.NopCloser(bytes.NewBufferString(payload))}, r.Context(), entry)
	if outcome != "success" || w.Body.String() != payload {
		t.Fatal(outcome, w.Body.String())
	}
	s.mu.Lock()
	n := len(s.active)
	s.mu.Unlock()
	if n != 0 {
		t.Fatal("leaked tracker")
	}
}

// Block a real downstream Write. The watchdog must capture it before Write returns.
type incidentBlockedWriter struct {
	*httptest.ResponseRecorder
	entered, release chan struct{}
}

func (w *incidentBlockedWriter) Write(b []byte) (int, error) {
	close(w.entered)
	<-w.release
	return w.ResponseRecorder.Write(b)
}
func TestIncidentWatchdogDuringBlockedWrite(t *testing.T) {
	s, _, _ := incidentFixture()
	s.active = map[*incidentRequest]bool{}
	g := &Gateway{incidents: s}
	g.buffers.New = func() any { b := make([]byte, 32768); return &b }
	payload := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
	w := &incidentBlockedWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(w.release)
		}
	}()
	entry := &auditEntry{RequestID: "id", started: time.Now(), generation: &generationResult{stream: true, observer: newSSEObserver(false)}}
	req := httptest.NewRequest("POST", "/", nil)
	done := make(chan string, 1)
	go func() {
		done <- g.stream(w, req, &http.Response{Body: io.NopCloser(strings.NewReader(payload))}, req.Context(), entry)
	}()
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("write never started")
	}
	now := time.Now()
	s.mu.Lock()
	for r := range s.active {
		r.writeStart = now.Add(-61 * time.Second)
	}
	s.mu.Unlock()
	s.tick(now)
	v := takeIncident(t, s)
	if v.Kind != "client_write_stall" || v.State != "ongoing" {
		t.Fatal(v)
	}
	select {
	case <-done:
		t.Fatal("write unexpectedly returned")
	default:
	}
	// Release explicitly, then prevent the deferred close from closing twice.
	close(w.release)
	released = true
	select {
	case result := <-done:
		if result != "success" {
			t.Fatal(result)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not finish")
	}
	if takeIncident(t, s).EndReason != "write_resumed" {
		t.Fatal("missing recovery")
	}
}

func TestIncidentRuntimeAuthAndRouteSwap(t *testing.T) {
	c := controlledFixture(t)
	snapshot := snapshotV2("r1", "http://127.0.0.1:19099", 1)
	snapshot.Settings = json.RawMessage(`{"profile":{"enabled":true,"api_key":"incident-key"}}`)
	activateV2(t, c, "first", snapshot)
	store := c.incidents
	if store == nil || c.active.g.incidents != store {
		t.Fatal("not enabled with profile")
	}
	now := time.Now().Add(-time.Minute)
	if e := store.save(streamIncident{ID: "before-swap", Router: "r1", TriggeredAt: now, State: "ended"}); e != nil {
		t.Fatal(e)
	}
	snapshot.RouterID = "r2"
	snapshot.BindingRevision = 2
	snapshot.Settings = json.RawMessage(`{"profile":{"enabled":true,"api_key":"new-key"}}`)
	activateV2(t, c, "second", snapshot)
	if c.incidents != store || c.active.g.incidents != store {
		t.Fatal("store changed on swap")
	}
	query := "/profile/v1/stream-incidents?start=" + fmt.Sprint(now.Add(-time.Second).Unix()) + "&end=" + fmt.Sprint(time.Now().Unix())
	for _, key := range []string{"", "incident-key", "new-key"} {
		req := httptest.NewRequest("GET", query, nil)
		req.Header.Set("Authorization", "Bearer "+key)
		w := httptest.NewRecorder()
		c.ProfileHandler().ServeHTTP(w, req)
		if key == "new-key" {
			if w.Code != 200 || !strings.Contains(w.Body.String(), "before-swap") {
				t.Fatal(w.Code, w.Body.String())
			}
		} else if w.Code != 401 {
			t.Fatal("auth bypass", w.Code)
		}
	}
	c.incidents.dropped.Add(2)
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Authorization", "Bearer new-key")
	w := httptest.NewRecorder()
	c.ProfileHandler().ServeHTTP(w, req)
	if !strings.Contains(w.Body.String(), "aigate_stream_incident_dropped_total 2") {
		t.Fatal("missing diagnostics metrics")
	}
}

func TestIncidentTextContextIndependentOfAudit(t *testing.T) {
	s, _, _ := incidentFixture()
	s.active = map[*incidentRequest]bool{}
	upstream := upstreamServer(t, "actual-model", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	})
	logs := &lockedBuffer{}
	g, err := newGateway(fixtureConfig(t, upstream.URL), logs)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.incidents = s
	w := &incidentBlockedWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(w.release)
		}
	}()
	body := `{"stream":true,"messages":[{"role":"user","content":"not-the-last-question"},{"role":"assistant","content":"not-input"},{"role":"user","content":[{"type":"text","text":"0123456789"},{"type":"image_url","image_url":{"url":"DO-NOT-SAVE"}},{"type":"text","text":"SECRET-MIDDLEabcdefghij"}]},{"role":"assistant","content":"later assistant"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	done := make(chan struct{})
	go func() { g.ServeHTTP(w, req); close(done) }()
	select {
	case <-w.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("stream never started")
	}
	now := time.Now()
	s.mu.Lock()
	for r := range s.active {
		r.writeStart = now.Add(-61 * time.Second)
	}
	s.mu.Unlock()
	s.tick(now)
	v := takeIncident(t, s)
	if v.QuestionHead != "0123456789" || v.QuestionTail != "abcdefghij" || v.QuestionLength != 33 || v.Model != "actual-model" {
		t.Fatalf("bad input context %+v", v)
	}
	raw := string(encodeJSON(v))
	for _, secret := range []string{"SECRET-MIDDLE", "DO-NOT-SAVE", "not-the-last-question"} {
		if strings.Contains(raw, secret) {
			t.Fatal("unexpected content", secret)
		}
	}
	close(w.release)
	released = true
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request stuck")
	}
	for _, content := range []string{"question_prefix", "0123456789", "abcdefghij", "SECRET-MIDDLE"} {
		if strings.Contains(logs.String(), content) {
			t.Fatal("incident text leaked to audit", content)
		}
	}
}

func TestIncidentParsingTimeIsNotReadWait(t *testing.T) {
	s, r, start := incidentFixture()
	o := newSSEObserver(false)
	o.Useful = true
	o.LastUsefulAt = start
	r.endRead(start, 10, o)
	r.startRead(start)
	r.readReturned(start.Add(59*time.Second), 1)
	s.tick(start.Add(90 * time.Second))
	noIncident(t, s) // delayed parser, not blocked Read
	r.observe(start.Add(90*time.Second), o)
	r.startRead(start.Add(90 * time.Second))
	r.readReturned(start.Add(92*time.Second), 1)
	if takeIncident(t, s).ReadWaitSeconds != 61 {
		t.Fatal("parsing counted as Read")
	}
}
