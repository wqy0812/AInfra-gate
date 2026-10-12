package aigate

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type diagnosticBenchBody struct{ remaining int }

func (r *diagnosticBenchBody) Read(p []byte) (int, error) {
	if r.remaining < 0 {
		return 0, io.EOF
	}
	frame := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n"
	if r.remaining == 0 {
		frame = "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
	}
	r.remaining--
	return copy(p, frame), nil
}
func (*diagnosticBenchBody) Close() error { return nil }

// Deliberately excludes network/model latency to expose forwarding/observation
// cost. This same benchmark can run unmodified on the pre-change revision.
func BenchmarkGatewayDiagnosticStream(b *testing.B) {
	c := fixtureConfig(b, "http://backend.invalid")
	g, err := newGateway(c, io.Discard)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	g.client.Transport = generationTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "GET" {
			return generationResponse(200, "application/json", `{"data":[{"id":"real"}]}`), nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: &diagnosticBenchBody{remaining: 64}}, nil
	})
	body := []byte(`{"messages":[{"role":"user","content":"test"}],"stream":true}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(body))
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != 200 {
			b.Fatal(w.Code)
		}
	}
}
