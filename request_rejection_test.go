package aigate

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"
)

func TestRejectedUploadReturnsAndClosesConnection(t *testing.T) {
	for _, state := range []struct {
		name, method, path, token string
		status                    int
	}{
		{"unconfigured", "POST", "/v1/chat/completions", "", 503},
		{"paused", "POST", "/v1/chat/completions", "", 503},
		{"expect_continue", "POST", "/v1/chat/completions", "", 503},
		{"retired", "POST", "/v1/chat/completions", "", 503},
		{"invalid_token", "POST", "/v1/chat/completions", "wrong", 409},
		{"wrong_method", "PUT", "/v1/chat/completions", "", 405},
		{"not_found", "POST", "/unknown", "", 404},
		{"health", "GET", "/health", "", 200},
		{"openapi", "GET", "/openapi.json", "", 200},
		{"models", "GET", "/v1/models", "", 200},
	} {
		for _, chunked := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/chunked=%v", state.name, chunked), func(t *testing.T) {
				c := controlledFixture(t)
				if state.name != "unconfigured" {
					upstream := newProtocolBackend(t)
					activateV3(t, c, "bind", switchSnapshot(t, "r1", upstream.server.URL, 1))
				}
				switch state.name {
				case "paused", "expect_continue":
					commandV3(t, c, "pause", "quiesce", map[string]any{})
				case "retired":
					commandV3(t, c, "retire", "retire", map[string]any{})
				}
				closed := make(chan struct{}, 1)
				server := httptest.NewUnstartedServer(c)
				server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
					if state == http.StateClosed {
						closed <- struct{}{}
					}
				}
				server.Start()
				defer server.Close()
				conn, err := net.Dial("tcp", server.Listener.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(3 * time.Second))
				framing, body := "Content-Length: 1000\r\n", "{"
				if chunked {
					framing, body = "Transfer-Encoding: chunked\r\n", "1\r\n{\r\n"
				}
				if state.name == "expect_continue" {
					framing += "Expect: 100-continue\r\n"
					body = ""
				}
				fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: local\r\nX-Code-Eval-Token: %s\r\n%s\r\n%s", state.method, state.path, state.token, framing, body)
				reader := bufio.NewReader(conn)
				response, err := http.ReadResponse(reader, nil)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || response.StatusCode != state.status || len(data) == 0 || !response.Close {
					t.Fatalf("status=%d close=%v body=%q err=%v", response.StatusCode, response.Close, data, err)
				}
				// Do not close the client's write side: finishRequest must not wait
				// for the rest of this upload after the response has been flushed.
				select {
				case <-closed:
				case <-time.After(2 * time.Second):
					t.Fatal("server retained a rejected upload after sending its response")
				}
				if _, err := reader.ReadByte(); err != io.EOF {
					t.Fatalf("connection did not finish cleanly: %v", err)
				}
			})
		}
	}
}

func TestRejectedHTTP2UploadKeepsConnection(t *testing.T) {
	c := controlledFixture(t)
	server := httptest.NewUnstartedServer(c)
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	client := server.Client()
	client.Timeout = 3 * time.Second
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	req, err := http.NewRequest("POST", server.URL+"/v1/chat/completions", reader)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.WriteString(writer, "{") }()
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.ProtoMajor != 2 || response.StatusCode != 503 || !strings.Contains(string(data), "gateway_not_accepting") {
		t.Fatalf("unexpected HTTP/2 rejection: %+v body=%q err=%v", response, data, err)
	}
	var reused bool
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
	})
	req, _ = http.NewRequestWithContext(ctx, "GET", server.URL+"/health", nil)
	response, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.ProtoMajor != 2 || !reused {
		t.Fatalf("HTTP/2 rejection closed the shared connection: status=%d protocol=%s reused=%v", response.StatusCode, response.Proto, reused)
	}
}
