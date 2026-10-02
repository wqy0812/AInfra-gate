package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRequestBodySizeLimit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		size   int64
		known  bool
		status int
	}{
		{"exact_limit", maxRequestBodyBytes, true, 0},
		{"declared_oversize", maxRequestBodyBytes + 1, true, 413},
		{"unknown_length_oversize", maxRequestBodyBytes + 1, false, 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(strings.Repeat(" ", int(tc.size))))
			if !tc.known {
				r.ContentLength = -1
			}
			data, err := readChatBody(httptest.NewRecorder(), r, requestBodyTimeout)
			if tc.status == 0 {
				if err != nil || int64(len(data)) != tc.size {
					t.Fatalf("unexpected result: length=%d, err=%v", len(data), err)
				}
			} else if err == nil || err.Status != tc.status || err.Code != "request_body_too_large" {
				t.Fatalf("expected body size rejection, got %v", err)
			}
		})
	}
}

func TestRequestBodyDeadlineHTTP(t *testing.T) {
	// Exercise real socket deadlines with a short duration; production uses five minutes.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := readChatBody(w, r, 100*time.Millisecond)
		if err != nil {
			sendError(w, &auditEntry{}, err)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		time.Sleep(200 * time.Millisecond)
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer server.Close()
	t.Run("slow_upload_returns_408", func(t *testing.T) {
		conn, err := net.Dial("tcp", server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		fmt.Fprint(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n{")
		reader := bufio.NewReader(conn)
		resp, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != 408 || !strings.Contains(string(body), "request_body_timeout") {
			t.Fatalf("status=%d body=%s err=%v", resp.StatusCode, body, err)
		}
		if _, err := reader.ReadByte(); err != io.EOF {
			t.Fatalf("timed-out upload left its connection open: %v", err)
		}
	})
	t.Run("completed_upload_clears_deadline", func(t *testing.T) {
		conn, err := net.Dial("tcp", server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(conn)
		for i := 0; i < 2; i++ {
			fmt.Fprint(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}")
			resp, err := http.ReadResponse(reader, nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != 200 || string(body) != "data: [DONE]\n\n" {
				t.Fatalf("request %d: status=%d body=%q err=%v", i, resp.StatusCode, body, err)
			}
		}
	})
}
