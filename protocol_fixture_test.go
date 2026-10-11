package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Protocol-capable local inference endpoint. Probe requests really traverse HTTP,
// the selected adapter and SSE parser; tests never contact DCU or A3.
type protocolBackend struct {
	server         *httptest.Server
	businessStatus atomic.Int64
	probeBad       atomic.Bool
	posts          atomic.Int64
	gets           atomic.Int64
	probeGate      chan struct{}
	probeStarted   chan struct{}
	streamGate     chan struct{}
	discoveryGate  chan struct{}
	mu             sync.Mutex
	models         []string
}

func newProtocolBackend(t *testing.T) *protocolBackend {
	p := &protocolBackend{}
	p.server = httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.server.Close)
	return p
}

func (p *protocolBackend) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		p.gets.Add(1)
		if p.discoveryGate != nil {
			select {
			case <-p.discoveryGate:
			case <-r.Context().Done():
				return
			}
		}
		io.WriteString(w, `{"data":[{"id":"local-protocol-model"}]}`)
		return
	}
	p.posts.Add(1)
	var b map[string]json.RawMessage
	if json.NewDecoder(r.Body).Decode(&b) != nil {
		w.WriteHeader(400)
		return
	}
	model, _ := rawString(b["model"])
	p.mu.Lock()
	p.models = append(p.models, model)
	p.mu.Unlock()
	var metadata map[string]any
	_ = json.Unmarshal(b["metadata"], &metadata)
	probe := metadata["mode"] == "gateway_warmup"
	if probe && p.probeGate != nil {
		if p.probeStarted != nil {
			select {
			case p.probeStarted <- struct{}{}:
			default:
			}
		}
		select {
		case <-p.probeGate:
		case <-r.Context().Done():
			return
		}
	}
	if probe && p.probeBad.Load() {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
		return
	}
	if !probe && p.businessStatus.Load() != 0 {
		w.WriteHeader(int(p.businessStatus.Load()))
		io.WriteString(w, "private backend failure")
		return
	}
	var messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	_ = json.Unmarshal(b["messages"], &messages)
	content := "hello"
	for _, msg := range messages {
		if msg.Role == "tool" {
			content = msg.Content
		}
	}
	if string(b["stream"]) != "true" {
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": content}, "finish_reason": "stop"}}, "usage": map[string]any{"total_tokens": 8}})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	if len(b["tools"]) > 0 {
		text := messages[0].Content
		start := strings.Index(text, "aigate-echo-")
		marker := strings.SplitN(text[start:], ".", 2)[0]
		args := string(encodeJSON(map[string]any{"value": marker}))
		for i, part := range []string{args[:len(args)/2], args[len(args)/2:]} {
			fn := map[string]any{"arguments": part}
			tool := map[string]any{"index": 0, "function": fn}
			if i == 0 {
				tool["id"] = "call-echo"
				tool["type"] = "function"
				fn["name"] = "aigate_echo"
			}
			fmt.Fprintf(w, "data: %s\n\n", encodeJSON(map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": []any{tool}}}}}))
			w.(http.Flusher).Flush()
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n")
	} else {
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
		w.(http.Flusher).Flush()
		if !probe && p.streamGate != nil {
			select {
			case <-p.streamGate:
			case <-r.Context().Done():
				return
			}
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
	}
	io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"total_tokens\":8}}\n\ndata: [DONE]\n\n")
}
