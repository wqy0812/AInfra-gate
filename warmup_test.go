package aigate

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSSEObserverProtocolBoundaries(t *testing.T) {
	good := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\r\n\r\ndata: [DONE]\r\n\r\n"
	for _, test := range []struct {
		name, data     string
		valid, unknown bool
	}{
		{"valid", good, true, false}, {"truncated", strings.Split(good, "data: [DONE]")[0], false, false}, {"error", good + "data: {\"error\":{\"message\":\"private\"}}\n\n", false, false}, {"oversize", "data: " + strings.Repeat("x", maxSSEEvent+1), false, true}, {"invalid", "data: nope\n\n" + good, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := newSSEObserver(false)
			for i := 0; i < len(test.data); i += 7 {
				end := i + 7
				if end > len(test.data) {
					end = len(test.data)
				}
				o.Feed([]byte(test.data[i:end]))
			}
			if o.valid() != test.valid || o.Unknown != test.unknown {
				t.Fatal("wrong classification")
			}
			if o.Content != "" || len(o.Tools) != 0 {
				t.Fatal("business observer retained output")
			}
		})
	}
}

func TestWarmupUsageOnlyKeepsNumericCounters(t *testing.T) {
	got := string(usageCounters(json.RawMessage(`{"total_tokens":8,"prompt_tokens":null,"private":"secret","completion_tokens_details":{"reasoning_tokens":2,"secret":"private"}}`)))
	if strings.Contains(got, "secret") || strings.Contains(got, "private") || strings.Contains(got, "prompt_tokens") || !strings.Contains(got, "reasoning_tokens") {
		t.Fatal("usage did not preserve only numeric counters")
	}
}

func TestWarmupPolicyDefaultsAndRequestBudget(t *testing.T) {
	var policy WarmupPolicy
	policy.defaults()
	if policy.Rounds != 2 || policy.StepSeconds != 120 || policy.TimeoutSeconds != 900 || policy.MaxTokens != 1024 || policy.validate() != nil {
		t.Fatalf("validation defaults changed: %+v", policy)
	}
	p := newProtocolBackend(t)
	g, _, _ := serveTest(t, fixtureConfig(t, p.server.URL))
	policy = WarmupPolicy{Rounds: 2, StepSeconds: 1, TimeoutSeconds: 5, MaxTokens: 37}
	// Keep the real protocol fixture, but inspect the payload that reaches HTTP.
	original := g.client.Transport
	var requests, defaultToolRequests int
	g.client.Transport = generationTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost {
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			var body map[string]any
			if err := json.Unmarshal(raw, &body); err != nil {
				return nil, err
			}
			if body["max_tokens"] != float64(37) || body["model"] != "local-protocol-model" {
				t.Errorf("probe budget/model changed: %s", raw)
			}
			if body["stream"] == true && body["tools"] != nil {
				if _, exists := body["tool_choice"]; exists {
					return &http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":"named tool choice unsupported"}`))}, nil
				}
				defaultToolRequests++
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
			requests++
		}
		return original.RoundTrip(r)
	})
	report, err := validateControlledRuntime(context.Background(), g, policy)
	if err != nil || !report.Passed || !report.Fresh || report.FinishedAt == nil || len(report.Steps) != 9 || requests != 8 || defaultToolRequests != 2 || p.gets.Load() != 1 {
		t.Fatalf("warmup failed: %+v %v requests=%d", report, err, requests)
	}
	if model, err := g.pool.discover(context.Background()); err != nil || model != report.Model || p.gets.Load() != 1 {
		t.Fatal("successful validation did not populate model cache")
	}
}

func TestWarmupCancellationAndTimeouts(t *testing.T) {
	for _, mode := range []string{"step timeout", "total timeout", "cancelled parent"} {
		t.Run(mode, func(t *testing.T) {
			p := newProtocolBackend(t)
			p.probeGate = make(chan struct{})
			p.probeStarted = make(chan struct{}, 1)
			g, _, _ := serveTest(t, fixtureConfig(t, p.server.URL))
			policy := WarmupPolicy{Rounds: 1, StepSeconds: 2, TimeoutSeconds: 2, MaxTokens: 32}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "step timeout":
				policy.StepSeconds = .02
			case "total timeout":
				policy.TimeoutSeconds = .02
			case "cancelled parent":
				go func() {
					select {
					case <-p.probeStarted:
						cancel()
					case <-ctx.Done():
					}
				}()
			}
			started := time.Now()
			report, err := validateControlledRuntime(ctx, g, policy)
			if err == nil || report.Passed || report.FinishedAt == nil || len(report.Steps) != 2 || report.Steps[1].Name != "json" || report.Steps[1].Passed || time.Since(started) > time.Second {
				t.Fatalf("probe did not stop within its budget: %+v %v", report, err)
			}
			if p.posts.Load() != 1 {
				t.Fatal("failed probe retried or advanced protocol steps")
			}
			g.pool.runtime.cache.mu.Lock()
			cached := g.pool.runtime.cache.model
			g.pool.runtime.cache.mu.Unlock()
			if cached != "" {
				t.Fatal("failed validation populated model cache")
			}
		})
	}
}
