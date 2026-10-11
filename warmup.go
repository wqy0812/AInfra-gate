package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// WarmupPolicy is the control v2 validation payload, independent of routing
// and persistence. ControlledGateway owns the probe's lifetime and accounting.
type WarmupPolicy struct {
	Rounds         int     `json:"rounds"`
	StepSeconds    float64 `json:"step_seconds"`
	TimeoutSeconds float64 `json:"timeout_seconds"`
	MaxTokens      int     `json:"max_tokens"`
}

func (p *WarmupPolicy) defaults() {
	if p.Rounds == 0 {
		p.Rounds = 2
	}
	if p.StepSeconds == 0 {
		p.StepSeconds = 120
	}
	if p.TimeoutSeconds == 0 {
		p.TimeoutSeconds = 900
	}
	if p.MaxTokens == 0 {
		p.MaxTokens = 1024
	}
}
func (p WarmupPolicy) validate() error {
	if p.Rounds < 1 || p.Rounds > 20 || p.StepSeconds <= 0 || p.TimeoutSeconds <= 0 || p.MaxTokens < 1 {
		return fmt.Errorf("invalid validation policy")
	}
	return nil
}

type WarmupStep struct {
	Round        int             `json:"round"`
	Name         string          `json:"name"`
	Passed       bool            `json:"passed"`
	Error        string          `json:"error,omitempty"`
	DurationMS   float64         `json:"duration_ms"`
	FirstTokenMS *float64        `json:"first_token_ms,omitempty"`
	Usage        json.RawMessage `json:"usage,omitempty"`
}

type WarmupReport struct {
	Model      string       `json:"model"`
	Passed     bool         `json:"passed"`
	Fresh      bool         `json:"fresh"`
	StartedAt  time.Time    `json:"started_at"`
	FinishedAt *time.Time   `json:"finished_at,omitempty"`
	Steps      []WarmupStep `json:"steps"`
}

func validateControlledRuntime(parent context.Context, g *Gateway, cfg WarmupPolicy) (*WarmupReport, error) {
	ctx, cancel := context.WithTimeout(parent, seconds(cfg.TimeoutSeconds))
	defer cancel()
	r := &WarmupReport{Fresh: true, StartedAt: time.Now(), Steps: []WarmupStep{}}
	defer func() { now := time.Now(); r.FinishedAt = &now }()
	started := time.Now()
	model, fetchErr := g.pool.fetchModel(ctx, g.pool.runtime.backend)
	step := WarmupStep{Name: "discovery", Passed: fetchErr == nil, DurationMS: float64(time.Since(started).Microseconds()) / 1000}
	if fetchErr != nil {
		step.Error = "目标后端模型发现失败"
	}
	r.Model = model
	r.Steps = append(r.Steps, step)
	if fetchErr != nil {
		return r, fmt.Errorf("目标后端模型发现失败")
	}
	for round := 1; round <= cfg.Rounds; round++ {
		marker := "aigate-echo-" + randomID()[:8]
		tool := map[string]any{"type": "function", "function": map[string]any{"name": "aigate_echo", "description": "Return the given value unchanged.", "parameters": map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []string{"value"}, "additionalProperties": false}}}
		var toolMessages []any
		for _, kind := range []string{"json", "sse", "tool_call", "tool_result"} {
			body := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "Reply with a short greeting."}}, "max_tokens": cfg.MaxTokens, "stream": kind == "sse" || kind == "tool_call", "metadata": map[string]any{"mode": "gateway_warmup", "workspace": map[string]any{"purpose": "protocol_validation"}}}
			if body["stream"] == true {
				body["stream_options"] = map[string]any{"include_usage": true}
			}
			if kind == "tool_call" {
				toolMessages = []any{map[string]any{"role": "user", "content": "Call aigate_echo once with value " + marker + ". After receiving the tool result, reply with that value."}}
				body["messages"] = toolMessages
				body["tools"] = []any{tool}
				body["tool_choice"] = map[string]any{"type": "function", "function": map[string]any{"name": "aigate_echo"}}
			}
			if kind == "tool_result" {
				body["messages"] = toolMessages
				body["tools"] = []any{tool}
				body["tool_choice"] = "none"
			}
			step, observed, callErr := probeRuntime(ctx, g, cfg.StepSeconds, model, body)
			step.Round, step.Name = round, kind
			if callErr == nil && kind == "tool_call" {
				v := observed.Tools[0]
				var args map[string]string
				if len(observed.Tools) != 1 || v == nil || v.ID == "" || v.Name != "aigate_echo" || json.Unmarshal([]byte(v.Arguments), &args) != nil || args["value"] != marker {
					callErr = fmt.Errorf("工具调用名称、ID 或参数验证失败")
				} else {
					toolMessages = append(toolMessages, map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": v.ID, "type": "function", "function": map[string]any{"name": v.Name, "arguments": v.Arguments}}}}, map[string]any{"role": "tool", "tool_call_id": v.ID, "content": marker})
				}
			}
			if callErr == nil && kind == "tool_result" && !strings.Contains(observed.Content, marker) {
				callErr = fmt.Errorf("工具结果续答验证失败")
			}
			step.Passed = callErr == nil
			if callErr != nil {
				step.Error = callErr.Error()
			}
			r.Steps = append(r.Steps, step)
			if callErr != nil {
				return r, callErr
			}
		}
	}
	r.Passed = true
	// Update only an idle cache: a concurrent normal discovery owns its own result.
	cache := g.pool.runtime.cache
	cache.mu.Lock()
	if cache.inflight == nil {
		cache.model = model
		cache.expires = time.Now().Add(seconds(g.config.ModelCacheSeconds))
	}
	cache.mu.Unlock()
	return r, nil
}

func probeRuntime(parent context.Context, g *Gateway, stepSeconds float64, model string, payload map[string]any) (WarmupStep, *sseObserver, error) {
	step := WarmupStep{}
	started := time.Now()
	observer := newSSEObserver(true)
	ctx, cancel := context.WithTimeout(parent, seconds(stepSeconds))
	defer cancel()
	ctx = context.WithValue(ctx, sseObserverContextKey{}, observer)
	body, err := parseChat(encodeJSON(payload))
	if err != nil {
		return step, observer, err
	}
	response, err := g.generate(ctx, body, g.pool.runtime.backend, model)
	if err != nil {
		return step, observer, fmt.Errorf("预热生成失败: %s", err.Message)
	}
	defer response.Body.Close()
	finish := func(e error) (WarmupStep, *sseObserver, error) {
		step.DurationMS = float64(time.Since(started).Microseconds()) / 1000
		step.FirstTokenMS = observer.FirstMS
		step.Usage = observer.Usage
		return step, observer, e
	}
	if body.stream {
		buffer := make([]byte, 32*1024)
		for {
			n, readErr := response.Body.Read(buffer)
			observer.Feed(buffer[:n])
			if observer.Unknown {
				return finish(fmt.Errorf("SSE 验证输出超过上限"))
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return finish(fmt.Errorf("预热流中断或超时"))
			}
		}
		if !observer.validProbe() {
			return finish(fmt.Errorf("%s", observer.errorMessage()))
		}
	} else {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, maxSSEEvent+1))
		if readErr != nil || len(data) > maxSSEEvent {
			return finish(fmt.Errorf("预热响应读取失败或超过上限"))
		}
		normalized, err := normalizeResponse(data, g.pool.runtime.backend, model, "")
		if err != nil {
			return finish(fmt.Errorf("%s", err.Message))
		}
		var decoded struct {
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
			Usage json.RawMessage `json:"usage"`
		}
		if json.Unmarshal(normalized, &decoded) != nil || len(decoded.Choices) == 0 || strings.TrimSpace(decoded.Choices[0].Message.Content) == "" {
			return finish(fmt.Errorf("预热响应没有有效文本"))
		}
		observer.Content = decoded.Choices[0].Message.Content
		observer.Usage = usageCounters(decoded.Usage)
	}
	return finish(nil)
}

// Persist numeric token counters only, never arbitrary content nested in usage.
func usageCounters(raw json.RawMessage) json.RawMessage {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil
	}
	result := map[string]any{}
	for _, name := range []string{"prompt_tokens", "completion_tokens", "total_tokens"} {
		var n uint64
		if v, ok := fields[name]; ok && !isNull(v) && json.Unmarshal(v, &n) == nil {
			result[name] = n
		}
	}
	for group, keys := range map[string][]string{"prompt_tokens_details": {"cached_tokens", "audio_tokens"}, "completion_tokens_details": {"reasoning_tokens", "audio_tokens", "accepted_prediction_tokens", "rejected_prediction_tokens"}} {
		var details map[string]json.RawMessage
		_ = json.Unmarshal(fields[group], &details)
		kept := map[string]uint64{}
		for _, key := range keys {
			var n uint64
			if v, ok := details[key]; ok && !isNull(v) && json.Unmarshal(v, &n) == nil {
				kept[key] = n
			}
		}
		if len(kept) > 0 {
			result[group] = kept
		}
	}
	if len(result) == 0 {
		return nil
	}
	return encodeJSON(result)
}
