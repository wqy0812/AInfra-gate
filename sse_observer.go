package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
)

const maxSSEEvent = 1 << 20
const maxSSEChoices = 1024

type sseObserverContextKey struct{}

type observedTool struct{ ID, Name, Arguments string }

// Observes a copy of streamed bytes. It never changes or delays the forward path.
// Business streams retain no output text; probes collect bounded synthetic output.
type sseObserver struct {
	line, data                               []byte
	skipLF                                   bool
	collect                                  bool
	profileUsage                             bool
	Unknown, Invalid, HasError, Done, Useful bool
	expectedChoices                          int
	finishedChoices                          map[int]bool
	Content                                  string
	Tools                                    map[int]*observedTool
	Usage                                    json.RawMessage
	FirstMS                                  *float64
	LastUsefulAt                             time.Time
	started                                  time.Time
	collected                                int
}

func newSSEObserver(collect bool) *sseObserver {
	return &sseObserver{collect: collect, started: time.Now(), Tools: map[int]*observedTool{}, expectedChoices: 1, finishedChoices: map[int]bool{}}
}

// Read the effective payload after overrides and compatibility adaptation.
// Observation limits never reject or modify the business request.
func (o *sseObserver) expectPayload(payload []byte) {
	var fields struct {
		N json.RawMessage `json:"n"`
	}
	if json.Unmarshal(payload, &fields) != nil {
		o.Unknown = true
		return
	}
	n := 1
	if raw := fields.N; len(raw) > 0 && !isNull(raw) {
		if json.Unmarshal(raw, &n) != nil || n < 1 {
			o.Unknown = true
			return
		}
	}
	o.expectedChoices = n
	if n > maxSSEChoices {
		o.Unknown = true
	}
}

func (o *sseObserver) Feed(p []byte) {
	if o.Unknown {
		return
	}
	for _, b := range p {
		if o.skipLF {
			o.skipLF = false
			if b == '\n' {
				continue
			}
		}
		if b == '\n' || b == '\r' {
			o.consumeLine()
			o.skipLF = b == '\r'
		} else {
			o.line = append(o.line, b)
		}
		if o.Unknown || len(o.line)+len(o.data) > maxSSEEvent || o.collected > maxSSEEvent {
			o.Unknown = true
			o.line = nil
			o.data = nil
			o.Content = ""
			o.Tools = nil
			o.finishedChoices = nil
			return
		}
	}
}

func (o *sseObserver) consumeLine() {
	line := o.line
	o.line = nil
	if len(line) == 0 {
		if len(o.data) > 0 {
			o.event(bytes.TrimSuffix(o.data, []byte{'\n'}))
			o.data = nil
		}
		return
	}
	if bytes.HasPrefix(line, []byte("data:")) {
		value := line[5:]
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		o.data = append(o.data, value...)
		o.data = append(o.data, '\n')
	}
}

func (o *sseObserver) event(data []byte) {
	if bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
		o.Done = true
		return
	}
	var event struct {
		Error   json.RawMessage `json:"error"`
		Usage   json.RawMessage `json:"usage"`
		Choices []struct {
			Index  json.RawMessage `json:"index"`
			Finish *string         `json:"finish_reason"`
			Delta  struct {
				Content       string `json:"content"`
				Refusal       string `json:"refusal"`
				Reasoning     string `json:"reasoning_content"`
				ReasoningText string `json:"reasoning"`
				Tools         []struct {
					Index    int    `json:"index"`
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal(data, &event) != nil {
		o.Invalid = true
		return
	}
	if len(event.Error) > 0 && !isNull(event.Error) {
		o.HasError = true
	}
	if len(event.Choices) == 0 && len(event.Usage) == 0 && !o.HasError {
		o.Invalid = true
	}
	if (o.collect || o.profileUsage) && len(event.Usage) > 0 && !isNull(event.Usage) {
		o.Usage = usageCounters(event.Usage)
	}
	for _, c := range event.Choices {
		index := 0
		if len(c.Index) == 0 {
			if o.expectedChoices != 1 {
				o.Invalid = true
				continue
			}
		} else if isNull(c.Index) || json.Unmarshal(c.Index, &index) != nil {
			o.Invalid = true
			continue
		}
		if index < 0 || index >= o.expectedChoices {
			o.Invalid = true
			continue
		}
		if _, exists := o.finishedChoices[index]; !exists && len(o.finishedChoices) >= maxSSEChoices {
			o.Unknown = true
			return
		}
		o.finishedChoices[index] = o.finishedChoices[index] || (c.Finish != nil && *c.Finish != "")
		useful := c.Delta.Content != "" || len(c.Delta.Tools) > 0 || c.Delta.Refusal != "" || c.Delta.Reasoning != "" || c.Delta.ReasoningText != ""
		if useful {
			o.Useful = true
			o.LastUsefulAt = time.Now()
			if o.FirstMS == nil {
				n := float64(time.Since(o.started).Microseconds()) / 1000
				o.FirstMS = &n
			}
		}
		if !o.collect {
			continue
		}
		o.Content += c.Delta.Content
		o.collected += len(c.Delta.Content)
		for _, t := range c.Delta.Tools {
			if t.Index < 0 || t.Index >= 32 {
				o.Invalid = true
				continue
			}
			if o.Tools[t.Index] == nil {
				o.Tools[t.Index] = &observedTool{}
			}
			v := o.Tools[t.Index]
			v.ID += t.ID
			v.Name += t.Function.Name
			v.Arguments += t.Function.Arguments
			o.collected += len(t.ID) + len(t.Function.Name) + len(t.Function.Arguments)
		}
	}
}

// completionClass is shared by business outcomes and generation metrics.
// Empty content is a valid business completion, not a failed model probe.
func (o *sseObserver) completionClass() string {
	if o.HasError {
		return "stream_error"
	}
	if o.Unknown {
		return "unknown"
	}
	if o.Invalid {
		return "protocol_error"
	}
	if !o.Done || len(o.data) > 0 || isDataLine(o.line) || len(o.finishedChoices) != o.expectedChoices {
		return "stream_incomplete"
	}
	for _, finished := range o.finishedChoices {
		if !finished {
			return "stream_incomplete"
		}
	}
	return ""
}

func (o *sseObserver) valid() bool { return o.completionClass() == "" }

func (o *sseObserver) validProbe() bool {
	return o.valid() && o.Useful && (strings.TrimSpace(o.Content) != "" || len(o.Tools) > 0)
}

func (o *sseObserver) errorMessage() string {
	if o.Unknown {
		return "SSE 事件或验证输出超过解析上限"
	}
	if o.HasError {
		return "SSE 包含下游错误事件"
	}
	if o.Invalid {
		return "SSE 事件格式无效"
	}
	if o.completionClass() == "stream_incomplete" {
		return "SSE 未完整结束"
	}
	if !o.Useful || (o.collect && strings.TrimSpace(o.Content) == "" && len(o.Tools) == 0) {
		return "SSE 未返回有效输出"
	}
	return ""
}

func isDataLine(line []byte) bool {
	return bytes.Equal(line, []byte("data")) || bytes.HasPrefix(line, []byte("data:"))
}
