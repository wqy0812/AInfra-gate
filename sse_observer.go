package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
)

const maxSSEEvent = 1 << 20

type observedTool struct{ ID, Name, Arguments string }

// Observes a copy of streamed bytes. It never changes or delays the forward path.
// Business streams retain no output text; probes collect bounded synthetic output.
type sseObserver struct {
	line, data                                         []byte
	skipLF                                             bool
	collect                                            bool
	profileUsage                                       bool
	Unknown, Invalid, HasError, Done, Finished, Useful bool
	Content                                            string
	Tools                                              map[int]*observedTool
	Usage                                              json.RawMessage
	FirstMS                                            *float64
	started                                            time.Time
	collected                                          int
}

func newSSEObserver(collect bool) *sseObserver {
	return &sseObserver{collect: collect, started: time.Now(), Tools: map[int]*observedTool{}}
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
		if len(o.line)+len(o.data) > maxSSEEvent || o.collected > maxSSEEvent {
			o.Unknown = true
			o.line = nil
			o.data = nil
			o.Content = ""
			o.Tools = nil
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
			Finish *string `json:"finish_reason"`
			Delta  struct {
				Content       string `json:"content"`
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
		if c.Finish != nil && *c.Finish != "" {
			o.Finished = true
		}
		useful := c.Delta.Content != "" || len(c.Delta.Tools) > 0 || (o.profileUsage && (c.Delta.Reasoning != "" || c.Delta.ReasoningText != ""))
		if useful {
			o.Useful = true
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

func (o *sseObserver) valid() bool {
	return !o.Unknown && !o.Invalid && !o.HasError && o.Done && o.Finished && o.Useful
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
	if !o.Done || !o.Finished {
		return "SSE 未完整结束"
	}
	if !o.Useful || (o.collect && strings.TrimSpace(o.Content) == "" && len(o.Tools) == 0) {
		return "SSE 未返回有效输出"
	}
	return ""
}
