package main

import (
	"bytes"
	"encoding/json"
	"math/big"
	"strconv"
	"strings"
	"time"
)

type rawObject map[string]json.RawMessage

type chatBody struct {
	fields   rawObject
	stream   bool
	question string
}

func encodeJSON(value any) []byte {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	// All callers pass already validated JSON, strings or internal response structs.
	if err := encoder.Encode(value); err != nil {
		panic(err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

func isNull(raw json.RawMessage) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }
func rawString(raw json.RawMessage) (string, bool) {
	var s string
	if len(raw) == 0 || isNull(raw) || json.Unmarshal(raw, &s) != nil {
		return "", false
	}
	return s, true
}

func parseChat(data []byte) (*chatBody, *GatewayError) {
	bad := fault(400, "invalid_request", "请求不符合固定 JSON 格式")
	var fields rawObject
	if decodeJSON(data, &fields) != nil || fields == nil {
		return nil, bad
	}
	b := &chatBody{fields: fields}
	if model, ok := fields["model"]; ok && !isNull(model) {
		if _, ok := rawString(model); !ok {
			return nil, bad
		}
	}
	if stream, ok := fields["stream"]; ok {
		if isNull(stream) || json.Unmarshal(stream, &b.stream) != nil {
			return nil, bad
		}
	}
	if temperature, ok := fields["temperature"]; ok && !isNull(temperature) {
		var value float64
		if json.Unmarshal(temperature, &value) != nil || value < 0 || value > 2 {
			return nil, bad
		}
	}
	if tokens, ok := fields["max_tokens"]; ok && !isNull(tokens) {
		value, ok := new(big.Int).SetString(string(tokens), 10)
		if !ok || value.Sign() <= 0 {
			return nil, bad
		}
	}
	var messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	if json.Unmarshal(fields["messages"], &messages) != nil || len(messages) == 0 {
		return nil, bad
	}
	lastUser := -1
	for i, message := range messages {
		if message.Role == "user" {
			lastUser = i
		}
	}
	for i, message := range messages {
		switch message.Role {
		case "system", "developer", "user", "assistant", "tool":
		default:
			return nil, bad
		}
		text := ""
		if len(message.Content) > 0 && !isNull(message.Content) {
			if raw := bytes.TrimSpace(message.Content); len(raw) > 0 && raw[0] == '"' {
				if i == lastUser {
					text, _ = rawString(raw)
				}
			} else {
				var parts []rawObject
				if json.Unmarshal(message.Content, &parts) != nil || parts == nil {
					return nil, bad
				}
				var joined strings.Builder
				for _, part := range parts {
					if part == nil {
						return nil, bad
					}
					if i != lastUser {
						continue
					}
					kind, _ := rawString(part["type"])
					if kind == "text" {
						if s, ok := rawString(part["text"]); ok {
							joined.WriteString(s)
						}
					}
				}
				text = joined.String()
			}
		}
		if i == lastUser {
			b.question = text
		}
	}
	return b, nil
}

func lookup(value any, path string) (any, bool) {
	for _, key := range strings.Split(path, ".") {
		switch v := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = v[key]
			if !ok {
				return nil, false
			}
		case []any:
			i, err := strconv.Atoi(key)
			if i < 0 {
				i += len(v)
			}
			if err != nil || i < 0 || i >= len(v) {
				return nil, false
			}
			value = v[i]
		default:
			return nil, false
		}
	}
	return value, true
}

func render(template any, context map[string]any) (any, *GatewayError) {
	switch v := template.(type) {
	case map[string]any:
		if ref, exists := v["$ref"]; exists {
			path, valid := ref.(string)
			value, found := lookup(context, path)
			if fallback, ok := v["$default"]; ok && (!found || !valid || value == nil) {
				return fallback, nil
			}
			if !valid || !found {
				return nil, fault(400, "missing_template_field", "请求缺少下游适配所需字段")
			}
			return value, nil
		}
		result := make(map[string]any, len(v))
		for key, item := range v {
			value, err := render(item, context)
			if err != nil {
				return nil, err
			}
			result[key] = value
		}
		return result, nil
	case []any:
		result := make([]any, len(v))
		for i, item := range v {
			value, err := render(item, context)
			if err != nil {
				return nil, err
			}
			result[i] = value
		}
		return result, nil
	default:
		return template, nil
	}
}

func applyCompatibility(payload rawObject, rules Compatibility) (rawObject, *GatewayError) {
	// Copy only the top-level map; unchanged nested JSON stays as raw bytes.
	result := make(rawObject, len(payload))
	for key, value := range payload {
		result[key] = value
	}
	for _, field := range rules.DropFields {
		delete(result, field)
	}
	if metadata, exists := result["metadata"]; exists && rules.MetadataValues == "workspace_object_json_string" {
		var values rawObject
		// Match the narrow Python compatibility layer: only an object-valued
		// workspace is converted. Other metadata shapes and values pass through.
		if json.Unmarshal(metadata, &values) == nil && values != nil {
			value := bytes.TrimSpace(values["workspace"])
			if len(value) > 0 && value[0] == '{' {
				var compact bytes.Buffer
				if err := json.Compact(&compact, value); err != nil {
					return nil, fault(400, "invalid_metadata", "workspace 不是有效的 JSON 对象")
				}
				values["workspace"] = encodeJSON(compact.String())
				result["metadata"] = encodeJSON(values)
			}
		}
	}
	if metadata, exists := result["metadata"]; exists && rules.MetadataValues == "json_string" {
		var values rawObject
		if json.Unmarshal(metadata, &values) != nil || values == nil {
			return nil, fault(400, "invalid_metadata", "此下游要求 metadata 为对象")
		}
		for key, value := range values {
			if _, ok := rawString(value); ok {
				continue
			}
			var compact bytes.Buffer
			if json.Compact(&compact, value) != nil {
				return nil, fault(400, "invalid_metadata", "此下游要求 metadata 为对象")
			}
			values[key] = encodeJSON(compact.String())
		}
		result["metadata"] = encodeJSON(values)
	}
	return result, nil
}

func makePayload(body *chatBody, b *Backend, model string) ([]byte, *GatewayError) {
	request := make(rawObject, len(body.fields)+1)
	for key, value := range body.fields {
		request[key] = value
	}
	request["model"] = encodeJSON(model)
	payload := request
	if b.Adapter == "openai" {
		for key, value := range b.Overrides {
			payload[key] = value
		}
		payload["model"] = encodeJSON(model)
		if _, exists := payload["stream"]; exists {
			payload["stream"] = encodeJSON(body.stream)
		}
	} else {
		var template, decoded any
		if decodeJSON(b.RequestTemplate, &template) != nil || decodeJSON(encodeJSON(request), &decoded) != nil {
			return nil, fault(400, "missing_template_field", "请求缺少下游适配所需字段")
		}
		value, err := render(template, map[string]any{"request": decoded, "model": model, "question": body.question})
		if err != nil {
			return nil, err
		}
		payload = nil // Unmarshal must not retain fields from the original request.
		if json.Unmarshal(encodeJSON(value), &payload) != nil || payload == nil {
			return nil, fault(400, "missing_template_field", "模板必须生成 JSON 对象")
		}
	}
	payload, err := applyCompatibility(payload, b.Compatibility)
	if err != nil {
		return nil, err
	}
	return encodeJSON(payload), nil
}

func truthy(raw json.RawMessage) bool {
	var value any
	if decodeJSON(raw, &value) != nil {
		return false
	}
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return v != ""
	case []any:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	case json.Number:
		f, _ := v.Float64()
		return f != 0
	default:
		return false
	}
}

func normalizeResponse(data []byte, b *Backend, model, id string) ([]byte, *GatewayError) {
	if !json.Valid(data) {
		return nil, fault(502, "invalid_upstream_json", "下游未返回合法 JSON")
	}
	bad := fault(502, "invalid_upstream_response", "下游未返回有效的聊天响应")
	if b.Adapter == "openai" {
		var fields rawObject
		if json.Unmarshal(data, &fields) != nil || fields == nil {
			return nil, bad
		}
		var choices []struct {
			Message rawObject `json:"message"`
		}
		if json.Unmarshal(fields["choices"], &choices) != nil || len(choices) == 0 {
			return nil, bad
		}
		for _, choice := range choices {
			role, _ := rawString(choice.Message["role"])
			_, contentOK := rawString(choice.Message["content"])
			if role != "assistant" || (!contentOK && !truthy(choice.Message["tool_calls"])) {
				return nil, bad
			}
		}
		fields["model"] = encodeJSON(model)
		return encodeJSON(fields), nil
	}
	var value any
	if decodeJSON(data, &value) != nil {
		return nil, bad
	}
	value, ok := lookup(value, b.ResponseContentPath)
	content, text := value.(string)
	if !ok || !text {
		return nil, fault(502, "invalid_upstream_response", "下游响应缺少文本结果")
	}
	return encodeJSON(map[string]any{"id": "chatcmpl-" + id, "object": "chat.completion", "created": time.Now().Unix(), "model": model,
		"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": content}, "finish_reason": "stop"}}}), nil
}
