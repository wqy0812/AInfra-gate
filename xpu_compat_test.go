package aigate

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestReasoningOnlyResponseHTTP(t *testing.T) {
	for _, tc := range []struct {
		name, message string
	}{
		{"null_content", `{"role":"assistant","content":null,"reasoning_content":"thinking","tool_calls":null}`},
		{"missing_content", `{"role":"assistant","reasoning_content":"thinking"}`},
		{"empty_reasoning", `{"role":"assistant","content":null,"reasoning_content":""}`},
		{"null_reasoning", `{"role":"assistant","content":null,"reasoning_content":null}`},
		{"missing_reasoning", `{"role":"assistant","content":null}`},
		{"object_reasoning", `{"role":"assistant","reasoning_content":{"text":"thinking"}}`},
		{"number_reasoning", `{"role":"assistant","reasoning_content":1}`},
		{"wrong_role", `{"role":"user","reasoning_content":"thinking"}`},
		{"wrong_content", `{"role":"assistant","content":{},"reasoning_content":"thinking"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"id":"xpu-probe","model":"router-model","choices":[{"index":0,"message":` + tc.message + `,"finish_reason":"length"}],"usage":{"completion_tokens":1,"large":9007199254740993},"metadata":{"weight_version":"default"}}`
			u := upstreamServer(t, "router-model", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) })
			_, s, _ := serveTest(t, fixtureConfig(t, u.URL))
			r, data := request(t, s, "POST", "/v1/chat/completions", basicBody, "")
			if r.StatusCode != 200 {
				t.Fatalf("status=%d body=%s", r.StatusCode, data)
			}
			equivalentJSON(t, data, []byte(body))
		})
	}
}

func TestJSONStringMetadataHTTP(t *testing.T) {
	for _, tc := range []struct {
		name, input, want string
		invalid           bool
	}{
		{"missing", "", "", false},
		{"null", `null`, `null`, false},
		{"empty", `{}`, `{}`, false},
		{"mixed", `{"workspace":{"n":9007199254740993},"s":"{\"x\":1}","a":[1,2],"n":123,"b":false,"nil":null}`, `{"workspace":"{\"n\":9007199254740993}","s":"{\"x\":1}","a":"[1,2]","n":"123","b":"false","nil":"null"}`, false},
		{"array", `[]`, "", true},
		{"string", `"x"`, "", true},
		{"number", `1`, "", true},
		{"boolean", `true`, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sent := make(chan []byte, 1)
			u := upstreamServer(t, "router-model", func(w http.ResponseWriter, r *http.Request) {
				data, _ := io.ReadAll(r.Body)
				sent <- data
				_, _ = io.WriteString(w, goodResponse)
			})
			c := fixtureConfig(t, u.URL)
			c.Backend.Compatibility.MetadataValues = "json_string"
			_, s, _ := serveTest(t, c)
			body := rawObject{"messages": json.RawMessage(`[{"role":"user","content":"test"}]`)}
			if tc.input != "" {
				body["metadata"] = json.RawMessage(tc.input)
			}
			r, data := request(t, s, "POST", "/v1/chat/completions", string(encodeJSON(body)), "")
			if tc.invalid {
				requireError(t, r, data, 400, "invalid_metadata")
				if len(sent) != 0 {
					t.Fatal("invalid metadata forwarded")
				}
				return
			}
			if r.StatusCode != 200 {
				t.Fatalf("status=%d body=%s", r.StatusCode, data)
			}
			body["model"] = encodeJSON("router-model")
			if tc.want != "" {
				body["metadata"] = json.RawMessage(tc.want)
			}
			equivalentJSON(t, <-sent, encodeJSON(body))
			again, err := applyCompatibility(body, c.Backend.Compatibility)
			if err != nil {
				t.Fatal(err)
			}
			equivalentJSON(t, encodeJSON(again), encodeJSON(body))
		})
	}
}
