package aigate

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestWorkspaceCompatibilityHTTP(t *testing.T) {
	cases := []struct {
		name, metadata, expected string
	}{
		{"object", `{"workspace": {"path":"/项目","name":"demo","n":9007199254740993},"other":{"x":1},"count":2,"flag":false,"nil":null,"list":[1]}`, `{"workspace":"{\"path\":\"/项目\",\"name\":\"demo\",\"n\":9007199254740993}","other":{"x":1},"count":2,"flag":false,"nil":null,"list":[1]}`},
		{"empty_object", `{"workspace":{}}`, `{"workspace":"{}"}`},
		{"string", `{"workspace":"{\"path\":\"/project\"}"}`, `{"workspace":"{\"path\":\"/project\"}"}`},
		{"array", `{"workspace":[{"path":"/project"}]}`, `{"workspace":[{"path":"/project"}]}`},
		{"number", `{"workspace":9007199254740993}`, `{"workspace":9007199254740993}`},
		{"bool", `{"workspace":false}`, `{"workspace":false}`},
		{"null_workspace", `{"workspace":null}`, `{"workspace":null}`},
		{"missing_workspace", `{"other":{"workspace":{}}}`, `{"other":{"workspace":{}}}`},
		{"null_metadata", `null`, `null`},
		{"array_metadata", `[]`, `[]`},
		{"string_metadata", `"metadata"`, `"metadata"`},
		{"missing_metadata", ``, ``},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, stream := range []bool{false, true} {
				name := "json"
				if stream {
					name = "sse"
				}
				t.Run(name, func(t *testing.T) {
					const events = "data: {\"choices\":[{\"delta\":{\"content\":\"你好\"}}]}\n\ndata: [DONE]\n\n"
					sent := make(chan []byte, 1)
					u := upstreamServer(t, "router-model", func(w http.ResponseWriter, r *http.Request) {
						data, _ := io.ReadAll(r.Body)
						sent <- data
						if stream {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, events)
						} else {
							_, _ = io.WriteString(w, goodResponse)
						}
					})
					c := fixtureConfig(t, u.URL)
					c.Backend.Compatibility.MetadataValues = "workspace_object_json_string"
					_, s, _ := serveTest(t, c)
					body := rawObject{"messages": json.RawMessage(`[{"role":"user","content":"test"}]`), "stream": encodeJSON(stream), "extra": json.RawMessage(`{"n":9007199254740993}`)}
					if tc.metadata != "" {
						body["metadata"] = json.RawMessage(tc.metadata)
					}
					r, data := request(t, s, "POST", "/v1/chat/completions", string(encodeJSON(body)), "")
					if r.StatusCode != 200 {
						t.Fatalf("status %d", r.StatusCode)
					}
					if stream && string(data) != events {
						t.Fatal("SSE changed")
					}
					if !stream {
						equivalentJSON(t, data, []byte(goodResponse))
					}
					body["model"] = encodeJSON("router-model")
					if tc.expected != "" {
						body["metadata"] = json.RawMessage(tc.expected)
					}
					equivalentJSON(t, <-sent, encodeJSON(body))
					// Applying the mode again must not double-encode workspace.
					again, err := applyCompatibility(body, c.Backend.Compatibility)
					if err != nil {
						t.Fatal(err)
					}
					equivalentJSON(t, encodeJSON(again), encodeJSON(body))
				})
			}
		})
	}
}
