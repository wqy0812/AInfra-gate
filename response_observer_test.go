package aigate

import (
	"net/http"
	"strings"
	"testing"
)

func observeResponse(t *testing.T, body string, sse bool, chunk int, transport string) responseObservation {
	t.Helper()
	h := http.Header{}
	if sse {
		h.Set("Content-Type", "text/event-stream")
	}
	o := newResponseObserver(h)
	for len(body) > 0 {
		n := min(chunk, len(body))
		o.Write([]byte(body[:n]))
		body = body[n:]
	}
	return o.finish(transport)
}

func TestResponseObserverChunkingAndLastUsage(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"长\\\"回答\"},\"finish_reason\":null}],\"usage\":null}\r\n\r\n" +
		"data: {\"choices\":[{\"finish_reason\":\"length\"}],\r\ndata: \"usage\":{\"prompt_tokens\":7,\"completion_tokens\":2,\"total_tokens\":9}}\r\n\r\n" +
		"data: {\"usage\":{\"completion_tokens\":0}}\n\ndata: [DONE]\n\n"
	for _, chunk := range []int{1, 2, 7, 65, 65536} {
		got := observeResponse(t, body, true, chunk, "")
		if !got.ProtocolComplete || !got.UsageKnown || got.Usage["completion_tokens"] != 0 || len(got.Usage) != 1 || len(got.FinishReasons) != 1 || got.FinishReasons[0] != "length" {
			t.Fatalf("chunk %d: %+v", chunk, got)
		}
	}
}

func TestResponseObserverLongContentBounded(t *testing.T) {
	for _, sse := range []bool{false, true} {
		h := http.Header{}
		if sse {
			h.Set("Content-Type", "text/event-stream")
		}
		o := newResponseObserver(h)
		prefix := `{"choices":[{"message":{"content":"`
		if sse {
			prefix = "data: " + prefix
		}
		o.Write([]byte(prefix))
		block := []byte(strings.Repeat("long content ", 4096))
		for i := 0; i < 400; i++ {
			o.Write(block)
		}
		if o.parser.length != 0 || o.parser.depth > 5 {
			t.Fatalf("content retained: %d", o.parser.length)
		}
		suffix := `"},"finish_reason":"stop"}],"usage":{"completion_tokens":800,"prompt_tokens":100,"total_tokens":900}}`
		if sse {
			suffix += "\n\ndata: [DONE]\n\n"
		}
		o.Write([]byte(suffix))
		got := o.finish("")
		if !got.ProtocolComplete || got.Usage["completion_tokens"] != 800 {
			t.Fatalf("%+v", got)
		}
	}
}

func TestResponseObserverFailureAndUnknown(t *testing.T) {
	for name, body := range map[string]string{
		"negative":           `{"usage":{"completion_tokens":-1}}`,
		"fraction":           `{"usage":{"completion_tokens":1.5}}`,
		"string":             `{"usage":{"completion_tokens":"3"}}`,
		"trailing":           `{"usage":{"completion_tokens":3}}x`,
		"depth":              `{"x":` + strings.Repeat("[", 128) + `0` + strings.Repeat("]", 128) + `}`,
		"escape":             `{"content":"\z"}`,
		"comma":              `{"usage":{"completion_tokens":3,}}`,
		"utf8":               "{\"content\":\"\xff\"}",
		"nested_usage_value": `{"usage":{"completion_tokens":{}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			got := observeResponse(t, body, false, 1, "")
			if got.ParseStatus != "failed" || got.ProtocolComplete {
				t.Fatalf("%+v", got)
			}
		})
	}
	got := observeResponse(t, `{"choices":[],"usage":null}`, false, 1, "")
	if got.UsageKnown || !got.ProtocolComplete {
		t.Fatalf("unknown: %+v", got)
	}
	got = observeResponse(t, "data: {\"usage\":{\"completion_tokens\":5}}\n\n", true, 2, "unexpected_eof")
	if !got.UsageKnown || got.ProtocolComplete || got.TransportError != "unexpected_eof" {
		t.Fatalf("lost usage: %+v", got)
	}
	o := newResponseObserver(http.Header{"Content-Encoding": []string{"gzip"}})
	o.Write([]byte("invalid"))
	if got := o.finish(""); got.ParseError != "unsupported_content_encoding" {
		t.Fatalf("%+v", got)
	}
}
