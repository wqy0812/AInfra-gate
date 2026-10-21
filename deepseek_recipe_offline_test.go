package aigate

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
)

// TestDeepseekRecipeOfflineBridge is a file-only bridge for the Python diagnostic
// command. It calls the real adapter and observer, without starting a gateway,
// discovering models, fetching images, or opening any network connection.
func TestDeepseekRecipeOfflineBridge(t *testing.T) {
	input, output := os.Getenv("AIGATE_RECIPE_INPUT"), os.Getenv("AIGATE_RECIPE_OUTPUT")
	if input == "" && output == "" {
		t.Skip("invoked explicitly by scripts/deepseek_recipe_compare.py")
	}
	if input == "" || output == "" {
		t.Fatal("both offline bridge file paths are required")
	}
	data, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	var tasks []struct {
		ID      string          `json:"id"`
		Kind    string          `json:"kind"`
		Request json.RawMessage `json:"request"`
		Backend struct {
			Overrides     rawObject     `json:"overrides"`
			Compatibility Compatibility `json:"compatibility"`
		} `json:"backend"`
		Model string `json:"model"`
		SSE   string `json:"sse"`
	}
	if err := strictDecode(data, &tasks); err != nil {
		t.Fatal(err)
	}
	results := map[string]any{}
	for _, task := range tasks {
		if task.ID == "" || results[task.ID] != nil {
			t.Fatal("missing or duplicate offline case ID")
		}
		switch task.Kind {
		case "request":
			body, e := parseChat(task.Request)
			var payload []byte
			if e == nil {
				backend := &Backend{Adapter: "openai", Overrides: task.Backend.Overrides, Compatibility: task.Backend.Compatibility}
				payload, e = makePayload(body, backend, task.Model)
			}
			if e != nil {
				results[task.ID] = map[string]any{"status": "error", "error": map[string]any{"code": e.Code}}
			} else {
				results[task.ID] = map[string]any{"status": "ok", "payload": json.RawMessage(payload)}
			}
		case "sse":
			o := newSSEObserver(true)
			o.expectPayload(task.Request)
			// Feed byte by byte to exercise UTF-8 and SSE boundaries too.
			for _, b := range []byte(task.SSE) {
				o.Feed([]byte{b})
			}
			indices := make([]int, 0, len(o.Tools))
			for index := range o.Tools {
				indices = append(indices, index)
			}
			sort.Ints(indices)
			tools := []any{}
			ids := map[string]bool{}
			validIDs := true
			for _, index := range indices {
				tool := o.Tools[index]
				validIDs = validIDs && tool.ID != "" && !ids[tool.ID]
				ids[tool.ID] = true
				tools = append(tools, map[string]any{"index": index, "name": tool.Name, "arguments": tool.Arguments})
			}
			results[task.ID] = map[string]any{
				"completion_class": o.completionClass(), "done": o.Done,
				"valid_probe": o.validProbe(), "useful": o.Useful,
				"content": o.Content, "tools": tools, "tool_ids_valid": validIDs,
			}
		default:
			t.Fatalf("unsupported offline case kind %q", task.Kind)
		}
	}
	data, err = json.Marshal(results)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, data, 0600); err != nil {
		t.Fatal(err)
	}
}
