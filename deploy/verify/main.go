// Command verify checks a deployed gateway without changing its configuration.
// --expect-unavailable validates failure handling; it is not model acceptance.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type result struct {
	Name         string `json:"name"`
	Passed       bool   `json:"passed"`
	Status       int    `json:"status"`
	Milliseconds int64  `json:"milliseconds"`
	Error        string `json:"error,omitempty"`
}

func main() {
	base := flag.String("base", "http://127.0.0.1:10015", "business URL")
	unavailable := flag.Bool("expect-unavailable", false, "check unavailable-backend behavior, not model acceptance")
	flag.Parse()
	client := &http.Client{Timeout: 150 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var results []result
	var mu sync.Mutex
	check := func(name, method, url, body, key, origin string, want int, validate func([]byte, http.Header) bool) {
		start := time.Now()
		r := result{Name: name}
		req, err := http.NewRequest(method, url, strings.NewReader(body))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			if key != "" {
				req.Header.Set("Authorization", "Bearer "+key)
			}
			if origin != "" {
				req.Header.Set("Origin", origin)
			}
			var response *http.Response
			response, err = client.Do(req)
			if err == nil {
				r.Status = response.StatusCode
				var data []byte
				data, err = io.ReadAll(io.LimitReader(response.Body, 2*1024*1024+1))
				response.Body.Close()
				if err == nil {
					r.Passed = r.Status == want && len(data) <= 2*1024*1024 && (validate == nil || validate(data, response.Header))
				}
			}
		}
		if err != nil {
			r.Error = err.Error()
		}
		r.Milliseconds = time.Since(start).Milliseconds()
		mu.Lock()
		results = append(results, r)
		mu.Unlock()
	}
	validJSON := func(b []byte, _ http.Header) bool { return json.Valid(b) }
	errorCode := func(code string) func([]byte, http.Header) bool {
		return func(b []byte, h http.Header) bool {
			var v struct {
				Error struct {
					Code string `json:"code"`
					ID   string `json:"request_id"`
				} `json:"error"`
			}
			return json.Unmarshal(b, &v) == nil && v.Error.Code == code && v.Error.ID != "" && v.Error.ID == h.Get("X-Request-ID")
		}
	}
	bkey := os.Getenv("GATEWAY_API_KEY")
	check("health", "GET", *base+"/health", "", "", "", 200, func(b []byte, _ http.Header) bool { return bytes.Contains(b, []byte(`"status":"ok"`)) })
	check("business-openapi", "GET", *base+"/openapi.json", "", "", "", 200, validJSON)
	check("business-docs-removed", "GET", *base+"/docs", "", "", "", 404, errorCode("not_found"))
	for _, path := range []string{"/", "/index.html", "/app.js", "/backends.js", "/style.css", "/admin/v1/status", "/admin/v1/backends"} {
		check("removed-management"+path, "GET", *base+path, "", "", "", 404, errorCode("not_found"))
	}
	for i, body := range []string{`null`, `{}`, `{"messages":[]}`, `{"messages":[{"role":"user","content":"probe"}],"stream":"true"}`} {
		check(fmt.Sprintf("invalid-request-%d", i), "POST", *base+"/v1/chat/completions", body, bkey, "", 400, errorCode("invalid_request"))
	}
	status := 200
	modelCheck := func(b []byte, _ http.Header) bool {
		var v struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		return json.Unmarshal(b, &v) == nil && len(v.Data) > 0 && v.Data[0].ID != ""
	}
	if *unavailable {
		status = 503
		modelCheck = errorCode("model_discovery_failed")
	}
	check("models", "GET", *base+"/v1/models", "", bkey, "", status, modelCheck)
	for _, stream := range []bool{false, true} {
		body := fmt.Sprintf(`{"messages":[{"role":"user","content":"Reply with a short greeting."}],"max_tokens":1024,"stream":%t,"metadata":{"workspace":{"purpose":"deployment_validation"}}}`, stream)
		validate := func(b []byte, _ http.Header) bool {
			var v struct {
				Choices []struct {
					Message struct {
						Content string `json:"content"`
					} `json:"message"`
				} `json:"choices"`
			}
			return json.Unmarshal(b, &v) == nil && len(v.Choices) > 0 && strings.TrimSpace(v.Choices[0].Message.Content) != ""
		}
		if stream {
			validate = func(b []byte, h http.Header) bool {
				if !strings.HasPrefix(h.Get("Content-Type"), "text/event-stream") {
					return false
				}
				done, finish, content := false, false, false
				for _, line := range strings.Split(string(b), "\n") {
					if !strings.HasPrefix(line, "data:") {
						continue
					}
					payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
					if payload == "[DONE]" {
						done = true
						continue
					}
					var v struct {
						Error   json.RawMessage `json:"error"`
						Choices []struct {
							Delta struct {
								Content string `json:"content"`
							} `json:"delta"`
							Finish *string `json:"finish_reason"`
						} `json:"choices"`
					}
					if json.Unmarshal([]byte(payload), &v) != nil || len(v.Error) > 0 {
						return false
					}
					for _, c := range v.Choices {
						content = content || c.Delta.Content != ""
						finish = finish || (c.Finish != nil && *c.Finish != "")
					}
				}
				return done && finish && content
			}
		}
		if *unavailable {
			validate = errorCode("model_discovery_failed")
		}
		check(fmt.Sprintf("generation-stream-%t", stream), "POST", *base+"/v1/chat/completions", body, bkey, "", status, validate)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			check(fmt.Sprintf("concurrent-health-%02d", i), "GET", *base+"/health", "", "", "", 200, validJSON)
		}(i)
	}
	wg.Wait()
	passed := 0
	for _, r := range results {
		if r.Passed {
			passed++
		}
	}
	mode := "model-smoke"
	if *unavailable {
		mode = "unavailable-backend-diagnostic"
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"mode": mode, "passed": passed, "total": len(results), "results": results})
	if passed != len(results) {
		os.Exit(1)
	}
}
