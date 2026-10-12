package aigate

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Both actual executables are exercised; neither relies on the other to start.
func TestBinaryLifecycle(t *testing.T) {
	if testing.Short() || runtime.GOOS == "windows" {
		t.Skip("Unix process integration")
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	binaries := map[string]string{}
	for _, name := range []string{"aigate-switch", "aigate-proxy"} {
		binaries[name] = filepath.Join(dir, name)
		if out, err := exec.CommandContext(ctx, "go", "build", "-mod=vendor", "-o", binaries[name], "./cmd/"+name).CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v %s", name, err, out)
		}
	}
	address := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		a := l.Addr().String()
		l.Close()
		return a
	}
	start := func(binary string, args []string, env []string) (*exec.Cmd, func()) {
		logs := &lockedBuffer{}
		command := exec.CommandContext(ctx, binary, args...)
		command.Env = append(os.Environ(), env...)
		command.Stderr = logs
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		waited := false
		stop := func() {
			if waited {
				return
			}
			command.Process.Signal(os.Interrupt)
			select {
			case err := <-done:
				waited = true
				if err != nil {
					t.Fatal(err, logs.String())
				}
			case <-time.After(10 * time.Second):
				command.Process.Kill()
				<-done
				waited = true
				t.Fatal("service failed to stop", logs.String())
			}
		}
		t.Cleanup(stop)
		return command, stop
	}
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	ready := func(url string) {
		until := time.Now().Add(5 * time.Second)
		for time.Now().Before(until) {
			r, err := client.Get(url + "/health")
			if err == nil {
				r.Body.Close()
				if r.StatusCode == 200 {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("not ready", url)
	}
	upstream := newProtocolBackend(t)
	proxyAddr, switchAddr, controlAddr := address(), address(), address()
	config := filepath.Join(dir, "proxy.json")
	cfg := ProxyConfig{ServiceID: "p", RouterID: "r1", Backend: &Backend{Adapter: "openai", URL: upstream.server.URL + "/v1/chat/completions", ModelsURL: upstream.server.URL + "/v1/models"}, Settings: json.RawMessage(`{"profile":{"enabled":true}}`)}
	if err := os.WriteFile(config, encodeJSON(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	_, stopProxy := start(binaries["aigate-proxy"], []string{"--config", config, "--listen", proxyAddr, "--profile-listen=", "--data-root", filepath.Join(dir, "observations"), "--shutdown-timeout", "8s"}, []string{"AIGATE_CONTROL_KEY=", "AIGATE_PROFILE_KEY="})
	proxyURL := "http://" + proxyAddr
	ready(proxyURL)
	response, err := client.Get(proxyURL + "/runtime/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	var state struct {
		Digest string `json:"config_digest"`
	}
	json.NewDecoder(response.Body).Decode(&state)
	response.Body.Close()
	if state.Digest == "" {
		t.Fatal("missing config digest")
	}
	_, stopSwitch := start(binaries["aigate-switch"], []string{"--id", "g1", "--listen", switchAddr, "--control-listen", controlAddr, "--shutdown-timeout", "2s"}, []string{"AIGATE_CONTROL_KEY=" + strings.Repeat("k", 32)})
	switchURL := "http://" + switchAddr
	ready(switchURL)
	statusReq, _ := http.NewRequest("GET", "http://"+controlAddr+"/control/v3/status", nil)
	statusReq.Header.Set("Authorization", "Bearer "+strings.Repeat("k", 32))
	response, err = client.Do(statusReq)
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		Boot string `json:"boot_id"`
	}
	json.NewDecoder(response.Body).Decode(&status)
	response.Body.Close()
	response, err = client.Get(switchURL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal("unconfigured switch served")
	}
	snapshot := SwitchSnapshot{RouterID: "r1", RouterVersion: 1, GatewayConfigVersion: 1, BindingRevision: 1, TargetURL: proxyURL, ConfigDigest: state.Digest}
	for i, action := range []string{"prepare", "quiesce", "activate"} {
		payload := any(map[string]any{})
		if action == "prepare" {
			payload = snapshot
		}
		if action == "activate" {
			payload = map[string]any{"admission": true}
		}
		command := RuntimeCommand{ProtocolVersion: 3, OperationID: "initial", GatewayID: "g1", ExpectedBootID: status.Boot, Sequence: uint64(i + 1), Action: action, Payload: encodeJSON(payload)}
		req, _ := http.NewRequest("POST", "http://"+controlAddr+"/control/v3/commands", bytes.NewReader(encodeJSON(command)))
		req.Header.Set("Authorization", "Bearer "+strings.Repeat("k", 32))
		response, err = client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		var result RuntimeResult
		if json.Unmarshal(body, &result) != nil || result.Status != "completed" {
			t.Fatal(action, string(body))
		}
	}
	response, err = client.Post(switchURL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`))
	body := splitRead(t, response, err)
	if response.StatusCode != 200 || !strings.Contains(body, "[DONE]") {
		t.Fatal(body)
	}
	for _, name := range []string{"X-Aigate-Backend", "X-Aigate-Route-Version", "X-Aigate-Model"} {
		if len(response.Header.Values(name)) != 1 {
			t.Fatalf("duplicated target header %s: %v", name, response.Header.Values(name))
		}
	}
	stopSwitch()
	response, err = client.Get(proxyURL + "/v1/models")
	body = splitRead(t, response, err)
	if response.StatusCode != 200 {
		t.Fatal("proxy depended on switch", body)
	}
	client.CloseIdleConnections()
	stopProxy()
	for _, addr := range []string{proxyAddr, switchAddr, controlAddr} {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal("listener leaked", addr, err)
		}
		l.Close()
	}
}
