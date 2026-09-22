package main

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

// Exercise the actual v2 executable. Runtime files and legacy CLI cannot bootstrap it.
func TestBinaryLifecycle(t *testing.T) {
	if testing.Short() || runtime.GOOS == "windows" {
		t.Skip("Unix process integration")
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "aigate")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if out, e := exec.CommandContext(ctx, "go", "build", "-mod=vendor", "-o", binary, ".").CombinedOutput(); e != nil {
		t.Fatalf("build: %v %s", e, out)
	}
	for _, args := range [][]string{{"switch", "single", "a"}, {"status"}, {"check", "--config", "missing.json"}, {"serve", "--config", "missing.json"}} {
		if e := exec.CommandContext(ctx, binary, args...).Run(); e == nil {
			t.Fatalf("legacy CLI accepted: %v", args)
		}
	}
	address := func() string {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		a := l.Addr().String()
		_ = l.Close()
		return a
	}
	business, control := address(), address()
	logs := &lockedBuffer{}
	command := exec.CommandContext(ctx, binary, "serve", "--id", "g1", "--listen", business, "--control-listen", control, "--profile-listen=", "--data-root", dir, "--shutdown-timeout", "3s")
	command.Env = append(os.Environ(), "AIGATE_CONTROL_KEY="+strings.Repeat("k", 32))
	command.Stderr = logs
	if e := command.Start(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			<-done
		}
	}()
	client := &http.Client{Timeout: 3 * time.Second}
	var status struct {
		Boot string `json:"boot_id"`
	}
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		req, _ := http.NewRequest("GET", "http://"+control+"/control/v2/status", nil)
		req.Header.Set("Authorization", "Bearer "+strings.Repeat("k", 32))
		r, e := client.Do(req)
		if e == nil {
			_ = json.NewDecoder(r.Body).Decode(&status)
			r.Body.Close()
			if status.Boot != "" {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status.Boot == "" {
		t.Fatalf("not ready: %s", logs.String())
	}
	r, e := client.Get("http://" + business + "/v1/models")
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	if r.StatusCode != 503 {
		t.Fatal("unconfigured process served business")
	}
	p := newProtocolBackend(t)
	send := func(seq uint64, action string, payload any) {
		cmd := RuntimeCommand{ProtocolVersion: 2, OperationID: "initial", GatewayID: "g1", ExpectedBootID: status.Boot, Sequence: seq, Action: action, Payload: encodeJSON(payload)}
		req, _ := http.NewRequest("POST", "http://"+control+"/control/v2/commands", bytes.NewReader(encodeJSON(cmd)))
		req.Header.Set("Authorization", "Bearer "+strings.Repeat("k", 32))
		res, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		var result RuntimeResult
		_ = json.NewDecoder(res.Body).Decode(&result)
		if result.Status != "completed" {
			t.Fatalf("%s: %+v", action, result)
		}
	}
	send(1, "prepare", snapshotV2("r1", p.server.URL, 1))
	send(2, "quiesce", map[string]any{})
	send(3, "activate", map[string]any{"admission": true})
	r, e = client.Post("http://"+business+"/v1/chat/completions", "application/json", strings.NewReader(basicBody))
	if e != nil {
		t.Fatal(e)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("%d %s", r.StatusCode, body)
	}
	if e = command.Process.Signal(os.Interrupt); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e != nil {
		t.Fatal(e, logs.String())
	}
	waited = true
	for _, name := range []string{"config.json", "management-state.json", "routing.json", "control.sock"} {
		if _, e = os.Stat(filepath.Join(dir, name)); !os.IsNotExist(e) {
			t.Fatal("runtime configuration persisted", name)
		}
	}
}
