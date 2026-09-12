package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// Exercise the actual executable, including signal handling with an active SSE.
func TestBinaryLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("binary lifecycle integration test")
	}
	if runtime.GOOS == "windows" {
		t.Skip("Unix signal lifecycle test")
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "aigate")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, output)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	u := upstreamServer(t, "binary-model", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
	c := fixtureConfig(t, u.URL)
	c.TimeoutSeconds = 10
	c.RoutingFile = filepath.Join(dir, "routing.json")
	if err := publishPolicy(c, Policy{Mode: "single", Backend: "api1"}); err != nil {
		t.Fatal(err)
	}
	configFile := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configFile, encodeJSON(c), 0600); err != nil {
		t.Fatal(err)
	}
	check := exec.CommandContext(ctx, binary, "check", "--config", configFile)
	if output, err := check.CombinedOutput(); err != nil || !bytes.Contains(output, []byte("配置有效")) {
		t.Fatalf("check failed: %v: %s", err, output)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	logs := &lockedBuffer{}
	command := exec.CommandContext(ctx, binary, "serve", "--config", configFile, "--listen", address, "--shutdown-timeout", "3s")
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatal(err)
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
	client := &http.Client{Timeout: 4 * time.Second}
	ready := false
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		response, err := client.Get("http://" + address + "/health")
		if err == nil {
			_ = response.Body.Close()
			ready = response.StatusCode == 200
			if ready {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("binary did not start")
	}
	if output, err := exec.CommandContext(ctx, binary, "switch", "single", "api1", "--config", configFile).CombinedOutput(); err != nil || !bytes.Contains(output, []byte("single")) {
		t.Fatalf("binary switch failed: %v", err)
	}
	response, err := client.Post("http://"+address+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[{"role":"user","content":"binary test"}],"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	if line, err := reader.ReadString('\n'); err != nil || line != "data: first\n" {
		t.Fatal("binary did not stream")
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	// Wait for the listener to close, proving shutdown has begun while the stream remains open.
	for time.Now().Before(until) {
		connection, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err != nil {
			break
		}
		_ = connection.Close()
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-done:
		waited = true
		t.Fatalf("process exited before SSE finished: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	rest, err := io.ReadAll(reader)
	if err != nil || !bytes.Contains(rest, []byte("data: [DONE]")) {
		t.Fatal("shutdown interrupted active SSE")
	}
	select {
	case err := <-done:
		waited = true
		if err != nil {
			t.Fatalf("shutdown failed: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("binary did not exit")
	}
	if !strings.Contains(logs.String(), `"outcome":"completed"`) {
		t.Fatal("binary audit did not complete")
	}
}
