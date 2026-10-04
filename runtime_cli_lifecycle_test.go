package main

import (
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRuntimeCLIStartsAndShutsDownGracefully(t *testing.T) {
	t.Setenv("AIGATE_ID", "gateway")
	t.Setenv("AIGATE_CONTROL_KEY", strings.Repeat("k", 32))
	// SIGTERM targets this disposable go test process, never a gateway service.
	freeAddress := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		listener.Close()
		return address
	}
	business, control, profile, data := freeAddress(), freeAddress(), freeAddress(), t.TempDir()
	done := make(chan error, 1)
	go func() {
		done <- run([]string{"serve", "--listen", business, "--control-listen", control,
			"--profile-listen", profile, "--data-root", data, "--shutdown-timeout", "2s"}, io.Discard, io.Discard)
	}()
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := client.Get("http://" + business + "/health")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				break
			}
		}
		select {
		case err := <-done:
			t.Fatal("startup failed", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("business listener did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CLI did not stop")
	}
	for _, address := range []string{business, control, profile} {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatal("listener leaked", address, err)
		}
		listener.Close()
	}
}
