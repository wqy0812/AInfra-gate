package main

import (
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func controlTempDir(t *testing.T) string {
	t.Helper()
	// Unix socket paths are limited to 104 bytes on macOS.
	dir, err := os.MkdirTemp("/tmp", "aigate-cli-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func startTestControl(t *testing.T, g *Gateway) *controlServer {
	t.Helper()
	server, err := listenControl(g.management, g.config.Management.Socket)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	t.Cleanup(func() { server.Close(); <-done })
	return server
}

func TestControlSocketPermissionsAndLifecycle(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	c := managedConfig(t, one, two)
	g, _, _ := serveTest(t, c)
	s := startTestControl(t, g)
	info, err := os.Stat(c.Management.Socket)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("socket is not private", err)
	}
	info, err = os.Stat(filepath.Dir(c.Management.Socket))
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal("socket directory is not private", err)
	}
	if other, err := listenControl(g.management, c.Management.Socket); err == nil {
		other.Close()
		t.Fatal("live socket replaced")
	}
	var status ManagementStatus
	if err := callControl(c.Management.Socket, "status", nil, &status); err != nil {
		t.Fatal(err)
	}
	if status.ActiveBackend != "api1" {
		t.Fatal("wrong status")
	}
	idle, err := net.Dial("unix", c.Management.Socket)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(c.Management.Socket); !os.IsNotExist(err) {
		t.Fatal("socket remained after close")
	}
	if err := callControl(c.Management.Socket, "status", nil, &status); err == nil {
		t.Fatal("closed socket accepted command")
	}
	// Recover a socket left by an unclean shutdown; ordinary files must survive.
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: c.Management.Socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	stale.Close()
	restarted, err := listenControl(g.management, c.Management.Socket)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Close()
	if err := os.WriteFile(c.Management.Socket, []byte("protected"), 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := listenControl(g.management, c.Management.Socket); err == nil {
		other.Close()
		t.Fatal("regular file replaced")
	}
	data, _ := os.ReadFile(c.Management.Socket)
	if string(data) != "protected" {
		t.Fatal("regular file modified")
	}
	os.Remove(c.Management.Socket)
	if err := os.Chmod(filepath.Dir(c.Management.Socket), 0755); err != nil {
		t.Fatal(err)
	}
	if other, err := listenControl(g.management, c.Management.Socket); err == nil {
		other.Close()
		t.Fatal("shared directory accepted")
	}
}

func TestControlRejectsMalformedAndOversizedCommands(t *testing.T) {
	one, two := newProtocolBackend(t), newProtocolBackend(t)
	c := managedConfig(t, one, two)
	g, _, _ := serveTest(t, c)
	startTestControl(t, g)
	for _, data := range []string{
		`{"command":"status","unknown":true}`,
		`{"command":"status","payload":{"unexpected":true}}`,
		`{"command":"unsupported"}`,
		`{"command":"switch","payload":{"unknown":true}}`,
		`{"command":"backend-create","payload":{"unknown":true}}`,
	} {
		conn, err := net.Dial("unix", c.Management.Socket)
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(time.Second))
		if _, err = io.WriteString(conn, data+"\n"); err != nil {
			t.Fatal(err)
		}
		var response controlResponse
		err = json.NewDecoder(conn).Decode(&response)
		conn.Close()
		if err != nil || response.Error == nil {
			t.Fatalf("invalid command accepted: %s (%v)", data, err)
		}
	}
	conn, err := net.Dial("unix", c.Management.Socket)
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(time.Second))
	_, _ = io.WriteString(conn, strings.Repeat("x", maxControlRequest)+"\n")
	var response controlResponse
	err = json.NewDecoder(conn).Decode(&response)
	conn.Close()
	if err == nil {
		t.Fatal("oversized frame accepted")
	}
	if g.management.status().ActiveBackend != "api1" {
		t.Fatal("invalid command changed route")
	}
	var status ManagementStatus
	if err := callControl(c.Management.Socket, "status", nil, &status); err != nil {
		t.Fatal("invalid clients blocked later commands", err)
	}
}
