package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const businessReadHeaderTimeout = 30 * time.Second

func businessHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: businessReadHeaderTimeout}
}

func run(args []string, out, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "version" {
		_, e := fmt.Fprintln(out, "aigate control/v2")
		return e
	}
	if len(args) > 0 && args[0] == "serve" {
		args = args[1:]
	}
	f := flag.NewFlagSet("aigate serve", flag.ContinueOnError)
	f.SetOutput(stderr)
	id := f.String("id", os.Getenv("AIGATE_ID"), "platform gateway ID")
	listen := f.String("listen", "127.0.0.1:8080", "business listener")
	control := f.String("control-listen", "127.0.0.1:18083", "platform control listener")
	profile := f.String("profile-listen", "127.0.0.1:18082", "observation listener")
	dataRoot := f.String("data-root", "/var/lib/aigate", "observation data root (no configuration/state files)")
	allowed := f.String("allowed-cidrs", "127.0.0.0/8,::1/128", "platform control source CIDRs")
	shutdown := f.Duration("shutdown-timeout", 32*time.Minute, "graceful shutdown deadline")
	if e := f.Parse(args); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	if f.NArg() != 0 {
		return fmt.Errorf("management CLI removed: use the platform API")
	}
	if *shutdown <= 0 {
		return fmt.Errorf("invalid shutdown timeout")
	}
	c, e := newControlledGateway(*id, os.Getenv("AIGATE_CONTROL_KEY"), strings.Split(*allowed, ","), stderr)
	if e != nil {
		return e
	}
	defer c.Close()
	c.dataRoot = *dataRoot
	servers := []*http.Server{businessHTTPServer(*listen, c), {Addr: *control, Handler: c.ControlHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}}
	if *profile != "" {
		servers = append(servers, &http.Server{Addr: *profile, Handler: c.ProfileHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second})
	}
	listeners := []net.Listener{}
	for i, s := range servers {
		l, e := net.Listen("tcp", s.Addr)
		if e != nil {
			for _, old := range listeners {
				_ = old.Close()
			}
			return e
		}
		if i == 0 {
			l = installConnectionDiagnostics(s, l, c.connections)
		}
		listeners = append(listeners, l)
		defer s.Close()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, len(servers))
	for i, s := range servers {
		go func(s *http.Server, l net.Listener) { done <- s.Serve(l) }(s, listeners[i])
	}
	select {
	case e := <-done:
		if !errors.Is(e, http.ErrServerClosed) {
			return e
		}
	case <-ctx.Done():
	}
	c.mu.Lock()
	c.admission = false
	c.retired = true
	c.cancel()
	c.mu.Unlock()
	closing, cancel := context.WithTimeout(context.Background(), *shutdown)
	defer cancel()
	if e := servers[0].Shutdown(closing); e != nil {
		return fmt.Errorf("graceful shutdown timed out: %w", e)
	}
	return nil
}
