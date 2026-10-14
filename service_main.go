package aigate

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

// RunSwitch starts only the switching data plane and authenticated controller.
func RunSwitch(args []string, out, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "version" {
		_, err := fmt.Fprintln(out, "aigate-switch control/v3 (v2 test compatibility)")
		return err
	}
	if len(args) > 0 && args[0] == "serve" {
		args = args[1:]
	}
	f := flag.NewFlagSet("aigate-switch serve", flag.ContinueOnError)
	f.SetOutput(stderr)
	id := f.String("id", os.Getenv("AIGATE_ID"), "platform entry ID")
	listen := f.String("listen", "127.0.0.1:10015", "business listener")
	control := f.String("control-listen", "127.0.0.1:18083", "authenticated platform control listener")
	allowed := f.String("allowed-cidrs", "127.0.0.0/8,::1/128", "control source CIDRs")
	forwardTimeout := f.Duration("forward-timeout", 62*time.Minute, "maximum forwarding duration including upload and client writes")
	shutdown := f.Duration("shutdown-timeout", 62*time.Minute, "graceful shutdown deadline")
	recordRoot := f.String("record-root", "", "traffic recording directory (empty disables recording)")
	recordMax := f.Int64("record-max-bytes", recordingDefaultMaxBytes, "traffic recording capacity in bytes")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if f.NArg() != 0 || *shutdown <= 0 || *forwardTimeout <= 0 || *recordMax <= 0 {
		return fmt.Errorf("invalid arguments; use platform control API for management")
	}
	markKey, err := replayMarkKeyFromEnv()
	if err != nil {
		return err
	}
	c, err := newSwitchGateway(*id, os.Getenv("AIGATE_CONTROL_KEY"), strings.Split(*allowed, ","), stderr)
	if err != nil {
		return err
	}
	defer c.Close()
	if *recordRoot != "" {
		if err = c.enableRecording(*recordRoot, *recordMax, markKey); err != nil {
			return err
		}
	}
	c.forwardTimeout = *forwardTimeout
	servers := []*http.Server{businessHTTPServer(*listen, c), {Addr: *control, Handler: c.ControlHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}}
	return serveProcess(servers, c.connections, func() { c.mu.Lock(); c.admission = false; c.retired = true; c.cancel(); c.mu.Unlock() }, *shutdown)
}

// RunProxy starts an unauthenticated proxy using only a local configuration file.
func RunProxy(args []string, out, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "version" {
		_, err := fmt.Fprintln(out, "aigate-proxy runtime/v1")
		return err
	}
	if len(args) > 0 && args[0] == "serve" {
		args = args[1:]
	}
	f := flag.NewFlagSet("aigate-proxy serve", flag.ContinueOnError)
	f.SetOutput(stderr)
	config := f.String("config", "", "required local proxy JSON config")
	checkOnly := f.Bool("check-config", false, "validate and print redacted normalized configuration without starting or writing data")
	listen := f.String("listen", "127.0.0.1:18084", "business listener")
	profile := f.String("profile-listen", "127.0.0.1:18082", "observation listener (empty disables separate listener)")
	root := f.String("data-root", "/var/lib/aigate-proxy", "observation data root")
	shutdown := f.Duration("shutdown-timeout", 62*time.Minute, "graceful shutdown deadline")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if f.NArg() != 0 || *shutdown <= 0 || *config == "" {
		return fmt.Errorf("--config is required; shutdown timeout must be positive")
	}
	cfg, err := readProxyConfig(*config)
	if err != nil {
		return err
	}
	if *checkOnly {
		p, err := prepareProxyGateway(cfg, *root, stderr, true)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(encodeJSON(p.status())))
		return err
	}
	markKey, err := replayMarkKeyFromEnv()
	if err != nil {
		return err
	}
	p, err := newProxyGateway(cfg, *root, stderr)
	if err != nil {
		return err
	}
	defer p.Close()
	p.markKey = markKey
	servers := []*http.Server{businessHTTPServer(*listen, p)}
	if *profile != "" {
		servers = append(servers, &http.Server{Addr: *profile, Handler: p.ObservationHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second})
	}
	return serveProcess(servers, p.connections, p.quiesce, *shutdown)
}

func serveProcess(servers []*http.Server, diagnostics *connectionDiagnostics, quiesce func(), timeout time.Duration) error {
	listeners := make([]net.Listener, 0, len(servers))
	defer func() {
		for _, s := range servers {
			_ = s.Close()
		}
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	for i, s := range servers {
		l, err := net.Listen("tcp", s.Addr)
		if err != nil {
			return err
		}
		if i == 0 {
			l = installConnectionDiagnostics(s, l, diagnostics)
		}
		listeners = append(listeners, l)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, len(servers))
	for i, s := range servers {
		go func(s *http.Server, l net.Listener) { done <- s.Serve(l) }(s, listeners[i])
	}
	var serveErr error
	select {
	case serveErr = <-done:
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
	case <-ctx.Done():
	}
	quiesce()
	closing, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Keep control/status available while business requests finish.
	if err := servers[0].Shutdown(closing); err != nil {
		return fmt.Errorf("graceful shutdown timed out: %w", err)
	}
	return serveErr
}
