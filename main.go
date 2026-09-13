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
	"path/filepath"
	"syscall"
	"time"
)

func configPath() string {
	if path := os.Getenv("AIGATE_CONFIG"); path != "" {
		return path
	}
	return "config.json"
}

func publishPolicy(c *Config, policy Policy) error {
	if c.RoutingFile == "" {
		return fmt.Errorf("请先在配置中设置 routing_file 并重启一次网关")
	}
	if err := policy.validate(c); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(c.RoutingFile), ".routing-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	// Preserve an existing file's permissions; a new file is owner-readable only.
	if info, err := os.Stat(c.RoutingFile); err == nil {
		if err := f.Chmod(info.Mode().Perm()); err != nil {
			return err
		}
	}
	if _, err := f.Write(append(encodeJSON(policy), '\n')); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), c.RoutingFile)
}

func switchCommand(args []string, out io.Writer) error {
	options, parseErr := parseAdminCLI(args)
	if parseErr == nil {
		if c, err := loadConfig(options.path); err == nil && c.Management.Enabled {
			return managedCLI("switch", args, out)
		}
	}
	path := configPath()
	positional := []string{}
	for i := 0; i < len(args); i++ {
		if args[i] == "--config" || args[i] == "-config" {
			i++
			if i == len(args) {
				return fmt.Errorf("--config 缺少路径")
			}
			path = args[i]
		} else {
			positional = append(positional, args[i])
		}
	}
	if len(positional) != 2 || positional[0] != "single" {
		return fmt.Errorf("用法: aigate switch single BACKEND [--config PATH]（仅支持单后端切换）")
	}
	policy := Policy{Mode: positional[0], Backend: positional[1]}
	c, err := loadConfig(path)
	if err != nil {
		return err
	}
	// Switching requires no upstream credentials and makes no network calls.
	if err := publishPolicy(c, policy); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(encodeJSON(policy)))
	return err
}

func run(args []string, out, stderr io.Writer) error {
	if len(args) > 0 {
		switch args[0] {
		case "switch", "status", "warmup", "maintenance", "restore", "confirm-drain":
			for _, arg := range args[1:] {
				if arg == "--help" || arg == "-h" {
					_, err := fmt.Fprintln(out, "管理命令: status | warmup BACKEND | switch single BACKEND | maintenance BACKEND | restore BACKEND | confirm-drain BACKEND\n选项: --config PATH --admin-url URL --operation-id ID --no-wait --json\n未启用 management 时，switch 保留 single 路由文件切换。")
					return err
				}
			}
		}
	}
	if len(args) > 0 {
		switch args[0] {
		case "status", "warmup", "maintenance", "restore", "confirm-drain":
			return managedCLI(args[0], args[1:], out)
		}
	}
	if len(args) > 0 && args[0] == "switch" {
		return switchCommand(args[1:], out)
	}
	command := "serve"
	if len(args) > 0 && (args[0] == "serve" || args[0] == "check") {
		command = args[0]
		args = args[1:]
	}
	flags := flag.NewFlagSet("aigate "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "用法: aigate [serve|check] [选项]\n管理命令: status, warmup, switch, maintenance, restore, confirm-drain；各命令支持 --help")
		flags.PrintDefaults()
	}
	path := flags.String("config", configPath(), "主配置路径，也可设置 AIGATE_CONFIG")
	listen := flags.String("listen", "127.0.0.1:8080", "监听地址")
	shutdown := flags.Duration("shutdown-timeout", 30*time.Second, "退出时等待在途请求的最长时间")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("不支持的参数: %s", flags.Arg(0))
	}
	if *shutdown <= 0 {
		return fmt.Errorf("shutdown-timeout 必须为正")
	}
	c, err := loadConfig(*path)
	if err != nil {
		return err
	}
	if command == "check" {
		c, err = effectiveManagedConfig(c)
		if err != nil {
			return err
		}
		if err = c.validate(); err != nil {
			return err
		}
		if c.Management.Enabled {
			_, err = readManagedState(c)
		} else {
			_, policyErr := newPool(c, nil).policy()
			if policyErr != nil {
				err = policyErr
			}
		}
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, "配置有效（未检测下游）")
		return err
	}
	gateway, err := newGateway(c, stderr)
	if err != nil {
		return err
	}
	defer gateway.Close()
	server := &http.Server{Addr: *listen, Handler: gateway, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	var admin *http.Server
	var adminListener net.Listener
	if c.Management.Enabled {
		adminListener, err = net.Listen("tcp", c.Management.Listen)
		if err != nil {
			return err
		}
		defer adminListener.Close()
		admin = &http.Server{Handler: gateway.adminHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	}
	var profileServer *http.Server
	var profileListener net.Listener
	if gateway.profile != nil {
		profileListener, err = net.Listen("tcp", c.Profile.Listen)
		if err != nil {
			return err
		}
		defer profileListener.Close()
		profileServer = &http.Server{Handler: gateway.profile.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
		defer profileServer.Close()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 3)
	if profileServer != nil {
		go func() { done <- profileServer.Serve(profileListener) }()
	}
	go func() { done <- server.Serve(listener) }()
	if admin != nil {
		go func() { done <- admin.Serve(adminListener) }()
	}
	select {
	case err := <-done:
		_ = server.Close()
		if admin != nil {
			_ = admin.Close()
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		stop()
		if gateway.management != nil {
			gateway.management.stop()
		}
		closing, cancel := context.WithTimeout(context.Background(), *shutdown)
		defer cancel()
		if admin != nil {
			if err := admin.Shutdown(closing); err != nil {
				_ = admin.Close()
			}
		}
		if err := server.Shutdown(closing); err != nil {
			_ = server.Close()
			return fmt.Errorf("优雅退出超时，已关闭剩余连接: %w", err)
		}
		return nil
	}
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "aigate:", err)
		os.Exit(1)
	}
}
