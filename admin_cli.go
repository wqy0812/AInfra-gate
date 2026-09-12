package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type adminCLIOptions struct {
	path, base, id string
	noWait, json   bool
	positional     []string
}

func parseAdminCLI(args []string) (adminCLIOptions, error) {
	o := adminCLIOptions{path: configPath()}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--config", "-config", "--admin-url", "--operation-id":
			flag := args[i]
			i++
			if i == len(args) {
				return o, fmt.Errorf("%s 缺少值", flag)
			}
			switch flag {
			case "--config", "-config":
				o.path = args[i]
			case "--admin-url":
				o.base = args[i]
			case "--operation-id":
				o.id = args[i]
			}
		case "--no-wait":
			o.noWait = true
		case "--json":
			o.json = true
		default:
			if strings.HasPrefix(args[i], "-") {
				return o, fmt.Errorf("不支持的参数 %s", args[i])
			}
			o.positional = append(o.positional, args[i])
		}
	}
	return o, nil
}

func managedCLI(kind string, args []string, out io.Writer) error {
	o, err := parseAdminCLI(args)
	if err != nil {
		return err
	}
	c, err := loadConfig(o.path)
	if err != nil {
		return err
	}
	if !c.Management.Enabled {
		return fmt.Errorf("此命令需要启用 management")
	}
	if kind == "switch" {
		if len(o.positional) != 2 || o.positional[0] != "single" {
			return fmt.Errorf("维护模式仅支持 switch single BACKEND")
		}
		o.positional = o.positional[1:]
	}
	if (kind == "status" && len(o.positional) != 0) || (kind != "status" && len(o.positional) != 1) {
		return fmt.Errorf("用法: aigate %s [BACKEND] [--config PATH] [--json] [--no-wait]", kind)
	}
	if o.base == "" {
		host, port, e := net.SplitHostPort(c.Management.Listen)
		if e != nil {
			return e
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "127.0.0.1"
		}
		o.base = "http://" + net.JoinHostPort(host, port)
	}
	u, err := url.Parse(o.base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("admin-url 必须为无凭据的 HTTP(S) 地址")
	}
	key := os.Getenv(c.Management.APIKeyEnv)
	if key == "" {
		return fmt.Errorf("缺少管理密钥环境变量 %s", c.Management.APIKeyEnv)
	}
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	call := func(method, path string, body any, result any) error {
		var data []byte
		if body != nil {
			data = encodeJSON(body)
		}
		req, e := http.NewRequest(method, strings.TrimRight(o.base, "/")+"/admin/v1/"+path, bytes.NewReader(data))
		if e != nil {
			return e
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, e := client.Do(req)
		if e != nil {
			return fmt.Errorf("无法访问管理服务")
		}
		defer resp.Body.Close()
		data, e = io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		if e != nil {
			return fmt.Errorf("管理响应读取失败")
		}
		if resp.StatusCode >= 300 {
			var envelope struct {
				Error ManagementError `json:"error"`
			}
			if json.Unmarshal(data, &envelope) == nil && envelope.Error.Message != "" {
				return fmt.Errorf("%s (%s; operation_id=%s)", envelope.Error.Message, envelope.Error.Code, envelope.Error.OperationID)
			}
			return fmt.Errorf("管理服务返回 HTTP %d", resp.StatusCode)
		}
		if decodeJSON(data, result) != nil {
			return fmt.Errorf("管理响应无效")
		}
		return nil
	}
	var state ManagementStatus
	if err = call("GET", "status", nil, &state); err != nil {
		return err
	}
	if kind == "status" {
		if o.json {
			_, err = fmt.Fprintln(out, string(encodeJSON(state)))
			return err
		}
		fmt.Fprintf(out, "当前后端: %s  路由版本: %d  操作: %s\n", state.ActiveBackend, state.RouteVersion, state.Busy)
		for _, name := range state.Order {
			b := state.Backends[name]
			n := state.Counts[name]
			fmt.Fprintf(out, "%s: 业务=%d SSE=%d 探测=%d 维护=%t 排空待确认=%t\n", name, n.Business, n.Streams, n.Probes, b.Maintenance, b.DrainUnknown)
		}
		if state.Alert != "" {
			fmt.Fprintln(out, state.Alert)
		}
		return nil
	}
	target := o.positional[0]
	if o.id == "" {
		o.id = randomID()
	}
	var op Operation
	if old := operationByID(&state.ManagedState, o.id); old != nil {
		if old.Kind != kind || old.Target != target {
			return fmt.Errorf("operation-id 已用于不同操作")
		}
		op = *old
	} else {
		path := "backends/" + url.PathEscape(target) + "/" + kind
		if kind == "switch" {
			path = "switches"
		}
		if kind == "warmup" {
			path = "warmups"
		}
		err = call("POST", path, OperationRequest{o.id, target, state.Revision}, &op)
		if err != nil {
			return fmt.Errorf("%w；可使用 --operation-id %s 查询或重试同一操作", err, o.id)
		}
	}
	if !o.json {
		fmt.Fprintf(out, "操作 %s: %s → %s\n", op.ID, op.Source, op.Target)
	}
	if !o.noWait {
		last := ""
		for op.Outcome == "running" {
			if !o.json && op.Phase != last {
				fmt.Fprintln(out, op.Phase)
				last = op.Phase
			}
			time.Sleep(time.Second)
			if err = call("GET", "operations/"+url.PathEscape(op.ID), nil, &op); err != nil {
				return fmt.Errorf("%w；操作 %s 可能仍在后台执行", err, op.ID)
			}
		}
	}
	if o.json {
		fmt.Fprintln(out, string(encodeJSON(op)))
	} else {
		fmt.Fprintf(out, "%s %s\n", op.Outcome, op.Error)
	}
	if op.Outcome != "completed" && op.Outcome != "running" {
		return fmt.Errorf("操作未成功完成: %s", op.Outcome)
	}
	return nil
}
