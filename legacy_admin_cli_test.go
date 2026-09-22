package main

import (
	"fmt"
	"io"
	"strings"
	"time"
)

type adminCLIOptions struct {
	path, socket, id string
	noWait, json     bool
	positional       []string
}

func parseAdminCLI(args []string) (adminCLIOptions, error) {
	o := adminCLIOptions{path: configPath()}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--config", "-config", "--socket", "--operation-id":
			flag := args[i]
			i++
			if i == len(args) {
				return o, fmt.Errorf("%s 缺少值", flag)
			}
			switch flag {
			case "--config", "-config":
				o.path = args[i]
			case "--socket":
				o.socket = args[i]
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
	if kind != "backends" && ((kind == "status" && len(o.positional) != 0) || (kind != "status" && len(o.positional) != 1)) {
		return fmt.Errorf("用法: aigate %s [BACKEND] [--config PATH] [--json] [--no-wait]", kind)
	}
	if o.socket == "" {
		o.socket = c.Management.Socket
	}
	call := func(command string, body, result any) error {
		return callControl(o.socket, command, body, result)
	}
	if kind == "backends" {
		return backendsCLI(o, call, out)
	}
	var state ManagementStatus
	if err = call("status", nil, &state); err != nil {
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
		err = call(kind, OperationRequest{o.id, target, state.Revision}, &op)
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
			if err = call("operation", op.ID, &op); err != nil {
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
