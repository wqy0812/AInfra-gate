package main

import (
	"fmt"
	"io"
	"os"
)

func backendsCLI(o adminCLIOptions, call func(string, any, any) error, out io.Writer) error {
	args := o.positional
	if o.id != "" || o.noWait {
		return fmt.Errorf("backends 不支持 --operation-id 或 --no-wait；写入操作的 operation_id 由请求文件指定")
	}
	if len(args) == 1 && args[0] == "list" {
		var list BackendList
		if err := call("backends-list", nil, &list); err != nil {
			return err
		}
		_, err := fmt.Fprintln(out, string(encodeJSON(list)))
		return err
	}
	create := len(args) == 2 && args[0] == "create"
	edit := len(args) == 3 && args[0] == "edit"
	if !create && !edit {
		return fmt.Errorf("用法: aigate backends list | create REQUEST.json | edit BACKEND REQUEST.json [--config PATH] [--socket PATH] [--json]")
	}
	f, err := os.Open(args[len(args)-1])
	if err != nil {
		return fmt.Errorf("无法打开后端请求文件: %w", err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil {
		return fmt.Errorf("无法读取后端请求文件: %w", err)
	}
	var req BackendWriteRequest
	if len(data) > 64<<10 || strictDecode(data, &req) != nil {
		return fmt.Errorf("后端请求文件必须为不超过 64 KiB 的有效 JSON 对象")
	}
	if req.OperationID == "" || req.ExpectedRevision == 0 {
		return fmt.Errorf("后端请求文件必须指定 operation_id 和非零 expected_revision；使用 backends list 查询当前 revision")
	}
	command := "backend-create"
	if edit {
		if req.Target != "" && req.Target != args[1] {
			return fmt.Errorf("请求文件的 target 与命令中的后端名称不一致")
		}
		req.Target = args[1]
		command = "backend-edit"
	}
	var op Operation
	if err := call(command, req, &op); err != nil {
		return fmt.Errorf("%w；可使用同一请求文件重试操作 %s", err, req.OperationID)
	}
	if o.json {
		_, err = fmt.Fprintln(out, string(encodeJSON(op)))
	} else {
		_, err = fmt.Fprintf(out, "操作 %s: %s；配置已保存，尚未验证\n", op.ID, op.Outcome)
	}
	return err
}
