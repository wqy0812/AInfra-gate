# DCU / A3 维护模式使用说明

管理模式支持单网关进程在两个或多个 OpenAI 兼容后端之间执行主备切换。管理仅通过本机 CLI，使用 Go 单二进制和 Unix socket，面向 Linux/macOS。所有模式只支持指定单后端接流；旧 `round_robin` 配置需要迁移为 `single`。

## 启动

```sh
go build -trimpath -o bin/aigate .
cp config.management.example.json config.management.json
export GATEWAY_API_KEY='replace-with-business-key'
./bin/aigate check --config config.management.json
./bin/aigate serve --config config.management.json --listen 127.0.0.1:8080 --shutdown-timeout 32m
```

启动前修改两端 `url`、`models_url` 和必要的下游鉴权环境变量。示例中的回环地址是占位地址；独立网关应使用真实稳定的 Router 服务地址。DCU 使用窄范围的 workspace 对象字符串化示例，A3 使用全部 metadata 值字符串化示例，最终按各下游实际要求设置。

业务默认访问 `127.0.0.1:8080`。管理不提供 Web 页面、HTTP API、OpenAPI 或 TCP 监听。CLI 使用 `management.socket` 指定的 Unix socket，默认位于状态文件同目录下的 `.aigate/control.sock`；相对路径按主配置目录解析。socket 父目录必须为私有目录（0700），socket 权限为 0600，以网关运行用户或 root 执行 CLI。远程操作通过 SSH MCP 在网关主机执行 CLI，不需要浏览器和端口转发。

升级时移除已废弃的 `management.listen`、`management.api_key_env`，改用 `management.socket`；旧字段不再生效。原管理环境变量不再用于授权，业务和画像所需环境变量仍须保留。状态文件格式及现有 v2 后端配置保持兼容。CLI 依赖已运行的网关，不能离线修改管理状态文件。

如果维护包含 DCU/A3 整机重启，网关及其必需的网络入口必须放在独立、持续运行的节点。所有需要排空的业务应经过本网关。

主配置可设置 `"public_model_name": "my-model"`，使模型列表、普通响应及 SSE 在切流前后使用统一对外名称。发给下游的请求、预热验证及管理状态继续使用真实模型名，修改此配置需重启。

## 维护操作

1. 执行 `aigate switch single a3 --config config.management.json`：DCU 继续服务，网关预热 A3，验证通过后切换新请求并等待 DCU 排空。也可先执行 `warmup a3`，切流时仍重新验证。
2. 排空完成后 DCU 保留自动回切资格。准备重启时执行 `maintenance dcu`，**收到锁定成功后再重启**。网关不会停止或重启推理服务。
3. DCU 恢复后执行 `restore dcu`。通过后解除维护锁并恢复备用资格，业务仍在 A3；需要 DCU 接流时再执行 `switch single dcu`。

执行 `status --json` 可查看业务占用、SSE 占用、探测占用、预热步骤和操作历史。SSE 占用是业务占用的子集。请求在模型发现前登记，因此等待发现和等待连接也纳入排空。CLI 退出不取消已受理操作。

排空超时报告 `drain_timeout`，新请求仍在目标端，不强制中断旧请求；计数归零后更新为 completed。统计仅覆盖本网关。取消、超时、断流等异常结束会单独计数，维护前仍需核对下游残留任务和其他入口流量。

## 在线新增与编辑后端

通过 `backends create REQUEST.json` 新增后端，或通过 `backends edit BACKEND REQUEST.json` 编辑已排空的备用端，可设置生成地址、模型发现地址、首选模型、鉴权请求头、模型发现路径、请求覆盖项与兼容参数。只支持 OpenAI 适配器，名称创建后不可改；本版不提供删除与全局配置热更新。

当前接流端必须先切到其他端；业务和探测占用须为零，且不能处于“排空待确认”。预热、切流、恢复或自动回切进行中也不能保存。维护端可在已确认排空后编辑，保存保留维护锁，接流前仍需恢复验证。

保存只做配置校验和持久化，不访问下游、不自动切流。成功后显示“配置已保存，尚未验证”，清除该端旧模型缓存、最近预热结果和自动回切候选资格；历史操作保留。然后使用 `warmup BACKEND` 或 `switch single BACKEND` 验证实际协议，失败不提交路由。修改其他后端不会中断当前业务或 SSE。

鉴权可直接录入完整请求头值，也可引用网关进程已有的环境变量并设置前缀。所有静态请求头值只写不读；编辑时可保留、替换或删除。环境变量新值必须存在于服务进程中；在线引用并不能更新父进程的环境。直接录入值以明文保存于权限为 `0600` 的管理状态文件，不进入 CLI 输出或审计日志；含静态密钥的请求文件须以 0600 保存。

CLI 命令（均支持 `--config PATH` 和 `--socket PATH`）：

```sh
./bin/aigate backends list --config config.management.json
./bin/aigate backends create new-backend.json --config config.management.json --json
./bin/aigate backends edit api3 edit-backend.json --config config.management.json --json
```

`list` 输出包含 `revision`、`config_version`、`order` 和脱敏配置的 JSON。新增与编辑读取请求文件；必须填写唯一 `operation_id` 和从 `list` 刚读取的 `expected_revision`，不会自动刷新过期版本或更换操作 ID。编辑时文件的 `target` 可省略，填写时必须与命令一致。写入为同步操作，成功才输出保存结果；不支持 `--no-wait` 或命令行 `--operation-id`，ID 使用文件中的值。

写入上限 64 KiB，沿用 `operation_id` 和 `expected_revision`。新增请求示例（密钥仅为示例）：

```json
{
  "operation_id": "create-api3-001",
  "expected_revision": 12,
  "target": "api3",
  "backend": {
    "url": "http://127.0.0.1:8003/v1/chat/completions",
    "models_url": "http://127.0.0.1:8003/v1/models",
    "preferred_model": null,
    "compatibility": {"metadata_values": "preserve", "drop_fields": []}
  },
  "credentials": {
    "Authorization": {"action": "replace", "value": "Bearer example-key"}
  }
}
```

编辑时省略的后端字段保持原值；提供的顶层字段整体替换，`preferred_model: null` 恢复自动选择。静态请求头不能通过 `backend.headers` 更新，统一使用 `credentials`：省略或 `{"action":"keep"}` 保留，`{"action":"delete"}` 删除，`{"action":"replace","env":"UPSTREAM_KEY","prefix":"Bearer "}` 引用环境变量。每个替换仅可提供 `value` 或 `env` 之一。

重复提交相同 ID 和完整请求返回已有结果；同 ID 不同请求或过期版本报版本或操作冲突错误。幂等记录与原管理操作一样保留最近 100 项，重启后仍有效。网络响应丢失时保持请求不变重试；不要立即换 ID 重复保存。

## 预热标准与默认参数

每次预热先强制刷新模型发现，然后用不含真实业务数据的合成请求验证：非流式回答、普通 SSE、SSE 工具调用、注入模拟工具结果后的非流式续答。工具验证校验名称、ID、拼接后参数及结果续答；不会执行外部工具。

普通响应需要合法且非空文本，SSE 需要有效输出、结束原因及 `[DONE]`。单个事件、合成输出和非流式验证响应限制为 1 MiB；任何步骤失败都不提交路由。记录下游 usage，缺失时不推算用量。只验证逻辑后端的这些请求通过，不宣称覆盖 Router 后每个副本、填满业务缓存或通过长上下文/吞吐验收。

| management 参数 | 默认值 / 行为 |
| --- | --- |
| `enabled` | false，需显式启用 |
| `socket` | 状态文件同目录下的 `.aigate/control.sock`，本机 CLI 通道 |
| `state_file` | `management-state.json`，相对主配置目录 |
| `warmup_rounds` | 2，每轮 4 次生成，串行执行 |
| `warmup_step_seconds` / `warmup_timeout_seconds` | 120 / 900 秒 |
| `warmup_max_tokens` | 1024；仍应用各后端 overrides，以实际发送参数为准 |
| `drain_timeout_seconds` | `max(600, timeout_seconds + discovery_timeout_seconds + 30)`，从路由提交时计算 |
| `auto_rollback` | true |
| `failure_threshold` / `failure_window_seconds` | 3 / 60 秒 |
| `rollback_retry_seconds` | 60 秒 |

未设置性能达标门槛。超时与输出预算需结合实际模型的思考模式调整；示例保留 1860 秒生成期限，排空和优雅退出时间也按长请求配置。

## 自动回切与维护锁

默认当前路由在 60 秒内连续出现 3 个可识别后端故障时，先重新验证原端，再切换后续请求。成功请求重置计数；入口格式错误、客户端取消和下游普通 4xx 不触发回切。发现/连接失败、429、5xx、超时、无效生成响应、SSE 内嵌 error 和异常结束计入故障。旧路由版本的迟到错误不影响新路由。完整空回答、拒答和内容过滤响应不会累积故障；多候选响应须按实际请求的 `n` 全部结束，缺少候选或结束原因计为异常结束。预热验证仍要求有效输出。

业务 SSE 未设置 `public_model_name` 时原样转发；设置后按事件改写顶层 model，旁路观察器仍读取原始下游字节且不积累完整模型文本。未设置对外名称时，单事件超过 1 MiB 标记“无法判定”并告警，不据此自动回切。设置对外名称时，事件超限或无法解析会返回流式错误并计入后端故障。已经发出的生成请求不会跨端重放，正在输出的流不能迁移；故障期间已经失败的请求仍会报错。

候选验证失败则保持当前路由并告警，至少经过重试间隔且出现新的故障才再尝试。每次人工切换最多成功自动回切一次，避免反复跳转；恢复验证成功可重新建立一次候选回切资格。告警显示在 CLI 状态及 stderr JSON 事件中，未接入短信、邮件等外部平台。

进入维护要求该端不是活动后端、业务/探测占用均为零且无冲突操作。锁定成功后禁止业务、模型发现、普通预热与自动回切访问。只有运维人员主动发起“恢复验证”才可探测维护端，失败时保留锁。

## CLI

```sh
./bin/aigate status --config config.management.json
./bin/aigate warmup a3 --config config.management.json
./bin/aigate switch single a3 --config config.management.json
./bin/aigate maintenance dcu --config config.management.json
./bin/aigate restore dcu --config config.management.json
```

可加 `--socket /path/to/private/control.sock`、`--json` 或 `--no-wait`。默认等待操作结束，失败或排空超时退出非零。退出 CLI 不取消后台操作；使用同一 `--operation-id ID` 可查询并继续等待仍在最近 100 条历史中的操作。

`aigate confirm-drain BACKEND` 用于人工核对下游后确认异常重启前的请求已结束。所有状态变更仍由运行中的网关统一校验和持久化，保留版本冲突保护、幂等重试和一次一个操作的限制。预热、切流、维护和恢复命令自动读取当前 revision；后端新增/编辑使用文件中的 revision。控制连接断开不取消已受理任务。

## 状态文件与迁移恢复

维护模式以状态文件持久化路由、维护锁、回切资格和最近 100 个操作，**不能同时配置 `routing_file`**。迁移时在主配置填写当前 single 目标，再移除 `routing_file`。首次启动初始化，后续以持久化状态为准，不使用配置默认值覆盖它。管理状态 v2 同时保存完整后端配置、声明顺序及配置版本；主配置中的 backends 仅用于首次初始化。已有 v2 状态时，重启和 check 先读取持久化后端再校验，直接编辑主配置不会覆盖在线后端。普通模式原有 `switch` 仍直接写路由文件；维护模式同名命令通过本机 socket 调用运行中的网关。

从 v1 首次启动升级时，先校验旧状态与主配置后端一致，再保存同目录 `状态文件.v1-backup-随机ID` 备份（0600）并原子迁移至 v2。`check` 不迁移、不创建备份。旧程序不支持 v2；回退前须停止新版并保留当前状态，再配套恢复旧配置和 v1 备份，避免丢失在线变更。

提交采用同目录临时文件、同步和原子替换。写入失败不改变内存路由或返回成功；操作结果无法落盘时保持管理屏障并告警，修复存储后重启并核对下游。进程文件锁防止两个实例共享同一状态文件运行。`check` 只读校验，不创建状态文件、不获取运行锁、不连接下游。

异常重启保留最后提交的路由和维护锁，将未完成操作标为 interrupted，不自动重放或继续声称就绪。不确定的后端显示“排空待确认”，不能直接进入维护或被自动回切。人工核对下游旧任务后，对非活动端使用 `aigate confirm-drain BACKEND`；确认本身不预热、不执行路由变更。活动端仍可服务，可先切到已确认的备用端，再核对旧端。

本地双后端测试验证行为与并发安全。真实 DCU/A3 上线需要确认稳定地址、网络、鉴权、共同模型协议和所需上下文，并演练 `DCU → A3 → DCU`；本地测试和 `/health` 成功不能替代该验收。

## 历史验证记录（2026-09-12，包含已移除的旧管理界面）

- `go test -race -timeout 60s ./...` 通过，共 39 个顶层测试，其中新增管理相关测试 15 个；覆盖旧长流、等待模型发现和连接池排队时切流、并发准入、维护锁、回切候选失败、写入失败、幂等和重启恢复。
- `go vet ./...`、当前 macOS 构建、Linux amd64/arm64 交叉构建、前端 JavaScript 语法及 JSON 文档检查通过。构建产物位于 `bin/`。
- 在浏览器通过两个本地模拟 HTTP 后端验证连接控制台、一键切到 A3、DCU 进入维护、恢复验证；对应路由、按钮禁用和回切资格符合预期，浏览器无控制台错误。模拟环境不包含真实业务或生产凭据。
- 当时的网页和可选 WebMCP 状态工具现已随管理 HTTP 层删除。
- 未连接真实 DCU/A3，未执行生产切流、服务重启或性能压测。


## 在线后端配置验证记录（2026-09-12）

- 新增 9 项测试，覆盖在线新增后真实协议预热和业务切流、编辑后的地址/鉴权/请求覆盖项、静态凭据保留与删除、旧 SSE 完整结束与排空约束、写盘失败不发布、自动回切互斥、并发版本冲突、v1 备份迁移及重启恢复。
- 全量 `go test -race -timeout 60s ./...`、`go vet ./...`、JavaScript 语法和 OpenAPI JSON 检查通过；macOS 与 Linux amd64/arm64 构建通过，产物在 `bin/`。
- 浏览器工具因请求头策略加载失败及本机窗口访问错误未能打开测试页，因此本次未完成真实浏览器表单操作验证。已关闭本次创建的本机模拟服务。
- 未部署到真实 DCU/A3，未执行生产切流或重启。源码修改前备份位于 `.backups/online-backends-20260912-180828/`；当前目录没有 `.git`，未创建提交。
