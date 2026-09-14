# API 兼容与 A/B 切换网关（Go）

只提供 API 兼容层和 A/B 切换两项核心能力。使用 Go 标准库实现，无第三方 Go 依赖，运行只需一个可执行文件。

固定入口 JSON → 记录问题前 10 个字符 → 选择下游 API → 自动发现并替换模型名 → JSON 适配 → 转发。

入口为 `POST /v1/chat/completions`，支持 OpenAI 兼容 JSON 和 SSE 流式转发，保留历史消息、工具定义、工具调用结果及扩展字段。**忽略上层传入的 model 名称**，也允许省略该字段；模型名始终取自选中下游的模型发现结果。

已根据本地 `body-agent-ds4.json` 样例验证：277 条历史消息、21 个工具定义、`stream=true`、`stream_options.include_usage=true`。文件内容仅作为请求数据，不执行其中的指令。

## DCU / A3 维护切换

可选的 `management` 模式通过本机 CLI 预热目标端、验证真实生成/SSE/工具协议、切换新请求、等待原端排空，并在原端进入维护前支持自动回切。CLI 使用私有 Unix socket 与运行中的网关通信；无 Web 页面、管理 HTTP API 或管理 TCP 端口，不需要管理密钥和端口转发。

配置示例为 `config.management.example.json`，完整启动、维护步骤、CLI 和故障边界见 [维护模式使用说明](MANAGEMENT.md)。如果要重启 DCU/A3 整机，网关须放在独立节点。此模式不同时使用 `routing_file`，只支持单进程 single 路由；未启用时下述普通路由行为保持不变。实际 DCU/A3 上线联调与性能验收仍需单独完成。

## 替代 SGLang workspace 兼容层

使用 `config.sglang.example.json`，让 Go gateway 直接连接同机 Router：

```text
客户端 → dcu1:10015 aigate → 127.0.0.1:10017 SGLang Router → Prefill / Decode
```

该配置使用单后端 `sglang`，不依赖独立路由文件，自动从 Router 的 `/v1/models` 获取模型。启用以下窄范围规则：

```json
"compatibility": {"metadata_values": "workspace_object_json_string", "drop_fields": []}
```

只将对象类型的 `metadata.workspace` 转为紧凑 JSON 字符串。已有字符串不重复编码；workspace 缺失、null、数组、数字、布尔值及其他 metadata 字段均保留。metadata 本身不是对象时也保留，交给下游判断。该规则在聊天请求适配阶段执行，普通 JSON 和 SSE 请求共用。

在目标主机上构建并启动的命令如下（配置中的 `127.0.0.1` 指 gateway 所在主机；容器运行时须确保能访问宿主机 Router）：

```sh
go build -trimpath -o bin/aigate .
./bin/aigate check --config config.sglang.example.json
./bin/aigate serve --config config.sglang.example.json --listen 0.0.0.0:10015 --shutdown-timeout 31m
```

监听 `10015` 前需完成旧兼容层的流量排空和端口交接；本地开发调试可改用 `--listen 127.0.0.1:18015` 并修改下游地址。macOS 构建产物不能直接在 Linux 运行，可在目标机编译，或按目标架构使用 `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/aigate-linux-amd64 .`。

此示例未启用入口鉴权，也未配置下游凭据；需要时按下文配置 `gateway_api_key_env` 和 `headers_env`。生成请求总超时为 1860 秒（含 SSE 全程）。

这项规则提供 workspace 转换功能，gateway 保留自动替换模型名、入口校验、网关自身错误响应和日志行为。聊天生成阶段透传后端非 2xx 状态码、错误正文及必要响应头。它提供聊天、模型发现及自身健康/文档接口，不透传 Router 的其他路径或客户端请求头；上线前需确认调用方所需接口。`check` 只做本地配置校验，本次本地测试不代表已部署或已通过真实 Router 验收。

## 通用 metadata 兼容规则

当 Router 要求 `metadata` 的每个值都是字符串时，对该下游启用：

```json
"compatibility": {"metadata_values": "json_string", "drop_fields": []}
```

原始请求：

```json
"metadata": {"mode": "agent", "workspace": {"file_count": 2175, "languages": ["java"]}}
```

发给 Router 的请求：

```json
"metadata": {"mode": "agent", "workspace": "{\"file_count\":2175,\"languages\":[\"java\"]}"}
```

只转换值，保留 `metadata` 对象本身。已有字符串不重复编码；对象、数组、数字、布尔值和 null 使用 JSON 编码为字符串。未配置该规则的下游默认保留原值。若后续确认下游拒绝 `patches`、`result_format`，可显式设置 `drop_fields: ["patches", "result_format"]`；当前配置保留它们。

## 构建与启动

需要 Go 1.23 或更高版本（本次使用 Go 1.25.12 验证）。

```sh
go build -trimpath -o bin/aigate .
cp config.router.example.json config.json
cp routing.example.json routing.json
```

修改 `config.json` 中 `backends.api1/api2` 的 `url`（生成接口）和 `models_url`（模型发现接口）。示例使用本机占位地址，不需要填写模型名。只有一个 API 时可删除 `api2`，或切换到 single 模式。需要下游 Bearer 鉴权时，设置 `headers_env: {"Authorization":"MODEL_API_KEY"}` 和 `header_prefixes: {"Authorization":"Bearer "}`，再提供对应环境变量；发现接口和生成接口使用同一组下游鉴权头。密钥不写进配置文件，缺少配置引用的环境变量会在启动时直接报错。

```sh
export GATEWAY_API_KEY='replace-with-gateway-key'
./bin/aigate check --config config.json
./bin/aigate serve --config config.json --listen 127.0.0.1:8080
```

可用 `AIGATE_CONFIG=/path/to/config.json` 或 `--config` 指定配置路径。`check` 仅校验主配置、环境变量和路由文件，不连接下游。`serve` 可省略；默认监听 `127.0.0.1:8080`。收到 SIGINT/SIGTERM 后停止接收新连接，默认最多等待在途请求 30 秒，可用 `--shutdown-timeout 5m` 调整，超时则关闭剩余连接并以非零状态退出。内网部署时按需要调整监听地址。后端地址、鉴权、适配规则等主配置修改后需重启；独立路由文件支持即时切换。`gateway_api_key_env` 控制网关入口 Bearer 鉴权，删除该项则关闭入口鉴权。

```sh
curl http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H 'Content-Type: application/json' \
  --no-buffer --data-binary @body-agent-ds4.json
```

`GET /health` 仅表示网关已启动，不检测下游模型是否就绪。业务 OpenAPI 规范位于 `/openapi.json`；HTML 文档页 `/docs` 已移除。

## 模型发现与 A/B 切换

首次使用某个下游时，GET 其 `models_url`，默认读取 `{"data":[{"id":"实际模型名"}]}`。每个 API 独立缓存模型 60 秒，过期后在下一次使用时刷新；并发请求共用一次发现请求。下游返回多个模型时默认选择列表第一个，可在该下游配置 `preferred_model`，但指定值必须存在于发现结果中。上层 model 不参与选择。

模型列表结构不同时，可配置 `models_list_path`（默认 `data`）和 `model_id_path`（默认 `id`），例如 `result.models` 和 `name`。模型发现要求下游提供可用的模型列表接口，不从生成请求或历史配置猜测名称。

`routing.json` 决定新请求发往哪个 API：

| 模式 | 文件内容 | 行为 |
| --- | --- | --- |
| 仅 API 1 | `{"mode":"single","backend":"api1"}` | 所有新请求只发 API 1 |
| 仅 API 2 | `{"mode":"single","backend":"api2"}` | 所有新请求只发 API 2 |

使用命令原子更新路由文件，无需重启：

```sh
./bin/aigate switch single api1
./bin/aigate switch single api2
```

每条命令将新请求切到指定后端。自定义配置路径可加 `--config /path/to/config.json`。`routing_file` 的相对路径基于主配置文件所在目录解析；配置的文件必须在启动前存在。每个新请求检查路由文件的身份、大小和修改时间；文件变化后才重新读取、解析，策略作为完整快照使用。使用上述命令原子替换文件，不要在修改后恢复旧时间戳。已选定下游的请求和正在传输的 SSE 不会迁移或中断。路由文件缺失、JSON 错误或 backend 不存在时，新请求返回 503，修复文件后自动恢复。

若不需要即时切换，可省略 `routing_file`，直接在主配置写 `routing: {"mode":"single","backend":"api1"}`；该方式修改后需重启。只配置一个后端且省略 routing 时自动使用该后端；多后端必须明确指定目标，或由 routing_file 指定目标。同一时刻只有一个后端接收新业务请求。模型发现缓存并发安全，一个 Go 进程可并发处理请求。

普通模式下，活动后端发现失败直接返回 503，不访问其他后端。发现超时默认 10 秒，失败缓存默认 5 秒，随后重试；发现结果过期后若刷新失败，不继续使用旧模型。生成请求已经发出后不自动重试或跨 API 重发。维护模式保留预热、排空、维护锁和可配置的故障回切，见 MANAGEMENT.md。

网关的 `GET /v1/models` 只查询活动后端并返回一个选定模型，与聊天入口使用相同鉴权。配置对外名称后返回该名称，但仍要求下游发现成功；发现成功不代表生成或工具调用协议已经验收。

本版本移除负载均衡。旧 `round_robin` 配置和命令会明确报错。升级前将主配置或路由文件改成 `{"mode":"single","backend":"api1"}`（替换为实际接流后端），命令改用 `switch single BACKEND`。非法路由文件使新请求返回 503；修复后自动恢复，错误切换命令不会修改原文件。

旧配置迁移：将按模型名组织的 `routes` 改为按 API 名称组织的 `backends`，删除 `upstream_model`，为每个下游添加 `models_url`。

## 自定义对外模型名称

在主配置设置一个全局名称，让 A/B 切换前后客户端看到的模型名一致：

```json
"public_model_name": "my-model"
```

该名称用于 `/v1/models` 的模型 ID、普通聊天响应和 SSE 聊天事件的顶层 `model`，也适用于模板适配器的普通响应。上层传入的 model 仍被忽略，可以省略；发给下游的始终是自动发现的真实模型名。管理状态、预热和审计日志保留真实名称。

省略或设置空字符串关闭此功能；非字符串、纯空白或含首尾空白的名称会被配置检查拒绝。修改后需重启。示例使用 `my-model`，请按需修改或删除该项。

开启后按完整 SSE 事件增量改写并立即 flush，支持跨网络分块、多行 data 和 LF/CRLF/CR 换行，输出换行统一为 LF。保留正文、工具调用增量、usage、事件属性、注释、错误事件和 `[DONE]`，不缓冲整条回答。单事件预算 1 MiB；超限、无效 JSON、未完整结束的事件或缺少 `[DONE]`（下游 error 事件除外）会发送 `invalid_upstream_stream` 错误事件并关闭，不回退到原始事件、不补发 `[DONE]`。未配置对外名称时保留原样转发路径。

## 下游差异如何适配

| 差异 | 配置 |
| --- | --- |
| URL、模型发现地址不同 | `url`、`models_url` |
| 多模型选择、模型列表结构不同 | `preferred_model`、`models_list_path`、`model_id_path` |
| Bearer / X-API-Key 等鉴权不同 | `headers_env`、`header_prefixes` |
| 同 OpenAI JSON，仅需覆盖固定参数 | `adapter: openai`、`overrides` |
| metadata 值必须为字符串 | `compatibility.metadata_values: json_string` |
| 仅对象类型的 metadata.workspace 需要字符串化 | `compatibility.metadata_values: workspace_object_json_string` |
| 下游明确拒绝某些扩展字段 | `compatibility.drop_fields`，仅显式删除指定顶层字段 |
| 字段改名、嵌套结构、固定值不同 | `adapter: template`、`request_template` |
| 返回文本路径不同 | `response_content_path`，如 `data.answer`、`results.0.text` |

模板中 `{"$ref":"request.messages"}` 将整个消息数组放入对应位置，保留 JSON 类型；`{"$ref":"request.max_tokens","$default":1024}` 支持字段缺失或 null 时使用默认值。上下文包含 `request`（入口字段，其中 model 已替换为自动发现的模型）、`model`（自动发现的模型名）、`question`（最后一条 user 消息全文，仅用于向下游转发）。普通字符串、数字、数组和对象作为固定结构。引用节点必须整体使用 `$ref`，不支持字符串内插值、表达式或消息列表逐项转换。

`config.example.json` 还包含通用 OpenAI 路由和自定义 JSON 模板路由的配置示例。

OpenAI 适配器保留历史消息、工具字段、显式 null、流式选项及额外字段，强制使用发现的模型名（`overrides.model` 也不能覆盖它）。非流式响应保留下游字段和 usage，model 为配置的对外名称，未配置时为此次选用的实际模型。SSE 未配置对外名称时原样转发事件内容（包括工具调用增量、usage 和 `[DONE]`）；开启后按上一节规则改写顶层 model。下游须提供 OpenAI 兼容的 SSE。

模板适配器仅支持非流式 JSON，只发送模板声明的字段，将指定文本字段封装为统一聊天响应，当前默认 `finish_reason=stop` 且不推算 usage。自定义流式协议、工具/多模态协议转换、复杂角色转换及停止原因/用量映射，需要按实际下游协议扩展适配器。

## 日志与错误

每个聊天请求输出一条 JSON 日志到 stderr，可由运行环境收集或重定向到文件：

```json
{"request_id":"...","question_prefix":"请帮我分析这个模型接","status":200,"outcome":"completed","backend":"api1","model":"discovered-model","duration_ms":123.45}
```

只截取最后一条 `role=user` 消息的前 10 个 Unicode 码点（中文不按字节截取，空格和标点计数，组合 emoji 可能包含多个码点）。不足 10 个则记录全部；未通过校验/鉴权时前缀为空。日志不记录问题剩余内容、其他历史消息、模型回答或密钥；转发时使用完整消息。JSON 编码会转义换行符，避免形成伪造日志行。

响应携带网关生成的 `X-Request-ID`。聊天生成阶段收到后端非 2xx 响应时，原样返回状态码和正文（包括后端 401/403、重定向、纯文本、HTML 和空正文），不再统一转换为 502，也不包装后端错误正文。透传 `Content-Type`、`Content-Encoding`、`Retry-After`、`WWW-Authenticate`、`Location`；过滤逐跳头和 `Connection` 指定的头，不传递后端 Cookie 或覆盖网关请求 ID。Go HTTP Transport 自动解压时，正文与响应头一起按解压后的表示转发。

错误正文使用固定缓冲区转发，不整体缓存或保存；响应头已发出后读取失败会中止响应，不向原始正文追加 JSON。后端 SSE error 事件保留内容，已发出的 HTTP 200 无法改写，失败由指标反映。

网关自身错误仍使用 `error.code/message/request_id`：入口格式错误或模板适配器收到流式请求返回 400，入口鉴权错误返回 401，连接失败或后端成功响应的协议错误返回 502，路由配置错误或模型发现失败返回 503，生成超时返回 504。模型发现及后台预热的错误约定不变。生成请求总超时由 `timeout_seconds` 配置（包括 SSE 和错误正文转发全程）；模型发现另有 `discovery_timeout_seconds` 限制，每个 API 各计一次。不自动重试生成或跟随重定向。

### 生成错误指标

开启现有 `profile.enabled` 后，通过原有鉴权 `GET /metrics` 暴露以下指标，vmagent → VictoriaMetrics 的抓取接口和鉴权方式不变：

| 指标 | 标签 | 含义 |
| --- | --- | --- |
| `aigate_generation_requests_ended_total` | `backend, model, stream, result` | 进入生成 HTTP 调用的业务请求结束数；`result` 为 `completed/error/client_cancelled/client_disconnected/unknown` |
| `aigate_upstream_errors_total` | `backend, model, stream, error_class, upstream_status` | 每个生成请求最多一个主要后端错误；收到响应时保留原始状态码（含 SSE 的 200），未收到响应记为 `none` |
| `aigate_error_metrics_start_time_seconds` | 无 | 本组新增指标的统计起点，正常关闭重启后保持不变 |

`error_class` 使用固定枚举：400/413/422 为 `request_rejected`，401/403 为 `auth`，404 为 `not_found`，429 为 `rate_limited`，5xx 为 `server_error`，3xx 为 `unexpected_redirect`，其他非 2xx 为 `http_other`。不解析错误正文猜测 OOM 或上下文超长等根因。

无有效 HTTP 错误响应时，按 `connection_error`、`timeout`、`response_read_error`、`stream_error`（明确 SSE error）、`stream_incomplete`（缺失结束证据）和 `protocol_error` 分类。HTTP 错误或明确 SSE error 一经观察到，后续断流/取消不会覆盖或重复计数；仅客户端取消/写入断开不算后端错误。观察器超过资源限制而不能判断时记为 `unknown`，不推断成功或失败。

业务 SSE 完成要求收到 `[DONE]`、所有预期候选均有结束原因，且没有残留数据事件或协议错误。候选数采用实际发送请求（含后端 overrides）的 `n`，缺省或 null 按 1；仅单候选兼容省略 `index`。完整空文本、拒答或内容过滤响应不计为后端故障；空响应不生成首 token 时间。预热仍要求有效输出并继续执行工具协议验证。候选跟踪最多 1024 项，超过上限只标记 `unknown`，不修改或拒绝业务请求。

入口拒绝、模型发现和后台探测不进入新增指标。已有请求指标保持原有口径，不能用其总数替代生成阶段分母。标签沿用后端/模型分组上限；不放入任意错误文本、后端错误代码或请求 ID。profile 结束事件仅增加 `upstream_status`、`error_class`、`generation_result` 结构化字段，不保存错误正文。

查询示例（按实际抓取标签补充环境/实例筛选）：

```promql
# 当前统计生命周期内的累计分类错误数
sum by (backend, model, error_class, upstream_status) (aigate_upstream_errors_total)

# 5 分钟分类错误速率（次/秒）
sum by (backend, model, error_class) (rate(aigate_upstream_errors_total[5m]))

# 5 分钟后端错误率：同窗口错误数 / 同窗口生成结束请求数
sum by (backend, model) (increase(aigate_upstream_errors_total[5m]))
/
sum by (backend, model) (increase(aigate_generation_requests_ended_total[5m]))
```

计数在请求结束时增加，错误率分母包含取消、断开及未知结果；未知数量应另查 `result="unknown"`。指标按实际出现的标签组合生成，无样本不直接补零；新序列不足两个采样点时速率不可判定。新增指标不回填升级前历史，跨统计起点或缺失抓取的窗口不能称为完整统计；使用 `aigate_error_metrics_start_time_seconds` 与采样覆盖范围判定。正常关闭沿用现有计数检查点，异常退出仍遵循原有重置语义。

流式响应已经发出 HTTP 200 后发生的异常无法再改 HTTP 状态码，会发送包含 `error` 的 SSE 数据事件并关闭流，不补发 `[DONE]`。日志在整条流结束后记录耗时，并通过 `outcome=upstream_stream_error/upstream_timeout` 区分异常；未发完响应时为 `incomplete`。`completed` 仅表示 HTTP 传输完成，不代表模型结果质量或任务成功。

当前未包含持久化业务日志库、业务限流、业务请求/响应体大小限制、自定义流式协议转换及后端配置热更新；维护模式单独持久化操作摘要并限制管理请求和预热验证响应大小。实际 Router 尚未联调；Go 测试使用本地真实 HTTP 下游验证完整样例、错误路径、模型发现、SSE 逐段返回、客户端取消，以及切换不影响正在进行的流。原始样例已加入 `.gitignore`，不随代码提交。

## 连接池与转发开销

主配置可增加以下可选字段；省略时使用这些默认值：

```json
"http_pool": {
  "max_idle_connections": 256,
  "max_idle_connections_per_host": 128,
  "max_connections_per_host": 0,
  "idle_timeout_seconds": 90
}
```

`max_connections_per_host: 0` 表示不额外限制每个下游主机的连接数；正数会让超出连接容量的请求等待，等待时间计入对应请求的超时。空闲连接参数仅控制连接复用，不是并发请求容量；HTTP/2 还受下游并发流限制约束。请结合下游容量调整，连接池不是限流器。

生成与模型发现共用 HTTP 客户端，默认不使用环境代理、不跟随重定向。OpenAI 路径使用 `json.RawMessage` 保留无需修改的嵌套字段，避免将所有扩展字段展开并深拷贝；保留大整数精度。仍会读取和校验完整 JSON 请求，并生成适配后的请求体，并非零拷贝。

SSE 使用可复用的 32 KiB 读取缓冲区。未配置对外名称时读到数据就写出并 flush；配置后每个完整事件改写完成即 flush，不等待整条回答。慢客户端的写入会施加背压，写入受生成期限约束；客户端断开会取消下游请求。模板适配路径按需展开 JSON，仅发送模板声明的字段。

## 验证

```sh
go test -race -timeout 60s ./...
go vet ./...
go build -trimpath -o bin/aigate .
```

测试覆盖鉴权和日志脱敏、请求校验、JSON/metadata/模板适配、模型发现与并发单后端路由、A/B 切换及对外模型名称、路由文件故障恢复、CLI 原子切换、真实 HTTP 流式转发和取消。根目录存在 `body-agent-ds4.json` 时，会额外验证该完整本地请求；缺少该文件时对应测试明确跳过。

这里只验证行为与并发安全，尚未完成性能压测，也未验收实际 Router 的生成能力。`/health` 成功、配置检查成功均不代表下游已就绪。

项目仅保留 Go 实现；启动与切换使用上面的 Go 命令。

### 在线后端配置

启用管理模式后，使用 `aigate backends list`、`aigate backends create REQUEST.json` 和 `aigate backends edit BACKEND REQUEST.json` 在线查看、新增和编辑后端，无需重启。当前接流端须先切走并排空再编辑；保存配置不会自动切流，切换时重新进行协议验证。地址、鉴权、首选模型及兼容参数均可编辑，在线配置在重启后保留。详见 [管理说明](MANAGEMENT.md#在线新增与编辑后端)。

## 请求画像

使用 [画像配置示例](config.profile.example.json)，设置独立 `AIGATE_PROFILE_KEY` 后启用。Gateway `/metrics` 经 vmagent 写入 VM，平台读取 VM；消息前缀的窗口详细分析读取网关 API。事件默认保留 72 小时或 10 GiB，不保存正文。指标与时间口径、资源限制及接口见 [画像说明](deploy/profile/README.md)。
