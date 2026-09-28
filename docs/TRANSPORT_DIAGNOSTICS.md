# 网关传输诊断

本功能只修改 aigate，不要求修改客户端、测评平台或 Router。业务 HTTP 监听器的 ReadHeaderTimeout 从 10 秒改为 30 秒；上传正文仍为 5 分钟 / 30 MB，生成时限仍按平台运行配置，控制和画像监听器的超时保持原值。控制状态 `/control/v2/status` 的 `business_read_header_timeout_seconds` 可检查编译版本使用的业务读头时限。

30 秒能容纳更长的请求头发送延迟，不能保证消除其他原因的 ReadError。历史事件没有完整连接证据，不能据此将其根因追认为读头超时。

## 日志位置与关联

输出仍进入网关原有审计输出（部署中为 systemd journal 或 Kubernetes 容器日志），没有新增持久化目录。只在网关正常受理的 `/v1/chat/completions` 审计中增加字段；控制层直接拒绝请求的现有响应路径不额外增加请求审计。本轮也不修改平台“异常诊断”页面或原有 stream-incidents 存储。

通过 SSH MCP 读取对应主机日志并按下列字段关联：

- `request_id`：网关自身 ID，仍通过响应 X-Request-ID 返回。
- `client_request_id`：客户端原有 X-Request-ID，只有 1–128 字节 ASCII 字母、数字、点、下划线和短横线才记录。没有或不合法时省略，不拒绝业务。
- `connection.connection_id`：入口 TCP 连接 ID。同一 keep-alive 连接的多次请求使用同一连接 ID，通过 `request_number` 区分。
- `connection.accepted_at/local/remote/handler_at`：接收连接和进入 handler 的时间、本地及对端 IP:port。
- 既有 `test_task/backend/route_version`：任务和绑定信息。

仅保存入站请求 ID 的映射，不改变客户端 ID、上游请求头或鉴权方式。请求头尚未读完整时没有可用请求 ID，此时按 connection_id、两端地址和时间关联。

## 请求头之前的连接异常

业务监听器安装连接包装器和 ConnContext/ConnState，在读取失败时保留原始错误链。连接在进入下一次 handler 前关闭，产生一条 `event=connection_diagnostic`，包括：

- `gateway_boot_id`、连接建立/关闭时间、连接端点。
- `entered_handler=false`；连接摘要中的 `request_number/handler_at` 表示这条连接上**已经处理过**的请求数和最后进入时间，新连接为 0/零值。
- `pending_header_bytes`：尚未进入 handler 的这一轮底层读取累计字节数；TCP 一次读取可能包含正文，也可能提前读入下一条请求，不能将它解释为精确 HTTP 头长度。下一条请求已在 net/http 缓冲区时，本轮计数可能为 0。
- `response_before_handler`：net/http 在进入 handler 前尝试写出响应，例如拒绝已缓冲的畸形请求；即使本轮没有新的底层读取，也会记录该次关闭。此标记不表示客户端已收到响应，不保存响应内容。
- `read_error_at/read_deadline/errors`：失败时刻、实际读取截止时间、错误类型链、系统 errno、timeout 标记。
- `reason`：`request_header_read_timeout`、`request_header_incomplete`、`request_header_read_error` 或证据不足时的 `closed_before_handler`。

正常 EOF 且既未观察到本轮请求数据、也未观察到进入 handler 前的响应（空连接探测、keep-alive 关闭）不逐条写异常日志。net/http 在处理请求后主动终止后台读取的内部超时也不记作读头失败。服务关闭引起的空闲连接关闭不记为读头故障。连接包装器保留底层 TCP 的 CloseWrite 能力，使 net/http 拒绝超大请求头或正文时仍能先半关闭再关闭连接。

这些日志证明的是网关进程观察到的连接行为。看到 reset 不能单凭网关日志断言由客户端程序主动发出，也可能存在中间网络设备。

## 已进入 handler 的请求

仍然每个生成请求一条结束审计。新增 `generation_result/error_class` 沿用已有观测分类，不改变错误指标语义；失败、拒绝、取消或写出断开时增加 `transport`：

- `steps`：自 `started_at` 起的累计毫秒偏移。包含 handler、读取请求体、路由/模型发现、准备上游、连接池等待、DNS/TCP/TLS、取得连接、写出请求、响应首字节、响应头、流开始和结束。相邻阶段之差可辅助判断耗时；并发拨号的阶段可能交错，不能直接相加。
- 详细 HTTP trace 针对**生成调用**；路由/模型发现只记录阶段边界，不为后台探测增加逐请求传输日志。
- `upstream_local/upstream_remote/connection_reused/connection_idle_ms`：实际连接端点和连接复用信息。拨号失败但已开始 connect 时仍保留尝试目标地址。
- `upstream_status/upstream_read_bytes/client_written_bytes/last_read_ms/last_write_ms`：上游状态和已经完成的读取/写出计数。写出成功表示交给 socket/HTTP writer，不保证对端应用已读取。
- `failures`：具体 operation（如 upstream_round_trip、upstream_body_read、client_body_write、client_body_flush）、错误链、errno，以及观察到的客户端和生成 context 状态。

context 状态在网关自身的正常清理 cancel 之前采集，避免把每条正常请求误标为取消。主动取消、客户端 deadline 和链路断开都可能导致 client context canceled，日志只描述观察结果，不推断发起者。已有上游错误不会被后续客户端取消覆盖。

错误链保留最多 6 层具体 Go 类型，保存系统 errno 及标准网络/上下文错误的安全消息；不直接保存 URL 包装错误或未知错误的完整字符串，避免泄露 URL 凭据、路径、响应头或错误正文。未知错误仍保留类型，不伪造 errno 或故障原因。

## 日志量与开销边界

正常成功请求没有 transport 阶段详情，不按 token 或 SSE 事件输出日志。每请求最多 24 个阶段、3 个失败、每条失败最多 6 层错误，超过后 `truncated=true`。流转发只更新时间和字节计数，不保存响应片段。

入口异常使用单个进程级异步写入器，队列 256 条、单条上限 16 KiB；满队列丢弃诊断，不阻塞请求。已有请求结束审计仍使用原来的审计写入方式，不在本轮更换其持久化策略。日志总保留量由现有 journal/容器日志轮转配置控制。

已鉴权的画像 `/metrics` 增加两个进程级累计计数：

- `aigate_connection_diagnostic_dropped_total`
- `aigate_connection_diagnostic_write_errors_total`

请求 ID、对端地址、原始错误文本不进入 Prometheus 标签。诊断队列可能丢失记录；零记录不能单独证明客户端没有连接过网关。进程突然退出也可能丢失尚未写出的诊断。

## 下次排查顺序

1. 用测评记录已有的请求 ID 找 `client_request_id`。若已进入网关，查看连接时间、最后阶段、错误方向及 errno。
2. 找不到请求审计时，在对应时间和源地址范围查 `connection_diagnostic`。只有同时出现未进入 handler、实际 read timeout 和读截止时间，才可确认网关读头超时。
3. 上游错误看实际 Router 连接端点和原始错误链；客户端写出错误看 client_body_write/flush。两者不要合并为同一种 ReadError。
4. 两种日志都没有证据时，检查诊断丢弃/写入计数；仍需客户端或网络侧证据，不能用日志缺失判定根因。

## 验证

在本地真实 TCP 连接上验证读头超时（包括 keep-alive 下一次请求）、10.2 秒后补完请求头仍返回 200、上游 reset、EOF、客户端取消、客户端写入/Flush 错误；同时验证 SSE 转发内容不变、正常请求无详细日志、错误链不含测试凭据、队列背压有界。补充超大请求头/正文的 431/413 响应正常 EOF、已缓冲畸形请求和不支持的 Expect 被拒绝时只记录一条诊断、正常管线请求不误报的回归测试。依赖保持仅 Go 标准库。

```sh
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go test -mod=vendor -race -count=1 ./...
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go vet -mod=vendor ./...
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go build -mod=vendor .
```
