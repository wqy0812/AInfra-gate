# aigate：平台控制的 API 网关

网关提供 OpenAI 兼容 JSON、SSE、模型发现、请求适配与观测。Router 配置、运行配置、绑定、操作日志和测试占用的持久化全部由平台负责。网关进程仅保留内存快照，重启后暂停业务，等待平台恢复。

## 使用场景与范围

- **目标使用场景为流式请求**（`stream: true`，SSE）。设计、代码审核、性能优化与验收以流式链路及其必要依赖（模型发现、上传、控制、排空和观测）为准；非流式业务不在当前考虑范围内。现有代码和下文中的非流式行为说明仅记录兼容能力。
- **当前网关与 Router 的业务接口均不使用 API Key 鉴权**，客户端到网关、网关到 Router 的业务调用均无需密钥。此约定仅适用于业务链路；平台控制接口仍要求控制密钥和允许的来源 CIDR，启用的观测接口仍需鉴权。

## 控制方式

业务入口为 `POST /v1/chat/completions` 和 `GET /v1/models`；模型由 Router 发现，模型列表使用发现的实际 ID，OpenAI 响应保留后端返回的模型名。元数据兼容规则和 overrides 随 Router 交换；入口名称、入口凭据、连接池与观测策略留在本网关。

客户端请求的 `model` 可以填写任意字符串，也可以省略或为 null；网关发往 Router 前替换为发现的实际模型名，不要求客户端名称与后端匹配。登记和更换模型时仍填写实际模型 ID。

`POST /v1/chat/completions` 请求体上传总时限为 5 分钟，大小上限为 30 MB（30,000,000 字节）。上传超时返回 HTTP 408，超限返回 HTTP 413；请求体读取完成后清除读取截止时间，后续生成与 SSE 使用原有生成超时。

暂停准入、无效测试令牌等提前拒绝不等待客户端上传完正文。HTTP/1.x 返回响应后关闭该连接；HTTP/2 只关闭该请求的正文流，其他请求可继续复用连接。

业务监听器读取 HTTP 请求头的时限为 **30 秒**。控制和观测监听器继续使用各自的 5 秒读头时限。请求头未读完即关闭的连接、生成请求的上游阶段和读写错误可通过[传输诊断](docs/TRANSPORT_DIAGNOSTICS.md)排查；不需要客户端改造。

所有管理动作经过平台 API。网关仅开放鉴权的 `/control/v2/status`、`/control/v2/commands`、`/control/v2/commands/{seq}` 给 Engine。二进制不再支持 `check`、`switch`、`status`、`backends` 等管理 CLI、`--config`、路由文件或 Unix 管理 socket。

两网关交换 Router：验证主动目标 → 双方暂停新请求并确认 → 平台事务交换绑定 → 主动方激活并单独成功 → 被动方直接激活。被动 Router 故障不阻塞、不回退主动方；被动网关控制不可达则继续暂停并重试。已受理请求，包括 SSE 和等待模型发现/连接池的请求，保持原快照直到结束。

普通请求审计仅记录请求 ID、后端、模型、耗时、状态和路由/任务等元数据，不再记录用户消息前缀（`question_prefix`）。

审计增加连接 ID、连接端点/接收时间、经校验的可选客户端请求 ID、生成结果和错误类别；失败或取消时附带有界传输诊断，正常成功请求不输出详细阶段。仅凭客户端 context 取消不能认定是用户主动取消。

## 启动

只需 Go 标准库，没有第三方依赖。构建与测试始终显式使用 vendor 模式。

```sh
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go build -mod=vendor -trimpath -o bin/aigate .
export AIGATE_ID=g1
export AIGATE_CONTROL_KEY='<至少 32 字节，由部署环境注入>'
./bin/aigate serve --listen 0.0.0.0:10015 --control-listen 0.0.0.0:18083 --allowed-cidrs '<Engine IP>/32' --profile-listen 0.0.0.0:18082 --data-root /var/lib/aigate --shutdown-timeout 62m
```

`--data-root` 只保存观测数据，不保存业务配置、凭据、路由或管理状态。相同观测策略复用写入器；不同策略使用不同目录，避免旧请求与新配置竞争文件。平台下发观测 API 密钥。控制监听地址、观测监听地址及数据根目录属于部署参数，不接受运行配置覆盖。

首次启动的业务接口返回 503；`/health` 仅表示进程存活。进程处于暂停状态时，新业务返回 503 和 `Retry-After: 1`。Router 非 2xx 仍按现有透传规则返回，网关不自动切回。

## 单元测试覆盖率

运行 `./scripts/test_coverage.sh`，可传 Go 测试参数（例如 `-run TestRuntime -race`）。入口使用离线 vendor 模式，覆盖网关及 `cmd/` 中的命令；本地忽略的 `deploy/` 发布工具不进入分母。结果输出到 `dist/coverage/coverage.out` 和 `dist/coverage/index.html`，测试失败时保留报告与失败退出码。

控制协议、配置边界、画像 HTTP/分析队列及预算、观测存储故障、控制进程启动与 SIGTERM 收尾均有离线回归。网络夹具仅使用本机临时监听器，磁盘夹具使用临时目录，不访问部署节点或真实模型。

## 业务响应与旁路观测

OpenAI 适配器的非流式响应采用“正文、状态码及指定响应头透传”；生成阶段的非 2xx 响应也采用此规则。指定头为 `Content-Type`、`Content-Encoding`、`Retry-After`、`WWW-Authenticate`、`Location`，过滤 `Connection` 指定的逐跳头，不传递上游 Cookie 或覆盖网关的请求 ID。`Cache-Control`、`Vary` 等不在透传名单中。Go Transport 自动解压时转发解压后的正文及对应响应头。SSE 成功响应支持未编码、`identity` 和单层 `gzip`；显式请求的 gzip 也会在观察和转发前流式解压，客户端收到未压缩 SSE。其他编码返回 HTTP 502 `invalid_upstream_stream`；流中解压失败沿用读取错误处理，不视为正常完成。观察器已识别并转发 `[DONE]` 后发生读取错误时，仍按读取错误记录，但网关不再追加错误事件；此规则不覆盖观察器因解析上限停止解析而未识别 `[DONE]` 的情况，也不拦截上游在 `[DONE]` 后发送的数据。模板适配器仍执行响应转换。

OpenAI 普通响应和 SSE 正文不改写模型名称，也不补写缺失的 `model` 字段。旧配置及历史运行快照中的 `public_model_name` 在读取时忽略；平台不再接受包含该字段的新运行配置，历史记录无需改写。

非流式 OpenAI 2xx 响应增加旁路观测，不改变返回状态、正文或接流状态，也不重试、切换 Router。启用 `profile.enabled` 后，由现有观测监听器的鉴权 `/metrics` 提供：

| 上游响应 | `generation_requests_ended_total` 的 `result` | `upstream_errors_total` 的 `error_class` |
| --- | --- | --- |
| JSON 顶层存在非 null 的 `error` | `error` | `response_error` |
| 非法 JSON、空正文或 HTML 等非 JSON 正文 | `error` | `protocol_error` |
| 非空 choices，每个候选均含 assistant message 和非空 finish_reason | `completed`（还需完成客户端写入） | 不增加错误计数 |
| 合法但无法识别的 JSON、缺少结束证据、空 choices | `unknown` | 不增加错误计数 |
| 正文超过 4 MiB，或仍带未解码的 Content-Encoding | `unknown` | 不增加错误计数 |

指标实际名称带 `aigate_` 前缀；错误标签保留原始 `upstream_status`，包括 HTTP 200。观测不要求生成文本非空，工具调用、仅 reasoning、拒答及内容过滤可正常完成。4 MiB 是旁路解析上限，不限制正文转发大小。非 2xx、SSE、网络异常和客户端中断继续使用既有分类。

审计中的 `outcome=completed` 表示响应已写给客户端；判断生成错误应查看 `generation_result`、`error_class` 及上述指标。画像结束事件记录分类和上游状态码，不保存响应正文或错误详情。新增规则生效前的累计计数不重算，排障宜查看部署后的时间窗口，同时关注 `error` 和 `unknown`：

```promql
sum by (backend, model, error_class, upstream_status) (
  increase(aigate_upstream_errors_total{stream="false"}[5m])
)
sum by (backend, model) (
  increase(aigate_generation_requests_ended_total{stream="false",result="unknown"}[5m])
)
```

完整接口、状态机、迁移和验收步骤见 [平台控制 v2 操作说明](MANAGEMENT.md)。systemd 模板见 [deploy/control-v2](deploy/control-v2/README.md)。历史配置与 CLI 文档保存于 [v1 历史归档](docs/archive/v1/README.md)，不可用于启动 v2。

### 流停顿方向

新增 `aigate_stream_backend_wait_max_seconds`（当前读取中的流，自上次有效输出后累计 Read 等待）及 `aigate_stream_write_active_max_seconds`（当前一轮 Write + Flush 持续时间）。两者单位秒，按 backend 输出，`request_scope="streaming"`。首次输出前从首次读取响应体计时，写出耗时不计入后端等待；心跳/usage 不重置，完成或观察未知的流退出后端等待统计，最终写出仍计时。没有符合条件的请求输出零；旧指标保持原义。指标只区分供数与输出方向，不直接判定模型故障或客户端不读。

### 流异常诊断

启用画像时，记录单次**输出中途读取等待超过 60 秒**或**客户端 Write + Flush 超过 60 秒**的异常；阻塞期间即可查询。仅保存最后一条用户文本的前 10 个和后 10 个字符，在平台独立“异常诊断”页面按关键词和触发时间搜索。默认保留 72 小时 / 256 MiB；原有超过 300 秒的慢请求捕获保持不变。接口、计时口径和存储说明见 [流异常诊断](docs/STREAM_INCIDENTS.md)。
