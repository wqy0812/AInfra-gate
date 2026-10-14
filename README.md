# aigate：独立切流与功能服务

当前源码提供三个独立二进制。2026-10-14 已部署版本、运行摘要和录放服务配置见[当前部署身份](docs/DEPLOYMENT.md)；后续操作仍分别核验源码、构建产物和在线版本。

| 服务 | 职责 | 入站认证 | 配置来源 |
| --- | --- | --- | --- |
| `aigate-switch` | 请求转发、在线切流、准入、绑定版本、在途计数、测试占用 | 仅控制接口需要 Bearer 密钥与来源 CIDR | code-eval 下发内存绑定快照 |
| `aigate-proxy` | 模型发现、适配、参数覆盖、SSE、指标、画像、慢请求与异常诊断 | 所有接口无需认证 | 本地 JSON 文件，修改后重启 |
| `aigate-replay` | 回放录制的业务流量到本地白名单中的功能服务，提供状态与发送进度 | 控制接口需要独立 Bearer 密钥与来源 CIDR | 本地目标白名单文件；run 由平台下发 |

每个功能服务固定连接一个 Router，可脱离切流服务和平台独立运行。多个切流入口可共享同一功能服务。指标与观测文件归属 Router，同时包含经过切流和直接访问的请求；切流不迁移这些数据。平台负责入口、目标登记、绑定及操作记录，功能参数的权威来源是功能服务本地文件。

## 业务行为与服务边界

两者都提供 `POST /v1/chat/completions`、`GET /v1/models`、`GET /health` 和 `GET /openapi.json`。`/health` 只表示进程可响应，不证明 Router 可用。切流服务启动时暂停接流，等待平台恢复绑定；功能服务读取合法配置后独立接流，Router 不可达时按业务错误返回，进程继续运行。

切流层只固定并转发每个已受理请求的目标，不改写 JSON、不重复适配或记录画像，不自动重试生成请求。单入口切换只改变该入口后续请求，多个入口可以共享同一功能目标；上传、等待上游和 SSE 直到结束都沿用原快照。

功能服务发现 Router 实际模型，并在发往 Router 前替换请求中的 `model`；客户端模型名可为任意字符串、省略或 null。响应保留后端模型名。发现响应解压后上限为 4 MiB，超过时返回 503 `model_discovery_failed`，不发送生成请求。模板适配、兼容规则与参数覆盖只在功能层执行一次。

请求上传总时限 5 分钟，大小上限 30 MB（30,000,000 字节）。超时返回 408，超限返回 413；上传后清除读取截止时间。业务读头时限为 30 秒，独立控制与观测监听器为 5 秒。切流的 `--forward-timeout` 默认 62 分钟，覆盖上传、上游等待与向客户端写出；功能服务的生成超时由本地 `settings.timeout_seconds` 决定。提前拒绝不等待上传完正文；HTTP/1.x 关闭连接，HTTP/2 仅关闭请求流。

保留 `X-Aigate-Backend`、`X-Aigate-Route-Version`、`X-Aigate-Model` 响应头；功能服务还返回 `X-Aigate-Config-Digest`。切流覆盖客户端提供的内部路由/测试元数据，并移除向功能服务转发的业务 Authorization 与测试令牌。已确认测试占用的模型和配置摘要随请求传给功能服务，在发起生成前检查。直连不需要这些字段；它们是每次请求断言，不是访问凭据或写配置接口。

## 构建与启动

仅使用 Go 标准库，根包 `aigate` 供三个命令导入。构建与测试显式使用 vendor 模式和本机已安装工具链：

```sh
./scripts/build.sh
# 等价的独立构建：
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go build -mod=vendor -trimpath -o bin/aigate-switch ./cmd/aigate-switch
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go build -mod=vendor -trimpath -o bin/aigate-proxy ./cmd/aigate-proxy
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go build -mod=vendor -trimpath -o bin/aigate-replay ./cmd/aigate-replay
./bin/aigate-switch version
./bin/aigate-proxy version
./bin/aigate-replay version
```

`scripts/build.sh [输出目录]` 默认生成 `bin/aigate-switch`、`bin/aigate-proxy` 和 `bin/aigate-replay`。版本命令分别标识 `control/v3 (v2 test compatibility)` 、`runtime/v1` 与 `replay/v1`；发布身份仍需记录源码 commit 和二进制摘要。

功能服务可单独启动，示例连接本机 Router 的 18077 端口：

```sh
./bin/aigate-proxy --check-config --config examples/proxy.example.json
./bin/aigate-proxy serve --config examples/proxy.example.json \
  --listen 127.0.0.1:18084 --profile-listen 127.0.0.1:18082 \
  --data-root /var/lib/aigate-proxy-r1 --shutdown-timeout 62m
```

`--check-config` 只解析、规范化并输出脱敏状态，不启动监听器、不访问 Router、不创建目录；输出 `boot_id=""`、`admission=false`。合法本地文件必须包含 `service_id`、`router_id`、`backend`，`settings` 可省略。示例明确设置生成时限为 1860 秒；省略时默认 120 秒。配置文件上限 1 MiB，未知字段或非法值使启动失败。入站业务/观测密钥、画像监听器和存储目录不能放入功能配置；后两者由启动参数管理。参考 [完整示例](examples/proxy.example.json)。

`/runtime/v1/status` 返回 `service`、`service_id`、`router_id`、`boot_id`、`config_digest`、`settings`、`backend`、`counts`、`admission`。状态不暴露上游 header 值、环境变量名、URL 查询参数、观测密钥或目录。摘要覆盖规范化配置及解析后的上游 header，适用于识别本地配置变化。

功能服务业务监听器也提供 `/runtime/v1/status`、`/metrics`、`/profile/v1/*`；`--profile-listen` 默认为 `127.0.0.1:18082`，设为空字符串可关闭独立观测监听器。关闭画像时 `/metrics` 与画像接口返回 503，健康与运行状态仍可查询。画像分析任务是观测查询，不提供配置写入或热更新接口。

切流服务只需要入口 ID、控制密钥和来源 CIDR；密钥由部署环境注入：

```sh
# 已通过环境注入 AIGATE_CONTROL_KEY；至少 32 字节，不写在命令行。
./bin/aigate-switch serve --id g1 --listen 127.0.0.1:10015 \
  --control-listen 127.0.0.1:18083 --allowed-cidrs 127.0.0.0/8,::1/128 \
  --forward-timeout 62m --shutdown-timeout 62m
```

`--id` 可由 `AIGATE_ID` 提供。控制端口仅提供鉴权 `/control/v3/*` 和旧固定执行镜像所需的有限 v2 接口；功能服务不提供切流控制。收到退出信号后先停止新业务并优雅收尾，默认上限 62 分钟。systemd 模板和逐实例目录约定见 [packaging](packaging/README.md)。

## 单元测试覆盖率

运行 `./scripts/test_coverage.sh`，可传 Go 测试参数（例如 `-run TestRuntime -race`）。入口使用离线 vendor 模式，覆盖网关及 `cmd/` 中的命令；本地忽略的 `deploy/` 发布工具不进入分母。结果输出到 `dist/coverage/coverage.out` 和 `dist/coverage/index.html`，测试失败时保留报告与失败退出码。

`deploy/go.mod` 仅为历史发布材料建立独立模块边界，使仓库根目录的 `go test -mod=vendor ./...`、`go vet -mod=vendor ./...` 与包枚举不递归进入 `deploy/`。Git 只保留该边界文件，其余发布材料继续忽略；原始证据、历史测试片段和 overlay 路径保持不变。历史片段不构成可独立测试的应用模块，不在 `deploy/` 内递归运行测试。

控制协议、配置边界、画像 HTTP/分析队列及预算、观测存储故障、控制进程启动与 SIGTERM 收尾均有离线回归。网络夹具仅使用本机临时监听器，磁盘夹具使用临时目录，不访问部署节点或真实模型。

## 业务响应与旁路观测

OpenAI 适配器的非流式响应采用“正文、状态码及指定响应头透传”；生成阶段的非 2xx 响应也采用此规则。指定头为 `Content-Type`、`Content-Encoding`、`Retry-After`、`WWW-Authenticate`、`Location`，过滤 `Connection` 指定的逐跳头，不传递上游 Cookie 或覆盖网关的请求 ID。`Cache-Control`、`Vary` 等不在透传名单中。Go Transport 自动解压时转发解压后的正文及对应响应头。SSE 成功响应支持未编码、`identity` 和单层 `gzip`；显式请求的 gzip 也会在观察和转发前流式解压，客户端收到未压缩 SSE。其他编码返回 HTTP 502 `invalid_upstream_stream`；流中解压失败沿用读取错误处理，不视为正常完成。观察器已识别并转发 `[DONE]` 后发生读取错误时，仍按读取错误记录，但网关不再追加错误事件；此规则不覆盖观察器因解析上限停止解析而未识别 `[DONE]` 的情况，也不拦截上游在 `[DONE]` 后发送的数据。模板适配器仍执行响应转换。

OpenAI 普通响应和 SSE 正文不改写模型名称，也不补写缺失的 `model` 字段。本地配置不接受 `public_model_name`，历史任务和历史响应不改写。

非流式 OpenAI 2xx 响应增加旁路观测，不改变返回状态、正文或接流状态，也不重试、切换 Router。启用 `profile.enabled` 后，由功能服务无需认证的 `/metrics` 提供：

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

设计边界与评审检查见 [设计原则](docs/DESIGN_PRINCIPLES.md)。完整接口、状态机、迁移和验收步骤见 [拆分服务操作说明](MANAGEMENT.md)。旧版配置、CLI 和设计方案通过 [Git 历史查阅](docs/archive/README.md)。

### 流量录制与回放

切流服务配置 `--record-root` 后，可由平台按整点、半点定时录制入口看到的原始业务流量（半小时分段、容量上限、可保留段），`aigate-replay` 把录制按原速或固定并发回放到候选功能服务，性能由监控观察。录制与回放只由平台控制，没有命令行入口。见 [流量录制与回放](docs/TRAFFIC_RECORDING.md)。

### 流停顿方向

新增 `aigate_stream_backend_wait_max_seconds`（当前读取中的流，自上次有效输出后累计 Read 等待）及 `aigate_stream_write_active_max_seconds`（当前一轮 Write + Flush 持续时间）。两者单位秒，按 backend 输出，`request_scope="streaming"`。首次输出前从首次读取响应体计时，写出耗时不计入后端等待；心跳/usage 不重置，完成或观察未知的流退出后端等待统计，最终写出仍计时。没有符合条件的请求输出零；旧指标保持原义。指标只区分供数与输出方向，不直接判定模型故障或客户端不读。经切流入口访问时，功能服务的写出对端为切流服务；该指标不能直接解释为最终客户端阻塞。

### 流异常诊断

启用画像时，记录单次**输出中途读取等待超过 60 秒**或**向连接对端 Write + Flush 超过 60 秒**的异常；阻塞期间即可查询。平台“异常诊断”页展示最后一条用户消息中问题正文的前后各 10 个 Unicode 码点，按首尾摘要及触发时间搜索。摘要保留 72 小时 / 256 MiB；点击摘要可按需查看完整原始请求和已保存的上游 SSE，采集文件及停顿位置存于服务器 `stream-incidents/requests/`，预算 72 小时 / 10 GiB，采集不完整时明确标记。原有超过 300 秒的慢请求捕获保持不变。接口、计时口径和存储说明见 [流异常诊断](docs/STREAM_INCIDENTS.md)。
