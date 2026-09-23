# aigate：平台控制的 API 网关

网关提供 OpenAI 兼容 JSON、SSE、模型发现、请求适配与观测。Router 配置、运行配置、绑定、操作日志和测试占用的持久化全部由平台负责。网关进程仅保留内存快照，重启后暂停业务，等待平台恢复。

## 控制方式

业务入口为 `POST /v1/chat/completions` 和 `GET /v1/models`；模型由 Router 发现，响应可使用本网关的 `public_model_name`。元数据兼容规则和 overrides 随 Router 交换；入口名称、入口凭据、连接池与观测策略留在本网关。

客户端请求的 `model` 可以填写任意字符串，也可以省略或为 null；网关发往 Router 前替换为发现的实际模型名，不要求客户端名称与后端匹配。`public_model_name` 用于对外响应中的模型别名，与请求模型的替换规则独立。

`POST /v1/chat/completions` 请求体上传总时限为 5 分钟，大小上限为 30 MB（30,000,000 字节）。上传超时返回 HTTP 408，超限返回 HTTP 413；请求体读取完成后清除读取截止时间，后续生成与 SSE 使用原有生成超时。

所有管理动作经过平台 API。网关仅开放鉴权的 `/control/v2/status`、`/control/v2/commands`、`/control/v2/commands/{seq}` 给 Engine。二进制不再支持 `check`、`switch`、`status`、`backends` 等管理 CLI、`--config`、路由文件或 Unix 管理 socket。

两网关交换 Router：验证主动目标 → 双方暂停新请求并确认 → 平台事务交换绑定 → 主动方激活并单独成功 → 被动方直接激活。被动 Router 故障不阻塞、不回退主动方；被动网关控制不可达则继续暂停并重试。已受理请求，包括 SSE 和等待模型发现/连接池的请求，保持原快照直到结束。

## 启动

只需 Go 标准库，没有第三方依赖。构建与测试始终显式使用 vendor 模式。以下命令是后续执行说明，当前改造尚未完成测试验收。

```sh
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go build -mod=vendor -trimpath -o bin/aigate .
export AIGATE_ID=g1
export AIGATE_CONTROL_KEY='<至少 32 字节，由部署环境注入>'
./bin/aigate serve --listen 0.0.0.0:10015 --control-listen 0.0.0.0:18083 --allowed-cidrs '<Engine IP>/32' --profile-listen 0.0.0.0:18082 --data-root /var/lib/aigate --shutdown-timeout 62m
```

`--data-root` 只保存观测数据，不保存业务配置、凭据、路由或管理状态。相同观测策略复用写入器；不同策略使用不同目录，避免旧请求与新配置竞争文件。平台下发观测 API 密钥。控制监听地址、观测监听地址及数据根目录属于部署参数，不接受运行配置覆盖。

首次启动的业务接口返回 503；`/health` 仅表示进程存活。进程处于暂停状态时，新业务返回 503 和 `Retry-After: 1`。Router 非 2xx 仍按现有透传规则返回，网关不自动切回。

## 业务响应与旁路观测

OpenAI 适配器的非流式响应采用“正文、状态码及指定响应头透传”；生成阶段的非 2xx 响应也采用此规则。指定头为 `Content-Type`、`Content-Encoding`、`Retry-After`、`WWW-Authenticate`、`Location`，过滤 `Connection` 指定的逐跳头，不传递上游 Cookie 或覆盖网关的请求 ID。`Cache-Control`、`Vary` 等不在透传名单中。Go Transport 自动解压时转发解压后的正文及对应响应头。SSE 成功响应使用既有事件流转发及响应头规则，模板适配器仍执行响应转换。

配置 `public_model_name` 后，非流式 2xx JSON 仅在 choices 非空且每个候选均有 assistant message 时改写顶层 `model`；不限制 message 的正文形式，也不要求 finish_reason 才能改写。顶层存在 `error`（包括 null）或无法识别为聊天响应时，正文逐字节保留，不因含有 `model` 就修改。非 2xx 错误正文始终透传。别名识别与下述完成状态观测相互独立。

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

完整接口、状态机、迁移和待执行验收步骤见 [平台控制 v2 操作说明](MANAGEMENT.md)。systemd 模板见 [deploy/control-v2](deploy/control-v2/README.md)。历史配置与 CLI 文档保存于 [v1 历史说明](docs/LEGACY_README_V1.md)，不可用于启动 v2。
