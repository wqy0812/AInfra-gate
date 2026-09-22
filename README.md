# aigate：平台控制的 API 网关

网关提供 OpenAI 兼容 JSON、SSE、模型发现、请求适配与观测。Router 配置、运行配置、绑定、操作日志和测试占用的持久化全部由平台负责。网关进程仅保留内存快照，重启后暂停业务，等待平台恢复。

## 控制方式

业务入口为 `POST /v1/chat/completions` 和 `GET /v1/models`；模型由 Router 发现，响应可使用本网关的 `public_model_name`。元数据兼容规则和 overrides 随 Router 交换；入口名称、入口凭据、连接池与观测策略留在本网关。

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

完整接口、状态机、迁移和待执行验收步骤见 [平台控制 v2 操作说明](MANAGEMENT.md)。systemd 模板见 [deploy/control-v2](deploy/control-v2/README.md)。历史配置与 CLI 文档保存于 [v1 历史说明](docs/LEGACY_README_V1.md)，不可用于启动 v2。
