# 独立切流与功能服务操作说明

本文描述当前拆分源码的行为；三台主机的已部署版本与验收范围见[当前部署身份](docs/DEPLOYMENT.md)。平台侧协议、共享目标协调与迁移详见 [code-eval 控制 v3](../code-eval/docs/gateway-control-v3.md)。历史 v2 发布记录只代表当次版本；新窗口必须核对源码 commit、三个二进制摘要和实际运行版本。

## 职责与认证

新模型绑定/开放准入之前，操作者必须先执行[固定请求门禁](docs/MODEL_ADMISSION.md)，确认 `body.json` 与 `body-agent-ds4.json` 经候选功能服务均正常完成，并保存通过报告。此额外门禁不替代下文 `validate`；当前状态机不会自动执行这两个长请求。

`aigate-switch` 保存内存绑定快照、准入、命令回执和测试占用；平台持久化入口、功能目标、绑定与操作记录。`aigate-proxy` 固定连接一个 Router，从本地文件读取功能参数并独立运行，保存该 Router 的画像与诊断。多个入口可共享一个功能目标，功能服务也可直接访问。

仅切流控制端口要求 `Authorization: Bearer <AIGATE_CONTROL_KEY>` 和实际 TCP 来源 CIDR，不信任转发 IP 头；回放控制服务使用独立的 `AIGATE_REPLAY_CONTROL_KEY` 和同样的来源校验。业务、模型、健康、功能状态、指标和画像接口无需认证。功能服务不提供配置写入、发布或热更新，配置变更须重启后读回平台；读回是观测，不自动替换切流绑定快照。

## 切流控制 v3 与旧执行镜像

| 接口 | 返回或动作 |
| --- | --- |
| `GET /control/v3/status` | boot、接流状态、当前与候选快照、当前及旧快照在途计数、测试占用、最后命令序号 |
| `POST /control/v3/commands` | 提交幂等命令 |
| `GET /control/v3/commands/{seq}` | 查询原命令回执 |
| `GET /control/v2/status` | 固定旧执行镜像兼容状态，`counts_scope=retiring_snapshots`，counts 只包含仍在排空的非当前快照 |
| `GET /control/v2/commands/{seq}` | 查询同一命令回执集合 |
| `POST /control/v2/commands` | 只接受 `test-acquire`、`test-release`、`test-status`；其他动作返回 `legacy_command_removed` |

v3 另有 `record-start`、`record-stop`、`record-status`、`record-pin`、`record-unpin` 录制动作，`/control/v3/status` 增加 `recording` 字段（未启用录制时为 `null`）。录制动作不改变准入、绑定和测试占用，字段与目录格式见 [流量录制与回放](docs/TRAFFIC_RECORDING.md)。

v3 的 `counts` 用于平台汇总排空；v2 兼容视图不能当作总负载。共享目标的普通业务流量不阻止旧执行镜像申请测试占用。所有接口使用同一 boot、单调命令序号及结果集合，v2 和 v3 不能各自从 1 开始编号。

命令体字段固定为 `protocol_version`、`operation_id`、`gateway_id`、`expected_boot_id`、`command_seq`、`expected_binding_revision`、`action`、`payload`。版本必须与路径一致；请求体上限 1 MiB，严格拒绝未知字段。相同序号和相同内容返回原结果，内容冲突、旧 boot、过期序号或绑定版本失配拒绝。过期序号返回 409 `stale_command` 且从不执行，同时保存 `failed / stale_command` 回执，丢失应答的调用方可据此改用新序号重发。最近 256 个序号范围内的已完成回执及进行中回执保留在内存，重启不保留。命令被受理不表示远端动作成功，调用方需检查 `status`、`error` 并查询异步验证结果。

`prepare` 的 v3 payload 只包含下列绑定字段，不接受旧 Router 后端、业务密钥或功能运行配置：

```json
{
  "router_id": "r1",
  "router_version": 1,
  "gateway_config_version": 1,
  "binding_revision": 2,
  "target_url": "http://proxy-host:18084",
  "config_digest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
```

版本号必须为正，绑定版本必须大于当前版本；`config_digest` 是功能服务实际返回的 64 位小写十六进制摘要，示例值不能用于真实接入。`target_url` 是功能服务根地址，无用户名、密码、查询参数或片段。平台保留 Router 版本与入口配置版本字段供固定旧执行镜像兼容读取，功能配置来源仍为本地文件。

- `prepare` 仅构造候选目标，不访问 Router；`validate` 先核对目标 `/runtime/v1/status` 的服务身份、Router 与摘要，再通过其实际业务接口执行发现、JSON、SSE、工具调用及工具结果续答。工具调用阶段提供工具定义并通过提示请求调用，不传强制指定函数的 `tool_choice`，不要求后端支持该选择方式；返回的工具调用名称、ID、参数及结果续答仍须正确。默认两轮、每步 120 秒、总计 900 秒、max_tokens 1024。验证只在候选快照被替换、丢弃或激活，或同一候选再次 `validate` 时返回 `validation_superseded`；期间的其他命令（录制、测试占用等）不影响结果。
- `quiesce` 立即关闭新请求准入，不等待旧请求结束。`activate` 原子替换当前快照并设置 `payload.admission`；调用方必须先确认自己拥有的暂停状态。
- `resume` 仅允许同一操作恢复其暂停的旧快照；`discard` 释放该操作的候选快照；`retire` 永久停止当前 boot 接流。
- `test-acquire`、`test-release`、`test-status` 使用 task ID 和 32～256 字节随机令牌。申请同时校验 Router、绑定版本、模型及配置摘要；旧客户端省略摘要时从当前快照取得。令牌只以摘要存储。释放留下 tombstone 防止迟到占用，在途测试结束前不清空占用；新 boot 不接受旧令牌。

## 在线切换与共享目标

单入口切换通过 `POST /api/gateways/{gid}/switch` 提交 `operation_id`、`expected_revision`、`router_id`。平台准备并验证目标、暂停该入口并确认、事务更新绑定、激活并读回。首次绑定也使用此入口。再次选择当前目标会重新读取功能配置并建立新绑定版本，可在功能服务修改本地文件并重启后更新该入口快照；其他入口各自更新。

多个入口可以共享同一功能目标。切换只更新所选入口，其他入口的绑定版本及准入不变。交换 API 已退役，终态历史从统一操作接口读取；存在非终态历史交换时，新控制服务及升级预检拒绝继续，不修改绑定、命令或锁。原版本须先完成原操作收尾。

切流不等待所有旧请求排空，已受理上传、等待响应及 SSE 保持原快照；生成请求不自动重放。提交前失败按操作归属恢复旧准入，并确认已发命令结果防止迟到暂停。提交后只向前完成，不能取消已提交绑定。测试占用和操作锁按 Router 协调；测试占用不等于阻断该 Router 的全部普通业务。

Router 维护关闭所有当前指向它的受管入口，并阻止新增绑定进入。平台保存本次维护原先关闭的入口，恢复时只恢复仍保持对应绑定的入口。排空汇总全部入口中指向该 Router 的当前和旧快照。功能服务继续接收直连请求；完全停用 Router 需要部署操作停止功能服务，并核对其自身在途状态。

## 重启、目录与维护窗口

切流服务重启后没有绑定快照，业务返回 503 和 `Retry-After: 1`，等待平台恢复；功能服务合法配置启动后无需平台即可接流。平台掉线不改变已有切流快照。检测新 boot 时，平台保留旧实例停止接流和历史 Router 排空两个独立确认，持久命令日志中可能执行的候选/激活目标也纳入排空义务。状态未知时保留测试锁、操作及证据。

按改动范围进入维护，安全收尾已接收请求、判题和网关占用，然后替换受影响组件。无需为软件升级维持零停机或等待整项评测结束；超时保持维护，由操作者决定后续，不能强制清除未知占用。启动后先验收功能服务直连与状态，再验收切流绑定和平台接入，最后恢复原准入。失败修复当前版本，不回退、不清空历史数据或自动重放中断任务。

每个 Router 使用独立功能服务数据根目录，例 `/var/lib/aigate-proxy-r1`。其中 `request-profile/<策略摘要>` 与 `slow-requests/<策略摘要>` 按当前本地策略清理，启动时和之后每分钟检查；关闭采集仍清理该数据根目录内的历史策略。正在写入及被其他进程锁定的目录受保护，占用计入总额；不沿符号链接清理外来内容。异常摘要目录为 `stream-incidents`，默认 72 小时 / 256 MiB；完整文件位于其 `requests/` 子树，独立限制为 72 小时 / 10 GiB，包含 pending 和正在写入文件，细节见 [流异常诊断](docs/STREAM_INCIDENTS.md)。切流服务不创建观测目录；配置 `--record-root` 时只写 `<record-root>/<入口ID>/` 录制目录，容量由 `--record-max-bytes` 限制。

拆分前的入口历史保留原归属，不按新 Router 重命名、重标或搬入新服务数据根目录，记录拆分生效时间。平台数据库、主密钥、固定执行和判题镜像、VM 历史、发送缓冲和处理水位均保留。模板见 [packaging](packaging/README.md)；远程操作遵守工作区 SSH MCP 约束，新建服务器目录必须在报告中列明。

## 配置导出与数据库迁移

使用平台的 [aigate_split_v3.py](../code-eval/deploy/migration/aigate_split_v3.py)，先按实际生效绑定导出，草稿不成为功能配置。准备既有权限 0700 的导出目录与完整映射清单，包含所有入口及已绑定 Router；`source_gateway_id` 必须是当时实际绑定该 Router 的入口。清单、导出回执和精确前提见平台 v3 指南。

在 code-eval 项目目录，依次按维护窗口执行：

```sh
PYTHONPATH=backend .venv/bin/python deploy/migration/aigate_split_v3.py \
  --db /path/to/platform.db --directory /path/to/existing-export-directory \
  --manifest /path/to/split-manifest.json --export

# 用导出的每个 Router 文件启动功能服务后，读取其实际运行状态并核对摘要：
PYTHONPATH=backend .venv/bin/python deploy/migration/aigate_split_v3.py \
  --db /path/to/platform.db --directory /path/to/existing-export-directory \
  --proxy-binary /path/to/aigate-proxy --check

# 确认原服务安全收尾、维护与备份完成后应用；不是普通在线管理动作：
PYTHONPATH=backend .venv/bin/python deploy/migration/aigate_split_v3.py \
  --db /path/to/platform.db --directory /path/to/existing-export-directory \
  --proxy-binary /path/to/aigate-proxy --apply --maintenance-confirmed
```

导出时记录原准入状态，后续暂停不能覆盖该意图。`--check`/`--apply` 需使用实际部署的功能服务二进制及相同上游环境变量，通过 `--check-config` 与已运行服务状态核验摘要；检查失败停止迁移。迁移保留入口和 Router ID、历史任务及操作，解除 Router 绑定唯一约束，功能配置只保存为带摘要的只读观测副本。

## 本地验证

```sh
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go test -mod=vendor . ./cmd/...
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go test -mod=vendor -race . ./cmd/...
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go vet -mod=vendor . ./cmd/...
./scripts/build.sh
./bin/aigate-proxy --check-config --config examples/proxy.example.json
./bin/aigate-replay version
```

`. ./cmd/...` 覆盖全部受 Git 管理的生产包和命令；忽略的 `deploy/` 内包含独立历史证据夹具，不作为生产包参与编译。Go 网络夹具使用本机临时监听器，覆盖独立功能服务、真实两层 HTTP、上传/取消/SSE、切流快照、共享目标、v2 测试兼容、配置变化与脱敏。平台验收覆盖三个入口、单入口切换、共享目标、共享维护与恢复、固定旧执行镜像路径及迁移保护；页面为桌面 Chrome 1280×720 和 iPhone 竖屏触控。以上本地验证不等于线上部署或真实流量验收。
