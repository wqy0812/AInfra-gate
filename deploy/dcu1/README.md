# dcu1 上线记录

以下按日期保存历史发布结果；2026-09-15 22:17 的 metric-v2 后续发布记录位于相邻 monitoring 仓库的 `docs/metric-scope-v2-20260915.md`，源码口径见 [项目说明](../../README.md)。操作前重新读取当前二进制、配置和路由，不能将下方旧版本当作实时状态。脚本依赖与入库规则见 [部署源码说明](../README.md)。

## 慢请求输入留存版本（2026-09-15）

DCU 网关已于 17:54:09 更新为与 A3 相同的 `slow-requests-20260915`，二进制 SHA-256 为 `a1cd6006464b14a1199c582f69350a0971c015a4facccb338728251e27be9fd2`。启用仅流式请求结束后耗时严格超过 300 秒的完整原始输入留存，目录 `/data4/aigate/slow-requests`，默认保留 72 小时／10 GiB。

路由保持 `active_backend=dcu`、`route_version=3`，A3 后端维护锁保持。DCU 同步采用流式画像和独立非流式计数，新流式计数起点独立，旧混合检查点和历史数据保留。本次未更新 A3 网关或推理服务。详情、验收证据和回退方法见 [本次发布记录](slow-requests-20260915/README.md)。以下为历史上线记录。

## 历史版本 e435f42（2026-09-14）

已通过 SSH MCP 升级至 `/data4/aigate/releases/e435f42-30a42c2020ea`，二进制 SHA256 `30a42c2020ea2445110289f5e606c6c1ddb4daa07aa3e687a845693367ca00e9`。用户选择保持 A3 接流：`active_backend=a3`、`route_version=2`，DCU 后端维护锁保持。

任意字符串 `model` 或省略模型名均可调用。候选及正式 HTTP 各 49/49、两轮完整协议验证，以及任意模型名 JSON/SSE 各 6/6 全部通过。管理改用私有 Unix socket，8081 已关闭；画像历史、累计计数起点、HMAC、凭据及后端注册表保留，新指标已进入 VictoriaMetrics。DCU 两台主机上的推理及其他容器未重启。

详细证据、回退位置及验收范围见 [本次部署验收](evidence/e435f42-20260914/README.md)。下方为迁移说明及历史记录。

## CLI 管理版本迁移（已于 2026-09-14 部署）

新版本移除 Web、管理 HTTP API 和 8081 监听，管理仅由本机 CLI 通过 Unix socket 完成。部署前在主配置的 management 中移除 listen、api_key_env，增加 `"socket": "/data4/aigate/.aigate/control.sock"`；状态文件保持原位，勿覆盖已有 v2 后端注册表。socket 目录由程序创建为 0700，文件为 0600，以服务用户或 root 运行 `aigate-admin`。

`admin.env` 可能同时包含业务、下游或 Profile 凭据，不能因为管理密钥移除而删除整个文件。包装脚本仍加载该环境文件，但 CLI 授权由本机 socket 权限控制。

升级仍需先备份版本、配置和状态，候选验证后排空并优雅重启。启动后执行 `aigate-admin status --json`、`aigate-admin backends list` 和真实协议预热，确认 TCP 8081 无监听；HTTP 验证工具改为只检查业务端口。失败时配套恢复旧版本和旧配置。下方旧版记录为历史证据，本次部署结果以上方 e435f42 验收为准。


状态：**2026-09-12 21:14（Asia/Shanghai）正式网关已升级，新版真实协议与 HTTP 验收全部通过。**

## DCU → A3 往返切换尝试（2026-09-12 21:20）

- 用户指定切往 A3 再切回。初始仅有 dcu，先备份状态，再在线新增 `a3`：`http://122.52.5.131:9090/v1`，首选模型 `dsv4-spark`。保存不切流，A3 配置已持久化，实际协议尚未验证通过。
- A3 统一模型发现接口连续返回 HTTP 503，响应指出全部 P/D 实例失败；从 dcu1 直连两台 A3 的 7100–7103 共 8 个端口均 Connection refused。
- 实际提交 `switch single a3`，操作 `switch-a3-20260912-1318` 在 discovery 阶段失败，`route_committed=false`。当前活动后端仍为 dcu，route_version 仍为 1；未切入 A3，因此没有执行实际 A3 → DCU 回程，不宣称往返验收完成。
- 切换尝试前后正常模式 HTTP 检查均 52/52，通过真实 JSON/SSE；最终在途/异常/连续故障均为 0，网关 PID 1696758 保持不变，没有重启网关或推理组件。
- 备份和证据：`/data4/aigate/{backups,evidence}/20260912T131800Z-switch/`。A3 保留为未就绪备用配置；待服务恢复后重新执行完整往返切换。

详细记录：[切换测试报告](evidence/20260912T131800Z-switch/REPORT.md)。

## 正式升级与验收（2026-09-12 21:14）

- 用户恢复 Router 后，现场确认 1 个健康 Prefill、1 个健康 Decode，模型发现恢复；P/D 地址已写入 Router 容器启动参数。本次未重启 Router 或推理组件，也未演练 Router 再次重启。
- 使用全新独立候选状态执行两轮真实预热：模型发现 + 8 次生成全部通过，覆盖 JSON、SSE、流式工具调用和工具结果续答；候选正常模式 HTTP 检查 52/52 通过。
- 核实正式入口在途业务/流/探测均为 0 后，优雅停止旧 Go 服务，备份最终 `clean_shutdown=true` 的 v1 状态；切换 current 链接并启动新版，自动迁移到 v2。正式服务于 21:12:40 启动，PID 1696758，systemd 为 active/enabled，NRestarts=0。
- 正式 `10015` 入口 HTTP 验收 52/52 通过，包含真实 JSON/SSE、metadata.workspace 对象兼容、模型发现、管理鉴权、跨域拒绝、非法请求、网页资源和 32 个并发健康检查。正式管理预热另执行两轮完整协议，共 8 次真实生成全部通过；普通 SSE 首个有效增量约 325–386 ms。
- 正式后端仅 `dcu`，下游仍是 `127.0.0.1:10016`；主配置、systemd 单元、管理密钥、业务/管理绑定地址保持不变，未复制旧候选合成后端。最终无在途请求，abnormal=0、consecutive_failures=0；候选 18015/18081 已停止。
- 运行版本：`/data4/aigate/releases/20260912T114800Z-online-backends/`，SHA-256：`afec57d7bc731b0a7e86f56ee74948ee1cf9cd2cce6c76ca636946a43a67e5db`。
- 备份：`/data4/aigate/backups/20260912T131100Z-upgrade/`；系统另自动保存状态 v1 备份。状态及备份权限 0600。
- `/data4/aigate/rollback.sh` 已改为调用该备份目录内的 Go 版本回退脚本，不再启动历史 Python 容器；回退会保留当前状态，再配套恢复旧 Go 二进制、配置和 v1 状态。未实际执行生产回退演练；回退后仍须验证真实生成。

本次完成短请求协议验收，未验收长上下文、生成吞吐、大并发、浏览器表单操作或 DCU/A3 双端切换。前一轮本地 55 项竞态测试和 Linux 回归对应同一网关二进制版本，结果继续适用。

完整报告：[正式升级验收](evidence/20260912T131100Z-upgrade/REPORT.md)。远程证据位于 `/data4/aigate/evidence/20260912T131100Z-upgrade/`。

## 本次新版部署与测试（2026-09-12 19:54）

- 现场核实 `aigate.service` 自 15:06 起运行，`10015` 已由 Go 网关提供，当前下游是 `127.0.0.1:10016`。下面早前记录中的 Python 入口、10017 和未安装 systemd 均为历史状态。
- 新版目录：`/data4/aigate/releases/20260912T114800Z-online-backends/`；二进制 SHA-256：`afec57d7bc731b0a7e86f56ee74948ee1cf9cd2cce6c76ca636946a43a67e5db`。
- 旧配置、状态、systemd 单元与版本链接备份：`/data4/aigate/backups/20260912T114800Z-online-backends/`。本次没有修改正式配置、状态或 current 链接，也没有重启正式网关或推理组件。
- 候选使用业务 `127.0.0.1:18015`、管理 `127.0.0.1:18081` 和独立状态。迁移运行中 v1 状态副本成功产生 v2，并正确标记 `drain_unknown`；随后使用新建候选状态执行独立检查。
- 本地 `go test -race -timeout 60s -json ./...`：55 个顶层测试全部通过，无跳过；包含完整本地请求样例、实际子进程生命周期、SSE、取消、切流、回切、维护与在线后端配置。`go vet ./...`、两个前端脚本语法检查、JSON 文档检查与 Linux amd64 构建通过。
- dcu1 上运行交叉编译的测试二进制：54 个顶层 PASS、1 个顶层 SKIP；跳过需要本机 Go 编译器的生命周期项，另有两个私有样例子项因不分发样例而跳过。这些测试使用模拟 HTTP 下游，不是实际模型验收。
- 实际候选 HTTP 检查：52/52 通过；包括页面资源、OpenAPI、管理鉴权、跨域保护、非法请求、下游不可用错误及 32 个并发健康请求。重启后同组检查再次 52/52 通过；不是生成吞吐压测。
- 实际候选管理 API：新增、编辑、幂等、过期版本冲突、活动端禁止编辑、凭据脱敏、状态 0600 和重启持久化通过。合成 `deployment_probe` 后端只写入候选状态，未切流，未加入正式配置。
- 实际 DCU 预热失败于 discovery，`route_committed=false`。Router `/workers` 返回 `total: 0`，`/v1/models` 返回 HTTP 503 `No models available`。真实 JSON、SSE、工具调用与续答没有达到执行条件，未验收长上下文、生成吞吐或 DCU/A3 双端切换。本次没有完成浏览器表单交互验收。
- 验证完成后停止候选，确认 18015/18081 无监听。正式 PID 3859081、版本哈希、配置、systemd 单元及所核对的下游容器启动时间保持不变。旧网关仍无法发现模型，不能宣称业务健康。

完整记录：[本次验收报告](evidence/20260912T114800Z-online-backends/REPORT.md)。远程原始证据在同名 `/data4/aigate/evidence/` 子目录；管理密钥留在服务器，未下载或打印。

后续先恢复实际 Router 的 Prefill/Decode 注册，再用干净的候选状态执行完整协议预热，全部通过后升级正式网关。不要将本次含合成后端的候选状态复制到生产。生产 v1 → v2 前须优雅停止旧版并再次备份最终状态；需要回退时配套恢复旧二进制和旧状态。

## 停止与迁移（2026-09-12 13:45）

- 向临时 aigate 进程 PID `3060410` 发送 SIGTERM，确认退出；`18015`、`8081` 均无监听，正式 `aigate.service` 未安装。
- 将 `/data2/aigate` 全部迁移到 `/data4/aigate`（约 11 MB），迁移前后核对 43 个条目的文件 SHA-256、权限、属主和链接；核对通过后删除旧目录。
- 更新运行配置、管理脚本、版本内服务模板及验收脚本中的目录引用，修正 `current` 链接；二进制 SHA-256 保持不变。
- 原始清单、修改前文件和迁移报告保存在 `/data4/aigate/backups/migration-to-data4-20260912T054527Z/`。
- 该临时实例使用的外部排障配置 `/data3/cp8-fix-20260912-121800/gateway-replay.json` 保留原位。
- 旧 Python `10015` 和 Router `10017` 保持运行；未重启 aigate 或推理组件。

以下为早前上线尝试的历史记录，其中 `/data2/aigate` 路径现已整体迁移到 `/data4/aigate`。

## 已完成

- 在 `/data2/aigate/` 建立版本、配置、管理密钥、状态、备份和证据目录。
- 备份旧 `sglang-workspace-compat` 容器配置、镜像元数据、程序及近期日志；保留容器和镜像。
- 上传 Linux amd64 二进制并核对 SHA-256：`7d12e125e64a98a0ca084e2bf2ecdfc1b686c88ad24fe2c4184202855aaef71e`。
- 使用 `127.0.0.1:18015` 和独立候选状态启动临时服务；管理接口绑定 `127.0.0.1:8081`。
- 模型发现、普通生成和 SSE 在真实 DCU 后端通过；前两次生成各返回 10 个 completion token。

## 暂停原因

候选预热操作于 08:52:12（Asia/Shanghai）开始。第一轮普通生成和 SSE 分别约 964 ms、994 ms 通过；SSE 首个有效增量约 637 ms。进入工具调用阶段后，Prefill 日志出现：

```text
gloo::EnforceNotMet
op.preamble.length <= op.nbytes. 8 vs 1
Received data size doesn't match expected size.
Fatal Python error: Aborted
Scheduler hit an exception
```

后续堆栈涉及 P/D bootstrap 和 HiCache 同步的分布式 all_reduce。08:54:14 工具调用预热以“下游请求超时”失败；未执行剩余预热及正式入口验收。异常与本次工具探测处于同一时间窗口，尚不能仅凭该时间关系认定唯一触发因素。

按上线门槛停止候选服务。`10015` 仍由原 Python 容器监听，`10017` 仍由原 Router 监听；尚未安装或启用正式 `aigate.service`。容器父进程与启动时间未变化，但 Prefill 调度子进程已报崩溃，因此不能把容器运行或端口监听视为推理恢复。

本次没有停止或重启 Router、Prefill、Decode、Mooncake，也未修改 SSH 配置。恢复或修改 Prefill 超出本次上线授权范围，需另行确认。

## 文件与后续操作

- 远程候选版本：`/data2/aigate/releases/20260912T004747Z-7d12e125/`。
- 远程备份：`/data2/aigate/backups/20260912T004747Z-7d12e125/`。
- 远程证据：`/data2/aigate/evidence/20260912T004747Z-7d12e125/`。
- `config.example.json`、`aigate.service`、`aigate-admin` 和 `rollback.sh` 为准备好的上线文件；管理密钥仅保存在服务器的 `admin.env`，未下载或打印。
- 原 Python 正式入口验收脚本在候选失败后未执行，现已随 Python 清理移除。再次上线前需补齐正式入口验收工具。

再次上线前，先解决下游 collective 异常，再确认业务健康和候选协议全项通过。禁止跳过工具验证直接交接 `10015`。实际停机交接仍按已批准计划执行，其他推理组件保持在授权边界内。
