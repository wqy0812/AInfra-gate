# 当前部署身份

## 2026-10-18 回放性能 v2：仅更新 replay

DCU、XPU、A3、PPU 的 **aigate-replay** 已更新至确定源码 `23ebe2c374924a5c435016c064994848e1ef4ea0`。四处 **switch、proxy 仍保持前次源码 `9cb5626dcd5a49be690b3bf86fd3bac421d285eb`**，本轮没有替换这些组件或模型服务。构建使用 Go 1.27.1、CGO_ENABLED=0、离线 vendor 模式和 trimpath，`vcs.modified=false`；实际运行进程二进制 SHA-256 与对应构建产物一致：

| 环境 / SSH MCP target | replay 部署方式 | 架构 | 实际进程 SHA-256 |
| --- | --- | --- | --- |
| DCU / `dcu1` | 原 `aigate-replay.service` | linux/amd64 | `f5cd614222a3dc070481b39e9ef283b0d8b6bc71ac587c5e2f4c27ec1513233f` |
| XPU / `xpu1` | 原 `aigate-replay.service` | linux/amd64 | `f5cd614222a3dc070481b39e9ef283b0d8b6bc71ac587c5e2f4c27ec1513233f` |
| A3 / `hwA3-1` | namespace `ai-gateway`，`a3-aigate-replay` | linux/arm64 | `5980f303c223fcffa0ef82e544c734332634103e318190311b12c25186b35fe6` |
| PPU / `ppu` | namespace `ai-gateway`，`ppu-aigate-replay` | linux/amd64 | `f5cd614222a3dc070481b39e9ef283b0d8b6bc71ac587c5e2f4c27ec1513233f` |

A3 当前宿主为 `hwA3-1`；旧 `A3-gate` 上已停止的容器仅保留作迁移证据，本轮未操作其服务。

四处 authenticated status 均正常，capabilities 保留 `recording-original-request-v1`、`recording-overlay-sync-v1`、`recording-multi-target-v1`，并新增 **`replay-performance-v2`**。替换前及启动后均确认 `active_run=null`、`active_runs=[]`、`active_batch=""`。目标白名单保持原值：DCU/XPU 各允许 DCU、XPU、A3；A3 仅允许 A3；PPU 仅允许 PPU。未认证 status 均返回 403；对专用不存在编号的只读 report 查询均返回 404 / `run_not_found`，没有创建回放任务。

DCU/XPU 仅为原 replay unit 新增 `zzzz-replay-performance-v2-20261018.conf`，覆盖 ExecStart 的二进制路径；原启动参数、配置、业务环境变量及凭据保持。systemd 自动生成的 `INVOCATION_ID`、`JOURNAL_STREAM` 随服务重启变化，已与业务环境分别核验。A3/PPU 保留原镜像，仅更换 `/opt/aigate` 的只读二进制挂载来源；进程环境、UID/GID、资源限制、安全限制、namespace 及数据挂载的规范化 OCI 比较通过。两处旧 replay 容器已停止并同时禁用 restart policy 和目标运行状态，保留为 `*-before-replay-performance-v2-20261018`；新容器分别恢复 A3 `unless-stopped`、PPU `always`。容器候选准备期间的挂载比较失败及修正证据保留在 `private/`，最终激活通过，未执行回退。

本轮保留原录制、回放结果和配置。DCU 录制目录 62,543 文件、4,588,288,633 字节，回放目录 32 文件、4,965,758 字节，发布前后逐文件内容与元数据清单一致，并保留原回放结果归档；XPU、A3、PPU 当前录制和回放数据目录均为 0 文件，前后清单一致。A3/PPU 配置文件内容摘要、switch/proxy 的容器 ID、PID、启动时间、程序摘要、环境和 boot_id 均保持；A3 binding revision=46、admission=true，PPU revision=4、admission=true。没有清理原始录制或结果，也没有修改业务准入。

**本次验收覆盖部署身份、能力接口、访问控制以及配置和数据保持，未发送真实模型请求，未进行真实回放负载验收。** 配套 code-eval engine/Web 已完成增量建表、四份历史报告的 30 秒重算及桌面/手机触控模拟验收，于 2026-10-18 01:31（北京时间）恢复原 `maintenance=false`；平台最终源码、镜像和报告核验结果见[平台发布记录](../../code-eval/deploy/releases/README.md)。

本轮 replay 发布新增服务器目录：

| 节点 | 新增发布目录 |
| --- | --- |
| `dcu1` | `/data4/aigate/releases/replay-performance-v2-20261018/` |
| `xpu1` | `/var/lib/ai-gate/releases/replay-performance-v2-20261018/` |
| `hwA3-1` | `/data/a3-standalone-20261017/aigate/releases/replay-performance-v2-20261018/` |
| `ppu` | `/bmcp_lvm_fs/aigate/releases/replay-performance-v2-20261018/` |

四处均含 `bin/`、`private/`；`private/` 权限为 0700，私有快照和环境文件为 0600。各服务器发布目录保存 `verification.json`。本地脱敏收据位于 `code-eval/deploy/evidence/replay-performance-v2-20261018/replay-systemd/{dcu1,xpu1}-verification.json` 和 `replay-containers/{a3,ppu}-verification.json`；私有配置和密钥不进入 Git。

## 前次发布：2026-10-17 23:38 多目标回放与跨 gateway 多密钥发布

DCU、XPU、A3、PPU 的 switch、proxy、replay 共 12 个运行进程均更新至确定源码 `9cb5626dcd5a49be690b3bf86fd3bac421d285eb`。构建使用 Go 1.27.1、CGO_ENABLED=0、离线 vendor 模式，`vcs.modified=false`；A3 使用 linux/arm64，其他三处使用 linux/amd64。实际进程二进制摘要与构建产物逐项一致：

| 二进制 | amd64 SHA-256 | arm64 SHA-256 |
| --- | --- | --- |
| switch | `3284d5bb98f67ce568882a8df068d0f29f820ef63aadb10f1693a54f8b086013` | `08a8ebc2506cc2b8710d7acac1f567ae469d1c115759a57e8cb10a835a020395` |
| proxy | `7c63090662081c7db271868bb7f04aa2e83d1a49b28214a2be1628207ac8fe55` | `ab35d346c4a96abe141e3da1feab44d029d51962a9b4a738bf9ac0cd7f3130d0` |
| replay | `86f343d0e78501036c7bbf4bc9062e27fe24e8b9a96729c0fee522d7de38649e` | `b272b1c92f3af6fba7a57e3af0b97992e31aaa9a2c79c65b4fa005b5fbe1ae12` |

四处原有签名密钥共有两把不同值。replay 保留各自单把 `AIGATE_REPLAY_MARK_KEY`；八个 switch/proxy 接收进程配置两把密钥的信任并集，实际进程环境核验为两把，原签名密钥未改。现场分别向每个 proxy 发送两种有效签名和一种未知签名，共 12 个检查：审计日志中有效签名均为 `traffic=replay`，未知签名均不标记回放。请求以故意不匹配的 expected-model 在生成前返回 409，没有发送模型生成请求。switch 的多密钥录制排除语义由本地 race 回归覆盖，线上核验其二进制和实际配置；未为此新建录制会话。配置契约见[打包说明](../packaging/README.md#回放标记与多密钥验签)。

| 环境 | 部署方式 | 恢复后的绑定 revision | 准入 |
| --- | --- | --- | --- |
| DCU / dcu1 | switch、proxy、replay 均为原 systemd 服务，新增末尾覆盖配置 | 52 | true |
| XPU / xpu1 | switch/proxy 为原 `ai-gate` Kubernetes Deployment，replay 为 systemd | 33 | true |
| A3 / hwA3-1 | `ai-gateway` namespace 的三个 nerdctl 容器 | 46 | true |
| PPU / ppu | `ai-gateway` namespace 的三个 nerdctl 容器 | 4 | true |

使用平台原生维护和恢复操作，等待已接收请求自然收尾后替换受影响组件。四个 `multi-key-restore-20261017-{dcu,xpu,a3,ppu}` 操作均 completed、committed=true；每处 discovery 及两轮 JSON/SSE/工具调用/工具续答共 9 项通过，总计 36 项。绑定目标保持原 Router，期望与观测 revision 一致，资源锁和候选绑定均为空；平台 `maintenance=false`、health=ok。这些恢复校验包含真实模型的小请求，但未新建实际多目标回放批次或进行负载验收。

四处 replay 均公布 `recording-original-request-v1`、`recording-overlay-sync-v1` 和 `recording-multi-target-v1`，无活动回放。目标白名单保持原值：DCU/XPU 各允许 DCU、XPU、A3；A3 仅允许 A3；PPU 仅允许 PPU。多密钥互信不扩大目标访问权限。现有监听、CIDR、控制凭据、配置摘要和数据目录保持；A3/PPU 保留原基础镜像，通过只读二进制挂载运行新版本，不能用旧镜像标签推断运行源码。资源限制、UID/GID、只读根文件系统、capability 与挂载已核对。

PPU 首次重建遇到旧 proxy 的 nerdctl 名称索引缺失；保存失败结果，确认旧容器已停止且禁止自动重启后，修正其保留名称并继续创建当前版本。DCU replay 启动后的首次即时进程校验碰到进程尚未就绪，随后实际摘要和健康检查通过。失败证据均保留，未回退，也未强制清理占用或删除旧数据。

本轮新增服务器目录：

| 节点 | 新增发布目录 |
| --- | --- |
| dcu1 | `/data4/aigate/releases/multi-target-replay-20261017/` |
| xpu1 | `/var/lib/ai-gate/releases/multi-target-replay-20261017/` |
| hwA3-1 | `/data/a3-standalone-20261017/aigate/releases/multi-target-replay-20261017/` |
| ppu | `/bmcp_lvm_fs/aigate/releases/multi-target-replay-20261017/` |
| test1 | `/data2/code-eval/releases/multi-target-replay-20261017/` |
| test4 | `/data2/monitoring/releases/multi-target-replay-20261017/` |

四个网关发布目录含 `bin/`、`private/`；dcu1、xpu1 另新增 `/etc/systemd/system/aigate-replay.service.d/`。XPU 新增 Secret `ai-gate/aigate-replay-verify-20261017`，Deployment 保持 Recreate 策略。受限目录 0700、凭据文件 0600，原容器和配置保留为证据。平台和 monitoring 的具体目录、镜像及验收见[平台发布记录](../../code-eval/deploy/releases/README.md)和[监控发布记录](../../monitoring/docs/releases/README.md)。本地脱敏收据为 `code-eval/deploy/evidence/multi-target-replay-20261017/public/`；私有密钥不进入 Git。

## 前次状态：2026-10-17 PPU 门禁通过并开放准入

用户取消“后端必须支持强制指定工具名”的接入要求后，`warmup.go` 的工具调用探针不再发送 `tool_choice.function.name`；工具定义、提示、返回调用名称/ID/参数校验和工具结果续答仍保留。此次只更新 PPU 的 `ppu-aigate-switch`，不修改 proxy、replay 或模型服务，不更新其他环境。

**PPU 已完成绑定并开放业务：`http://122.209.21.209:60010/v1`，模型 `Qwen3.6-Plus`，gateway `ppu`，Router `ppu-qwen36plus`，binding revision=1，admission=true。** 平台操作 `ppu-default-tools-bind-20261017` 为 completed、committed=true；Router 版本 2、runtime 版本 1，目标为 `http://122.209.21.209:60013`。入口 `/v1/models` HTTP 200，平台健康 ok，全局维护 false，资源锁为空。未创建录制计划或回放任务。

固定请求门禁通过目标 proxy 原样发送两个样例，使用完整消息、工具和 `max_tokens=28000`，没有降参或请求重试：

| 样例 | prompt / completion tokens | 请求总耗时 | 结果 |
| --- | --- | --- | --- |
| `body.json` | 62,284 / 631 | 12.578752 秒 | HTTP 200、完整 `[DONE]`、`finish_reason=tool_calls`，返回 `grep_code`、`search_file` |
| `body-agent-ds4.json` | 127,305 / 201 | 5.045079 秒 | HTTP 200、完整 `[DONE]`、`finish_reason=tool_calls`，返回 `run_in_terminal` |

脚本退出码 0，`report.json.passed=true`；仅验证响应结构，没有执行返回工具。proxy 的 boot_id `df96da1c978f5228aca680c0ea8412c1`、配置摘要 `f9c7e523067e9358ab62da73f0c3a3bcb28808de55a075a7f9101ba6a38123c9` 在门禁前后及绑定前后保持一致。固定门禁通过后才提交绑定；新绑定校验的 discovery 及两轮 JSON/SSE/默认工具调用/工具结果续答共 9 项全部通过。

新 switch 镜像为 `local/aigate-switch:default-tools-20261017`，OCI manifest `sha256:1723323454b2033c078b84fbb961afcd869d711a07b9cdc5e74d49e7131d42cd`；进程二进制 SHA-256 `9d1323e3fcb05061cc1fe606a9b40ffcba4f2e92fb3c8ceaf5de13067b4eeb0b`。构建基于 `d5d43bfb3150e7413241ec499d27dff89eb3af18` 加当时未提交的本地补丁，**构建时 vcs.modified=true**；补丁 SHA-256 `afdd9a95ceb10b1c467dd67c1427fa82120c280b4d18876b56bd8b878f1d8ca3`。离线 vendor 构建、完整 Go 回归、vet、相关 race 回归和 5 项门禁脚本测试通过。实际进程环境、启动参数、资源限制、挂载与旧 switch 一致；旧实例已停止且禁止自动恢复，保留为证据。

新增服务器目录：PPU `/bmcp_lvm_fs/aigate/releases/default-tools-20261017/` 及 `private/`；test1 `/data2/code-eval/releases/ppu-admission-20261017/`，含 `scripts/`、`testdata/model-admission/`、`private/`、`gate/`。原始响应留在 test1 的 `gate/`，不提交 Git。受限环境文件权限 0600。本地脱敏证据在 `deploy/ppu-admission-20261017/evidence/`（`admission-report.json`、`switch-verification.json`、`platform-verification.json`）。

## 前次状态：2026-10-17 PPU 新部署与 A3 容器迁移

PPU、A3 的三个 aigate 服务均已运行于 containerd namespace **`ai-gateway`**。PPU 首次绑定的工具调用验证失败，用户明确选择暂不修改兼容代码、保持暂停；**容器部署完成不等于 PPU 业务接入通过**。

| 环境 | 容器 | 版本与准入 |
| --- | --- | --- |
| PPU | `ppu-aigate-switch`、`ppu-aigate-proxy`、`ppu-aigate-replay` | 源码 `d5d43bfb3150e7413241ec499d27dff89eb3af18`；未绑定，admission=false |
| A3 | `a3-aigate`、`a3-aigate-proxy`、`a3-aigate-replay` | 程序与迁移前完全一致；原 A3 Router 维护、admission=false 保持，绑定 revision 由 39 恢复至 40 |

PPU 镜像 `local/aigate:d5d43bf-20261017`，OCI manifest 为 `sha256:8275040f69aae87e964db5dd3ff76943a64b9fe04f19cdb1d8f9956ec83f3b93`。Go 1.27.1、linux/amd64、CGO_ENABLED=0，离线 vendor 构建，`vcs.modified=false`。实际进程二进制 SHA-256：

| 二进制 | PPU | A3（保持原版） |
| --- | --- | --- |
| switch | `41677f7cfd6338370314a7b0f4b848ff74540f96df63a2335febd15883547f6a` | `d69e3c2c9ec6517e68eda2c757f198a57550be9fdbc7f92bd72f34dd738870d9` |
| proxy | `bd42d8e962809ed1238d169d1cd4489de63211f944f7ff0083f0feb8cedd5a38` | `ec987fbe1e2ac6f8428e66b367a0111d48d95acf67a8e007422fb8aec918fa8b` |
| replay | `1c476929b5c096f6826473156d286bd70a525567d2afdde99383746912056ade` | `01dce22c25f5eab35250d29c846d7126c81e6a326bd7ede3238f2bd7a16a6213` |

PPU 仅连接本机 `8008` 的 `Qwen3.6-Plus`；平台 gateway 为 `ppu`、Router 为 `ppu-qwen36plus`、环境类型为 `unmanaged`。最终业务入口 `http://122.209.21.209:60010/v1`，控制/观测/proxy/replay 分别使用 60011/60012/60013/60014。最初 60000 与 `ahm-web` 的既有 CNI 映射冲突，用户随后指定整体改用 60010–60014；旧管理服务、模型容器及防火墙规则保持。端口检查必须覆盖容器 NAT 映射，不能只检查监听 socket。

平台没有修改独立网关登记地址的 API。本次地址修正通过维护窗口内的显式数据迁移完成：保存 SQLite 一致性备份，只迁移 PPU 当前地址并追加 Router 版本 2，原版本 1 和失败操作记录保留；engine 使用原容器和镜像重启载入登记。Web/engine/database/executors 健康为 ok，全局维护恢复 false，PPU 准入保持 false。初次未固定 WAL 读快照的备份被主动终止，未进入停机和数据修改阶段，文件标记为 `.incomplete`；最终固定读快照备份成功并核对原地址/版本，不能使用未完成备份恢复。

首次绑定操作 `ppu-initial-bind-20261017` 已明确失败且未提交：模型发现、JSON 和 SSE 成功，工具调用名称、ID 或参数校验失败。直接向上游发送同类合成请求复现：`[{"name":"aigate_echo","parameters":{...}}]` 被放进普通 `content`，没有标准 `tool_calls`。没有跳过校验、改写响应、修改模型容器或重发绑定；不创建录制计划和回放任务。60010 业务查询返回 `503 gateway_not_accepting`。

A3 从 `a3-delivery` 迁移三个容器，实际程序摘要、进程环境、配置摘要、持久挂载、资源限制和 replay 白名单核对一致。`a3-aigate.service`、`a3-aigate-proxy.service` 已禁用并移除，查询为 `not-found`；`aigate-split-access.service` 保持 enabled/active，原访问控制链保留。旧容器停止并关闭自动重启，仅保留作证据。

生命周期有现场版本差异：A3 使用 `unless-stopped`；PPU 定制运行时只支持 `always`，维护停止需同时设置 restart policy=no 和 restart.status=stopped。两台均已用临时容器验证进程异常退出自动恢复、人工停止后保持停止，测试资源已清理；未执行主机或 containerd 重启。详见[容器维护](../packaging/CONTAINERS.md)。

源码 Go 回归与 vet 通过；容器摘要、权限、访问控制和状态验收通过。PPU 真实模型验收仅达到上述边界，不进行负载测试。PPU 主机时钟现场比 test1/A3 快约 29 分钟，本轮未调整时钟；启用未来同步录放前需处理时钟一致性。

本轮新增服务器目录：

- PPU：`/bmcp_lvm_fs/aigate`，含 `config`、`proxy`、`recordings`、`replay`、`releases/container-unification-20261017/private`；proxy 运行产生画像子目录。
- A3：`/var/lib/ai-gate/releases/container-unification-20261017` 及 `private`；原数据目录不迁移。
- test1：`/data2/code-eval/releases/aigate-containers-20261017` 及 `private`，保存登记、失败验证、地址迁移及受限数据库备份。

本地脱敏证据：`aigate/deploy/container-unification-20261017/evidence/{ppu,a3,platform}`。私有快照和密钥不进入 Git。

## 前次发布：回放性能报告

2026-10-17 17:04（北京时间）完成回放性能报告发布。通过 SSH MCP 更新 A3-gate、xpu1、dcu1 的 **aigate-replay** 至源码 `23cbcf7e60227f1f240e158a63ed22c83e84d48b`。Go 1.27.1、linux/amd64、CGO_ENABLED=0，离线 vendor 构建，`vcs.modified=false`；三台实际运行进程的二进制 SHA-256 均为 `01dce22c25f5eab35250d29c846d7126c81e6a326bd7ede3238f2bd7a16a6213`。

| 节点 | replay 部署方式 | 本轮发布目录 |
| --- | --- | --- |
| A3-gate | nerdctl namespace `a3-delivery`，容器 `a3-aigate-replay`，restart=no | `/var/lib/ai-gate/releases/replay-reports-20261017/` |
| xpu1 | `aigate-replay.service` | `/var/lib/ai-gate/releases/replay-reports-20261017/` |
| dcu1 | `aigate-replay.service` | `/data4/aigate/releases/replay-reports-20261017/` |

三处均保留原监听地址、CIDR、控制密钥、回放标记密钥、三 Router 白名单、录制目录和回放数据目录。A3 保留原镜像、UID/GID 65532、只读根文件系统、录制只读挂载和回放可写挂载，以及 1 GiB 内存、2 CPU、128 进程限制；只更换 `/opt/aigate` 的二进制挂载来源。没有新建 replay systemd unit。XPU/DCU 保留原 unit 配置，只替换 ExecStart 二进制路径。

本次未替换 switch、proxy、模型后端或 monitoring。switch/proxy 的前次登记源码及部署证据可用 `git show 23cbcf7:docs/DEPLOYMENT.md` 查阅。现有 schema-2 录制继续适用于新 replay；新性能计时由 replay 采集，不依赖 switch 升级。

## 发布后检查

- 发布前及替换前均确认三处无活动回放；未发起、停止或重放真实模型请求。
- 三处 authenticated status 正常，`active_run=null`，目标白名单与窗口前一致；无凭据访问返回 403。
- 新 `GET /control/replay/v1/runs/{id}/report` 已上线；对专用不存在编号的只读查询返回 404 / `run_not_found`，验证报告路由生效，未创建运行目录。
- 配套 code-eval engine/Web 已发布源码 `ba50860cfe766bceab9db0d49599693fd1e1f82d`，镜像、数据库、静态资源和页面检查见[平台发布身份](../../code-eval/deploy/releases/README.md)。全局维护恢复为 false，health=ok。
- DCU 仍指向 DCU Router，revision=48、admission=true；XPU 仍指向 XPU Router，revision=29、admission=true；A3 仍指向 A3 Router，revision=39、admission=false。三个 switch 的 boot_id、绑定、配置版本及原准入均与本轮窗口前一致。**A3 原 Router 维护状态保持，本次不代表其业务入口或上游恢复。**
- 线上三来源 × 桌面 Chrome 1280×720 / iPhone 390×844 触控模拟共六组页面检查通过，无横向溢出、页面脚本错误或业务 HTTP 错误，API 写请求为零。当前平台回放记录为空，新报告的实际内容及负载表现尚未进行真实模型验收；计时、统计、归档故障及交互由本地合成回归覆盖。

## 证据与数据边界

各主机本轮目录含 `bin/`、`private/` 和 `verification.json`；private 为 0700，快照和环境文件为 0600。A3 旧容器保留为 `a3-aigate-replay-before-replay-reports-20261017`；旧二进制、配置及镜像保留，未回退。A3 nerdctl 重建产生相同值的 HOSTNAME 和附加组重复项，已按完整环境赋值集合、组成员及其他原样配置复核；首次严格列表比较失败及核验修正均保留在 private 证据中。

本地脱敏收据位于 `code-eval/deploy/evidence/replay-reports-20261017/deployment/`，线上页面证据位于 `code-eval/output/playwright/replay-reports-release-20261017/`。没有清理或迁移录制文件、回放数据，没有修改定时规则或历史任务版本。前次完整录制与原请求回放发布、A3 容器迁移的历史记录通过上面的固定 Git 提交查阅。
