# 当前部署身份

## 2026-10-24 Prefill 导出修复已合入 main 并发布

北京时间 18:33:41 完成恢复后的在线核验。Prefill 分支及修复已快进合入 main 并推送，实际 proxy 构建源码为 `d013132e110aa89344b1dbafac1243125cc97d45`。修复长导出继承观测服务 10 秒写入截止时间而中断，以及待写丢弃区间暂时不可见、误报数据完整的问题。构建使用 Go 1.27.1、CGO_ENABLED=0、离线显式 vendor、trimpath，vcs.modified=false。

| 架构 / 实际更新节点 | aigate-proxy SHA-256 |
| --- | --- |
| linux/amd64：dcu1、xpu1、A3-gate | `afc8a01abaedc208635c7bb1391b8040167e39a4c841b3910714cbb39daaf06a` |
| linux/arm64：A3-3、A3-5 | `34f628a0eab1cb1961d18b020ab47b3f1d9ae906669d100e14c5e689e3348fa2` |

五个实际进程的二进制摘要、配置文件摘要、settings/backend、Router/功能服务身份、启动参数、环境和原 key_id 均核对通过，采集状态 collecting，画像 write_errors=0。XPU 仅更新 proxy Deployment 的 binary hostPath，Recreate 后恢复 1/1。A3-gate 仅替换 proxy，旧容器 `a3-aigate-proxy-before-prefill-prefix-fix-20261024` 停止并禁止自启；规范化 OCI 参数保持。DCU、A3-3、A3-5 沿用原 systemd unit，两个本地服务发布根设为 0711，私有快照仍为 0700/0600。switch、模型服务、缓存、固定执行/判题/回放镜像未更新，独立 Motor 测试 proxy 未触碰。

维护按进入窗口时的实际绑定恢复：DCU、XPU、A3、a3-34-local、a3-56-local 的 revision 为 64、36、76、5、5，admission 均 true。后两个入口在发布前即绑定 A3-1/2 Router `4cc8257f0fec429180d14f220ab816ef`，本轮未重新绑定。四条录制计划恢复原值（两条 XPU 启用、两条 DCU 停用），发布 owner 的锁已释放，原 maintenance=false。正常停止并保留结果的 DCU 准确率任务 `f1e0753f3f86459fbae2ff54c0f6eb6d` 已恢复 running，保留其正常任务占用锁。

平台逐项核对现有 Web 的 130 个生产文件，无差异；配套 code-eval main 为 `7841437f566e34ce68157f1dbe104e153bb5e58e`，修复阶段仅增加测试，无需替换 Web/engine。通过平台下载五个来源最近一小时的 NDJSON，结束标记、来源、记录计数和 SHA-256 均通过，invalid=0。DCU、XPU 的 data_complete=true；A3-3/4、A3-5/6 有既存 input_not_prepared，data_complete=false。A3-1/2 在本轮第一次停止旧容器时新增 unclean_restart=1：发现其 containerd 停止信号标签继承 SIGQUIT，已改为 SIGTERM，随后正常停止退出码为 0，再启动未增加该计数。相关历史缺口如实保留，A3-1/2 此窗口 data_complete=false，不能解释为下载失败或数据已补齐。维护脚本同步显式设置 SIGTERM，相关 4 项测试通过。

本轮复审 Go 全量测试、vet、14 项 Prefill race 回归以及平台 API 24 项通过；真实 TCP 慢读 16.9 MB 导出持续约 27 秒仍取得完整结束标记及正确校验和，真实观测 Handler 的 11 秒回归也通过。新实现的核心回归在旧实现覆盖下按预期失败。没有执行新模型接入或新增真实模型合成请求。

新增服务器发布根如下；五个 proxy 根含 bin/private，test1 根含 exports 和受限 private/platform-window，继续使用原指纹数据目录。

| 节点 | 新增发布根 |
| --- | --- |
| dcu1 | `/data4/aigate/releases/prefill-prefix-fix-20261024/` |
| xpu1 | `/var/lib/ai-gate/releases/prefill-prefix-fix-20261024/` |
| A3-gate | `/var/lib/ai-gate/releases/prefill-prefix-fix-20261024/` |
| A3-3 | `/var/lib/aigate/releases/prefill-prefix-fix-20261024/` |
| A3-5 | `/var/lib/aigate/releases/prefill-prefix-fix-20261024/` |
| test1 | `/data2/code-eval/releases/prefill-prefix-fix-20261024/` |

本地发布材料与脱敏回执位于原独立工作树 `.worktrees/aigate-prefill-prefix/deploy/prefill-prefix-fix-20261024/`，汇总为 verification.json，逐节点为 evidence/。配置和进程环境原文仅保留在服务器 private 中。以下 00:19 的记录为首版部署历史，其源码、绑定版本及未推送状态不代表当前状态。

## 2026-10-24 Prefill 原始指纹持续采集

北京时间 00:19:56 完成五套在线 proxy 的最终核验，并恢复原 `maintenance=false`。源码 `411bc900e8d5163222bc7bc00b660da6c37b5e7a`，包含持续采集、容量轮转、流式导出和按到达时间筛选分段。Go 1.27.1、CGO_ENABLED=0、离线显式 vendor、trimpath、vcs.modified=false 构建。实际进程摘要如下：

| 架构 / 节点 | aigate-proxy SHA-256 |
| --- | --- |
| linux/amd64：dcu1、xpu1、A3-gate | `b10e95ed7fcd69c7447141ab11abe9899554741a3d8037139881805170cf9c04` |
| linux/arm64：A3-3、A3-5 | `19e01258f6ec9d0d9fe96005f517a2954de3d85595339dc7a251577e956b5805` |

DCU、XPU、A3-1/2、A3-3/4、A3-5/6 的 binding revision 分别保持 63、36、76、4、4，准入均恢复 true。配置文件、有效 settings/backend、Router/功能服务身份、配置摘要、启动参数及环境逐项一致；原画像策略目录继续可读。switch、推理后端、模型及缓存配置保持原版本，PPU 维持退役状态。XPU proxy Deployment 恢复 1/1；A3-gate 旧 proxy 容器停止且禁止自启，规范化 OCI 配置保持。

采集点为准备发送的适配后 payload，使用 UTF-8 字节、1024 字节边界和 HMAC 前缀链；保留完整输入指纹以及可为空的结果用量。每 Router 默认 10 GiB，仅按容量轮转，与录制会话和短期画像事件存储独立。观测输入不证明后端已执行 Prefill。详细算法、关联方式及导出完整性规则见 [Prefill 指纹说明](PREFILL_PREFIX.md)。

五处采集均为 collecting，写入、队列丢弃、容量丢弃及超大记录计数为零。平台按实际保留起点导出，五个文件的结束标记、记录来源与 SHA-256 均通过，invalid=0、data_complete=true；即时输入/结果分别为 DCU 21/21、A3-1/2 5/5、XPU 14/14、A3-3/4 21/21、A3-5/6 21/21。这些观测记录包含既有自动请求，不视作真实业务压测。初版到最终版重启的四处采集器 key_id 保持；所有目录 0700、密钥 0600/32 字节。没有回填启用前数据。

Go 全量测试、vet、9 项指纹相关 race 测试各重复 3 次通过，覆盖规范化、中文及尾块、不同前缀、适配后 payload、结果缺失、重启、队列/磁盘/容量异常、大记录、导出取消及固定快照、分段筛选。平台 API 和双视口下载验收见 code-eval 发布索引。停用并按发布前快照恢复四条录制计划，所有资源锁已释放；已受理请求自然排空后替换。恢复阶段首次 prepare 因原 revision 不递增而被控制面拒绝；确认五处 switch 当前绑定完整后，使用原 quiesce owner 的 resume 恢复准入，绑定表未改变。

服务器新增发布根及原始数据目录如下；发布根含 bin/private，私有配置与进程环境只保存在服务器。

| 节点 | 新增发布根 | 新增指纹数据目录 |
| --- | --- | --- |
| dcu1 | `/data4/aigate/releases/prefill-prefix-20261023/` | `/data4/aigate/proxy-31d935c8d7c6449d85a78ea486ac2912/prefill-prefix/` |
| xpu1 | `/var/lib/ai-gate/releases/prefill-prefix-20261023/` | `/var/lib/ai-gate/proxy-13415960b76b43378cca684fa51550e3/prefill-prefix/` |
| A3-gate | `/var/lib/ai-gate/releases/prefill-prefix-20261023/` | `/var/lib/ai-gate/releases/relocate-back-20261019/proxy/prefill-prefix/` |
| A3-3 | `/var/lib/aigate/releases/prefill-prefix-20261023/` | `/var/lib/aigate-proxy-a3-34-local-router/prefill-prefix/` |
| A3-5 | `/var/lib/aigate/releases/prefill-prefix-20261023/` | `/var/lib/aigate-proxy-a3-56-local-router/prefill-prefix/` |

配套平台在 test1 新建 `/data2/code-eval/releases/prefill-prefix-20261023-be217bd/`，含受限维护快照和 exports。本地脱敏总回执为 `deploy/prefill-prefix-20261023/verification.json`，逐节点回执在其 `evidence/`；初版构建与验收证据保留，最终源码身份以上述 411bc90 为准。代码位于本地 `codex/prefill-prefix-trace`，未推送。

## 2026-10-22 A3 本地双网关与统一授时

A3 当前只保留三套在线入口：原 A3-gate `122.52.5.131:9090/v1` 继续服务 A3-1/2；A3-3 新建 `a3-34-local`，接管 `122.20.125.28:8000/v1`；A3-5 新建 `a3-56-local`，接管 `122.20.125.29:8001/v1`。新两套为独立 switch/proxy，使用新的身份、控制密钥和数据，未迁移原网关。两套临时 `9091/9092` switch/proxy 已停止且 restart.policy=no、restart.status=stopped，相关八个端口均无监听；历史容器、数据及平台登记保留，不计为在线入口。原 `9090` 进程和 binding revision=72、admission=true 保持。

新服务使用源码 `03e84d391448ad9626c4a280a2446134d6581c4e`，Go 1.27.1、linux/arm64、CGO_ENABLED=0、离线 vendor、trimpath、vcs.modified=false 构建。switch SHA-256 `7872c3ffdb9279b7f7b703d8b0b531dca3a189745ff4e7c6e507d20b6f6f2ea7`；proxy SHA-256 `1775ab922a8a111d48c528b842a57e8909877152558b2822e4dd86a1fcca5c93`。使用现有 systemd 模板，active/enabled、NRestarts=0；proxy 4 GiB、switch 512 MiB，各 2 CPU。proxy/观测/控制分别为主机地址上的 18084/18082/18083，来源限制持久化；控制仍要求 Bearer。Router 只替换监听和容器启动方式，分别改为 `127.0.0.1:18000/18001`，复用镜像 config `sha256:3c94101c0c3e6527227b46ac41ff7458d1110589377d971b53770265fc7dc286`，P/D、Master、Store、权重及缓存参数未修改。

两套 proxy 的两个完整固定门禁均通过，分别耗时 121.495/41.293 秒和 131.718/18.698 秒，HTTP 200、完整 DONE、合法结构化工具调用及有效 usage。首次平台绑定的九项校验通过，但因节点时钟偏差导致控制器 freshness 判断失败，未提交。按用户要求统一授时后，以新操作 ID `aigate-local-20261022-bind-synced-34/56` 完成绑定，均 completed、committed=true，revision=1、admission=true、desired/observed 一致。最终业务 JSON/SSE、远端无法访问内部 Router、未认证控制 403、来源限制、40 个端点空闲健康、画像 write_errors=0 均通过。

A3-3/4/5/6 原 chronyd 无可用源，分别慢约 150/329/861/103 秒。四台改用 eval 平台与 DCU 既有时间源：主 `122.16.46.15`、备 `122.16.46.16`，主源 prefer、minpoll=4/maxpoll=6，chronyd 开机启用。最终四台 NTPSynchronized=yes、Leap Normal、选择主源，跟踪 System time 偏差均小于 1 毫秒；另核验 test1/2/3、A3-gate、hwA3-1/2、dcu1 也使用同组源。未发布 code-eval 程序改动，未重放回放或准确率任务；原 dual16 保留 awaiting_final_review。

新目录：A3-3、A3-5 各 `/data0/aigate-local-20261022/`、`/etc/aigate/`、`/var/lib/aigate-record/`、各自 `/var/lib/aigate-proxy-a3-34-local-router/` 或 `/var/lib/aigate-proxy-a3-56-local-router/` 及对应两个 systemd 实例 drop-in 目录；test1 `/tmp/aigate-local-20261022/`。两台本地原始门禁和最终检查位于新根目录 `evidence/`，平台操作与业务检查在 test1 新根目录 `evidence/`，临时容器停止收据在 A3-gate 原发布根 `evidence/local-gateways-stopped-20261022.json`。授时四台备份为 `/etc/chrony.conf.aigate-local-20261022.bak`，收据为 `/var/tmp/chrony-unify-20261022.json`。可维护的启动包装与验证脚本见 [A3 本地接管](../../environment/A3/patch/image-pd-router/20261022-aigate-local/README.md)。

## 2026-10-22 控制回执与录制 pin 修复发布

四套在线 switch/proxy 更新至 main 源码 `b2a772c9a0f18d6f1f6db968fd71383b2c1422cf`。北京时间 00:31:25 开启维护，00:35:03 完成网关验收并恢复原 `maintenance=false`，约 217 秒。XPU 三条已受理 SSE 自然排空后替换；其他空闲网关先替换并分别恢复原绑定。常驻 replay 保持退役，平台固定回放镜像及推理服务未替换。

修复 validate 被无关控制命令错误标为 superseded、丢失响应后 stale_command 结果不可查询，以及未登记或已不存在片段的 unpin 清理；保留真实存储错误。Go 全量测试、vet、相关控制及录制 race 回归通过。构建使用 Go 1.27.1、linux/amd64、CGO_ENABLED=0、离线显式 vendor、trimpath；内嵌 revision 匹配、vcs.modified=false。

| 程序 | 四处实际进程 SHA-256 |
| --- | --- |
| aigate-switch | `e9f04c320f742daa4910b3d0c9ca28b668469d2078bbe156af2cc48d34c7f986` |
| aigate-proxy | `8b4c923f054bfc0572f2d74d014bfabf0e391194a9decafc98f59c406cf5d509` |

四处配置文件、有效 settings/backend、service_id、环境、启动参数及访问限制均保持；配置摘要未变，无需画像目录迁移。DCU 沿用 systemd；XPU 两个 Deployment 仅更改二进制 hostPath，恢复 1/1；A3/PPU 保留基础镜像，规范化 OCI 参数一致，旧容器停止且禁止自启。health、profile 状态/write_errors=0、metrics、未认证控制接口 403 及 OpenAPI 字节摘要通过。

维护窗口即时快照及最终绑定均为 DCU revision=61/admission=true、XPU 36/true、A3 70/true、PPU 11/false。准备早期 A3 为 69/false，进入维护前已为 70/true；本轮以窗口即时快照为准，没有改写该绑定。三个开放入口模型发现正常；本轮未发送合成模型生成请求、创建真实回放或执行新模型接入。52 条任务、32 条固定执行版本、9 条录制归档、9 条回放执行及完整绑定表摘要保持，四条原录制定时计划恢复，无资源锁。

新增服务器发布目录：dcu1 `/data4/aigate/releases/latest-code-20261022/`；xpu1、A3-gate `/var/lib/ai-gate/releases/latest-code-20261022/`；ppu `/home/wqy/aigate/releases/latest-code-20261022/`，均含 bin/private/checks；配套平台 test1 `/data2/code-eval/releases/latest-code-20261022/`。私有进程环境和 OCI 快照仅保留服务器；本地脱敏收据在 `deploy/latest-code-20261022/evidence/`。code-eval 的独立平台维护窗口见其发布记录。

## 2026-10-21 四套网关更新至最新 main

北京时间 05:51:40 完成发布验收。再次 fetch 确认远端 main 为 `eefb6c8b111a811cb06248f8e2f75df1b41e41df`；DCU、XPU、A3、PPU 共八个实际 switch/proxy 进程均已核对新程序 SHA-256。使用 Go 1.27.1、linux/amd64、CGO_ENABLED=0、离线显式 vendor、trimpath 构建，内嵌 revision 匹配且 vcs.modified=false。Go 全量测试、vet 和画像相关 race 回归通过。

| 程序 | 四处实际进程 SHA-256 |
| --- | --- |
| aigate-switch | `378d239017fab22c5d17186d5b07a440cd0020ca0f5a0b20b66bef7a6ef4f213` |
| aigate-proxy | `7031f90fbb49d4b8ff8b545931c311e0547b61251192f407b91e9030d50cf1d6` |

| 环境 / SSH MCP target | 部署方式 | Router 版本 / binding revision | 恢复后的业务准入 |
| --- | --- | --- | --- |
| DCU / dcu1 | 原 aigate.service、aigate-proxy-dcu.service | 5 / 59 | true |
| XPU / xpu1 | 原 ai-gate 两个 Kubernetes Deployment，均 1/1 | 5 / 36 | true |
| A3 / A3-gate | 原 ai-gateway namespace 的 switch/proxy 容器 | 8 / 66 | true |
| PPU / ppu | 原 ai-gateway namespace 的 switch/proxy 容器 | 3 / 11 | false，保持原暂停状态 |

使用平台维护、资源锁及认证 quiesce，停用并最终恢复原两条录制定时计划；XPU 原有三条 SSE 自然排空后才替换。后端地址、Router 身份、原配置文件、控制凭据、业务环境、监听、数据挂载及资源限制保持。A3/PPU 规范化 OCI 一致，XPU Deployment spec 仅二进制 hostPath 改变；DCU 在原 unit.d 目录新增末尾 ExecStart 覆盖文件。旧容器停止并禁止自动重启，保留作发布证据。

新版移除 profile 的 max_index_entries/max_payload_bytes 两项旧零值字段，使规范化配置摘要及画像策略目录哈希改变；已核对其他有效 settings 与 backend 完全相同。通过现有平台 refresh_target 追加观察版本及原生 recover 状态机恢复原后端与原准入，四项操作均 completed、committed=true。旧 Router 版本及固定历史任务版本保留，没有将历史任务改为新配置。

为继续读取画像与累计计数，将原 request-profile/fa94c4f0381ba13d 中 20,339 文件、117,551,878 bytes 逐文件 SHA-256 校验复制到新 request-profile/57276ab4504021a8，保留原目录、文件权限及属主。四处 profile 状态与指标可读、write_errors=0。52 条评测任务、32 条固定执行版本、9 条录制归档、9 条回放执行记录的完整表摘要前后一致；网关绑定仅按本次新摘要追加 Router 版本与 revision。录制原始目录及已有结果没有手工清理。

四处 health、profile 状态、metrics、未认证 control 状态拒绝 403 及与源码逐字节一致的 OpenAPI 通过。DCU/XPU/A3 的业务模型分别为 DeepSeek-V4-Flash-0731-FP8-Channel、glm-5.2、dsv4-spark；三个原后端均通过真实业务入口的单次合成 SSE，HTTP 200、有效输出、finish_reason、完整 DONE 与 EOF 通过，耗时 0.500 / 2.556 / 0.690 秒。PPU 保持暂停，models 返回 503。最终 maintenance=false、health=ok、active_count=0、reservations=[]、scheduler_error=null、无网关资源锁或候选配置。

本次覆盖四套在线网关的 switch/proxy。四处常驻 replay 保持退役且禁止自启动，旧端口均无监听；原 hwA3-1 无活动网关及对应端口。平台一次性回放任务继续使用其固定镜像，回放内核自已部署的 3ba7952 后没有新的源码逻辑改动，本次未替换该平台镜像、重放任务或修改可复用方案。

新增服务器发布根目录如下，各含 bin/、private/；四个网关还含受限 private/profile-new-before-migration/。私有配置和逐文件迁移清单仅保存在服务器，未下载凭据或数据库。

| 节点 | 新增发布目录 | 新版自动创建并承接历史的画像目录 |
| --- | --- | --- |
| dcu1 | `/data4/aigate/releases/latest-aigate-20261021/` | `/data4/aigate/proxy-31d935c8d7c6449d85a78ea486ac2912/request-profile/57276ab4504021a8/` |
| xpu1 | `/var/lib/ai-gate/releases/latest-aigate-20261021/` | `/var/lib/ai-gate/proxy-13415960b76b43378cca684fa51550e3/request-profile/57276ab4504021a8/` |
| A3-gate | `/var/lib/ai-gate/releases/latest-aigate-20261021/` | `/var/lib/ai-gate/releases/relocate-back-20261019/proxy/request-profile/57276ab4504021a8/` |
| ppu | `/home/wqy/aigate/releases/latest-aigate-20261021/` | `/bmcp_lvm_fs/aigate/proxy/request-profile/57276ab4504021a8/` |
| test1 | `/data2/code-eval/releases/latest-aigate-20261021/` | 无 |

脱敏完成汇总在 deploy/latest-aigate-20261021/verification.json，各节点实际进程、HTTP、画像迁移及平台收据在同目录 evidence/。构建包装脚本的跨平台执行报错、DCU Python 3.7 的 shlex.join 不支持、首次立即读取维护缓存及摘要变化导致的首次绑定恢复失败均已留痕并修正当前发布工具，没有回退或启动旧程序。

## 2026-10-20 唯一回放执行器与历史数据统一

平台已于北京时间 14:46:10 完成历史数据统一和当前执行器发布。Go 回放源码 `3ba79524f87ba5b9f0dfc54ad49bd11b7a421a86`，linux/amd64 二进制 SHA-256 `db976655546e212b89ad5e03a5a278d5ca8917538cce48bad3ca9fea0b7e3680`，固定任务镜像 `sha256:36b0d32bfe02f3888e1c13ca0b1962d6527c7b1556629ae292f30de4df7468be`。test2/test3 新任务只允许该镜像；旧任务固定身份和历史镜像实体保留。

常驻 replay 服务、模板、HTTP 控制/同步/租约代码及专属测试已从当前源码删除。唯一执行器为 aigate-replay-task，直接复用规划、发送、停止收尾与报告内核。原速和固定并发真实 Docker/NAS 模拟 SSE 验收、Go 全量测试、vet 与关键 race 回归通过；未新建真实模型负载。历史 14 条网关回放纳入统一列表，12 份原始结果、72 文件逐哈希迁入 replay-runs；报告正文和真实 gateway 执行身份保留。当前 OpenAPI 已移除八个退役端点，本次未替换现有 switch/proxy 或其静态接口文档。

四处旧 replay 继续停用且禁止自启动。当前 engine/Web 和数据完成证据、服务器新增目录见[平台发布记录](../../code-eval/deploy/releases/README.md)。以下均为此前发布身份，不能据其重新启用旧服务。

<a id="nas-main-20261019"></a>

## 2026-10-19 NAS 录制与平台独立回放

状态：与平台在同一维护窗口发布完成，北京时间 22:15:47 开启、23:15:24 恢复原维护状态，共 3,577.74 秒。四处 switch/proxy 均已替换并验证实际运行程序，旧常驻 replay 已退役。源码 [PR #7](https://github.com/wqy0812/AInfra-gate/pull/7) 合并提交 `40372f27ddcd51e85cd68c45820bb5239a9af689`；配套平台 Web/worker/回放镜像源码 `967403be606912c4ae2333fb13db76f3c78dd920`、engine 含四并发导入的提交 `9c56a5ff901c372aa686584c030961d6ff9666b2`。Go 1.27.1、CGO_ENABLED=0、离线 vendor、trimpath 构建，内嵌 revision 匹配且 `vcs.modified=false`。

| 程序 | linux/amd64 SHA-256 |
| --- | --- |
| aigate-switch，DCU/XPU/A3/PPU 实际进程 | `f8f964507197121f2740b65a68a41306310353836a57bcec6b741630b73ab79c` |
| aigate-proxy，DCU/XPU/A3/PPU 实际进程 | `a2b934db94631ee0f317b13280670c68a633f376bc373bcf02549f1f826eddfd` |
| aigate-replay-task，新固定回放镜像内 | `075ca574491795b914c23eb10cef4267eecef4c8f80b6103767486329a1ef9d8` |

四个当前网关节点均为 amd64，A3 使用 A3-gate。DCU 沿用 systemd，XPU 沿用 Kubernetes ai-gate，A3/PPU 沿用 containerd ai-gateway。等待所有已接受请求自然结束后替换程序；配置、权限、资源、目标及签名密钥信任保持，仅 switch 本地软额度使用本次 10 GiB 默认值。A3/PPU 的规范化 OCI、XPU 除二进制 hostPath 外的 Deployment spec 均一致；PPU 保留 proxy 4 GiB、switch 512 MiB、2 CPU、只读根目录及 uid 65532。

新平台回放在 test2/test3 的一次性容器中运行。两台 worker 的 IP 仅获 DCU/XPU/A3 proxy TCP 18084 来源规则，18082 未向其开放；持久化入口为原访问控制 unit 的 ExecStartPost。PPU 发压 proxy 使用 60013，旧 replay 为 60014。实际 proxy 的 Router、service_id、config_digest 及两个签名验证密钥已核对，回放镜像在两节点通过真实 Docker/NAS 和模拟模型验收。

DCU 的七个旧保留片段共 34,862 个原始文件、2,804,618,924 bytes。首片段已由 test1 发布；按用户指示临时挂载 NAS，使用四线程经本机认证 switch 接口导出其余六片段，并复核首片段，完成来源与 NAS 逐文件哈希校验、持久化和原子发布。平台已接管，全部 saved/local_released=true，原始文件保留。暂存阶段约 55.5 秒，停止 engine 后七片段完整校验与发布约 75.84 秒。临时挂载已正常卸载，未写 fstab。旧 runs 的 72 文件、28,585,563 bytes 已迁入 NAS 并完整核验，历史产品报告可读。

四处旧 replay 确认空闲后停止：DCU/XPU unit inactive/masked，A3/PPU 容器 restart.policy=no、restart.status=stopped；18085（PPU 60014）无任何地址监听。停止容器的 nerdctl NetNS 警告与陈旧 PID 已通过进程不存在、容器非运行和端口关闭证据核实，未重启旧服务。原容器、数据、历史登记和私有 unit 备份保留。恢复原 Router/配置版本后，DCU/XPU/A3 revision=54/35/55、admission=true，PPU revision=10、admission=false。两条原录制计划恢复，平台 maintenance=false、health 全组件 ok、容量新鲜、无资源锁或未完成控制操作。

新增发布根目录：dcu1 `/data4/aigate/releases/nas-main-20261019/`；xpu1、A3-gate `/var/lib/ai-gate/releases/nas-main-20261019/`；ppu `/home/wqy/aigate/releases/nas-main-20261019/`，与 `/bmcp_lvm_fs/aigate` 为同一部署数据。各含 bin/、private/ 及脱敏 verification.json、replay-retired.json。DCU 另有新建临时挂载点 `nas/`，现为空且未挂载，卸载回执为 temporary-nas-unmounted.json。DCU 两个 zzzzzz-nas-main-20261019.conf 写入既有 unit.d 目录；没有新建 systemd 目录。原程序、停止容器、配置和数据保留，不回退。平台镜像、完整迁移与窗口完成证据详见[平台发布记录](../../code-eval/deploy/releases/README.md#nas-main-20261019)。

## 2026-10-19 10:58 A3 网关迁回原节点

A3 switch、proxy、replay 已从 `hwA3-1` 迁回 `A3-gate（122.52.5.131）`，继续使用 containerd + nerdctl、namespace `ai-gateway`、restart=unless-stopped。业务 `9090/v1`、控制 `18083`、proxy `18084`、replay `18085`、诊断/指标 `18082` 恢复原地址。模型/P/D 留在现有节点，proxy 后端使用 `122.209.21.24:8077`。源节点三个网关容器已停止并禁止自动重启，历史文件保留。

当前 switch/proxy 源码仍为 `9cb5626dcd5a49be690b3bf86fd3bac421d285eb`，replay 仍为 `0de4f2a2fd0189d1f46ce528bdedd203614c776f`，改用已验证的对应 amd64 产物。实际 SHA-256 分别为 `3284d5bb98f67ce568882a8df068d0f29f820ef63aadb10f1693a54f8b086013`、`7c63090662081c7db271868bb7f04aa2e83d1a49b28214a2be1628207ac8fe55`、`caa2bdf030ab99470bc83cb7444f7067a4b455076b8ce4e7c70c9942fe5c019f`。基础镜像保留，通过只读程序挂载运行上述版本，镜像标签不代表当前程序源码。

两个完整固定请求门禁均通过（31.707 / 22.996 秒），9 项原绑定校验及业务 SSE 冒烟通过；Router 版本 7、binding revision 52、admission=true，平台 maintenance=false。迁移数据 3,573 个条目核验通过。监控采集与自动探针正常；DCU replay 仅更新 A3 目标引用并重启同版本，XPU 无需修改。

新增服务器根目录及完整证据索引见 [迁回记录](../../environment/A3/changes/20261019-aigate-relocate-back/README.md)。原始门禁响应保留在 A3-gate `/var/lib/ai-gate/releases/relocate-back-20261019/private/gate/`，不进入 Git。以下各节为历史部署记录，涉及 A3 宿主和架构时以本节为准。

## 2026-10-19 00:46 回放器重启中断不再生成报告

DCU、XPU、A3、PPU 的 aigate-replay 已部署 `0de4f2a2fd0189d1f46ce528bdedd203614c776f`。重启恢复保留状态与原始结果，不生成性能报告；报告读取也不返回旧缓存或重建报告。普通失败、主动停止和重启前已完成的任务保持原有规则。配套 code-eval engine/Web 发布 `a0ff6eb5c05251c6d97d23fad1af10ad1f74a491`，同时上线重启确认、安全收尾及报告隐藏。

| 环境 / SSH MCP target | 部署方式 | 实际运行二进制 SHA-256 |
| --- | --- | --- |
| DCU / dcu1 | 原 `aigate-replay.service`，linux/amd64 | `caa2bdf030ab99470bc83cb7444f7067a4b455076b8ce4e7c70c9942fe5c019f` |
| XPU / xpu1 | 原 `aigate-replay.service`，linux/amd64 | `caa2bdf030ab99470bc83cb7444f7067a4b455076b8ce4e7c70c9942fe5c019f` |
| A3 / hwA3-1 | `ai-gateway/a3-aigate-replay`，linux/arm64 | `f55afafa7a8c816768a425e69fcd8285231bd3e41625f8a1e1917d10de620e54` |
| PPU / ppu | `ai-gateway/ppu-aigate-replay`，linux/amd64 | `caa2bdf030ab99470bc83cb7444f7067a4b455076b8ce4e7c70c9942fe5c019f` |

二进制从已推送提交使用 Go 1.27.1、CGO_ENABLED=0、离线显式 vendor 和 trimpath 构建，内嵌 revision 匹配且 `vcs.modified=false`。四处启动参数、凭据、权限、限制、白名单和已有能力保持，认证 status 正常、活动回放为空，未认证 status 返回 403。systemd 只新增本轮 ExecStart 覆盖文件；A3/PPU 保留原镜像，替换只读二进制挂载来源，规范化 OCI 配置一致，旧容器已停止并禁止自动恢复。

DCU 的历史重启任务 `batch-ppu-proxy4g-20261018-r1` 在回放器报告接口和平台报告接口均返回 404，平台正常/已删除列表及模型画像均不再显示；其原始文件和已归档正文保留。DCU/XPU 的录制、结果文件内容与元数据核验保持；A3/PPU 的配置及数据核验保持。A3/PPU switch/proxy 身份与窗口前一致；四项网关绑定未改，DCU/XPU revision=53/34、admission=true，A3/PPU revision=48/8、admission=false。

平台于北京时间 00:42:46 开启维护，00:46:13 恢复原 `maintenance=false`，窗口 206.84 秒。engine/Web healthy、restart_count=0，平台 health=ok。线上 Chrome 1280×720 和 iPhone 390×844 竖屏触控模拟通过报告隐藏与正常报告查看检查，页面脚本错误、API 失败和写请求均为零。没有新建回放、发送模型请求或人为制造重启中断；重启后收尾状态机由本地回归验证。

本轮新增服务器发布目录：dcu1 `/data4/aigate/releases/replay-restart-20261019/`；xpu1 `/var/lib/ai-gate/releases/replay-restart-20261019/`；hwA3-1 `/data/a3-standalone-20261017/aigate/releases/replay-restart-20261019/`；ppu `/bmcp_lvm_fs/aigate/releases/replay-restart-20261019/`。各含 `bin/` 和 `private/`，私有目录 0700；平台目录见[平台发布记录](../../code-eval/deploy/releases/README.md#replay-restart-20261019)。本地脱敏证据在 `code-eval/deploy/evidence/replay-restart-20261019/` 的四份 `*-verification.json` 与 `replay-verification.json`，私有配置和密钥未下载。

## 2026-10-18 05:11 双录制独立并发：仅更新 replay

DCU、XPU、A3、PPU 的 aigate-replay 已部署 `0be523881cebe01292513e13a2f0da707a4d736c`，新增 `recording-concurrency-per-source-v1`。每份录制拥有独立并发额度，目标级收尾按总额度计算；报告提供逐录制 `source_inputs`。Go 1.27.1、CGO_ENABLED=0、离线显式 vendor、trimpath 构建，二进制内嵌上述 Git revision 且 `vcs.modified=false`。

| 环境 / SSH MCP target | 部署方式 | 实际运行二进制 SHA-256 |
| --- | --- | --- |
| DCU / dcu1 | 原 `aigate-replay.service`，linux/amd64 | `96baafd732a68c7f1f509ca10f4e9f248d1766e30f92e2fbdc9b222824e89512` |
| XPU / xpu1 | 原 `aigate-replay.service`，linux/amd64 | `96baafd732a68c7f1f509ca10f4e9f248d1766e30f92e2fbdc9b222824e89512` |
| A3 / hwA3-1 | `ai-gateway/a3-aigate-replay`，linux/arm64 | `b854ece67813174243dff838812716363f994910738db22f08d6ee68f56b53da` |
| PPU / ppu | `ai-gateway/ppu-aigate-replay`，linux/amd64 | `96baafd732a68c7f1f509ca10f4e9f248d1766e30f92e2fbdc9b222824e89512` |

四处 status 已核验新能力、schema=2、无活动回放及原目标白名单；未认证 status 为 403，不存在的报告返回 404。原启动参数、凭据、服务权限和限制保持，A3/PPU 的规范化 OCI 配置一致，旧 replay 容器停止并禁用自动重启。DCU/XPU 增加 `zzzzz-model-standards-20261018.conf`，其排序在此前覆盖文件之后。switch、proxy、模型服务未替换；四入口原绑定 revision 为 52/33/46/4，准入仍为 true。

DCU 的 62,543 个录制文件（4,588,288,633 bytes）和 32 个回放文件（4,965,758 bytes）内容、元数据清单前后一致；其他三处空录制和回放目录保持，未清理历史数据。配套 engine/Web 与标准结果迁移已完成，平台于 05:11:34 恢复原 `maintenance=false`，详见 [平台发布记录](../../code-eval/deploy/releases/README.md#model-standards-20261018)。本次未创建真实回放负载；独立并发及自动截断的行为验证来自本地合成回归，线上验收覆盖部署身份、能力和数据保持。

新增服务器发布目录如下，均含 `bin/`、`private/`、`checks/`；发布根目录 0711、二进制目录 0755、私有目录 0700，数据库和凭据未下载或进入 Git。

| 节点 | 新增目录 |
| --- | --- |
| dcu1 | `/data4/aigate/releases/model-standards-20261018/` |
| xpu1 | `/var/lib/ai-gate/releases/model-standards-20261018/` |
| hwA3-1 | `/data/a3-standalone-20261017/aigate/releases/model-standards-20261018/` |
| ppu | `/bmcp_lvm_fs/aigate/releases/model-standards-20261018/` |

本地二进制位于 `code-eval/deploy/dist/model-standards-20261018-gate/`，四处脱敏验收收据位于 `code-eval/deploy/evidence/model-standards-20261018/checks/{dcu1,xpu1,a3,ppu}/verification.json`。未回退旧版本。

## 2026-10-18 02:59 单份报告收尾：仅更新 replay

四处 aigate-replay 已更新至 `f519363f3ddd594438ad3343e4f6240efaf79d1d`，固定读取当前 v2 报告，`?version=1`、`?version=2` 和空 version 均拒绝为 400。源码来自该 commit 的 Git 归档，Go 1.27.1、CGO_ENABLED=0、离线显式 vendor、trimpath、buildvcs=false 构建，归档身份与二进制摘要保存在本地 `deploy/replay-ui-final-20261018/manifest.json`。

| 环境 / target | 部署方式 | 运行二进制 SHA-256 |
| --- | --- | --- |
| DCU / dcu1 | `aigate-replay.service`，linux/amd64 | `8d53ac066de582a4f9bf4f70b0b9ce484778bbd164b93cd241cc05c5545940fb` |
| XPU / xpu1 | `aigate-replay.service`，linux/amd64 | `8d53ac066de582a4f9bf4f70b0b9ce484778bbd164b93cd241cc05c5545940fb` |
| A3 / hwA3-1 | `ai-gateway/a3-aigate-replay`，linux/arm64 | `a6f0b1e972e75b6fc28da2effa0ed2c69927a3d81a4c265eaf6c703525b0c250` |
| PPU / ppu | `ai-gateway/ppu-aigate-replay`，linux/amd64 | `8d53ac066de582a4f9bf4f70b0b9ce484778bbd164b93cd241cc05c5545940fb` |

四处认证 status 正常、无活动回放，原 capabilities 和目标白名单保持；未认证访问返回 403，不存在报告返回 404。DCU/XPU 保持 unit、参数、用户及业务环境，仅新增 `zzzz-replay-ui-final-20261018.conf` 指向新二进制。A3/PPU 保持镜像、规范化 OCI 配置、环境和数据挂载，只替换 `/opt/aigate` 的二进制挂载源；旧 replay 容器停止且禁用自动重启。switch、proxy 和模型服务未替换。

XPU 首次启动报 203/EXEC：本轮发布父目录为 0700，而 service User/Group=65532。保留失败日志后将该目录改为 0711，private 仍为 0700，启动同一新二进制并完成全部核验；未回退。DCU 62,543 个录制文件（4,588,288,633 bytes）和 32 个回放文件（4,965,758 bytes）的完整内容/元数据摘要保持，其他三处空数据目录保持。四入口 Router、binding revision=52/33/46/4、admission=true 均保持。

配套平台于北京时间 02:54:22 开启维护，02:59:27 恢复 `maintenance=false`，窗口用时 304.65 秒。平台健康、当前报告读取、版本参数拒绝、数据保护、桌面 Chrome 与 iPhone 竖屏触控模拟验收通过；未创建真实新回放或发送模型生成请求。平台源码、镜像与备份见[本轮发布记录](../../code-eval/deploy/releases/README.md#replay-ui-final-20261018)。

新增发布根目录分别为 dcu1 `/data4/aigate/releases/replay-ui-final-20261018/`、xpu1 `/var/lib/ai-gate/releases/replay-ui-final-20261018/`、hwA3-1 `/data/a3-standalone-20261017/aigate/releases/replay-ui-final-20261018/`、ppu `/bmcp_lvm_fs/aigate/releases/replay-ui-final-20261018/`，均含 bin/ 与 private/。核验收据位于各根目录 `verification.json`；本地副本在 `code-eval/deploy/evidence/replay-ui-final-20261018/checks/{dcu1,xpu1,a3,ppu}/`，受限配置及失败日志保留在服务器 private/。

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
