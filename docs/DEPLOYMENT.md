# 当前部署身份

2026-10-15 04:11（北京时间）完成双录制叠加发布，通过 SSH MCP 更新 A3-gate、xpu1、dcu1 的 replay 至 `b460fcca67417a73e1a1a045fc2ce2bc21a04e32`。Go 1.27.1，linux/amd64，CGO_ENABLED=0，离线 vendor 构建，vcs.modified=false；三台实际运行进程的二进制摘要逐一核验。switch/proxy 保持当日 02:57 发布的 `0f6ff9a5f46038ea1840f3209e413169887d2f25`，本次未替换。

| 当前二进制 | 运行 SHA-256 |
| --- | --- |
| aigate-switch（沿用上次发布） | `56a6994202f3ce36a07871179501be95e3de00cf233564bfd8b6324054778400` |
| aigate-proxy（沿用上次发布） | `ec987fbe1e2ac6f8428e66b367a0111d48d95acf67a8e007422fb8aec918fa8b` |
| aigate-replay（本次更新） | `c56a90e3e54bfc69b034ee2b00f198271e51087c019de6c3d6f24f7831c9ba96` |

三处回放服务均返回 `recording-v2-timed-unlimited-output-verified` 与 `recording-overlay-sync-v1`。致命故障立即公布 stopping/error，平台停止同组另一侧新发送，已发请求继续收尾；启动相关容差放宽至 10 秒。窗口前后均无活动回放，未产生真实模型负载。平台认证访问与无凭据 403、目标白名单、启动参数、环境配置摘要和数据路径均已核验；两条每天 14:00–16:00 的录制规则保持不变。

A3 replay 继续使用 namespace `a3-delivery` 下的直接 nerdctl 容器 `a3-aigate-replay`，新容器 ID `2a3fef236b4b97ac924d391b5b8c131ad5dd7674d694587b7db73b5cb85cba97`，镜像 config ID 仍为 `sha256:5441cfcb8dab40c8058ab59ef724427facaba19ef3d7a40c53386852c670165c`。仅二进制只读挂载换到本次发布目录；UID/GID、资源限制、配置与数据挂载、只读根文件系统、restart=no 均保持，无 systemd 服务。启动脚本为本次目录下 `start-aigate-replay.sh`。XPU/DCU 沿用 `aigate-replay.service`，仅替换 ExecStart 二进制路径。旧容器及受限配置快照保留作证据，未回退。

业务入口、管理入口、Router 绑定与 revision/admission 均保持窗口前状态：DCU/XPU revision=45/24、admission=true；**A3 沿用前次原上游 `122.209.21.24:8077` 拒绝连接后保留的 Router 维护，revision=38、admission=false。本次不代表 A3 业务入口恢复。** 无未结束控制操作或资源锁；平台全局维护已关闭，未改动模型后端。

新增发布目录：A3-gate/xpu1 `/var/lib/ai-gate/releases/overlay-sync-20261015/`，dcu1 `/data4/aigate/releases/overlay-sync-20261015/`，包含 `bin/` 和受限 `private/`。每台 `verification.json` 记录运行身份；本地汇总在 `code-eval/deploy/evidence/overlay-sync-20261015/`。Go 回放 race 测试与 vet 通过；配套平台源码 `5e104cff19515301e47336c1338fd2539e1d3650`，实际镜像与 Chrome 两档视口验收见 code-eval 发布索引。真实双录制负载未执行；故障传播和 10 秒边界由本地回归验证。

## 上次部署及 A3 replay 生命周期背景

2026-10-14（北京时间）通过 SSH MCP 更新 A3-gate、xpu1、dcu1 的 switch/proxy，并安装三台主机的 replay 服务。源码 `34edf7fe5080ce44a78835f9c77c8b7280ef02b8` 整合远端主线 `62c553a` 与异常留存读取功能 `a809dc0`。Go 1.27.1，linux/amd64，CGO_ENABLED=0，离线 vendor 模式；构建信息 vcs.modified=false。

| 二进制 | 运行文件 SHA-256（三台逐一核验） |
| --- | --- |
| aigate-switch | `68314cdfd5388c9d0fb5e89cb5a5e7e5825ef351e63d2b416303c8c620f516f3` |
| aigate-proxy | `989f347cab3f605f24ea335a6d0a98adeef650861ed1c443613714aecf1c0b17` |
| aigate-replay | `db3e4b1f8441b197ce9c9e9f2681206621b2291a6332a65015a9874a5bbc6105` |

A3 延用 nerdctl / systemd 管理，XPU 延用 Kubernetes Recreate，DCU 延用 systemd。代理配置摘要及历史数据根保持原值，旧实例/配置保留，不回退。用户明确允许 A3 不等待请求排空；本次 A3 服务停止/替换使用该授权，其余入口确认安全收尾。为了优先提供新版 Gateway，先替换程序并恢复接流，再完成录放配置窗口。

录制能力已启用，每入口容量上限 50 GiB；没有创建定时规则或启动录制。初次发布时各主机安装了 `aigate-replay.service`；A3 于 2026-10-14 19:21（北京时间）按用户要求改为直接运行的 nerdctl 容器 `a3-delivery/a3-aigate-replay`，旧 replay unit 及开机启用链接已删除，systemd 查询为 `not-found`。A3 replay 使用 `--restart=no`，异常退出或主机重启后人工检查再启动，不建立替代 systemd 服务；A3 switch/proxy 及 XPU/DCU 的部署方式未改动。

回放通过宿主 IP 的 `18085` 监听，仅允许 test1 `122.247.53.250/32` 和回环 `127.0.0.1/32`，另需独立 Bearer 控制密钥；无凭据及使用正确凭据但非许可来源均为 403。三个 replay/switch/proxy 的回放标记密钥匹配，密钥保存在受限环境文件或 Kubernetes Secret，不写入本文。

A3 replay 迁移沿用上述二进制摘要和现有镜像 `local/a3-delivery-aigate:20261010`（镜像 config ID `sha256:5441cfcb8dab40c8058ab59ef724427facaba19ef3d7a40c53386852c670165c`），通过只读挂载发布目录运行。UID/GID 65532、只读根文件系统、录制目录只读、回放数据目录可写，限制 1 GiB 内存、2 CPU、128 进程。启动脚本在 A3-gate 的 `/var/lib/ai-gate/releases/latest-20261014-34edf7f/start-aigate-replay.sh`，验收在同目录 `replay-container-verification.json`；旧 unit 仅作为证据存于 `private/aigate-replay.before-container.service`。迁移前无活动回放；状态、现有录制目录读取、访问控制、进程摘要和挂载权限核验通过，未发起模型回放负载。

A3/XPU 增加 `aigate-switch` 无登录系统账号/组（UID/GID 65532），匹配现有容器录制文件所有者。首次 replay 启动因宿主缺少该账号而返回 217/USER；创建账号后已修复并保留日志。DCU 延用网关 root 身份。回放服务仅写独立数据目录，根文件系统只读，内存上限 1 GiB、CPUQuota 200%、TasksMax 128。平台已登记回放服务与三 Router 白名单，没有发起真实回放。

新增服务器目录（含子目录）：

- A3-gate、xpu1：`/var/lib/ai-gate/releases/latest-20261014-34edf7f/`（bin、private、replay-config）、`/var/lib/ai-gate/state/traffic-recordings/`、`/var/lib/ai-gate/replay/`。录制器在前者中创建入口 ID 子目录。
- dcu1：`/data4/aigate/releases/latest-20261014-34edf7f/`（bin、private、replay-config）、`/data4/aigate/recordings/`、`/data4/aigate/replay/`、`/etc/systemd/system/aigate-proxy-dcu.service.d/`。
- 配套平台 test1：`/data2/code-eval/releases/latest-20261014/`，完整平台身份见 code-eval 发布索引。

本地 Go 全部生产包回归通过；发布后逐进程摘要、配置摘要、回放服务身份/白名单、访问控制与平台接流状态通过。平台正常恢复后 A3/DCU 入口仍指向 A3 Router、XPU 入口指向 XPU，外部业务 URL 保持。原生 restore 自带生成协议验证通过；没有追加手工模型冒烟或录放负载。

原始证据保留在各主机新发布目录的 `verification.json`、private 快照、systemd/Kubernetes 日志；本地汇总在工作区 `work/latest-deploy-20261014/`。monitoring 源码及部署配置已一致，本轮不重启 monitoring。
