# 当前部署身份

2026-10-15 02:57（北京时间）通过 SSH MCP 将 A3-gate、xpu1、dcu1 的 switch/proxy/replay 更新至 `0f6ff9a5f46038ea1840f3209e413169887d2f25`。Go 1.27.1，linux/amd64，CGO_ENABLED=0，离线 vendor 构建，vcs.modified=false；三台运行进程摘要均逐一核验。

| 当前二进制 | 运行 SHA-256 |
| --- | --- |
| aigate-switch | `56a6994202f3ce36a07871179501be95e3de00cf233564bfd8b6324054778400` |
| aigate-proxy | `ec987fbe1e2ac6f8428e66b367a0111d48d95acf67a8e007422fb8aec918fa8b` |
| aigate-replay | `3d3579f78975ae8f994784a25e029a00d9bfb232c5a0a2473e59e394f2e2f3cf` |

三处回放服务均返回 `recording-v2-timed-unlimited-output-verified`，配置摘要、对外地址与路由绑定保持原值。按用户本轮明确授权，停止采集/回放进程后删除 A3 `rec-a3-20261014T0600-a241e255`（2071 条、661906733 字节）及 DCU `rec-dcu-20261014T0600-5137eb48`（4920 条、1581282571 字节）目录，平台对应记录也已删除。旧 schema 1 不作兼容转换。录制目录与服务列表均为空，两条每天 14:00–16:00 的规则保留并迁移为新版参数。没有创建真实录制或回放负载。

部署前所有入口停止准入并确认在途为零。部署后 DCU/XPU 原生 restore 完成，revision 为 45/24，admission=true。**A3 restore 在模型发现阶段失败，原上游 `122.209.21.24:8077` 拒绝连接；A3 新版进程已验证，revision=38、admission=false，保留 Router 维护状态。** 无未结束控制操作或资源锁，不绕过校验强行开放，不更改上游模型服务。平台全局维护已关闭。A3 replay 继续为直接 nerdctl 容器、restart=no、无 systemd 服务。

新增发布目录：A3-gate/xpu1 `/var/lib/ai-gate/releases/recording-v2-20261015/`，dcu1 `/data4/aigate/releases/recording-v2-20261015/`，包含 `bin/` 和受限 `private/`。每台 `verification.json` 为运行验收，A3/DCU `private/deleted-recording.json` 为删除回执；本地汇总 `work/recording-v2-20261015/evidence/`。Go 生产包回归通过；平台与浏览器验收见 code-eval 发布索引。

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
