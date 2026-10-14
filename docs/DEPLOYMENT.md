# 当前部署身份

2026-10-14（北京时间）通过 SSH MCP 更新 A3-gate、xpu1、dcu1 的 switch/proxy，并安装三台主机的 replay 服务。源码 `34edf7fe5080ce44a78835f9c77c8b7280ef02b8` 整合远端主线 `62c553a` 与异常留存读取功能 `a809dc0`。Go 1.27.1，linux/amd64，CGO_ENABLED=0，离线 vendor 模式；构建信息 vcs.modified=false。

| 二进制 | 运行文件 SHA-256（三台逐一核验） |
| --- | --- |
| aigate-switch | `68314cdfd5388c9d0fb5e89cb5a5e7e5825ef351e63d2b416303c8c620f516f3` |
| aigate-proxy | `989f347cab3f605f24ea335a6d0a98adeef650861ed1c443613714aecf1c0b17` |
| aigate-replay | `db3e4b1f8441b197ce9c9e9f2681206621b2291a6332a65015a9874a5bbc6105` |

A3 延用 nerdctl / systemd 管理，XPU 延用 Kubernetes Recreate，DCU 延用 systemd。代理配置摘要及历史数据根保持原值，旧实例/配置保留，不回退。用户明确允许 A3 不等待请求排空；本次 A3 服务停止/替换使用该授权，其余入口确认安全收尾。为了优先提供新版 Gateway，先替换程序并恢复接流，再完成录放配置窗口。

录制能力已启用，每入口容量上限 50 GiB；没有创建定时规则或启动录制。各主机的 `aigate-replay.service` 已 enabled/active，通过宿主 IP 的 `18085` 监听，仅允许 test1 `122.247.53.250/32` 和回环 `127.0.0.1/32`，另需独立 Bearer 控制密钥；无凭据及使用正确凭据但非许可来源均为 403。三个 replay/switch/proxy 的回放标记密钥匹配，密钥保存在受限环境文件或 Kubernetes Secret，不写入本文。

A3/XPU 增加 `aigate-switch` 无登录系统账号/组（UID/GID 65532），匹配现有容器录制文件所有者。首次 replay 启动因宿主缺少该账号而返回 217/USER；创建账号后已修复并保留日志。DCU 延用网关 root 身份。回放服务仅写独立数据目录，根文件系统只读，内存上限 1 GiB、CPUQuota 200%、TasksMax 128。平台已登记回放服务与三 Router 白名单，没有发起真实回放。

新增服务器目录（含子目录）：

- A3-gate、xpu1：`/var/lib/ai-gate/releases/latest-20261014-34edf7f/`（bin、private、replay-config）、`/var/lib/ai-gate/state/traffic-recordings/`、`/var/lib/ai-gate/replay/`。录制器在前者中创建入口 ID 子目录。
- dcu1：`/data4/aigate/releases/latest-20261014-34edf7f/`（bin、private、replay-config）、`/data4/aigate/recordings/`、`/data4/aigate/replay/`、`/etc/systemd/system/aigate-proxy-dcu.service.d/`。
- 配套平台 test1：`/data2/code-eval/releases/latest-20261014/`，完整平台身份见 code-eval 发布索引。

本地 Go 全部生产包回归通过；发布后逐进程摘要、配置摘要、回放服务身份/白名单、访问控制与平台接流状态通过。平台正常恢复后 A3/DCU 入口仍指向 A3 Router、XPU 入口指向 XPU，外部业务 URL 保持。原生 restore 自带生成协议验证通过；没有追加手工模型冒烟或录放负载。

原始证据保留在各主机新发布目录的 `verification.json`、private 快照、systemd/Kubernetes 日志；本地汇总在工作区 `work/latest-deploy-20261014/`。monitoring 源码及部署配置已一致，本轮不重启 monitoring。
