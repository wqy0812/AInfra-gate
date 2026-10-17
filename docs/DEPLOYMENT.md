# 当前部署身份

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
