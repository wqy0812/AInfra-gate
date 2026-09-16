# A3 aigate 直连部署及旧流程清理 — 2026-09-15

**已部署、已验收、已清理。** 使用 SSH MCP 直连 `A3-gate`（worker-131），完成产物下载校验、重新上传、节点直接落盘、Kubernetes 切换和真实请求验证。未创建传输 Pod。

## 发布结果

- 切换通过：2026-09-15 **17:49:18 +08:00**。
- 请求验收通过：**17:50:11 +08:00**。
- 新 Pod：`aigate-6f5fd9cfc8-6l8dz`，UID `227f86a0-b73c-43ce-9510-644aca9b3dc5`，Ready，容器重启次数 0。
- 二进制：`a1cd6006464b14a1199c582f69350a0971c015a4facccb338728251e27be9fd2`，与发布前正在运行的慢请求留存版本一致；本次重新部署验证传输和发布流程，没有重新构建工作区程序。
- 新版本目录：`/var/lib/ai-gate/releases/direct-20260915/`。
- 发布记录与回退：`/var/lib/ai-gate/deployments/direct-20260915/`。
- `active_backend=a3`、`route_version=3`、回退候选 dcu、未 armed；两个后端的 maintenance 和 drain_unknown 均为 false，保持基线状态。

## 验证范围

- HTTP **49/49**；模型发现、两轮 JSON/SSE/工具调用/工具结果续答共 **9 步**全部通过。
- 实际运行进程 `/proc/1/exe` 的 SHA-256 与上传清单一致。
- 旧进程干净退出；**78 组**累计计数恢复且计数起点保持。
- **465 个**已有画像/HMAC/慢请求相关文件通过历史内容前缀校验；允许运行中的画像文件继续追加。
- ConfigMap 内容、后端注册表、路由和 HMAC 保持；**6 个** `zwt` Pod 的 UID、启动时间、容器 ID、重启次数保持。
- 同一套检查在节点清理后再次通过；HTTP/模型协议验收结果为清理前本次新 Pod 的结果。

本次是短请求协议及部署流程验收，未重复执行 300 秒慢上传测试、长上下文或并发吞吐测试。回退副本和模板已校验，未执行第二次生产重建演练回退。

## 删除范围

| 位置 | 已删除内容 | 文件数量 / 字节 |
| --- | --- | --- |
| `A3-master` / master-228 | `/root/aigate-deploy/`，含首次部署、profile、e435f42、live、streaming、slow-requests 六套旧目录 | 127 / 82,495,520 |
| `A3-gate` / worker-131 | `releases/` 下 bf273b8698c0、e435f42-30a42c2020ea、live-20260915、streaming-20260915、slow-requests-20260915 | 8 / 72,155,149 |
| 本地 `deploy/a3/` | 旧脚本、配置/候选清单、systemd 模板、历史产物及缓存 | 99 / 16,312,424 |

另外删除 6 个旧零副本 ReplicaSet、2 个无引用旧 ConfigMap（`aigate-config`、`aigate-profile-config-20260914`），并移除 Deployment 中仍指向旧版本的 last-applied 注解。Kubernetes 删除使用 UID/resourceVersion 前置条件，文件删除核对 inode、设备号和哈希。

主控清理前未发现打开文件、检查范围内的 systemd/cron 配置或 Pod hostPath 引用。节点扫描发现 HAMI DaemonSet 的 `/var` 祖先目录挂载；核实其 desired/current 实例数均为 0，并在清理后确认该对象完全未变。没有把该宽范围模板当作 aigate 文件的直接依赖。

保留当前唯一版本目录、正式状态和慢请求数据、画像 Secret、Router Service。当前旧二进制另存于新发布目录 `rollback/aigate`，回退模板已改用此路径，删除旧节点目录不会破坏本次回退入口。共享 live 发布脚本的 DCU 分支保留。

## 证据与使用

- [清理后的最终状态](evidence/final.json)
- [正式验收结果](evidence/acceptance.json)
- [切换与数据保留](evidence/switched.json)
- [HTTP 验收](evidence/http-verification.json)
- [模型协议验收](evidence/warmup.json)
- [主控清理结果](evidence/master-cleanup-result.json)
- [节点清理结果](evidence/node-cleanup-result.json)
- [本地清理结果](evidence/local-cleanup-result.json)

原始清单、源码归档、基线和回退模板在同目录 `evidence/`，受 Git 排除规则保护；服务器保存在本次发布目录。后续发布和回退操作见 [A3 部署入口](../README.md)。
