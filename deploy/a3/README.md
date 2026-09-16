# A3 aigate 直连部署

A3 网关统一通过 **SSH MCP → `A3-gate`（worker-131）** 完成文件上传、Kubernetes 管理、验收和回退。发布工具为 [direct_release.py](direct_release.py)，公共依赖与本地回归见 [部署说明](../README.md)。入口为 `ai-gate/aigate` Deployment，单副本 Recreate。

2026-09-15 已完成一次直连部署及旧流程清理，详见 [本次发布记录](direct-20260915/README.md)。这次复用已验收的慢请求留存版本二进制，未构建或发布工作区其他改动。

## 2026-09-15 直连部署快照

下表属于 17:50 的 `direct-20260915` 发布记录；同日后续已有 metric-v2 发布记录，不能将此表的版本目录、哈希及回退入口当作实时状态。操作前通过 SSH MCP 重新读取 Deployment、运行二进制与配置；源码指标定义见 [请求监控口径](../../README.md)。

| 项目 | 当前值 |
| --- | --- |
| SSH MCP 目标 | `A3-gate`；主机 `worker-131`，Linux amd64，具备 kubectl 权限 |
| 业务入口 | `http://122.52.5.131:9090/v1` |
| Deployment | `ai-gate/aigate`，单副本 Recreate |
| 节点版本目录 | `/var/lib/ai-gate/releases/direct-20260915/` |
| 发布、证据与脚本目录 | `/var/lib/ai-gate/deployments/direct-20260915/` |
| 独立回退二进制目录 | `/var/lib/ai-gate/deployments/direct-20260915/rollback/` |
| 正式状态目录 | 节点 `/var/lib/ai-gate/state/` → 容器 `/var/lib/aigate/` |
| 当前 ConfigMap | `ai-gate/aigate-slow-requests-20260915` |
| 画像凭据 | 现有 Secret `ai-gate/aigate-profile-20260914`，不复制到文档 |
| 二进制 SHA-256 | `a1cd6006464b14a1199c582f69350a0971c015a4facccb338728251e27be9fd2` |

截至本次验收，`active_backend=a3`、`route_version=3`，A3/DCU 的 maintenance 和 drain_unknown 均为 false。后续操作前必须实时读取状态，不能把这份记录当作实时路由。

A3 下游继续使用 `pd-proxy.zwt.svc.cluster.local:8077`。Router Service 定义保留在 [pd-proxy-service.yaml](pd-proxy-service.yaml)，本次未修改该服务。Pod 继续使用显式集群 DNS `172.25.0.10` 和节点本地 hostPath，不能仅修改 nodeSelector 就迁移节点。

## 后续发布流程

所有远程命令及传输使用 SSH MCP；不得在本地命令行或脚本中自行调用 ssh/scp 绕过 MCP。新增服务器目录前先明确告知用户其完整路径。SSH MCP 连接或能力失败时停止相关远程操作，修复所需配置后再继续。

1. 冻结待发布源码、生成源码哈希清单并构建 Linux amd64 静态二进制。代码升级须完成 `go test -race -timeout 60s ./...`、`go vet ./...` 和 `git diff --check`。已有正式二进制重新部署时，可以复用其原始源码归档和清单，并核对运行中 `/proc/1/exe` 的哈希。
2. 选择新且未使用的发布名。提前告知将创建的 `/var/lib/ai-gate/deployments/发布名/`、其 `rollback/` 子目录，以及 `/var/lib/ai-gate/releases/发布名/`。
3. 经 SSH MCP 在节点创建发布目录和 `rollback/`。发布目录权限 0700；`rollback/` 权限 0755，供非 root 容器作为只读 hostPath 挂载。版本目录由 prepare 创建并设为 0755。
4. 使用 SSH MCP `ssh_scp` 直接上传 `direct_release.py`、`../profile_checks.py`、`aigate`、`verify`、`source-manifest.json`、`source.tar.gz` 至新发布目录，两个 Python 文件放在同一目录。清单至少包含 `binary_sha256`、`verify_sha256`、`source_archive_sha256`，保留原始源码文件哈希及构建来源。验收工具须有执行权限。
5. 通过 SSH MCP 顺序执行 `prepare`、`switch`、`accept`。每步成功后才能进入下一步。切换及验收建议使用 SSH MCP 后台任务并检查退出状态，最长优雅退出可达 60 分钟。

以已完成的本次发布名说明命令格式；**不要对该目录重新执行 prepare**，它保留原始基线并拒绝覆盖：

```bash
python3 /var/lib/ai-gate/deployments/direct-20260915/direct_release.py prepare \
  --root /var/lib/ai-gate/deployments/direct-20260915
python3 /var/lib/ai-gate/deployments/direct-20260915/direct_release.py switch \
  --root /var/lib/ai-gate/deployments/direct-20260915
python3 /var/lib/ai-gate/deployments/direct-20260915/direct_release.py accept \
  --root /var/lib/ai-gate/deployments/direct-20260915
```

- `prepare`：核对节点、工作负载、在途状态和所有产物哈希；保存基线，复制旧二进制到独立 rollback 目录，生成使用该新路径的回退模板，并直接写入节点新版本目录。
- `switch`：检查并发模板/路由变化，使用 resourceVersion 条件更新；缩容触发 SIGTERM 并等待干净退出，按实际 legacy / streaming / metric-v2 起点保存状态和对应计数检查点，切换 binary hostPath 后恢复单副本。相同口径验证计数和直方图恢复，口径升级验证新起点及旧检查点保留；验证失败时尝试回退，具体结果必须检查。
- `accept`：HTTP 检查、模型发现、两轮 JSON/SSE/工具调用及续答；检查实际进程二进制、配置、路由、注册表、HMAC、已有历史数据前缀、计数起点和累计值，以及推理 Pod 指纹。

脚本只更新二进制引用及发布注解，保持当前 ConfigMap、Secret、状态目录和管理路由。配置变更另行明确实施，不能假设修改 ConfigMap 会覆盖已持久化的后端注册表。

## 查询与回退

通过 SSH MCP 连接 `A3-gate`：

```bash
kubectl exec -n ai-gate deployment/aigate -- \
  /opt/aigate/aigate status --config /etc/aigate/config.json --json
kubectl get pods -n ai-gate -o wide
```

需要回退本次发布时，通过同一 SSH MCP 入口执行：

```bash
python3 /var/lib/ai-gate/deployments/direct-20260915/direct_release.py rollback \
  --root /var/lib/ai-gate/deployments/direct-20260915
```

回退使用 `rollback-template.json` 和 `rollback/aigate`，保留当前持久化状态，不恢复过期状态快照。模板出现不属于本次发布的并发修改时拒绝覆盖。回退同样会触发一次优雅停止和 Pod 重建，完成后需重新检查实际业务请求。本次验证了回退文件哈希及模板引用，未再次重启服务演练回退。

画像目录 `/var/lib/ai-gate/state/request-profile/`、慢请求原始输入 `/var/lib/ai-gate/state/slow-requests/` 继续保留。慢请求规则仍为流式请求结束后总耗时严格超过 300 秒时异步保存，默认 72 小时／10 GiB。

## 已退役的流程

已删除主控 `/root/aigate-deploy/`、旧节点版本目录、对应零副本 ReplicaSet、未使用的旧 ConfigMap，以及本地旧部署脚本/候选清单/systemd 模板/产物。主控的现有 SSH 别名是 `A3-master`；今后 A3 aigate 发布无需使用该主机。

本次清理清单及删除结果保存在新发布目录和本地 `direct-20260915/evidence/`。现有业务、监控、Router Service、画像 Secret、正式状态与独立回退副本继续保留。共享 `deploy/live-20260915/rollback.py` 仅保留 DCU 分支。
