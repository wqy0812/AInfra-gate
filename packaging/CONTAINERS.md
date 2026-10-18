# PPU / A3 容器维护

2026-10-17 起，这两个环境的 aigate 使用 containerd namespace `ai-gateway`。实际版本、准入与验收边界见 [部署身份](../docs/DEPLOYMENT.md)。容器启动不代表已绑定或已恢复业务准入。

| 环境 | 容器名 | 生命周期 |
| --- | --- | --- |
| PPU | `ppu-aigate-switch`、`ppu-aigate-proxy`、`ppu-aigate-replay` | 定制 containerd 1.6.39 仅支持 `always`；维护停止需要同时禁用 policy 和目标运行状态 |
| A3 | `a3-aigate`、`a3-aigate-proxy`、`a3-aigate-replay` | `unless-stopped` |

两个环境均无需 aigate 进程托管 unit。A3 保留 `aigate-split-access.service` 安装原有 proxy 访问控制规则；系统 containerd 服务仍使用原开机启动机制。PPU 不新增防火墙规则。

## 状态、停止和启动

使用 [container-service.py](../scripts/container-service.py)，参数为 `<ppu|a3> <status|stop|start> <switch|proxy|replay>`。通过 SSH MCP 在对应目标执行；脚本不自行连接远程节点。

PPU 已安装副本：

```sh
python3 /bmcp_lvm_fs/aigate/releases/container-unification-20261017/container-service.py ppu status switch
```

A3 已安装副本：

```sh
python3 /var/lib/ai-gate/releases/container-unification-20261017/container-service.py a3 status switch
```

维护前先从平台关闭受影响入口准入并核对直连流量，确认控制操作、录制、回放和在途请求结束。按 switch、replay、proxy 顺序停止，按 proxy、switch、replay 顺序启动，再验收平台绑定及原准入意图。脚本在发现服务仍有占用或状态无法确认时拒绝停止。switch/proxy 停止上限 62 分钟，replay 为 60 秒；安全检查不替代平台维护。

PPU 不要直接使用普通 `nerdctl stop` 作为持久维护停止。现场验证表明，仅设置 `--restart=no` 仍可能被旧 restart 插件重新拉起；脚本同时将 `containerd.io/restart.status` 改为 `stopped`。启动后恢复 `always`。A3 使用原生 `unless-stopped`。临时无业务容器已验证异常退出恢复和人工停止保持；未重启宿主机或 containerd 验收。

## 端口与数据

PPU 使用 host 网络，仅接入 `127.0.0.1:8008` 的 Qwen3.6-Plus：

| 端口 | 用途 |
| --- | --- |
| 60010 | switch 业务 |
| 60011 | switch 控制 |
| 60012 | proxy 观测 |
| 60013 | proxy 业务与运行状态 |
| 60014 | replay 控制 |

60000 已被现有 `ahm-web` 的 CNI 映射占用（转发至容器 7001）；不能只看 `ss` 判断端口空闲。新部署同时核查监听、各 namespace 的容器映射、NAT 规则和实际 HTTP 响应。60010–60014 已加入持久化临时端口保留项 `/etc/sysctl.d/90-aigate-ports.conf`。

PPU 配置和数据在 `/bmcp_lvm_fs/aigate/{config,proxy,recordings,replay}`，发布证据在 `releases/container-unification-20261017`。配置及密钥受限，程序和配置只读挂载；镜像不含模型、PPU 设备或控制密钥。三个进程以 UID/GID 65532 运行。switch 为 2 CPU、512 MiB；proxy 为 2 CPU、4 GiB；replay 为 2 CPU、1 GiB、128 进程。

2026-10-18 的 PPU 回放中，proxy 的原 512 MiB 限制触发 5 次容器 OOM。按用户要求使用 `nerdctl -n ai-gateway update --memory 4g --memory-swap 4g ppu-aigate-proxy` 调整为 4 GiB；实时 cgroup 与 containerd 保存的 OCI Spec 均已核验，PID、镜像和其余配置保持不变。后续重建 proxy 也应使用此限制，不能照搬历史部署脚本中的 512 MiB。原失败报告保留，扩大上限后的相同录制回放另行记录，资源更新本身不代表性能验收通过。

A3 保留迁移前的端口、环境、程序摘要、资源限制和原数据路径。旧 namespace 中停止的容器仅作证据，禁止与新容器同时启动或共享写入数据目录。

PPU 的 proxy 沿用无入站认证语义；60012、60013 的可达范围由现有网络控制。控制接口另有独立密钥和来源 CIDR。2026-10-17 取消强制指定工具名的校验后，两个固定请求门禁及默认工具选择绑定校验均通过，60010 业务入口已开放。后续切换仍须满足当前门禁，不通过直连 proxy 绕过验收。
