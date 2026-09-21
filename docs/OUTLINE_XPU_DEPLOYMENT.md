核对日期：2026-09-21（北京时间）。本文记录当前 XPU 上 GLM-5.2 的 Prefill、Decode、Router 和 aigate Gateway 部署，以及网关介质、配置和操作方法。部署信息来自当日 SSH MCP 实查；协议验收来自当日兼容修复发布证据。Pod 名称、IP、revision 和计数属于快照，操作前应重新查询。

本文中的远程命令均应通过 **SSH MCP，target=xpu-1** 执行；不使用本地命令行 ssh 绕过。SSH MCP 不可用或连接失败时停止远程操作。本文整理过程未修改或重启运行服务。

## 1. 总体部署与访问入口

| 组件 | 节点与地址 | 工作负载 / Pod | 当前状态 |
| --- | --- | --- | --- |
| Prefill（P） | xpu-2，122.209.21.34 | ns-1 / ms-5cdb-1-prefill-0 | 2/2 Ready，两个容器均 0 重启 |
| Decode（D） | xpu-1，122.209.21.33 | ns-1 / ms-5cdb-1-decode-0 | 2/2 Ready，两个容器均 0 重启 |
| Router / Proxy | xpu-2；Pod IP 172.16.5.33 | ns-1 / ms-5cdb-1-proxy-0 | 2/2 Ready，两个容器均 0 重启 |
| Gateway | xpu-1，122.209.21.33 | ai-gate / Deployment aigate；当前 Pod aigate-cdb66878-bgk25 | 1/1 Ready，0 重启 |

两台 XPU 主机各有 8 张 P800 OAM，单卡 96 GiB。用户称 xpu-1 为 master，但 Kubernetes 中两台都是工作节点；控制平面在其他节点。模型为 `glm-5.2`，P/D 各使用 8 卡。

```text
客户端
  → http://122.209.21.33:30002/v1  [aigate Gateway]
  → ms-5cdb-1.ns-1.svc.cluster.local:8501
    Service ClusterIP 192.168.12.1:8501
  → Proxy Pod 172.16.5.33:8502    [sidecar-nginx]
  → 127.0.0.1:8501               [sglang::router]
      → Prefill 122.209.21.34:8501
      → Decode  122.209.21.33:8501
      P → D：Mooncake KV 传输
```

| 接口 | 地址 / 用途 |
| --- | --- |
| 客户端 Base URL | `http://122.209.21.33:30002/v1` |
| 模型发现 | `GET http://122.209.21.33:30002/v1/models` |
| 生成 | `POST http://122.209.21.33:30002/v1/chat/completions` |
| 网关存活 | `GET http://122.209.21.33:30002/health`；不等同于后端推理验收 |
| 画像 / 指标 | `http://122.209.21.33:18082`，独立 Bearer 凭据 |
| 网关管理 | 容器内 CLI + `/var/lib/aigate/control.sock`；没有管理网页或管理 TCP 端口 |
| Router Service 指标端口 | `ms-5cdb-1:9092`，对应 sidecar 的指标监听；本文未核实指标集合，不当作网关画像端口 |

当前业务入口未配置 `gateway_api_key_env`，不要求网关 Bearer 鉴权。Profile 凭据不能当作业务凭据。网络可达范围仍由现有网络策略决定。

不要把 `ms-5cdb` 当作 Router 专用 Service：它的 ClusterIP 为 `192.168.13.145`，Endpoints 同时包含 P、D、Proxy 的 8501。网关实际使用的是 **ms-5cdb-1**。Pod IP 可能随重建变化，配置应使用已核对的 Service DNS。

## 2. P / D / Router 的控制关系和介质

三个角色均由平台控制：

```text
Inference ns-1/ms-5cdb-1
  → LeaderWorkerSet ms-5cdb-1-prefill / decode / proxy
    → 同名 StatefulSet（各 replicas=1，RollingUpdate）
      → 同名 -0 Pod
```

不要把底层 StatefulSet 当作唯一配置来源；平台 Inference/LeaderWorkerSet 的协调可能覆盖底层修改。持久化调整应通过当前平台 Inference 配置入口完成，并核对生成的模板。本文未执行推理组件的重启、参数修改或镜像更新。

每个推理 Pod 包含 `main` 与 `sidecar-nginx`：

- main 镜像：`registry.tce.com/ti-platform/llm-infer-w4a8-xpu:sglang_p800_glm5_int8-2026-07-30-15-09-db12744-ti0811`。
- sidecar 镜像：`registry.tce.com/ti-ems-server/openresty:openresty_feature_private_3.12.0-patch01_ee09f77_260716`。
- main 启动包装：`/usr/local/bin/run_role`；内部启用 conda 环境 `python310_torch25_cuda` 和 xSGL 路径 `/workspace/baidu/xpu/xSGL/python`。
- Nginx 配置来源：ConfigMap `ns-1/ms-5cdb-1-cm` 的 `NGINX_CONF`，挂载到 sidecar `/etc/configmap/nginx.conf`。实查监听 8502 并转发 `http://127.0.0.1:8501`；另有 8503 HTTP/2 和 9092 监听。
- 模型通过 Unified CSI 挂载到 `/data/model`，相对共享存储路径为 `tione/ti-template-server/llm_internal/model/GLM-5.2-W4A8-taco-xpu/`，StorageClass 为 `csi-dfs-ti-platform-fs`。共享 NFS 为 `NAS-WQ2.SDC.CS.ICBC:/ai-cloud-nas`。
- `/opt/ml/pretrain_model` 挂载共享模型父目录；另有 codekit 挂载。P/D 的 `/dev/shm` 为 memory emptyDir、上限 512 GiB，Proxy 为 8 GiB。

### P/D 关键参数

| 参数 | Prefill | Decode |
| --- | --- | --- |
| 网络 | hostNetwork，122.209.21.34:8501 | hostNetwork，122.209.21.33:8501 |
| CPU / 内存 / 卡资源请求与上限 | 200 CPU、1 TiB、8 XPU、1 RDMA HCA | 220 CPU、1 TiB、8 XPU、1 RDMA HCA |
| served-model-name / model-path | glm-5.2 / /data/model | 同左 |
| quantization / KV dtype / page-size | w4a8_int4 / int8 / 64 | 同左 |
| context-length | 204800 | 204800 |
| TP / EP | 8 / 8 | 8 / 8 |
| dp-size | 未显式传入 | 8 |
| DP attention / DP LM head | 启用 / 启用 | 启用 / 启用 |
| max-running-requests | 16 | 64 |
| mem-fraction-static | 0.80 | 0.88 |
| chunked-prefill-size / max-prefill-tokens | 32768 / 32768 | 32768 / 32768 |
| attention / prefill / decode backend | nsa / klxdsa / klxdsa | 同左 |
| DeepEP | normal | low_latency |
| 缓存 | hierarchical cache；ratio=32；write_through；page_first_direct | disable-radix-cache |
| CUDA graph | disable-cuda-graph | max-bs=8；bs=1..8 |
| 额外调度与并行 | nsa prefill context parallel、cp-layer-split、schedule-policy=lpm | prefill-round-robin-balance、load-balance-method=shortest_queue |
| speculative | NEXTN，steps=3，topk=1，draft tokens=4 | 同左 |
| tool / reasoning parser | glm47 / glm45 | 同左 |
| model override | index_topk_freq=4 | 同左 |

Decode 明确传入 `--disaggregation-transfer-backend mooncake`；启动包装的 P/D 默认传输后端也是 Mooncake。这里描述的是 P/D KV 传输，不据此宣称已部署或验证 Mooncake Store 的 L3 缓存。

### Router 配置

Router 独立运行在 Proxy Pod，main 资源为 8 CPU / 16 GiB，无 XPU 资源请求，进程名 `sglang::router`。`run_role` 根据平台环境变量生成以下启动逻辑：

```text
python3 -m sglang_router.launch_router
  --pd-disaggregation
  --prefill http://ms-5cdb-1-prefill-0.ms-5cdb-1-prefill-0:8501
  --decode  http://ms-5cdb-1-decode-0.ms-5cdb-1-decode-0:8501
  --host 0.0.0.0 --port 8501
  --request-timeout-secs 100000
  [追加 ROUTER_EXTRA_PARAMS]
```

上述短 DNS 在 ns-1 的 Pod DNS 搜索域下使用。当前 `ROUTER_EXTRA_PARAMS` 为：

```text
--request-timeout-secs 100000
--prefill-policy cache_aware
--decode-policy round_robin
--cache-threshold 0.6
--balance-abs-threshold 3
--balance-rel-threshold 1.3
--max-tree-size 5664384
--eviction-interval-secs 60
--disable-circuit-breaker
--cb-timeout-duration-secs 5
```

Router 的 P/D 调度策略与 aigate 的单活动后端路由是不同层次。当前只有 1P+1D；网关没有轮询 A3/DCU/XPU。Router timeout 为 100000 秒，网关生成期限为 3600 秒，客户端也可能有自己的更短超时。

## 3. Gateway 部署、介质和目录

Gateway 为独立的 `ai-gate/aigate` Deployment，单副本、Recreate，hostNetwork，nodeSelector 固定 `kubernetes.io/hostname=122.209.21.33`。请求资源 100m CPU / 128 MiB，上限 2 CPU / 512 MiB。以 UID/GID 65532 运行，根文件系统只读、禁止提权、丢弃全部 capabilities。

容器使用现有 nginx 基础镜像，但 command 已覆盖为 Go 二进制；**该容器实际运行 aigate，不运行 nginx 来代理网关请求**。

```text
/opt/aigate/aigate serve
  --config /etc/aigate/config.json
  --listen 122.209.21.33:30002
  --shutdown-timeout 60m
```

终止宽限为 3660 秒，preStop 显式向 PID 1 发送 SIGTERM 并等待退出。保留此设置，确保优雅排空及累计指标落盘。DNS 为显式 `192.168.0.10`，搜索域包含 `ai-gate.svc.cluster.local`、`svc.cluster.local`、`cluster.local`，ndots=5。

### 介质清单（均在 xpu-1）

| 介质 | 路径 / 标识 | 用途 |
| --- | --- | --- |
| 当前二进制 | `/var/lib/ai-gate/releases/xpu-compat-20260921/aigate` | 正在运行的 JSON 兼容修复版，Linux amd64 静态 Go 二进制 |
| 当前发布副本和操作脚本 | `/var/lib/ai-gate/deployments/xpu-compat-20260921/` | aigate、compat_release.py、compat_verify.py、verify.py、source-manifest.json、部署前后快照及验收结果 |
| 旧二进制 | `/var/lib/ai-gate/releases/xpu-initial-20260921/aigate` | 原版本，保留用于回退；存在仅思考 JSON 和 metadata:null 的兼容问题 |
| 初次部署介质 | `/var/lib/ai-gate/deployments/xpu-initial-20260921/` | 镜像 tar、deploy.py、config.json、manifest.json、deployment-final.json 和初次验收记录 |
| 离线镜像包 | `/var/lib/ai-gate/deployments/xpu-initial-20260921/xpu-aigate-image-20260921.tar` | 约 55 MiB，导入 containerd 的 k8s.io namespace |
| 容器镜像 | `fydockerhub.test.sdc.cs.icbc/f-ucmp-submix/nginx@sha256:2468d48e476b6a079eb646e87620f96ce1818ac0c5b3a8450532cea64b3421f4` | 网关运行基础环境；二进制通过只读 hostPath 挂载 |

SHA-256：

```text
当前 aigate:
79169b92dbd746d7d60870adbdbf8bf2b8759061f670c3ba88533d050c4e7789
旧 aigate:
8e2c8536bc0c0e820b9c205ae0d4b95deceba31e8472b554e60eab7dbcb4b050
xpu-aigate-image-20260921.tar:
33174e78a15e67ec0a49958310fa7064215fad8bd183e52197c50cd4577a9999
```

本地源码为 `/Users/wqy/Downloads/AI/aigate`；本次脚本为 `deploy/xpu/compat_release.py`、`compat_verify.py` 和 `verify.py`。本地介质及测试证据在 `deploy/xpu/evidence/compat-20260921/`，首次镜像包在 `deploy/xpu/evidence/`。这些是操作者电脑路径，不是服务器路径，也不是 Outline 下载链接。本文登记介质位置和校验值，未向 Outline 上传二进制或镜像。

基线提交为 `fa0813d62704f335e0a206e7660acf590b3425c8`，用 Go 1.25.14 重建旧版已与线上哈希完全一致；当前版在此基线上修改 adapter.go 的两处兼容判断，另增加测试。不能只 checkout 基线提交就声称得到修复版。重建应使用包含此次修复的源码及 source-manifest.json 校验，执行：

```sh
go version  # 发布使用 go1.25.14
go test ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/aigate-linux-amd64 .
sha256sum bin/aigate-linux-amd64
```

源码树状态和构建元数据不同可能改变二进制哈希；记录新发布的哈希，不能把重新构建的不同产物冒认为上述已验收介质。

### 配置、状态与挂载

| 来源 | 容器内路径 | 说明 |
| --- | --- | --- |
| ConfigMap ai-gate/aigate-config 的 config.json | `/etc/aigate/config.json` | 主配置，改变全局参数后需重启；部署目录中的 config.json 只是历史快照 |
| hostPath 当前 releases 目录 | `/opt/aigate` | 只读二进制；升级采用新目录，避免覆盖正在运行的文件 |
| `/var/lib/ai-gate/state` | `/var/lib/aigate` | 持久状态；属主 65532:65532，目录权限 0700 |
| 状态目录中的 management-state.json | `/var/lib/aigate/management-state.json` | v2 后端注册、活动路由、维护锁和操作历史，不能手工编辑 |
| 状态目录中的 control.sock | `/var/lib/aigate/control.sock` | 0600 Unix socket，CLI 通过容器运行用户访问 |
| 状态目录中的 request-profile | `/var/lib/aigate/request-profile` | 画像事件、HMAC 密钥、累计检查点；保留整个目录 |
| 状态目录中的 slow-requests | `/var/lib/aigate/slow-requests` | 慢请求记录；按既有访问权限管理 |
| Secret ai-gate/aigate-profile，字段 key | 环境变量 AIGATE_PROFILE_KEY | 仅用于独立画像鉴权；本文不记录明文 |

已有 v2 状态时，**生效后端取自管理状态中的注册表，活动路由取自 active_backend**。修改主配置中的 backends 或 routing.backend 不会覆盖已有状态。主配置负责全局参数和首次初始化；用 `backends list` 核对实际后端。

## 4. 当前完整主配置

以下为核对时的 ConfigMap 内容，不含 Secret 值。唯一注册后端为 xpu，实际地址和 metadata 规则与本配置一致；active_backend=xpu、route_version=1。revision 会随操作变化，不应复制快照数字发起写操作。

```json
{
  "public_model_name": "glm-5.2",
  "timeout_seconds": 3600,
  "discovery_timeout_seconds": 10,
  "model_cache_seconds": 60,
  "discovery_retry_seconds": 5,
  "routing": {
    "mode": "single",
    "backend": "xpu"
  },
  "management": {
    "enabled": true,
    "state_file": "/var/lib/aigate/management-state.json",
    "socket": "/var/lib/aigate/control.sock",
    "warmup_rounds": 2,
    "warmup_step_seconds": 120,
    "warmup_timeout_seconds": 900,
    "warmup_max_tokens": 1024,
    "drain_timeout_seconds": 3660,
    "auto_rollback": false
  },
  "backends": {
    "xpu": {
      "adapter": "openai",
      "url": "http://ms-5cdb-1.ns-1.svc.cluster.local:8501/v1/chat/completions",
      "models_url": "http://ms-5cdb-1.ns-1.svc.cluster.local:8501/v1/models",
      "compatibility": {
        "metadata_values": "json_string",
        "drop_fields": []
      }
    }
  },
  "profile": {
    "enabled": true,
    "listen": "122.209.21.33:18082",
    "api_key_env": "AIGATE_PROFILE_KEY",
    "directory": "/var/lib/aigate/request-profile",
    "retention_hours": 72,
    "max_disk_bytes": 10737418240
  },
  "slow_requests": {
    "enabled": true,
    "threshold_seconds": 300,
    "directory": "/var/lib/aigate/slow-requests",
    "retention_hours": 72,
    "max_disk_bytes": 10737418240,
    "max_queue_bytes": 67108864
  }
}
```

业务使用 OpenAI chat completions 协议。XPU 的 `json_string` 模式把 metadata 对象内所有非字符串值紧凑序列化为字符串，已有字符串不重复编码；metadata 缺失/null 透传。顶层 metadata 数组、字符串仍拒绝。不能改成只处理 workspace 的模式，因为 XPU Router 对其他 metadata 字段值也要求字符串。

响应兼容修复允许 assistant 的 content=null/缺失且 reasoning_content 非空字符串的非流式响应返回 200，原样保留 reasoning_content、finish_reason、usage 等字段。思考阶段耗尽输出预算时，finish_reason=length 并不意味着已经生成最终答案；客户端应处理这一语义。

画像与慢请求各保留最多 72 小时、各最多 10 GiB；任一条件触发清理，实际历史可能少于 72 小时。慢请求阈值 300 秒。此处未确认 XPU 已接入 vmagent/VM/Perses 或测评平台画像页面，不能套用 A3/DCU 的监控接入结论。

## 5. 日常查看、预热和请求验证

### 查看服务与路由

```sh
kubectl -n ai-gate get deployment,pods -o wide
kubectl -n ns-1 get pods -o wide
kubectl -n ns-1 get inference ms-5cdb-1
kubectl -n ns-1 get leaderworkerset,statefulset
kubectl -n ns-1 get svc ms-5cdb-1
kubectl -n ns-1 get endpoints ms-5cdb-1

kubectl -n ai-gate exec deployment/aigate -- /opt/aigate/aigate status --config /etc/aigate/config.json --json
kubectl -n ai-gate exec deployment/aigate -- /opt/aigate/aigate backends list --config /etc/aigate/config.json
kubectl -n ai-gate exec deployment/aigate -- /opt/aigate/aigate check --config /etc/aigate/config.json
```

`check` 只读校验配置/已有注册状态，不联系 Router，也不证明推理可用。`status` 中的 business/streams/probes 是本网关看到的占用，不能覆盖绕过网关直连 Router 的请求。运行中 clean_shutdown=false 本身不代表故障。

### 预热：发合成请求，不切流

```sh
kubectl -n ai-gate exec deployment/aigate -- /opt/aigate/aigate warmup xpu --config /etc/aigate/config.json --json
```

预热包含模型发现和两轮 JSON、SSE、工具调用、工具结果续答；不执行外部工具。会产生真实推理负载，建议在需要验收时执行。它不是吞吐、长上下文或缓存命中验收。CLI 断开不取消已受理的后台操作，重新查看 status，避免重复发起。

### 客户端请求

```sh
curl -fsS http://122.209.21.33:30002/health
curl -fsS http://122.209.21.33:30002/v1/models
curl -fsS http://122.209.21.33:30002/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"glm-5.2","messages":[{"role":"user","content":"Reply only OK."}],"max_tokens":1024,"metadata":null}'

curl -N -fsS http://122.209.21.33:30002/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"glm-5.2","messages":[{"role":"user","content":"Reply only OK."}],"max_tokens":1024,"stream":true}'
```

SSE 要同时检查有效事件、结束原因和 `[DONE]`，不能只看 HTTP 200。兼容边界可把非流式请求的 max_tokens 改为 1，检查 content=null、非空 reasoning_content、finish_reason=length 与 HTTP 200。

### 查看日志与画像

```sh
kubectl -n ai-gate logs deployment/aigate --tail=100
kubectl -n ns-1 logs ms-5cdb-1-proxy-0 -c main --tail=100
kubectl -n ns-1 logs ms-5cdb-1-proxy-0 -c sidecar-nginx --tail=100
kubectl -n ns-1 logs ms-5cdb-1-prefill-0 -c main --tail=100
kubectl -n ns-1 logs ms-5cdb-1-decode-0 -c main --tail=100
```

在 xpu-1 上运行下面的 Python，只在内存读取 Secret 并请求画像状态，不打印凭据：

```python
import base64, json, subprocess, urllib.request
secret = json.loads(subprocess.check_output([
    'kubectl', '-n', 'ai-gate', 'get', 'secret', 'aigate-profile', '-o', 'json']))
key = base64.b64decode(secret['data']['key']).decode()
req = urllib.request.Request(
    'http://122.209.21.33:18082/profile/v1/status',
    headers={'Authorization': 'Bearer ' + key})
with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=10) as r:
    print(r.read().decode())
```

把路径改为 `/metrics` 可查看 Prometheus 指标。未鉴权返回 401 是预期行为。不要把 Secret 明文粘贴进 Outline、命令行参数或日志。

## 6. 重启、停止与配置修改

### 只重启 Gateway

先停止上游新增请求并查看业务/探测占用，再执行：

```sh
kubectl -n ai-gate rollout restart deployment/aigate
kubectl -n ai-gate rollout status deployment/aigate --timeout=3700s
```

单副本 Recreate 会有短暂入口不可用；长请求可能需要等待优雅退出。不要删掉 preStop、缩短 3660 秒终止期限或强杀旧 Pod。等待命令只是人工操作示例；自动化通过 SSH MCP 执行时应采用工具支持的后台任务/有界等待，不能因一次等待超时就强杀。

停止/恢复独立网关入口：

```sh
kubectl -n ai-gate scale deployment/aigate --replicas=0
# 待旧 Pod 完成优雅退出；P/D/Router 继续运行。
kubectl -n ai-gate scale deployment/aigate --replicas=1
kubectl -n ai-gate rollout status deployment/aigate --timeout=3700s
```

当前仅注册 xpu，没有可切换的备用后端。不能对当前活动端直接执行 maintenance，也不能把停止网关理解为停止所有 Router 入口。P/D/Router 维护必须另外确认直连流量和平台控制关系。

### 修改全局配置

1. 备份当前 ConfigMap、Deployment 和受控状态目录；不要把凭据或 HMAC 密钥写入文档。
2. 从 ConfigMap 导出真实配置，编辑候选文件。只改所需全局项；保留全部绝对路径和现有状态文件。
3. 将候选文件暂放到网关可写的状态挂载路径，用当前二进制 check；通过后才发布 ConfigMap。
4. 重启 Gateway，核对状态、路由及协议，失败恢复原 ConfigMap 并重启。

```sh
kubectl -n ai-gate get configmap aigate-config -o jsonpath='{.data.config\.json}' > /tmp/xpu-config-before.json
cp /tmp/xpu-config-before.json /tmp/xpu-config-candidate.json
# 编辑 /tmp/xpu-config-candidate.json 后：
kubectl -n ai-gate exec -i deployment/aigate -- sh -c 'cat > /var/lib/aigate/config-candidate.json && /opt/aigate/aigate check --config /var/lib/aigate/config-candidate.json' < /tmp/xpu-config-candidate.json
# check 成功才继续：
kubectl -n ai-gate create configmap aigate-config --from-file=config.json=/tmp/xpu-config-candidate.json --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ai-gate rollout restart deployment/aigate
kubectl -n ai-gate rollout status deployment/aigate --timeout=3700s
```

`/tmp` 备份应使用每次操作独立名称，以上名称只作一次操作示例。ConfigMap 更新不等于进程热加载。修改后端地址不要沿用此流程只改主配置的 backends：已有 v2 状态会继续使用注册表。

### 后端地址/兼容参数与切流

日常只需 `backends list`、`status`、`warmup xpu`。未来新增备用或替代后端时，使用 CLI 注册 → 预热 → 切流，而非直接编辑状态文件。当前接流端不能在线 edit；必须先切到已验证的其他注册后端，再编辑原端。

```text
backends create REQUEST.json --config /etc/aigate/config.json --json
warmup BACKEND --config /etc/aigate/config.json --json
switch single BACKEND --config /etc/aigate/config.json --json
backends edit OLD_BACKEND REQUEST.json --config /etc/aigate/config.json --json
```

上述是容器内 `/opt/aigate/aigate` 的子命令，REQUEST.json 也必须在容器可读位置。写入请求需唯一 operation_id 和刚从 backends list 读取的 expected_revision；不要复用本文的 revision 快照。具体请求结构见 [aigate 网关操作手册](http://100.127.166.46:3000/doc/aigate-Gc1Fh569kY)。XPU 的兼容规则使用 json_string，不能照抄 DCU 的 workspace-only 配置。

## 7. 升级、回退和重建

### 后续升级

1. 核对当前二进制哈希、Deployment 的 binary hostPath、主配置与生效注册表，保存本次升级的独立快照。
2. 本地测试并构建 Linux amd64 介质，通过 SSH MCP 的文件传输能力上传；使用新的 releases/版本号 目录，校验哈希，设置二进制可执行。新增服务器目录应记录在变更结果中。
3. 停止新增流量，确认占用；保留现有 hostNetwork、节点固定、Secret 引用、状态挂载、preStop 和终止期限。
4. 只调整 Deployment 中 binary volume 的 hostPath 与版本/哈希 annotation，等待 Recreate 完成。
5. 对照升级前后的路由、后端注册、操作历史、累计指标以及 P/D/Router UID/重启数，执行短协议验收。
6. 失败则恢复上一次二进制挂载及标识；不能直接把历史 management-state.json 覆盖回去而丢失后续注册和路由变更。

`compat_release.py` 是 **本次发布专用脚本**：写死了原版 SHA、发布目录，并拒绝覆盖 deployment-before.json。不能原样重复 prepare/upgrade 用于未来版本。后续版本需新的目录、基线哈希和快照，不要删除旧快照来绕过保护。

### 回退本次兼容修复

确认当前仍是本次修复版、并处于维护窗口后，通过 SSH MCP 执行：

```sh
cd /var/lib/ai-gate/deployments/xpu-compat-20260921
python3 compat_release.py rollback
kubectl -n ai-gate rollout status deployment/aigate --timeout=3700s
kubectl -n ai-gate exec deployment/aigate -- /opt/aigate/aigate status --config /etc/aigate/config.json --json
```

脚本恢复保存的旧 volumes 和版本 annotation，保留状态文件。本次升级只有二进制挂载/版本标识变化；若以后其他 volume 也发生变化，先比较当前清单，不能盲用此历史脚本覆盖后续改动。回退后仅思考 JSON 和 metadata:null 问题会重新出现。

### 离线恢复与首次部署边界

导入已有基础镜像的命令：

```sh
ctr -n k8s.io images import /var/lib/ai-gate/deployments/xpu-initial-20260921/xpu-aigate-image-20260921.tar
```

这只导入镜像，不会创建完整网关。首次部署还需要：正确版本的二进制、节点/DNS/端口、ConfigMap、Profile Secret、UID/GID 65532 可写且权限 0700 的状态目录、Deployment 及只读二进制挂载。

初次 `deploy.py` 仅适用于不存在 ai-gate Namespace、使用独立空状态目录的初始化；它包含旧版二进制哈希，不能重复运行来升级现有实例。当前配置与 Deployment 以 Kubernetes 中的对象为准，历史 manifest.json 不能直接当作当前版本重建清单。恢复现有实例应使用受控备份的当前配置/Deployment/Secret 和整个状态目录；迁移节点必须同时迁移 hostPath 数据并调整节点固定和监听 IP。

## 8. 验收记录与排障

2026-09-21 兼容修复验收：

- Go 全量测试通过，367 个测试项（含子测试）。
- 12 项 Router/网关对照检查通过：仅思考 JSON/SSE、metadata=null、混合类型和非法顶层 metadata。
- 两轮普通 JSON/SSE、工具调用及工具结果续答通过。
- 升级后 199 项累计指标未丢失；route_version=1、active_backend=xpu；后端注册和预热历史保留。
- P/D/Router 的 Pod UID 与重启次数未变；A3/DCU 网关未升级。
- 本记录为短协议验收，不包含长上下文、持续大并发、吞吐、缓存淘汰或命中率验证；未专门制造超过 300 秒的慢请求。

| 现象 | 排查顺序 |
| --- | --- |
| 网关 30002 不通 | Deployment/Pod 是否 Ready、nodeSelector、监听 IP、端口占用及网关日志 |
| /health 正常但生成失败 | /v1/models、backends list、生效 Service 地址及 Endpoints；再看 nginx、Router、D/P 日志 |
| 直连 Router metadata 报 expected a string | 核对请求类型；通过网关时保留 json_string；不要据此改 Router |
| 仅思考 JSON 被网关判 502 | 核对运行 binary hostPath/哈希是否是兼容修复版，再检查 role/content/reasoning_content 类型 |
| Router / Decode 返回 400 | 按 request_id 核对具体参数与 Decode 错误；不能把所有 400 都归为 metadata 问题 |
| SSE 200 后断流或报错 | 检查最后事件、finish_reason、[DONE]、网关 outcome 及 Router/P/D 错误 |
| Profile 401 | 检查独立 Secret 引用；业务健康与画像鉴权分别验证 |
| 修改主配置后后端地址未变 | 已有 v2 注册状态优先；查 backends list，使用管理 CLI 更新 |
| 重启后累计指标丢失 | 检查 preStop/SIGTERM、退出时间、状态目录持久化及权限；异常强杀不能保证尾部计数持久化 |

证据位于 xpu-1 的 `/var/lib/ai-gate/deployments/xpu-compat-20260921/`：deployment-before/after.json、config-before.json、status-before/after.json、metrics-before/after.txt、compat-verification.json、http-verification.json、persistence-check.json。重跑 verify.py 会发合成请求；compat_verify.py 会写固定名称的结果文件，复核前应保存历史证据或改用新的输出位置。
