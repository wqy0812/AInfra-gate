# DCU aigate 慢请求留存发布

本页为当次历史发布快照，`release.py` 的固定目录和“首次启用 slow_requests”前提仍只适用于这次发布。源码内的检查逻辑已兼容 legacy / streaming / metric-v2；使用修订工具建立新基线时需将 `deploy/profile_checks.py` 同时上传到脚本目录。新基线增加 `profile-stopped.json`，旧基线及回退继续使用远端原版工具，不能混用。公共校验与回归见 [部署说明](../../README.md)。

部署完成时间：**2026-09-15 17:54:09 +08:00**；新 PID 为 **3560260**，与 A3 使用相同二进制。生产留存与回放验收结果见下方证据。

最终检查时间：**18:02:38 +08:00**。`aigate.service` 为 active/running，`NRestarts=0`；检查时正在处理 1 条正常业务流式请求。

## 版本与配置

本次使用 A3 已验收的相同冻结源码二进制，SHA-256 为 `a1cd6006464b14a1199c582f69350a0971c015a4facccb338728251e27be9fd2`。

- 目标：dcu1 的 `aigate.service`，业务端口 10015。
- 版本目录：`/data4/aigate/releases/slow-requests-20260915`。
- 主配置：`/data4/aigate/config.json`，仅增加 `slow_requests`。
- 留存目录：`/data4/aigate/slow-requests`。
- 远端发布、回退与验收证据：`/data4/aigate/evidence/slow-requests-20260915`。
- 原版本：`live-20260915`，SHA-256 `6ddbcd5fd94b4242d543262915a31cf184d48d7457bb0e52e1681718e0d5a55b`，保留用于回退。

仅记录通过校验的流式聊天请求，处理结束后总耗时严格超过 300 秒才保存原始 JSON。默认保留 72 小时／10 GiB，后台队列最多 1024 条，等待及正在写入的正文合计最多 64 MiB。目录为 0700，文件为 0600。

每条记录使用 `request-<request_id>/request.json` 和 `meta.json`，不保存鉴权头或模型输出；原始请求体自身完整保留。正常完成、客户端取消、下游超时和流中断都会参与判断。运行时存储失败只报告诊断，不改变业务响应。

## 发布边界

发布基线为 `active_backend=dcu`、`route_version=3`、`a3 maintenance=true`、`dcu maintenance=false`，两个后端均 `drain_unknown=false`。本次保持路由、维护锁、后端注册表、凭据、HMAC 和 systemd 配置。

发布前先等待空闲窗口，但 DCU 连续数分钟保持 1～4 条流式请求，因此改用 `switch-graceful`：按服务现有的 61 分钟关闭等待配置停止接收新连接，等待已接入请求自然完成。确认 `clean_shutdown=true` 后保存最终状态和旧画像计数，原子替换配置与 `current` 链接，再启动并验证新进程；失败时配套恢复旧程序和配置，保留当前状态。没有停止推理容器或修改评测器。

DCU 旧版画像包含混合流式/非流式统计。此版本与 A3 一致，仅记录流式画像，非流式只保留独立请求数，指标带 `request_scope` 标签。新流式计数使用独立起点和 `counters-streaming.json`；原混合 `counters.json` 和历史事件保留，不把旧混合计数解释为流式历史。

已复用 A3 的完整竞态测试、冻结源码测试、`go vet` 和差异检查结果；部署前再次核对本地源码、上传二进制和冻结清单一致。没有重新编译不同的网关程序。此次不修改 A3 网关，连接别名为 `A3-master`。

切换校验通过：10 个运行中容器指纹、850 个原画像事件文件、旧混合计数检查点、凭据/HMAC、systemd 单元和 drop-in、后端注册表均保留。旧网关的 `clean_shutdown=true` 已确认；优雅更新脚本总耗时约 60.7 秒，其中包含等待已接入请求结束，不把此时长等同于精确端口不可用时长。

A3 只读复核发现独立的并发变更：其 Pod 于 17:49:14 +08:00 被替换，版本注解为 `direct-20260915`，二进制哈希仍与本次相同。此次 DCU 部署没有对 A3 执行变更，也不宣称 A3 在整个时间窗口内未变化；前后观察记录见 `evidence/a3-observation.json`。

## 线上验收

- HTTP 检查 **49/49**，协议预热 **9 步**通过。
- 保持正式阈值 300 秒，通过受控慢上传小请求验证总耗时筛选；实测 **301.693973434 秒**，205 字节原始输入逐字节一致。
- 保存请求 ID：`e54c4d9ad167cf9ea0b43a1c060a50d7`；SHA-256：`ba1fc7d39606f0fc7ea39e1dd2591a36855a6e8d039a5054434352012114f81c`；生成结果为 completed，错误分类为空。
- 原始文件直接经 curl 回放成功，新请求 ID 为 `e211e01716e676abe7a0779e9139a615`；短回放请求没有被留存。
- 留存目录及记录目录 0700，两个文件 0600；日志确认样例 `result=saved`，没有写入或清理失败。
- 画像写入错误和丢事件为 0。最终再次核对 10 个运行中容器、850 个旧画像事件、旧混合计数检查点、凭据/HMAC/systemd 和注册表保留。

300 秒验收使用受控慢上传，证明包含读取输入在内的网关总耗时筛选、完整落盘与回放；没有让模型持续生成 5 分钟，也未做并发压测或长上下文性能验收。

证据：[最终状态](evidence/final.json)、[留存与回放](evidence/slow-acceptance.json)、[保存元数据](evidence/saved-meta.json)、[HTTP 检查](evidence/http-verification.json)、[切换保留检查](evidence/switched.json)、[构建清单](evidence/source-manifest.json)。证据与源码归档位于 Git 排除目录；远端发布目录保留原版本、配置、状态、检查点和回退脚本，本次未执行生产回退。

## 查询与回放

通过 SSH MCP 连接 dcu1 后查询：

```bash
ls -l /data4/aigate/slow-requests
cat /data4/aigate/slow-requests/request-请求ID/meta.json
```

回放到 DCU 网关：

```bash
curl -f -N -H 'Content-Type: application/json' \
  --data-binary @/data4/aigate/slow-requests/request-请求ID/request.json \
  http://127.0.0.1:10015/v1/chat/completions
```

回放生成新的请求 ID，按当前后端和适配配置执行。仍在执行的请求不会提前落盘；异常退出可能丢失尚未写入的记录。队列满或存储故障会输出带请求 ID 的未保存原因。

## 回退

通过 SSH MCP 连接 dcu1，确认当前服务和发布状态后执行：

```bash
python3 /data4/aigate/evidence/slow-requests-20260915/release.py rollback
```

回退配套恢复旧二进制和主配置，保留当前路由、状态及已保存的请求输入。脚本拒绝覆盖并发修改后的配置或未知版本。单实例更新会短暂影响新连接，原有流按优雅退出规则完成。
