# 实时等待与流停顿监控发布（2026-09-15）

本记录描述当时两套网关上线 `live-20260915` 的历史验收；A3 当前部署见 [直连部署说明](../a3/README.md)。历史版本二进制 SHA-256：`6ddbcd5fd94b4242d543262915a31cf184d48d7457bb0e52e1681718e0d5a55b`。基于 e435f42 加本次工作区修改，未创建提交；冻结源码清单见 source-manifest.json。

`source-manifest.json` 和原始验收输出保留本地并从 Git 排除。修订后的 `accept.py` 依赖同目录上传的 `deploy/profile_checks.py`，兼容旧指标及 metric-v2；脚本本地校验见 [部署说明](../README.md)。以下线上结果仍为历史记录。

用户实施中明确要求立即更新正式网关、先上线再验证，因此跳过候选部署。本地完整 Go 回归、竞态检查、go vet 和隔离 VM 测试已先通过；现场无在途时逐套优雅更新正式实例。

## 验收

- DCU：`/data4/aigate/releases/live-20260915`；active_backend=dcu，route_version=3。
- A3：worker-131 `/var/lib/ai-gate/releases/live-20260915`；active_backend=dcu，route_version=2。
- 两端保持 a3 maintenance=true、dcu maintenance=false；注册表、配置、凭据/HMAC、画像历史与累计计数起点保留。
- 正式入口各 HTTP 49/49、协议验证 9 步通过；真实 HTTP 检查期间分别采到 18、37 份在途快照。
- DCU 核对 18 条、A3 核对 25 条原有计数序列未倒退；画像丢事件和写入错误为零。DCU1 容器及 A3 模型 Pod 指纹与升级基线一致。
- gateway-generation 看板共 17 项，新增 11 项实时诊断，保留原六项图表。42 个隔离 VM 场景、126 次查询及 96 组生产 VM/代理查询对照通过。

A3 表示网关位置；本次历史验收时转发到 DCU。同一请求可能经过两层网关，不能相加为全局请求量。此验收覆盖短请求协议与监控，不代表长期吞吐或模型性能验收。

## 指标接口

新增 Gauge 共用 backend 标签；environment/instance/node/service 由采集端注入。未路由为 unrouted，最多登记 64 个实际后端，超出归 overflow。经原鉴权 `/metrics`、5 秒 vmagent 采集进入 VM。

| 指标（前缀 `aigate_`） | 口径 |
| --- | --- |
| streams_waiting_first_output | 已解析为流式且仍等待有效增量的请求数 |
| stream_first_output_wait_max_seconds | 上述请求从网关到达起的最大等待时间 |
| stream_idle_requests{threshold_seconds} | 有效输出后无新内容 ≥5/15/30/60 秒的流数，各档累计包含 |
| stream_idle_max_seconds | 最长无新内容间隔 |
| inflight_requests_by_stage{stage} | 流式及非流式各阶段在途数 |
| inflight_oldest_age_seconds | 每个后端最老请求年龄 |
| inflight_oldest_stage{stage} | 最老请求当前阶段为 1，其余为 0；空闲全部为 0 |
| stream_observation_unknown | 解析异常或超出观察上限的在途流 |
| live_backend_groups | 内部完整性指标，防止后端系列消失后只汇总剩余部分 |

有效输出沿用正文、推理、拒绝及工具增量语义。心跳、空增量、usage、结束标记不重置停顿计时；完整结束或明确错误退出等待/停顿分类。未知流仍计入在途年龄和阶段。观察点为网关收到下游内容，不代表客户端实际接收确认。

历史版本包含 receiving_request 阶段；当前源码在解析成功后建立画像，阶段为 routing_discovery、preparing_upstream、waiting_upstream_headers、waiting_first_output、streaming、reading_response、writing_client、finishing_stream，共 8 个。画像建立后的年龄包含输入读取时间，但读取完成前没有画像在途条目。内存快照和单调时间在请求结束前更新；实时状态不持久化，不改变超时、转发或重试行为。

## 证据与回退

- DCU：`/data4/aigate/evidence/live-20260915`，保留配置、原版本路径、干净退出状态和计数备份。
- A3：旧主控发布目录及其回退入口已于直连部署清理，当前发布与回退使用 [A3 直连流程](../a3/README.md)。
- 看板：test4 `/data2/monitoring/perses/evidence/gateway-live-20260915`，保留旧/新定义与查询验证。

本目录回退脚本仅用于 DCU：通过 SSH MCP 连接 dcu1 执行 `python3 rollback.py dcu`，先核对当前版本适用范围。脚本要求无在途，保留当前路由、注册表和画像；回退后须重做实际请求验证。未执行生产回退演练。

看板回退用 dashboard-before.json 的 spec 更新同名看板并读回；不删除 VM 历史。后续定向更新使用 monitoring/perses/publish_gateway_live.py，按固定 ID 合并实时区域，保留其他图表。
