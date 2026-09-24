# 流异常诊断

平台独立“异常诊断”页面搜索网关保留的异常记录。部署新版本后开始记录，不回填历史。功能随 `profile.enabled` 启用，不增加运行配置项。

## 捕获口径

只捕获流式响应中的两类异常，阈值均为**严格大于 60 秒**：

- `output_stall`：已识别有效输出后，直到下一次有效输出之间累计等待上游响应体 Read 返回的时间。客户端写出时间不计入。心跳、空事件、usage 不重置；有效输出恢复、生成结束或异常终止关闭本次事件。首次有效输出前不捕获，无法解析 SSE 时标记观测未知并停止此类检测。
- `client_write_stall`：单轮 Write + Flush 持续时间。包括最终输出的写出，分别记录 Write 和 Flush 耗时。只能说明向客户端方向写出受阻，不能确定是用户程序、代理或网络故障。

进程级检查器每秒检查，因此阻塞期间通常在超过阈值后的下一次检查触发；读写结束时补查。请求不必总耗时超过 300 秒。同一次异常只创建一次，恢复或结束更新记录。记录本身不改变转发、超时、重试或 Router 切换行为。

## 文本与记录

文本提取由异常诊断独立执行，通过请求上下文传递首尾片段，不依赖审计日志字段或截取函数。

仅保存原始请求最后一条 `role=user` 消息文本的前 10 个和后 10 个 Unicode 码点，以及原文本长度。多模态消息只提取 text 部分；不保存中间正文、其他消息、模型回复、图片或凭据。首尾片段分别匹配，不能通过拼接跨过中间省略内容命中关键词。

每个异常有独立 ID，关联请求 ID、网关进程、Router、模型及绑定版本；记录请求开始、异常起点、触发、恢复/结束时间。停顿记录读取等待、输出间隔、最后上游字节时间与接收字节数；写出记录环节、字节数及 Write/Flush 时长。请求 ID 和文本不放入 Prometheus 标签。

状态为 `ongoing`、`ended` 或 `unknown`。进程退出后没有结束证据的记录显示“结束未知”。写入字节数在 Write 返回前为未知。并非每个事件对应一个 token。

现有超过 300 秒的慢请求捕获保持原样：旧慢请求功能仍可能另存完整 `request.json`，但这些文件不进入本功能搜索。

## 存储与健康

使用部署的 `data-root/stream-incidents`，默认按小时分目录，文件权限 0600、目录 0700，原子发布/更新。共享存储跨 Router 切换保留，独立于画像策略代际。默认保留 72 小时、256 MiB，每分钟清理一次，两个条件任一达到即清理最旧记录；清理间隔内可能短暂超过容量。进程间目录锁避免多个写入器竞争。

异步队列最多 1024 条，每条不超过 16 KiB。队列满或磁盘故障不阻塞业务，观测监听器 `/metrics` 暴露：

- `aigate_stream_incident_dropped_total`
- `aigate_stream_incident_write_errors_total`

初始化存储失败记录 `stream_incident_storage_unavailable`，查询返回 503；请求转发继续。新运行快照准备时可重试初始化。查询显示本进程丢弃/失败计数，未完成的持久化记录不会被推断为成功。

## 查询 API

网关观测监听器：`GET /profile/v1/stream-incidents`，使用当前画像 Bearer 密钥；平台代理：`GET /api/request-profile/stream-incidents`，另带 `environment=dcu-pd|a3-vllm|xpu-pd`，浏览器不获取网关凭据。

参数：

| 参数 | 口径 |
|---|---|
| `start`, `end` | UTC epoch 秒，按触发时间 `[start,end)` 匹配；平台接受最近 30 天内的时间，实际记录受 72 小时及磁盘容量限制 |
| `keyword` | 可选，最多 256 个码点；首尾任一片段的字面子串，英文忽略大小写，不支持正则 |
| `limit` | 默认 50，范围 1～100 |
| `cursor` | 返回的不透明分页游标，必须与原关键词和时间范围一起使用 |

响应含 `records`、`next_cursor`、`retained_since`、`retention_hours`、`window_partially_expired`、`incomplete`、`dropped_total`、`write_errors_total`。结果按触发时间倒序；页间保持固定查询时间范围。`retained_since` 是时间保留策略边界，容量淘汰可能使更近记录也已删除。

同时最多两次查询，忙时 429；每次扫描限时 5 秒，超时或存储不可用返回 503，不将未扫描完的结果作为完整结果。损坏记录跳过且置 `incomplete=true`。活跃异常可在异步落盘前查询到。

## 本地验证

```sh
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go test -mod=vendor -race ./...
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go vet -mod=vendor ./...
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go build -mod=vendor -o bin/aigate .
```

平台 API 测试位于 `backend/tests/python/test_request_profile_api.py`；1080p 本地 fixture 测试为 `frontend/tests/browser_stream_incidents.cjs`，支持 `BROWSER_EXECUTABLE` 和 `BROWSER_EVIDENCE`。这些检查不能证明线上数据已产生或 Router 内部故障原因。
