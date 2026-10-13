# 流异常诊断

平台独立“异常诊断”页面搜索功能服务保留的异常记录。当前拆分源码把观测归属固定到 Router；部署后开始新服务记录，不回填或重新标记入口历史。功能随本地 `settings.profile.enabled` 启用。

## 捕获口径

只捕获流式响应中的两类异常，阈值均为**严格大于 60 秒**：

- `output_stall`：已识别有效输出后，直到下一次有效输出之间累计等待上游响应体 Read 返回的时间。客户端写出时间不计入。心跳、空事件、usage 不重置；有效输出恢复、生成结束或异常终止关闭本次事件。首次有效输出前不捕获，无法解析 SSE 时标记观测未知并停止此类检测。
- `client_write_stall`：单轮 Write + Flush 持续时间。包括最终输出的写出，分别记录 Write 和 Flush 耗时。只能说明向当前连接对端写出受阻。经切流入口访问时，对端是切流服务；直接访问时，对端是直接客户端或其代理，不能据此认定最终客户端、用户程序或网络故障。

进程级检查器每秒检查，因此阻塞期间通常在超过阈值后的下一次检查触发；读写结束时补查。请求不必总耗时超过 300 秒。同一次异常只创建一次，恢复或结束更新记录。记录本身不改变转发、超时、重试或 Router 切换行为。

## 文本与记录

文本提取由异常诊断独立执行，通过请求上下文传递首尾片段，不依赖审计日志字段或截取函数。

公开摘要先按顺序拼接最后一条 `role=user` 消息的 text 分片；若存在大小写敏感、无属性的完整 `<user_query>` 外层配对，选择最后一组正文并去除两端 Unicode 空白，否则使用原消息。嵌套按深度配对，尾部残缺标签不覆盖此前完整配对，完整空标签产生空摘要。保存正文首尾各 10 个 Unicode 码点及总码点数。原始请求及请求模板保持原样。首尾片段分别匹配，不能通过拼接跨过中间省略内容命中关键词。

每个异常有独立 ID，关联请求 ID、观测服务进程、Router、模型及绑定版本；可选 `connection_peer` 记录实际入站对端 IP:port，旧记录没有该字段时显示未提供。历史字段名 `gateway_id` 保留，新记录值为功能服务 `service_id`。记录请求开始、异常起点、触发、恢复/结束时间。停顿记录读取等待、输出间隔、最后上游字节时间与接收字节数；写出记录环节、字节数及 Write/Flush 时长。请求 ID 和文本不放入 Prometheus 标签。

状态为 `ongoing`、`ended` 或 `unknown`。进程退出后没有结束证据的记录显示“结束未知”。写入字节数在 Write 返回前为未知。并非每个事件对应一个 token。

超过 300 秒的慢请求捕获保持独立，同一请求可以分别在慢请求和异常目录保存一份；删除规则不互相耦合。完整文件不进入摘要搜索。

## 存储与健康

使用部署的 `data-root/stream-incidents`，默认按小时分目录，文件权限 0600、目录 0700，原子发布/更新。每个功能服务的数据根目录固定归属一个 Router，入口切换不迁移或清理这些数据；异常目录独立于画像策略代际。默认保留 72 小时、256 MiB，每分钟清理一次，两个条件任一达到即清理最旧记录；清理间隔内可能短暂超过容量。进程间目录锁避免多个写入器竞争。

异步队列最多 1024 条，每条不超过 16 KiB。队列满或磁盘故障不阻塞业务，观测监听器 `/metrics` 暴露：

- `aigate_stream_incident_dropped_total`
- `aigate_stream_incident_write_errors_total`

初始化存储失败记录 `stream_incident_storage_unavailable`，查询返回 503；请求转发继续。修复存储条件后重启功能服务可重试初始化。查询显示本进程丢弃/失败计数，未完成的持久化记录不会被推断为成功。

## 完整异常现场

开启 profile 时，功能服务在 `data-root/stream-incidents/requests/.pending-<request_id>/` 异步暂存响应及时间线。首次异常后提升为 `request-<request_id>/`；请求正常结束且没有异常时删除临时记录。同一请求的多次异常共用四个文件：

| 文件 | 口径 |
| --- | --- |
| `request.json` | proxy 收到、适配前的完整请求 body 原始字节，不保存 HTTP headers |
| `response.sse` | Router 返回、完成 HTTP 内容解码后的原始字节，保留心跳、usage、错误和换行 |
| `timeline.jsonl` | schema_version=1、请求内序号、UTC 时刻与单调相对耗时；Read/Write/Flush、SSE 事件边界、异常及终止 |
| `meta.json` | schema_version=1、请求/服务/Router/boot、异常关联、文件长度与 SHA-256、同步边界、采集状态及业务结果 |

事件范围是解码后 SSE 的从零开始字节区间 `[start,end)`，并非 Unicode 字符数或 token 数。SSE data 事件从 1 编号；心跳和 usage 不推进最后有效输出位置。CRLF 的事件完成点遵循解析器消费 CR 时的边界，随后跳过的 LF 仍计入累计响应偏移。事件跨 Read 保持位置；解析未知时保留字节位置，事件位置明确未知。一次 Read 中恢复时记录首个有效恢复事件。Read 新字节只归于返回时刻，不倒算为等待期间连续接收。

Write 未返回时写入量未知，Flush 未返回时完成状态未知；成功写入 HTTP 接口不证明最终客户端已接收。网关自行补发的错误单独以 `origin=gateway` 记录生成字节和写出结果，不加入 `response.sse`。gzip 同时识别 Transport 自动解压及 proxy 手动解压。

采集状态为 `recording`、`complete`、`truncated`、`failed`、`end_unknown`，与 EOF、`[DONE]`、取消、超时和生成结果分别记录。`complete` 仅表示实际观测到的数据完整保存，不代表模型正常完成。记录接收/保存字节数、最后同步位置及首个缺失位置；请求文件缺失或部分写入不能宣称完整。录制中每秒尽力同步一次，触发和结束时安排同步，meta 不超前声明持久化；哈希仅在后台计算。

每个功能服务限制 payload 内存 64 MiB、活跃句柄 1024 个、payload 队列 1024 项；原始请求按持有容量计费，正在写入的片段仍计入预算。完整文件总额 10 GiB、保留 72 小时，包含临时目录、活动文件及 meta 临时副本。先回收已结束旧记录，不为新正常请求删除仍录制的现场。容量不足、慢盘、队列满或写入失败只停止该请求详细采集，保留连续前缀及缺口原因，业务、取消和原摘要检测继续。

请求线程不执行文件操作、哈希、扫描或等待存储确认，不增加上游 reader，不重放请求。HTTP 排空后全部观测收尾共享最多 5 秒；超时退出不等待阻塞磁盘，目录锁由实际 worker 完成或进程退出释放。重启保留有异常标记或可关联持久摘要的目录，未封尾记为 `end_unknown`；残缺 JSONL 尾行不当作完整事件。尚未落盘的字节和标记无法保证恢复。

平台可按需读取完整请求及已保存响应；时间线仍仅供服务器排查。原有两个 Prometheus 丢弃/写入失败计数包含完整采集故障。历史摘要不回填为完整文件。

## 查询 API

功能服务业务或独立观测监听器：`GET /profile/v1/stream-incidents`，无需认证；平台代理：`GET /api/request-profile/stream-incidents`，另带 `environment=dcu-pd|a3-vllm|xpu-pd`。监控与平台只需配置对应功能服务 URL。

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
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go test -mod=vendor -race . ./cmd/...
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go vet -mod=vendor . ./cmd/...
./scripts/build.sh
```

平台 API 测试位于 `backend/tests/python/test_request_profile_api.py`；浏览器 fixture 为 `frontend/tests/react_monitoring.cjs`，覆盖 Chrome 1280×720、iPhone 竖屏窄屏、连接对端字段及旧记录兼容。这些检查不能证明线上数据已产生或 Router 内部故障原因。

## 全文只读 API

`GET /profile/v1/stream-incidents/requests/{request_id}` 返回 `request_id`、原始 `meta`、`files.request` / `files.response`（`available`、`bytes`）和不透明 `snapshot`。只读取已关联异常并保留的目录，ID 必须是 32 位小写十六进制。功能服务业务及观测监听器均可使用，沿用观测接口的网络访问边界，不要求新凭据。

同一地址带 `part=request|response&snapshot=...&offset=0&limit=262144` 读取内容。返回 `part`、`offset`、`next_offset`、`total_bytes` 和 Base64 `data`；所有偏移和长度均以字节计，每块最多 256 KiB。请求边界取元数据已保存字节，录制中响应边界取已同步字节，其他状态取已保存字节（包括重启恢复后校验的文件）；一次查看固定这些边界，不包含后续追加内容。客户端需增量解码 UTF-8，读取到边界后主动刷新元数据才能查看新增输出。文件缺失与空文件通过 `available` 区分。

两个并发读取槽位独立于采集线程，读取不持有业务锁，响应禁止缓存。拒绝路径穿越、符号链接和非普通文件。参数错误返回 400；未留存/已清理/尚未发布返回 404 `capture_not_retained`；快照文件被替换或缩短返回 409；繁忙返回 429；存储不可用或损坏返回 503。旧版本未知路由的 404 与 `capture_not_retained` 区分，供平台提示服务升级。

此 API 不触发新采集、重放或回填；`complete` 仍只表示采集完整，不证明模型成功。录制、截断和异常终止状态以及首个缺口直接沿用元数据。测试使用临时目录的合成文件：`GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go test -mod=vendor -race -run 'TestEvidence|TestStreamCapture' .`。
