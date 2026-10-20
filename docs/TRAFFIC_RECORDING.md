# 流量录制与回放

录制由 aigate-switch 执行，保存、调度和历史报告由 code-eval 管理，回放仅通过固定任务容器内的 aigate-replay-task 执行。NAS 结构、生命周期和进度协议见 [NAS 与平台任务执行](NAS_RECORDING.md)，部署身份见[部署记录](DEPLOYMENT.md)。

## 录制（aigate-switch）

`aigate-switch serve --record-root <目录>` 启用录制能力，`--record-max-bytes` 为总容量（默认 10 GiB）；不传 `--record-root` 时录制关闭，`/control/v3/status` 的 `recording` 为 `null`。启用后切流服务只写 `<record-root>/<gateway_id>/`，该目录必须为 0700 并由进程独占加锁。请求线程只做内存拷贝和入队，文件创建、写入和封段由录制 worker 完成，容量维护由每日独立 worker 执行；准入锁内不做磁盘 I/O，录制故障不影响转发和响应字节。

业务请求的采样发生在准入判断之前，因此入口拒收（503、409、405 方法错误、413、508）也会写记录。所有带非空 `X-Code-Eval-Token` 的请求均不录制，包括无效、过期和迟到的测试标记，避免评测输入混入业务回放。这个 header 本身不证明调用方身份；准入逻辑独立校验当前测试占用，无效或过期标记返回 409。签名有效的回放请求也不录制。正文从实际转发给 proxy 的字节旁路拷贝；`Authorization` 与平台内部 header 永不记录。

### 控制命令

录制命令走切流控制端口，沿用 `/control/v3/commands` 的认证、幂等序号、boot 与绑定版本校验，不改变准入、绑定和测试占用。

| 动作 | payload | 行为 |
| --- | --- | --- |
| `record-start` | `recording_id`、`start_at`、`end_at`（必填，UTC 整点或半点）、可选 `schedule_id`、`sample_one_in`（默认 1）、`max_bytes`（默认且不超过部署容量） | 回执先为 `running`，建好会话目录后为 `completed`。`start_at` 已过而 `end_at` 未到时立即开始，用于重启后补发剩余时段；`end_at` 已过返回 `window_ended` |
| `record-stop` | `recording_id` | 任何时刻可发。录制中的会话不再采样，在途请求录完后封尾；`scheduled` 会话直接取消。结束原因 `stopped` |
| `record-status` | `recording_id` | 回执的 `recording` 字段为会话摘要：状态、计数、各段覆盖范围与是否保留 |
| `record-pin` / `record-unpin` | `recording_id`、`segment` | 在已封尾的段写入或删除 `pinned` 标记；淘汰跳过保留段 |

一个切流服务同时只有一个录制中或待开始的会话；待开始的会话可排在一个录制中的会话之后，前提是其 `start_at` 不早于后者的 `end_at`，以便平台提前下发相邻窗口。冲突返回 `recording_active`，ID 已存在返回 `recording_exists`，保留段占满容量返回 `capacity_reserved`，未启用录制返回 `recording_disabled`。

会话结束原因：`completed`（窗口到期）、`stopped`、`retired`、`process_exit`、`max_bytes`、`capacity`、`disk_failed`、`end_unknown`（上一 boot 中断）。会话状态只在内存中，重启后没有活动会话，启动时把上一 boot 未结束的会话和未封尾的段标为 `end_unknown`，由平台决定是否补发剩余时段。

### 目录与文件

```
<record-root>/<gateway_id>/<recording_id>/
  session.json              参数、定时规则 ID、boot、状态、起止、结束原因、计数、段列表
  20261012T1800/            半小时段，按请求进入采样锁的时刻（UTC）归入
    segment.json            段终点已过且在途请求全部结束后封尾：实际覆盖起止、partial、计数、router/revision 列表
    index.jsonl             每条记录封尾时追加一行：序号、相对段起点的到达秒数、状态、结果、正文状态、目录
    pinned                  可选，保留标记
    r-<request_id>/
      request.json          客户端发来的原始正文字节
      response.body         从头保存完整响应（JSON 或 SSE 原始字节）
      timeline.jsonl        到达、上传完成、响应头、每次 Write/Flush、结束
      meta.json             入口、快照、header 白名单、长度与 SHA-256、采集状态、response_result
```

文件 0600、目录 0700，`meta.json`、`segment.json`、`session.json` 原子发布。采样时刻和封段判断共用录制器锁，避免段已封尾后又接受旧时间戳的请求。跨段的长请求仍归入采样开始时所在段，该段等它结束后封尾。中途终止和重启补发会产生只覆盖部分时间的段，`segment.json` 标 `partial` 并记录实际覆盖起止。

### 上限与降级

| 项 | 默认 |
| --- | --- |
| 待写内存预算 | 64 MiB |
| 队列 / 活动记录 | 1024 / 1024 |
| 单条时间线事件 | 8192，超出标 `timeline_truncated` |
| 总容量 | `--record-max-bytes`，每天 05:00 从最旧的已封尾、未保护段整段清理；白天超额等待维护 |
| 收尾 | 进程退出时最多 5 秒写完队列，未完成部分标 `end_unknown` |

请求与响应缓冲按实际分配容量计费，扩容时同时计入新旧缓冲。内存不足时停止对应正文的后续采集，记为 `truncated`；元数据都无法入队时计入 `dropped`；磁盘写失败结束所有会话（`disk_failed`）。保留丢弃与截断计数供排查；回放发送可用记录，不因此判定整次回放无效。磁盘状态发布失败时停止采样并保留会话重试，只有结束状态写入成功才移出活动列表。会话所有段淘汰后同步清理其目录和 session.json，避免元数据无限积累。

## 回放标记

回放执行器使用自身的 `AIGATE_REPLAY_MARK_KEY`（至少 32 字节）签名，请求携带 `X-Aigate-Replay: <run_id>/<seq>/<HMAC-SHA256>`。switch/proxy 接收端信任自身该变量与 `AIGATE_REPLAY_MARK_VERIFY_KEYS` JSON 字符串数组的并集，原始字节去重后最多 8 把，每把至少 32 字节；回放执行器不读取附加验证密钥。跨 gateway 回放时无需把所有签名端改成同一密钥，但接收端必须包含来源的签名密钥。完整格式和启动校验见[打包说明](../packaging/README.md)。签名有效时，switch 不录制该请求，proxy 在审计日志、画像事件、慢请求和流异常记录里标 `traffic=replay`，画像指标增加 `traffic="replay"` 标签以便监控排除；转发不变。签名无效或未配置密钥时按普通业务处理，不拒绝请求。目标身份校验独立保持。

## 固定任务回放

平台先校验 NAS 中封尾片段的路径、成员集合、长度和 SHA-256，固定方案的输入摘要、模式、每来源并发与执行镜像，然后为每个目标启动一个容器。Go 只读取固定任务描述和已验证输入，直接调用唯一的规划、发送、收尾和报告内核。任务无 HTTP 服务、跨目标启动屏障、时钟同步或来源服务租约。

开始前读取目标 proxy runtime，核对 Router、服务身份和配置摘要；每个请求继续携带预期 Router 和配置摘要。请求只允许改写 model，保持原输出上限，支持原速、每来源固定并发及最多两份录制叠加。正文总预算为 512 MiB。

每个目标独立停止和生成报告。停止后不再派发，已发送请求继续收尾；自动尾部截断和失败计数由同一执行器处理。持久化 execution.started 阻止重启后重复发压。NAS 保存逐请求结果及单份 v2 报告，平台报告查询统一读取数据库，不重新计算历史统计。

报告保留 input_digest、逐来源 source_inputs、完整窗口、部分结果及实际执行身份。历史网关发压仍标为 gateway，新容器任务记录 code-eval、节点和固定镜像。历史文件已转入相同 replay-runs 结构，旧常驻服务实现和控制入口已删除；旧发布证据从 Git 历史及部署记录查阅。
