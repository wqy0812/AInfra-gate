# 流量录制与回放

本文描述当前源码行为，尚未部署；线上版本另行核验。录制与回放只由 code eval 平台控制，主机上没有录制或回放的命令行入口。一套平台管理多个 aigate。回放负责发送负载，操作者查看中间稳定时段的监控；头尾完整性不作为整次回放的有效性门槛。

## 录制（aigate-switch）

`aigate-switch serve --record-root <目录>` 启用录制能力，`--record-max-bytes` 为总容量（默认 50 GiB）；不传 `--record-root` 时录制关闭，`/control/v3/status` 的 `recording` 为 `null`。启用后切流服务只写 `<record-root>/<gateway_id>/`，该目录必须为 0700 并由进程独占加锁。请求线程只做内存拷贝和入队，文件创建、写入、封段、容量淘汰全部由一个 worker 完成；准入锁内不做磁盘 I/O，录制故障不影响转发和响应字节。

业务请求的采样发生在准入判断之前，因此入口拒收（503、409、405 方法错误、413、508）也会写记录。所有带非空 `X-Code-Eval-Token` 的请求均不录制，包括无效、过期和迟到的测试标记，避免评测输入混入业务回放。这个 header 本身不证明调用方身份；准入逻辑独立校验当前测试占用，无效或过期标记返回 409。签名有效的回放请求也不录制。正文从实际转发给 proxy 的字节旁路拷贝；`Authorization` 与平台内部 header 永不记录。

### 控制命令

录制命令走切流控制端口，沿用 `/control/v3/commands` 的认证、幂等序号、boot 与绑定版本校验，不改变准入、绑定和测试占用。

| 动作 | payload | 行为 |
| --- | --- | --- |
| `record-start` | `recording_id`、`start_at`、`end_at`（必填，UTC 整点或半点）、可选 `schedule_id`、`sample_one_in`（默认 1）、`response`（`summary`/`full`，默认 `summary`）、`max_bytes`（默认且不超过部署容量） | 回执先为 `running`，建好会话目录后为 `completed`。`start_at` 已过而 `end_at` 未到时立即开始，用于重启后补发剩余时段；`end_at` 已过返回 `window_ended` |
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
      response.tail         响应最后 8 KiB（response=full 时为 response.sse 全量）
      timeline.jsonl        到达、上传完成、响应头、每次 Write/Flush、结束
      meta.json             入口、快照、header 白名单、长度与 SHA-256、采集状态
```

文件 0600、目录 0700，`meta.json`、`segment.json`、`session.json` 原子发布。采样时刻和封段判断共用录制器锁，避免段已封尾后又接受旧时间戳的请求。跨段的长请求仍归入采样开始时所在段，该段等它结束后封尾。中途终止和重启补发会产生只覆盖部分时间的段，`segment.json` 标 `partial` 并记录实际覆盖起止。

### 上限与降级

| 项 | 默认 |
| --- | --- |
| 待写内存预算 | 64 MiB |
| 队列 / 活动记录 | 1024 / 1024 |
| 单条时间线事件 | 8192，超出标 `timeline_truncated` |
| 总容量 | `--record-max-bytes`，从最旧的已封尾、未保留段整段淘汰；当前段不删，只剩当前段仍超限则结束会话（`capacity`） |
| 收尾 | 进程退出时最多 5 秒写完队列，未完成部分标 `end_unknown` |

内存不足时只停止该请求的正文或全量响应采集，记为 `truncated`；元数据都无法入队时计入 `dropped`；磁盘写失败结束所有会话（`disk_failed`）。保留丢弃与截断计数供排查；回放发送可用记录，不因此判定整次回放无效。磁盘状态发布失败时停止采样并保留会话重试，只有结束状态写入成功才移出活动列表。会话所有段淘汰后同步清理其目录和 session.json，避免元数据无限积累。

## 回放标记

`AIGATE_REPLAY_MARK_KEY`（至少 32 字节）配置在 switch、proxy 和回放服务。回放请求携带 `X-Aigate-Replay: <run_id>/<seq>/<HMAC-SHA256>`。签名有效时，switch 不录制该请求，proxy 在审计日志、画像事件、慢请求和流异常记录里标 `traffic=replay`，画像指标增加 `traffic="replay"` 标签以便监控排除；转发不变。签名无效或未配置密钥时按普通业务处理，不拒绝请求。

## 回放（aigate-replay）

`aigate-replay serve` 常驻录制主机，与 switch 使用同一系统账号以读取录制文件。它是回放的唯一入口，调用方只有平台 engine：

- 控制监听默认 `127.0.0.1:18085`，要求 `Authorization: Bearer <AIGATE_REPLAY_CONTROL_KEY>`（至少 32 字节，独立于切流密钥）和实际 TCP 来源 `--allowed-cidrs`。
- `--targets` 指向本地 JSON 文件 `{"targets": {"r1": "http://127.0.0.1:18084"}}`，控制请求只能按名字选择目标，不能传任意地址。名字必须等于该功能服务的 `router_id`，平台按这个名字锁定 Router。开始前读取目标 `/runtime/v1/status`，只接受 `service=aigate-proxy`（否则 `target_not_proxy`），从而拒绝把回放打到切流业务入口；`router_id` 与名字不符时返回 `target_router_mismatch`。目标的 Router 与配置摘要写入运行状态，之后每个回放请求都带 `X-Aigate-Expected-Router` 和 `X-Aigate-Expected-Config`，目标中途换配置或换 Router 时功能服务返回 409，并计入失败，便于发现目标变更。
- `--record-root` 为切流录制根目录（只读），`--data-root` 保存每次 run 的 `request.json`、`state.json`、`plan.json`、`plan.jsonl`，最多保留 200 个 run：每次开始新 run 前删除最旧的已结束 run，启动时同样清理。重启时把进行中的 run 标为 `end_unknown`。

| 接口 | 说明 |
| --- | --- |
| `GET /control/replay/v1/status` | 服务、boot、目标名、当前 run |
| `GET /control/replay/v1/recordings?gateway_id=` | 列出录制会话及各段元数据（覆盖范围、partial、计数、是否保留），不含正文 |
| `POST /control/replay/v1/runs` | 按 `run_id` 幂等；同时只跑一个 run，否则 `run_active` |
| `GET /control/replay/v1/runs/{id}` | 状态、进度、起止时间与最近错误 |
| `POST /control/replay/v1/runs/{id}/stop` | 停止新发送，已发请求继续收尾，状态 `stopped`；尚未开始的 ID 先持久化 `start_cancelled=true`，阻止迟到的启动请求（同 run 保留期限） |

请求字段见 `openapi.json` 的 `ReplayRunRequest`：`segments` 只能选已封尾的段，不再区分预热和测量段；默认只回放 `/v1/chat/completions` 的 2xx、正文完整、采集完整的请求，排除入口拒收；`timed` 按录制到达时刻原速发送，目标变慢不降低到达率，触及 `max_inflight` 只记录发送延误；`concurrency` 按计划顺序固定并发。不提供倍速、多份叠加或阶梯加压。正文改写只有 `set_model`、`set_max_tokens`、`force_stream` 和 `fix_output`（把 `max_tokens`、`min_tokens` 设为录制时的 completion tokens 并加 `ignore_eos`）；任一选中请求缺少有效的正整数 completion tokens 时，`fix_output` 在计划阶段失败为 `fix_output_usage_unknown`，不会发送部分改写的负载。改写后计划的 `original_bytes=false`。不改写时发送录制时的原始字节，发送前核对 SHA-256。

回放只维护计划数、发送数、完成数、失败数、在途数、运行时长、起止时刻、最近错误及最大发送延误。不解析响应 usage、TTFT 或输出文本，不生成分位数报告、录制基线对照和整次有效性判定；性能分析交给监控。停止确认和发送前的领取动作共用锁，已领取的请求继续收尾，其他请求不再发送。

各发送器共享 512 MiB 的正文预算，按源正文大小的 6 倍加 1 MiB 预留读取、改写和 JSON 临时空间；等待预算可以被停止打断。正文不超过录制器 64 MiB 内存上限，读取前验证长度，发送前核对哈希；响应直接流式排空，不在内存里累计。计划只在启用等输出时读取响应最后 8 KiB 的 usage。
