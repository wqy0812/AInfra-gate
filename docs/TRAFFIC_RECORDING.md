# 流量录制与回放

本文描述当前源码行为；2026-10-16 的部署身份、验收范围及 A3 保留的维护状态见[部署记录](DEPLOYMENT.md)。录制与回放只由 code eval 平台控制，主机上没有录制或回放的命令行入口。一套平台管理多个 aigate。回放发送负载并生成客户端性能报告；服务端指标由监控观察。报告保留完整窗口和部分结果标记。

2026-10-16 05:23（北京时间）已协调发布默认完整录制、按原请求回放及平台自动填写模型；线上身份及真实负载未执行的验收边界以部署记录为准。

## 录制（aigate-switch）

`aigate-switch serve --record-root <目录>` 启用录制能力，`--record-max-bytes` 为总容量（默认 50 GiB）；不传 `--record-root` 时录制关闭，`/control/v3/status` 的 `recording` 为 `null`。启用后切流服务只写 `<record-root>/<gateway_id>/`，该目录必须为 0700 并由进程独占加锁。请求线程只做内存拷贝和入队，文件创建、写入、封段、容量淘汰全部由一个 worker 完成；准入锁内不做磁盘 I/O，录制故障不影响转发和响应字节。

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
| 总容量 | `--record-max-bytes`，从最旧的已封尾、未保留段整段淘汰；当前段不删，只剩当前段仍超限则结束会话（`capacity`） |
| 收尾 | 进程退出时最多 5 秒写完队列，未完成部分标 `end_unknown` |

请求与响应缓冲按实际分配容量计费，扩容时同时计入新旧缓冲。内存不足时停止对应正文的后续采集，记为 `truncated`；元数据都无法入队时计入 `dropped`；磁盘写失败结束所有会话（`disk_failed`）。保留丢弃与截断计数供排查；回放发送可用记录，不因此判定整次回放无效。磁盘状态发布失败时停止采样并保留会话重试，只有结束状态写入成功才移出活动列表。会话所有段淘汰后同步清理其目录和 session.json，避免元数据无限积累。

## 回放标记

回放服务使用自身的 `AIGATE_REPLAY_MARK_KEY`（至少 32 字节）签名，请求携带 `X-Aigate-Replay: <run_id>/<seq>/<HMAC-SHA256>`。switch/proxy 接收端信任自身该变量与 `AIGATE_REPLAY_MARK_VERIFY_KEYS` JSON 字符串数组的并集，原始字节去重后最多 8 把，每把至少 32 字节；回放服务不读取附加验证密钥。跨 gateway 回放时无需把所有签名端改成同一密钥，但接收端必须包含来源的签名密钥。完整格式和启动校验见[打包说明](../packaging/README.md)。签名有效时，switch 不录制该请求，proxy 在审计日志、画像事件、慢请求和流异常记录里标 `traffic=replay`，画像指标增加 `traffic="replay"` 标签以便监控排除；转发不变。签名无效或未配置密钥时按普通业务处理，不拒绝请求。目标白名单和控制认证独立保持。

## 回放（aigate-replay）

`aigate-replay serve` 常驻录制主机，与 switch 使用同一系统账号以读取录制文件。它是回放的唯一入口，调用方只有平台 engine：

- 控制监听默认 `127.0.0.1:18085`，要求 `Authorization: Bearer <AIGATE_REPLAY_CONTROL_KEY>`（至少 32 字节，独立于切流密钥）和实际 TCP 来源 `--allowed-cidrs`。
- `--targets` 指向本地 JSON 文件 `{"targets": {"r1": "http://127.0.0.1:18084"}}`，控制请求只能按名字选择目标，不能传任意地址。名字必须等于该功能服务的 `router_id`，平台按这个名字锁定 Router。开始前读取目标 `/runtime/v1/status`，只接受 `service=aigate-proxy`（否则 `target_not_proxy`），从而拒绝把回放打到切流业务入口；`router_id` 与名字不符时返回 `target_router_mismatch`。目标的 Router 与配置摘要写入运行状态，之后每个回放请求都带 `X-Aigate-Expected-Router` 和 `X-Aigate-Expected-Config`，目标中途换配置或换 Router 时功能服务返回 409，并计入失败，便于发现目标变更。
- `--record-root` 为切流录制根目录（只读），`--data-root` 保存每次 run 的 `request.json`、`state.json`、`plan.json`、`plan.jsonl`、`results.jsonl`，最多保留 200 个 run：每次开始新 run 前删除最旧的已结束 run，启动时同样清理。重启时把进行中的 run 标为 `end_unknown`。

| 接口 | 说明 |
| --- | --- |
| `GET /control/replay/v1/status` | 服务、boot、目标名、当前 run |
| `GET /control/replay/v1/recordings?gateway_id=` | 列出录制会话及各段元数据（覆盖范围、partial、计数、是否保留），不含正文 |
| `POST /control/replay/v1/runs` | 按 `run_id` 幂等；同时只跑一个 run，否则 `run_active` |
| `GET /control/replay/v1/runs/{id}` | 状态、进度、起止时间与最近错误 |
| `POST /control/replay/v1/runs/{id}/stop` | 停止新发送，已发请求继续收尾，状态 `stopped`；尚未开始的 ID 先持久化 `start_cancelled=true`，阻止迟到的启动请求（同 run 保留期限） |

请求字段见 `openapi.json` 的 `ReplayRunRequest`。默认 `mode=timed`、`filter.status=all`，回放 `/v1/chat/completions` 的完整请求正文，包含 HTTP 失败、取消及有完整正文的入口拒收记录；响应采集截断或 usage 缺失不排除请求。正文未读完、截断或损坏时明确记录排除或校验失败。`filter.status` 只接受 `all`，不提供仅成功筛选。仅支持 schema 2；旧格式明确返回 `recording_schema_unsupported_rerecord`，不读取旧响应文件。段按覆盖时长拼接，筛选后仍保留原偏移。`concurrency` 必填 1～1024；`timed` 没有在途上限。旧 `fix_output`、`force_stream`、`max_inflight`、`set_max_tokens` 和录制 `response` 字段均被严格拒绝。

`timed` 使用单调时钟，到期即派发，磁盘、校验、改写和网络在独立发送路径执行。不存在活动连接数限制；空闲连接池不是并发上限。正文预算仍为 512 MiB，按 `6×原始大小+1 MiB` 保守预留，覆盖读取、改写及上传，在 transport 关闭上传并释放正文后归还。原速派发先非阻塞申请，预算不足立即以 `replay_resource_exhausted` 停止新派发，等待已经发送的请求收尾后失败；固定并发使用可被停止打断的预算等待。两者都不取消已发请求，也不自动重试。

平台从目标 proxy 的 `/v1/models` 自动获取唯一模型名，创建意图时固定为 `set_model`，用户不再填写。查询使用目标 Router/config 校验头；无法获取模型时拒绝新建回放，不创建任务或发送负载。同编号重复提交沿用已保存模型，不重新查询或修改已接受意图。两份叠加录制共用同一模型快照。

回放只替换 `model`，在 `rewritten` 中记录 `set_model`；`stream`（含缺省）、`stream_options`、`max_tokens`、`max_completion_tokens`、`min_tokens`、`ignore_eos`、`stop`、`stop_token_ids` 和其他字段均保留。没有模型改写时保留整个请求的原始字节。无效 JSON 或非对象正文无法填写模型时也按原字节发送，逐请求结果不标记模型改写。不使用录制输出长度设定生成预算，不限制 n 为 1，不注入 usage 选项。目标参数、上下文或模型错误正常记录，后续请求继续；不续写、补发或降参重试。

### 响应观察与核验

录制和回放共享有界观察器：64 KiB 输入缓冲、最大 JSON 深度 128、最多 128 个结束原因（每个最多 256 字节）和 4 KiB 错误摘要。SSE 增量解析跨块、CRLF、多行 data、最终 usage 与 `[DONE]`；JSON 跳过长字符串，观察器自身不累计完整回答（录制正文由独立缓冲完整保存）。多个有效 usage 使用最后一个，`usage_known` 表示 completion_tokens 存在；0 与未知分开。只保存实际存在的 prompt/completion/total token 字段。压缩或协议错误标记解析失败，不影响业务转发；usage 已收到而后传输失败时同时保存两者。录制不改写在线请求来索要 usage。回放按原请求消费 JSON/SSE，响应观察仅提取诊断，不改写请求。

`results.jsonl` 0600 串行写入，每条包含计划序号、原/本次请求 ID、计划/HTTP 调用/成功写出偏移、HTTP 与传输结果、实际 token 数、结束原因、解析状态及改写摘要，不保存请求或完整回答。成功写出由 `httptrace.WroteRequest` 观测，失败或未写出为 null。文件写入或同步失败停止新派发，以 `replay_results_failed` 失败，`results_failed=true` 禁止报告核验成功。明细仅留回放主机，没有下载接口。

新任务不做等输出达标判断。`results.jsonl` 继续记录实际 usage、HTTP/传输错误及 SSE 错误事件；HTTP 200 的 SSE 错误仍计入失败。旧任务保留已有 `output_verification` 历史字段及结果恢复能力。`completed` 只表示发送和收尾完成，不等于全部请求成功。状态包含未发送数、排除原因、结束原因和最大写出延迟；这些时间是回放器观测，不证明目标收到时间。

运行期间通过每段文件锁阻止容量淘汰；所有已启动路径收尾后才发布终态并释放占用。首次状态写盘失败不启动负载，运行或结束状态写盘失败报 `storage_failed`。服务状态的 capabilities 包含 `recording-original-request-v1`，平台缺少该能力时禁止新建回放。

运行中异常退出后，服务从 `results.jsonl` 的完整行恢复核验汇总与已确认的发送/完成/失败计数，仍将任务标记为 `end_unknown`，不重放请求。缺少完整结果的计划项计入 `progress.unconfirmed`，不能当作未发送；`unsent` 只统计明确未发送的结果。`output_verification.incomplete=true` 表示仅恢复了部分核验结果。末尾残行、重复序号或损坏记录会停止继续恢复并设置 `results_failed=true`，保留此前完整行的统计和原始文件；已终结任务不重新计算，重复启动也不会重复累计。

2026-10-19 本地规则更新（未部署）：被回放器重启中断、原因或结束原因是 `service_restarted` 的任务不再生成性能报告。报告接口返回 404，不读取旧缓存或从结果重建报告；任务状态、恢复计数与原始结果仍保留用于收尾和排障。平台对应报告不归档、不展示；普通失败、主动停止及重启前已完成的任务仍沿用现有报告规则。

### 协调升级

本次改造已按 code-eval 停机窗口规则协调更新平台 Web/engine、三台 switch 和 replay，实际身份见[部署记录](DEPLOYMENT.md)。普通 proxy/模型后端不新增预检接口，monitoring 无改动。发布时没有活动录制或回放；DCU 旧绑定的一个流式请求自然结束后才替换 switch。本次不改变数据库结构或既有规则；历史 v2 迁移脚本 `deploy/migration/recording_v2.py` 仅适用于更早版本的录制规则，本轮没有执行。所有历史录制、任务结果保留，不转换旧 schema 或清理文件。schema 2 的旧尾部录制仍可回放完整请求，但无法补回未曾保存的响应前缀。后续窗口使用新版录制器；失败修复当前版本，不回退。


### 双录制同步叠加（2026-10-15 已部署）

`status.capabilities` 新增 `recording-overlay-sync-v1`。`runs` 接受与 `recording` 互斥的 `recordings` 数组（1～2 个 gateway_id/recording_id/segments 引用），最多合计 96 段和 20 万条计划请求。当前源码两份支持 timed 与 concurrency。timed 各自拼接选中段后从相对 0 秒叠加，时长取最大值；concurrency 按来源分别维护固定 worker 数。计划稳定排序并分配唯一计划序号。计划、结果和 `sources` 进度包含来源与录制标识，避免不同来源请求 ID 重名。

平台使用 `prepare_only=true` 创建准备任务。服务锁段、校验文件、生成计划并核验目标，进入 `prepared` 后不发模型请求。`POST runs/{id}/start` 接收 `{boot_id,start_at}`：必须为未来 30 秒内的时刻，持久化 `scheduled` 后按单调时钟等待起跑；同时间重复调用幂等，不同时间冲突，迟到命令拒绝，不补发。已约定起跑的实际唤醒允许最多 60 秒迟到，超过则失败；允许范围内从实际起点按原速发送，操作者观察中间稳定时段。`GET clock` 返回 boot_id 与 received_at/sent_at，用于平台估算时钟差和网络误差；使用现有控制认证。平台的时钟估计不确定度、估计起点偏差及起跑后状态确认窗口均采用 60 秒宽松阈值，不宣称精确同步。

`POST runs/{id}/lease` 接收 `{boot_id}`，续期 15 秒；租约过期停止新发送并收尾，不允许复活过期或终态任务。准备总时限 120 秒。内存预算耗尽、结果写入失败等致命故障立即公布 `stopping` 与 `error`，平台据此停止另一侧新发送；已发请求继续收尾，故障侧最终为 `failed`。普通单条请求失败只累计失败数，不触发整组停止。`stop` 保留先停后启的取消屏障；prepared/scheduled 状态重启也恢复为 end_unknown，不自动发送。跨服务由平台约定同步起点并聚合进度，同服务仍只占用一个活动 run；平台等待所有子任务确认终态后才释放 Router 锁。两侧服务均需能访问相同目标，原始录制文件始终留在本机；新能力无需 switch、proxy 或模型改动。

## 性能报告与收尾 v2（当前源码，待部署）

新增能力 `replay-performance-v2`。新回放计划要求片段已封尾且保留；只接收请求体明确的 `stream: true`，分开统计 `nonstream`、`stream_unspecified`、`stream_unclassifiable`。结果增加 `request_stream`，与响应类型 `stream` 分开记录，失败响应不会改变请求资格。

`auto_truncate` 控制收尾截断，平台默认启用。`coordinated_truncation=true` 时由平台对同一目标各来源的总在途判断并调用 `POST runs/{id}/truncate`。全部计划请求实际派发后：原速须超过计划时长且在途为 1–5；固定并发总在途须严格低于每目标总并发的 50%。普通 `stop` 只取消派发，`truncate` 使用独立请求取消上下文终止残余在途。自然完成的结果不覆盖；截断结果记 `tail_truncated`，单列 `progress.truncated`，终态 `truncated`。

`GET /control/replay/v1/runs/{id}/report` 默认返回 version=2，可显式选择 version=1。查询只读原结果，不发送模型请求；读取期间保护结果目录不被轮转删除。旧缓存版本不符时从结果重新聚合，原始结果缺失则不可用，平台保留已归档旧版。

v2 固定 30 秒桶，曲线仍最多七天。原速延迟按计划发送时间、固定并发按实际发送时间归组；同请求晚完成的延迟保留在发送桶。完成计数与已知成功输出 token 按完成桶归组；并发按实际发送/结束积分。直方图相对桶宽 2%，分位数按合并直方图计算；截断不伪造成完成延迟，缺少 usage 不补零。

报告包含 `bucket_seconds`、`latency_basis`、路径无关的 `input_digest`、`filter_policy`、`filter_evidence`、`truncation` 和每桶 `cohort`。历史流式资格只在 hash 核验的原始请求可读时确认，不用 SSE 响应推断原请求；无法确认则标 `legacy_unverified`。平台同时归档 v1/v2，重算不改写执行状态或结果文件。

发布仅需新版 aigate-replay 与 code-eval engine/Web；现有录制器、switch/proxy 协议和实时监控不变。平台必须在目标侧可观测在途归零、身份一致后才释放截断任务占用。

## 多目标批次（2026-10-17 已协调发布）

平台 Web/engine 及 DCU、XPU、A3、PPU 的 switch/proxy/replay 已更新，跨 gateway 多密钥互信和恢复准入校验通过。版本、白名单、现场证据与实际回放验收边界见[部署记录](DEPLOYMENT.md)。

`recording-multi-target-v1` 支持同一批次的多个目标任务在同一回放服务中并行运行。平台分别提交单目标 `runs`，每个任务携带共同 `batch_id`、独立 `run_id`、`prepare_only=true` 和通过模型发现冻结的 `expected_target`。不同批次仍互斥，同一批次不能重复占用同一目标；没有批次字段的历史调用仍保持单任务互斥。

一份或两份录制均支持原速与固定并发；`concurrency` 表示每份录制的额度，每目标总额度为录制数乘以该值。来源之间不借额度；同服务和跨服务语义一致。同服务同批次共享一个片段文件锁并按使用者计数，最后一个使用者结束后释放；不复制录制正文。正文内存预算仍为整个服务共享的 512 MiB，各目标拥有独立发送循环和结果文件。资源不足或某个任务失败不主动取消其他目标。

`status` 保留 `active_run`（任一活跃任务编号），新增完整的 `active_runs` 数组和 `active_batch`；新平台使用这些字段识别占用。历史清理保护所有活跃任务及同批次兄弟任务的证据。进程重启将未结束任务逐个恢复为 `end_unknown`，不会自动重放。

平台在全部目标及来源准备完成后统一下发起点，时钟不确定度、实际起点偏差和迟醒门槛为 60 秒；准备超时 120 秒、租约 15 秒及未来 30 秒内起点的命令校验保持不变。准备失败取消整批；启动命令部分送达时保留已发送与未知证据；运行后按目标停止全部来源，其他目标继续。共享来源服务故障可能同时影响多个目标。

## 每份录制并发与分组证据（2026-10-18 当前源码，待部署）

`recording-concurrency-per-source-v1` 能力允许双录制固定并发，单录制旧请求含义保持。平台在双录制并发子请求中附带 `target_concurrency`（每目标总并发），仅允许 coordinated 模式且取值介于本地来源总额度与两份录制总额度之间。平台确认目标所有来源派发完成后调用 truncate；回放端以目标总额度复核阈值，普通 stop 保持已发请求自然收尾。

报告新增 `source_inputs`，每项保存 gateway_id、recording_id 与该来源的 input_digest。逐来源摘要不包含目标或本地目录，组合按来源排序，使同机／跨机发送和录制选择顺序不影响平台分组。已有 input_digest 证据继续保留。升级范围仅 aigate-replay，固定请求门禁不由普通本地测试触发。
