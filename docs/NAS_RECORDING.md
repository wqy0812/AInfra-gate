# 原始录制、定时清理与平台独立回放

状态：[PR #7](https://github.com/wqy0812/AInfra-gate/pull/7) 已合入 main，发布源码 `40372f27ddcd51e85cd68c45820bb5239a9af689`，2026-10-19 与平台在同一停机窗口部署完成，旧常驻 replay 已停止并禁用自启动。平台方案见 [code-eval 设计](../../code-eval/docs/平台/设计方案/录制NAS与容器压测.md)。

## 网关本地存储

录制、慢请求正文、流异常正文分别默认 10 GiB，可配置。录制按 switch gateway 目录，诊断按 proxy 所有者统计，覆盖历史配置目录、临时文件和待删除文件。取消自动压缩与 72 小时阈值，保存原始字节；NAS 也不压缩。

已有 slow_requests.retention_hours 字段仍接受旧配置输入，但不参与正文清理。画像事件及流异常事件索引等非正文数据继续使用其原有规则。

每天 Asia/Shanghai 05:00 独立维护 worker 按最旧已完成半小时片段清理到软额度内。白天超额等待维护，错过不补跑，启动只恢复中断操作。活动写入、上传 pin、旧保留数据不淘汰；关闭采集后历史目录继续维护。磁盘写满记录采集失败，不影响转发。

诊断容量清理逐批读取目录项，候选在全部历史配置目录间按时间统一选择，内存最多保留 4,096 项。超额时逐批处理，跨批次仍保持半小时分组及 pin/读取保护；被保护项不阻塞后续候选。清理扫描不使用导出清单的 65,536 项上限，慢请求恢复预扫描也使用流式遍历。

读取/导出取得同一目录占用。清理先持久化 `.deleting`、拒绝新读取和 pin；已有读取结束后取得独占占用，将目录原子改名为 `.trash-*` 再物理删除。待删数据在真正移除前仍计入容量；选候选时扣除已计划回收的大小，避免等待期间多删。重启恢复标记，不把已标记内容重新开放。

## 控制接口

沿用 switch 控制认证和来源限制：

- `GET /control/v3/recordings`：原始录制与封尾片段清单。
- `GET /control/v3/recordings/{rid}/segments/{segment}/manifest`：身份、不可变 segment 元数据和 version=2/format=tar 文件清单，成员记录 name/bytes/sha256。
- `GET /control/v3/recordings/{rid}/segments/{segment}/archive`：普通 `application/x-tar` 流，不生成本地包，不接受压缩索引格式。

导出仅允许封尾数据，传输全程持读取占用；平台 pin 保护不同 HTTP 请求及重试之间的数据，NAS 提交成功后才 unpin。文件清单最多 65,536 项、16 MiB，展开总量最多 100 GiB，拒绝链接和非普通文件；超限明确失败，原数据不变。身份摘要由平台按原始文件清单计算，与 tar 编排无关。数据被清理时新导出返回不可用，不能生成空包表示成功。

## 一次性回放程序

`scripts/build.sh` 使用 vendor 模式生成 aigate-replay-task，作为平台固定执行镜像内 `/usr/local/bin/aigate-replay-task`。平台 Python 入口先校验全部 NAS 输入；Go 读取固定任务描述，直接调用规划、发送和报告内核。每个容器只执行一个目标一次；不监听端口，不调用旧 HTTP handler，不使用 commands/responses、来源租约、跨目标时钟同步或 finish 命令。

输入目录只读，通过平台方案引用防删除，Go 不在输入中创建锁或 pinned，也不删除输入。结果目录保存 execution.started、stop.json、逐请求结果及报告。execution.started 禁止进程重启后重复发压。停止仅停止新派发，已发请求收尾后持久化报告并退出；原速、每来源固定并发和双录制叠加保留，正文预算 512 MiB，只允许改写 model。

进度查询已于 2026-10-20 接入 worker 管理接口：平台经 `replay_status`/`replay_stop` 获取进度，不直接轮询 NAS 状态文件。具有 `code-eval.replay-progress=uds-v1` 标签的执行镜像通过本地 UDS 从 Go 内存按需读取整体及各来源的已发送、成功、失败、在途和未发送计数；停止新派发后仍服务到已发送请求收尾及报告发布完成。页面更新取决于既有平台轮询周期。

worker 为每个任务在本地 `DATA_DIR/replay-ipc/<任务身份摘要>/` 创建 0700 目录，仅将本任务目录挂载至 `/run/replay`；Go 通过 `AIGATE_REPLAY_PROGRESS_SOCKET=/run/replay/progress.sock` 启动 0600 socket，不开放 TCP 端口。查询为换行 JSON，只接受 `op=status` 和匹配的 `run_id`、`boot_id`、原始 `task.json` 的 `task_sha256`；请求不超过 2 KiB，响应不超过 64 KiB，查询总超时 1 秒、服务端最多四个并发连接。响应仅含采样时间、计数和状态，不含任务正文或规划内容；socket 创建失败在派发前退出。

`state.json` 增加 `sampled_at`，开始、首次停止或致命错误、截断决定和结束立即原子保存，运行与收尾期间每 30 秒保存恢复检查点。写盘失败以 `storage_failed` 停止新派发，已发送请求继续收尾；结束前等待后台写入退出，再保存终态和报告。已有固定旧镜像由 worker 读取文件快照；新镜像运行中查询失败返回 unavailable、保留平台上次有效计数，不读取旧 NAS 快照冒充实时进度，也不据此停止任务或释放占用。确认容器退出后由 worker 读取最终或崩溃前快照，并清理该任务 IPC 目录。可复用方案在核验参数和输入摘要后更新到当前执行镜像；历史任务保留原镜像身份。

平台把独立任务放到 test2/test3。报告记录方案/输入摘要、执行镜像、节点和同机判题观察；多目标分别从自己的时间零开始。网关本地副本可在 NAS 保存后正常淘汰。

## 迁移与部署边界

历史录制已完整保存到 NAS，历史回放文件统一位于 replay-runs/<run_id>/results/runs/<run_id>，报告统一读取平台数据库。历史批次与单目标任务进入同一 replay_submissions 列表，原任务 ID、报告正文、输入摘要和真实执行身份保留。旧常驻服务、监听和自启动已退役，旧实现、协调协议与专用读取路径从当前源码移除。旧任务不会重跑，失败修复当前版本。具体完成身份和数据校验证据见[平台发布记录](../../code-eval/deploy/releases/README.md)。
