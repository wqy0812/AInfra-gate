# Prefill 原始输入指纹

随 `settings.profile.enabled` 持续全量采集网关适配后、准备发给 Router 的输入。观测点为 `proxy_upstream_payload`，不证明后端已执行 Prefill。只保存元信息、长度和 HMAC，不保存消息正文、token ID 或回答。此功能提供离线计算的原始记录，不计算 KV 容量或固定 TTL。

这是 **UTF-8 字节近似**，不是后端真实 token/KV 块。`messages` 用 Go JSON 排序对象键、保留数组顺序、UseNumber、禁用 HTML 转义，去掉编码器末尾换行；数字字面量 `1` 和 `1.0` 保持不同。每 1024 字节及尾部生成一个标记，UTF-8 字符可以跨边界。消息序列化与后端 chat template 不同，真实 token 边界和 KV 字节数需另行校准。

算法 `hmac-sha256-chain-utf8-json-v1`：初值为 HMAC(key, algorithm + `:messages`)；每块为 HMAC(key, `block:` + 前一块的32字节摘要 + 8字节大端累计字节终点 + 本块字节)。每个 `prefix_marks` 项包含 `end_bytes` 和十六进制摘要。其他顶层字段各自记录规范化字节长度及 HMAC(key, `field:` + 字段名UTF-8字节数十进制 + `:` + 字段名 + `:` + 规范化值)。字段名、算法、密钥身份及模型/配置身份独立保留，离线消费者按所需语义分组。

## 生命周期与存储

`input` 包含 request_id、boot_id、sequence、arrived_at、at、Router/功能服务、后端、实际模型、配置摘要、绑定版本、traffic、stream、输入字节长度、前缀标记和其他字段摘要。`traffic` 区分 business、evaluation、replay。

`result` 用同一 `(boot_id, request_id, sequence)` 关联，包含结束时间、结果和 HTTP 状态、`prompt_tokens`、`cached_tokens`。缺失用量为 null，真实零保留为 0；没有 result 时结果未知。失败、取消以及没有准备好输入的请求不按成功过滤。输入结构不支持时 input 保留原因。当前支持非空的对象消息数组，其余结构记录为 unsupported。

独立目录为 proxy 数据根下的 `prefill-prefix/`，与画像策略代际无关。`key` 是 0600 的专用32字节随机密钥，`key_id` 为其 SHA-256；同一 Router 重启后保持。存储身份不匹配、已有状态丢失密钥等情况停止该采集器并报告 unavailable，不阻断业务。目录、密钥、JSONL 和状态文件不能作为公共静态资源。

容量默认 10 GiB/Router，可通过 `settings.prefill_prefix_max_disk_bytes` 调整（0 为默认，非零至少 1 MiB）。仅按容量淘汰最旧分段，不设天数。队列最多256条且总预算64 MiB，单条落盘记录最多8 MiB；超限、写入失败、重启中断有质量计数及时间缺口。导出 pin 阻止删除所需分段；容量不足且不能清理时记录 capacity_dropped。状态中的队列字节包含正在处理的一条任务。数据段按8 MiB或一分钟轮转，密钥和少量状态/索引文件不计入数据容量。

## 只读接口与离线消费

- `GET /profile/v1/prefill-prefix/status`：算法、来源、容量、实际覆盖时间、队列、质量计数及缺口。
- `GET /profile/v1/prefill-prefix/export?start=<Unix秒>&end=<Unix秒>`：按到达时间 `[start,end)` 过滤，可选 backend/model；不受旧画像24小时分析或30天查询上限限制。历史不足以显式 partial 返回，不伪造空白期间的完整覆盖。

导出为 `application/x-ndjson`，顺序为 manifest、input/result、complete。manifest 中 source 是快照状态；记录按追加顺序返回，离线先用 arrived_at、boot_id、sequence 排序，再关联 result。每条记录包含自身算法和 key_id，不能假设不同 key_id 可比较。最多同时两个导出，仅保持正在读取的一个文件描述符。

末行 `kind=complete, complete=true` 表示传输完整；`records_sha256` 是所有导出 input/result 原始行（含每行换行）的 SHA-256，不含 manifest 和 complete。`data_complete` 另行表示该快照没有已知覆盖、队列或解析缺口；结果仍可能在途。下载缺少完成标记、校验和不符时不得作为完整数据使用。质量计数为采集器生命周期累计，窗口相关缺口单独列出。崩溃恢复保守标记上一个采集进程期间可能丢失，保留原始数据。
