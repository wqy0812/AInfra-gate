> 历史线上快照（2026-09-13）。2026-09-14 本地源码已删除网关 Web、管理 HTTP API 和 8081 监听，改用本机 CLI；本次尚未部署。本文的旧管理入口、管理鉴权和页面操作仅供追溯。新版本操作与迁移见 [CLI 管理说明](../MANAGEMENT.md)，请求画像平台的独立功能保持原有设计。

更新核对：2026-09-13 11:46（Asia/Shanghai）。本次只读核对正式网关版本、管理状态与新增请求画像，补充第 9 节。未特别更新的全局参数和旧协议验收仍为 2026-09-12 快照，操作前需重新读取。本文供内部运维使用，不包含密钥值。

## 1. 历史结论（2026-09-13 快照）

* 网关服务 `aigate.service` 已运行并启用开机启动，业务入口为 `http://dcu1:10015`。
* 核心功能为 **API 兼容适配**、**单后端 A/B 切流与维护管理**；现已新增 **请求监控与画像**（见第 9 节）。同一时刻只有一个后端接收新请求，不做轮询或权重负载均衡。
* 当前已注册 `a3`、`dcu`，活动端为 `dcu`。A3 最近保留的验证记录为 2026-09-12 21:20 模型发现失败，本次未重新探测；**注册不代表可接流，先修复并重新验证目标端，再切流**。
* **已废弃的管理入口**：旧管理页为网关主机本机的 `http://127.0.0.1:8081/`，当前源码已移除该页面和监听，改用 [本机 CLI](../MANAGEMENT.md)。本快照的 `auto_rollback=false`，不会因故障自动回切。
* 当前采用 v2 管理状态：后端配置和活动路由以状态文件为准。修改主配置中的 `backends`、`routing.backend` 后重启，**不会覆盖已有的 v2 后端和活动路由**。
* 本次文档核对只读检查运行状态和配置，未修改生产配置、未重启、未切流、未重新发起生成测试。

当前请求路径：

```text
客户端
  → dcu1:10015 aigate
  → 同机 127.0.0.1:10016 Router
  → 推理后端
```

这里的 `127.0.0.1` 指网关所在主机。旧资料里的 `/data2/aigate`、Router `10017` 和 Python 正式入口均为历史信息，不能作为当前操作依据。

## 2. 核心转发与切流功能

### 功能一：API 兼容适配

对客户端提供固定接口：

| 接口  | 用途  |
|-----|-----|
| `POST /v1/chat/completions` | 普通 JSON 与 SSE 流式聊天，支持 OpenAI 兼容的工具调用字段 |
| `GET /v1/models` | 返回当前活动后端选定的模型 |
| `GET /health` | 仅检查网关进程，不验证下游生成能力 |
| `GET /docs`、`GET /openapi.json` | 业务 API 说明与规范 |

请求处理顺序：选择活动后端 → 自动发现真实模型 → 替换请求中的模型名 → 应用兼容规则 → 转发。

客户端传入的 `model` 被忽略，也可以不传。网关从目标后端的模型列表选取模型；当前未设置 `preferred_model`，选择列表第一项。每个后端独立缓存发现结果。

当前使用 `openai` 适配器，保留完整历史消息、工具定义、工具结果、流式参数及其他未显式删除的字段。只将**对象类型**的 `metadata.workspace` 转为紧凑 JSON 字符串：

```json
{"metadata":{"workspace":{"file_count":2175,"languages":["java"]}}}
```

转发为：

```json
{"metadata":{"workspace":"{\"file_count\":2175,\"languages\":[\"java\"]}"}}
```

字符串不会再次编码；workspace 缺失、null、数组、数字、布尔值及其他 metadata 字段均保留。当前 `drop_fields=[]`，没有配置额外删除项；未配置参数覆盖 `overrides`。

可选能力：`public_model_name` 可统一对外模型名称，适用于模型列表、普通响应及 SSE 顶层 model；当前未启用。通用 `json_string` 会转换所有非字符串 metadata 值，与当前窄范围规则不同，不能随意替换。普通模式另支持模板适配器，但当前管理模式仅支持 `openai`。

网关不会自动重发已经发出的生成请求。下游错误会统一包装，响应携带 `X-Request-ID`；日志记录后端、真实模型、耗时、结果及最后一条用户消息前 10 个 Unicode 码点，不记录完整请求或回答。

### 功能二：单后端 A/B 切流与维护管理

管理模式提供完整流程：

```text
目标端预热与协议验证
  → 成功后切换新请求
  → 原端继续完成在途请求和 SSE
  → 原端排空
  → 人工进入维护
  → 服务恢复后执行恢复验证
  → 按需切回
```

每次切流都重新验证目标端：模型发现、普通 JSON、普通 SSE、流式工具调用、工具结果续答。当前为两轮，每轮 4 次生成；工具结果为模拟数据，不执行外部工具。任一步失败，均不提交路由。

切流只影响新请求，已开始的请求保持原后端。旧 SSE 不迁移，也不跨端重放。进入维护成功后禁止普通探测和回切访问该端，恢复验证是显式解锁路径。

## 3. 配置文件和程序在哪里

以下均为 **dcu1 服务器路径**，不是操作者电脑上的路径。

| 文件或目录 | 当前路径 | 用途 / 修改方式 |
|-------|------|-----------|
| 主配置   | `/data4/aigate/config.json` | 全局超时、连接池、入口鉴权、对外模型名、管理配置；修改后重启 |
| 管理状态  | `/data4/aigate/state/management-state.json` | v2 后端注册表、活动路由、维护锁、回切资格、最近 100 项操作；通过网页/API/CLI 修改，勿在线手改或删除 |
| 管理密钥环境文件 | `/data4/aigate/admin.env` | 定义 `AIGATE_ADMIN_KEY`；权限 0600；文档不记录值 |
| systemd 单元 | `/etc/systemd/system/aigate.service` | 业务监听、启动命令、退出等待、环境文件；修改单元后需 daemon-reload 和重启 |
| 当前程序入口 | `/data4/aigate/current/aigate` | current 指向实际版本 |
| 当前版本目录 | `/data4/aigate/releases/gateway-profile-v1/` | 当前正式二进制   |
| 运维 CLI 包装脚本 | `/data4/aigate/aigate-admin` | 自动加载服务器密钥和正式配置，默认执行 status |
| 备份与证据 | `/data4/aigate/backups/`、`/data4/aigate/evidence/` | 变更备份和验收记录 |
| 版本回退入口 | `/data4/aigate/rollback.sh` | 恢复配套旧 Go 版本和状态；不是后端切流命令 |

> **已废弃**：表中的网页/API 管理方式及 `AIGATE_ADMIN_KEY` 管理授权属于旧版；现行 CLI 通过 Unix socket 和文件权限授权。旧包装脚本的密钥加载行为仅供追溯，迁移要求见 [CLI 管理说明](../MANAGEMENT.md)。业务与 Profile 凭据仍按各自配置使用。

本地源代码目录：`/Users/qyw/Downloads/aigate`。其中 `config.*.example.json` 和 `deploy/dcu1/config.example.json` 是示例，**不是当前服务直接加载的配置文件**。`README.md`、`MANAGEMENT.md` 介绍通用能力，`deploy/dcu1/README.md` 按时间记录部署；旧章节和模板值须以线上核对结果为准。

配置优先关系：


1. 主配置路径：显式 `--config` 优先，其次 `AIGATE_CONFIG`，都未提供时使用工作目录的 `config.json`。正式服务已显式指定绝对路径。
2. 管理状态不存在时，主配置中的后端和 single 路由用于首次初始化。
3. 已有 v2 状态时，后端从 `registry.configs` 读取，当前路由从 `active_backend` 读取；主配置仍提供全局参数。
4. 当前未使用 `routing_file`。管理模式不能同时启用它。

## 4. 当前生效参数

以下“默认生效”表示文件中未写出、由当前程序补齐；其余来自线上配置、状态或 systemd。数值为本次核对时的快照。

### 入口与全局参数

| 参数  | 当前值 | 含义  |
|-----|-----|-----|
| 业务 `--listen` | `0.0.0.0:10015` | 业务端口 |
| `timeout_seconds` | 1860 秒 | 生成总期限，包含整个 SSE 过程 |
| `discovery_timeout_seconds` | 10 秒 | 模型发现超时 |
| `model_cache_seconds` | 60 秒 | 模型缓存有效期，过期后按需刷新 |
| `discovery_retry_seconds` | 5 秒 | 发现失败缓存期，之后可重试 |
| `gateway_api_key_env` | 未配置 | 业务入口当前未启用 Bearer 鉴权 |
| `public_model_name` | 未配置 | 对外显示实际模型名 |
| `routing.mode` / 活动后端 | `single` / `dcu` | 当前新请求全部进入 dcu |
| `routing_file` | 未配置 | 使用管理状态切流 |
| `http_pool.max_idle_connections` | 256，默认生效 | 总空闲连接上限 |
| `http_pool.max_idle_connections_per_host` | 128，默认生效 | 每个主机空闲连接上限 |
| `http_pool.max_connections_per_host` | 0，默认生效 | 不额外限制每主机连接数；不是业务限流 |
| `http_pool.idle_timeout_seconds` | 90 秒，默认生效 | 空闲连接保留时间 |
| `--shutdown-timeout` | 31m | 网关优雅退出最大等待时间 |
| systemd `TimeoutStopSec` | 31min 40s | systemd 停止期限 |
| systemd `Restart` / `RestartSec` | `on-failure` / 3s | 异常退出重启策略 |

当前 HTTP 客户端不使用环境代理、不跟随重定向。生成与模型发现共用连接池。

### 当前后端 dcu

| 参数  | 当前值 |
|-----|-----|
| `adapter` | `openai` |
| `url` | `http://127.0.0.1:10016/v1/chat/completions` |
| `models_url` | `http://127.0.0.1:10016/v1/models` |
| `models_list_path` / `model_id_path` | `data` / `id`，默认生效 |
| `preferred_model` | 未设置，选择列表第一个 |
| 最近成功预热使用的模型 | `DeepSeek-V4-Flash-FP8-Channel` |
| `headers` / `headers_env` / `header_prefixes` | 未配置下游鉴权或自定义头 |
| `overrides` | 未配置 |
| `compatibility.metadata_values` | `workspace_object_json_string` |
| `compatibility.drop_fields` | `[]` |
| `request_template` / `response_content_path` | 未配置，当前 openai 路径不使用 |

### management 参数（旧快照，含已废弃字段）

> **已废弃**：下表的 `management.listen`、`management.api_key_env` 不再生效；新版本使用 `management.socket`，不开放管理 TCP 端口，也不使用管理 Bearer 密钥。其他参数仍需按实际版本核对，当前定义见 [CLI 管理说明](../MANAGEMENT.md)。

| 参数  | 当前值 | 含义  |
|-----|-----|-----|
| `enabled` | true | 已启用管理模式 |
| `listen` | `127.0.0.1:8081` | 仅本机管理入口 |
| `api_key_env` | `AIGATE_ADMIN_KEY` | 独立管理鉴权 |
| `state_file` | `/data4/aigate/state/management-state.json` | 状态版本为 2，权限 0600 |
| `warmup_rounds` | 2   | 每轮四类生成验证 |
| `warmup_step_seconds` | 120 秒 | 单步预热期限 |
| `warmup_timeout_seconds` | 900 秒 | 整次预热期限 |
| `warmup_max_tokens` | 1024 | 预热输出预算，后端 overrides 仍可影响实际请求 |
| `drain_timeout_seconds` | 1900 秒 | 路由提交后等待原端排空的期限 |
| `auto_rollback` | **false** | 当前自动回切关闭 |
| `failure_threshold` | 3   | 启用自动回切后的连续故障阈值 |
| `failure_window_seconds` | 60 秒 | 故障统计窗口 |
| `rollback_retry_seconds` | 60 秒 | 回切候选验证失败后的重试间隔 |

2026-09-13 核对时 `revision=35`、`route_version=1`，活动端 `dcu`；`a3` 和 `dcu` 均为 `maintenance=false`、`drain_unknown=false`。A3 最近保留的预热在模型发现阶段失败，DCU 最近完整预热记录仍为 2026-09-12。操作前必须重新读取状态、计数与 revision，不能复制快照值到未来请求。运行中 `clean_shutdown=false` 本身不代表故障。

## 5. 怎么修改配置

### 5.1 在线新增或编辑后端：旧管理页流程（已废弃）

> **已废弃**：以下页面和按钮已从源码移除，仅保留历史操作记录。当前通过 `aigate backends create` / `aigate backends edit` 管理后端，参数、权限及版本校验见 [CLI 管理说明](../MANAGEMENT.md)。

在网关主机访问 `http://127.0.0.1:8081/`，输入受控获取的管理密钥后连接。远程电脑的 `127.0.0.1` 不是 dcu1；远程访问应使用已批准的管理隧道或受控入口。CLI 不需要手工显示密钥。

新增后端流程（A3 当前已注册，勿重复创建；可按以下字段要求检查其配置）：


1. 点击“新增后端”，名称填写 `a3`。
2. 填写**从网关主机可达**的真实生成地址和模型发现地址，配置目标需要的鉴权头。本文不填虚构的 A3 地址。
3. 首选模型可以留空自动选择；高级配置按 A3 的实际协议设置模型发现路径、参数覆盖和 metadata 规则，不直接照搬 DCU 规则。
4. 保存后出现“配置已保存，尚未验证”。这一步只校验并持久化，不访问下游、不切流。
5. 点击“仅预热”验证协议。通过后再按第 6 节切流；切流会重新验证。

编辑已有后端：

* 当前接流端不能在线编辑。先切到其他已验证后端，等待原端业务和探测归零，再在原端卡片点击“编辑”。
* 不能处于预热、切流、恢复、自动回切或“排空待确认”等冲突状态。
* 当前活动端为 dcu；a3 虽已注册但最近验证失败。修改 dcu 前，仍需先准备并验证可接流的第二后端，再切走和排空。
* 保存后清除该端旧模型缓存、最近预热结果及回切候选资格，需重新验证；维护锁会保留。
* 名称创建后不可修改，当前不支持删除后端和全局配置热更新。
* 静态请求头只写不读；编辑时可以保留、替换或删除。直接填写 Authorization 时包含完整的 `Bearer ` 前缀。静态值以明文保存于 0600 状态文件；不要复制状态原文到文档。
* 引用环境变量时，变量必须已存在于**网关服务进程**。只在个人终端 export 或修改 env 文件，都不会更新已经运行的服务环境。

### 5.2 修改全局参数：编辑主配置，校验后重启

适用项：超时、连接池、对外模型名、入口鉴权、自动回切开关、预热参数及第 9 节 profile 监控配置。旧“管理监听”字段已废弃，现行配置使用 `management.socket`，迁移见 [CLI 管理说明](../MANAGEMENT.md)。以下保留旧部署的重启范例，包装脚本、配置和服务路径须按实际部署核对；本次编写文档未执行这些变更。

先冻结并行管理操作，备份主配置；如修改密钥环境文件或 systemd 单元，也各自备份。备份含凭据时保留受限权限。

```sh
sudo /data4/aigate/aigate-admin status --json
sudo cp -p /data4/aigate/config.json /data4/aigate/config.json.bak-$(date +%Y%m%d-%H%M%S)
sudoedit /data4/aigate/config.json
sudo /data4/aigate/aigate-admin check
```

例如固定对外模型名，在主配置**顶层增加**：

```json
"public_model_name": "my-model"
```

例如开启自动回切，将现有 `management.auto_rollback` 改为 `true`；需先具备可验证的备用后端，开关本身不会创建后端或保证回切可用。

`check` 通过只代表配置有效，不连接下游。已有 v2 状态时，它校验的是叠加持久化后端后的配置。

安排重启窗口：暂停入口新请求，并确认业务/SSE/探测归零，再执行：

```sh
sudo systemctl restart aigate.service
sudo systemctl is-active aigate.service
sudo /data4/aigate/aigate-admin status --json
sudo /data4/aigate/aigate-admin warmup dcu
sudo journalctl -u aigate.service -n 100 --no-pager
```

最后恢复入口，并通过业务 `10015` 检查所需 JSON、SSE 和工具协议。单网关重启会有入口中断；只把请求从 dcu 切到 a3，并不能消除网关自身重启的影响。

仅修改主 JSON 无需 `daemon-reload`；修改 systemd 单元时先执行 `sudo systemctl daemon-reload`。业务监听由 `ExecStart --listen` 控制，不是 JSON 配置项。改变生成超时还需核对排空、优雅退出和 systemd 停止期限是否足够。

若变更失败，恢复本次配套备份并重新检查、重启和验证。不要删除管理状态来强迫主配置生效；这会丢失在线后端、路由、维护锁和操作历史。运行中复制的状态也不应直接当作已排空的恢复快照；涉及状态恢复时应保留优雅停止后的最终状态。

## 6. 怎么切流：历史网页与 CLI 流程

**前提：目标端已注册且重新通过协议验证。下面以 dcu → a3 为例；A3 最近保留的模型发现验证失败，尚不能据此认定可接流，修复并重新验证后再执行。**

### 网页流程（已废弃）

> **已废弃**：以下网页步骤不适用于当前源码。切流、排空、维护和恢复能力保留，通过本机 CLI 操作，见 [CLI 管理说明](../MANAGEMENT.md)。

1. 连接管理页，确认“新请求正在进入”为 dcu，目标 a3 不在维护或排空待确认状态。
2. 在 a3 卡片点击“切到此端”并确认。网关自动执行“预热验证 → 切换新请求 → 等待 dcu 排空”。
3. 等操作完成，确认活动端为 a3，dcu 业务和探测占用为 0；SSE 是业务占用的子集。
4. 若准备维护 dcu，在其卡片点击“进入维护”。**收到锁定成功后**，再由运维系统操作推理服务。网关自身不会停止或重启下游。
5. dcu 恢复后点击“恢复验证”。成功后解除维护锁，但业务仍在 a3。
6. 需要回 dcu 时，再点击 dcu 的“切到此端”。

若要重启的是 dcu1 **整机**，当前网关也在 dcu1 上，必须先把网关及必需入口部署到独立持续运行节点。仅切换后端不能保护同机网关免于整机停机。

### CLI 等价流程

以下保留旧 dcu1 包装脚本的命令示例。**旧管理密钥授权已废弃**；现行 CLI 使用 Unix socket，具体命令与权限见 [CLI 管理说明](../MANAGEMENT.md)，不能依赖旧脚本加载密钥来获得授权。

```sh
# 查看状态
sudo /data4/aigate/aigate-admin status --json

# 可选：只验证 a3，不切流
sudo /data4/aigate/aigate-admin warmup a3

# 一条命令：重新预热 a3、切换新请求、等待 dcu 排空
sudo /data4/aigate/aigate-admin switch single a3

# 确认排空后锁定 dcu，随后才能维护其推理服务
sudo /data4/aigate/aigate-admin maintenance dcu

# 推理服务恢复后验证 dcu 并解除锁定；此时仍由 a3 接流
sudo /data4/aigate/aigate-admin restore dcu

# 验证并切回 dcu，等待 a3 排空
sudo /data4/aigate/aigate-admin switch single dcu
```

CLI 默认等待后台操作结束；`--no-wait` 只提交不等待，`--json` 输出结构化结果。关闭网页、CLI 退出或网络断开不会取消已经受理的后台任务。网络响应丢失时使用原 `--operation-id ID` 和相同参数查询/继续等待，不要立即换 ID 重复提交。

### 自动回切和失败判断

当前自动回切关闭。将来启用后，达到阈值会先重新验证有资格的原端，验证成功才切换后续请求；每次人工切换最多成功自动回切一次。维护锁定端不能作为回切目标。

| 现象  | 判断与处理 |
|-----|-------|
| 预热失败，`route_committed=false` | 没有切流；原端继续接新请求，修复目标端后重新验证 |
| `drain_timeout` | 已切新请求，但旧端未排空；不会强制杀旧请求。继续看状态，不能只因 CLI 非零退出就断定未切流 |
| `completed` 且原端占用为 0 | 网关内排空完成；维护前再核对其他入口和下游残留任务 |
| `drain_unknown=true` | 异常重启后旧任务是否结束不确定；人工核对非活动端后执行 `confirm-drain BACKEND` |
| HTTP 409 | 状态版本过期、冲突操作或同 ID 参数不一致；刷新状态再判断 |
| SSE 已返回 HTTP 200，随后 error/断流 | 请求仍可能失败；核对 SSE error、完整结束和日志 outcome |
| `/health` 成功、模型列表成功 | 不能替代生成和工具协议验证 |

## 7. API 自动化与普通模式的区别

### 管理 HTTP API（已废弃）

> **已废弃**：以下 `/admin/v1/*`、管理 OpenAPI、8081 监听及 Bearer 鉴权均已从源码删除，仅供历史追溯，不能继续作为自动化调用入口。当前自动化应在网关主机调用 [本机 CLI](../MANAGEMENT.md)；远程操作遵循工作区 SSH MCP 约束。独立 Profile HTTP API 不属于本次废弃范围。

管理 API 全部使用管理端口和独立 Bearer 鉴权：

| API | 用途  |
|-----|-----|
| `GET /admin/v1/status` | 活动端、revision、计数、维护与操作状态 |
| `GET /admin/v1/backends` | 脱敏后端配置及配置版本 |
| `POST /admin/v1/backends` | 新增后端 |
| `PATCH /admin/v1/backends/{id}` | 编辑已排空非活动端 |
| `POST /admin/v1/warmups` | 仅预热 |
| `POST /admin/v1/switches` | 预热、切流、排空 |
| `POST /admin/v1/backends/{id}/maintenance` | 进入维护 |
| `POST /admin/v1/backends/{id}/restore` | 恢复验证 |
| `POST /admin/v1/backends/{id}/confirm-drain` | 人工确认旧任务已结束 |
| `GET /admin/v1/operations/{id}` | 查询任务结果 |

切流请求体结构如下；`expected_revision` 必须来自刚读取的状态，示意值 123 不可直接使用：

```json
{
  "operation_id": "switch-a3-unique-id",
  "expected_revision": 123,
  "target": "a3"
}
```

预热/切流等异步操作受理返回 202；后台完成状态以操作查询为准。新增和编辑后端成功分别返回 201、200。同 ID 同完整请求可幂等重试，最近 100 项记录持久化保存。管理 OpenAPI 位于 `http://127.0.0.1:8081/openapi.json`。

### 普通模式（仍支持）

普通模式是另一个部署选项，**本历史快照的生产部署未使用**。关闭 management 并配置 `routing_file` 时，`aigate switch single BACKEND --config PATH` 原子修改路由文件，不执行预热、排空或维护管理；旧请求仍留在原端。路由文件错误或缺失会让新请求返回 503，修复后自动恢复。普通模式若只使用主配置内联 routing，则修改后需重启。不要在当前管理模式另外创建 routing.json 来切流。

## 8. 核验记录与使用边界

2026-09-13 新增监控与实时状态核对见第 9.6 节。以下是 2026-09-12 升级前一版的历史核验记录，旧版本 SHA、仅 dcu 注册表及回退脚本描述均不作为当前状态：

* systemd 为 active/running 且 enabled；实际启动使用正式 config、业务 10015、管理 8081 和 `--shutdown-timeout 31m`。
* 当前版本 SHA-256：`afec57d7bc731b0a7e86f56ee74948ee1cf9cd2cce6c76ca636946a43a67e5db`。
* 主配置、v2 后端注册表和状态均确认仅 dcu；`aigate-admin check` 通过。
* 状态中最近正式预热记录为 2026-09-12 21:13，两轮 8 次生成全部通过，覆盖 JSON、SSE、流式工具调用、工具结果续答。

已有部署验收记录：2026-09-12 21:10–21:14 正式 HTTP 检查 52/52 通过；证据在 `/data4/aigate/evidence/20260912T131100Z-upgrade/`，本地报告为 `deploy/dcu1/evidence/20260912T131100Z-upgrade/REPORT.md`。这是已记录的短请求协议验收，本次未重跑。尚未完成 DCU/A3 双端切换、长上下文、生成吞吐、大并发、浏览器表单和生产版本回退演练。

版本回退与后端切流不同：当前 `rollback.sh` 指向升级前的 Go 版本备份，恢复配套旧配置和 v1 状态，可能回退后续在线后端变更；不作为日常切流使用。

源码核对位置：`config.go`、`management_config.go`（参数与默认值），`backend_registry.go`（v2 后端配置优先级），`management_operations.go`、`warmup.go`（切流与验证），`main.go`、`admin_cli.go`（启动和 CLI），`MANAGEMENT.md`（管理使用说明）。

## 9. 请求监控与画像怎么使用

2026-09-21 已部署 DCU/A3/XPU 网关、监控查询服务和 code-eval Web：移除消息数、工具数和整条前缀分析采集链路（指纹、索引、指标、事件字段、详细计算、页面及导出字段）。旧配置的 max_index_entries/max_payload_bytes 仅兼容读取，不再参与画像处理；恢复检查点时丢弃已退休指标。上线验证与回退记录见 [发布结果](../deploy/profile-removal-20260921/STATUS.md)。

### 9.1 数据来源与入口

新增能力用于观察经过 aigate 的请求大小、usage、耗时及采集质量。它不发起模型推理、不控制切流，也不提供告警通知配置。

```text
aigate 独立 Profile 端口 /metrics
  → vmagent（每 5 秒抓取）
  → VictoriaMetrics（当前保留 30 天）
  → 独立监控查询服务
  → 测评平台「请求画像」

展开详细分析
  → 平台服务端代理
  → aigate 短期事件与异步分析 API
```

日常入口是测评平台侧栏的「请求画像」，不是网关管理页。普通图表读取 VM；只有展开补充分析和计算窗口时才访问网关事件。旧的启动/停止采集、画像数据库导入和拟合流程已移除：网关启用 profile 后随业务自动观测，平台不再建立画像数据库。

### 9.2 页面操作


1. 打开「请求画像」，选择最近 5 分钟、最近 1 小时（默认）、最近 24 小时或自定义时间。自定义输入按浏览器本地时间解释，API 使用 UTC epoch 秒。
2. 选择后端和模型，点击「刷新画像」。先核对显示的统计窗口和查询时间，再查看请求量、输入/输出 token、已报告缓存比例、长度分布、首增量与总耗时、输入/输出联合分布及趋势。
3. 查看「采集质量」：抓取完整性、事件丢弃、写入错误、usage 覆盖率及请求 outcome。空值显示为「—」，不能当作 0。
4. 查看「同时间段后端缓存监控」，用于对照 Prefill/Decode 缓存比例和 Mooncake 已分配容量。当前只对映射到 DCU 的后端有效；未映射后端会提示未配置。这里是同期聚合，不能与单条请求逐一对应。
5. 需要按到达时间汇总请求时，展开「请求详细分析 · 读取网关短期事件」。确认事件保留起止时间，选择不超过 24 小时且仍在保留范围内的窗口，点击「计算所选窗口」。
6. 等待 completed 后查看按到达时间统计的请求数、在途 pending、结果未知 unknown。
7. 点击「下载 JSON」或「下载 HTML」，导出当前成功查询的 VM 统计、同期监控及已计算的详细分析。改变窗口后需重新刷新和计算；导出只包含汇总，不含原始请求或指纹。

图表不是逐请求账本。最近数秒受抓取和异步入库影响，宜稍后刷新；查询失败时界面可能保留旧结果，必须看原查询时间，不能当作新窗口数据。

### 9.3 指标含义与统计边界

| 指标  | 正确解释 |
|-----|------|
| 请求量 | 未筛选时为网关到达数；按后端/模型筛选后为已选路请求数，两者分母可能不同 |
| 输入/输出 token | 来自后端实际 usage，在请求完成时计入；没有 usage 就保持缺失，不估算补零 |
| SSE usage | 累计 usage 用新值覆盖旧值，不对多帧重复求和 |
| 已报告缓存比例 | 仅对同时有有效 cached/input 反馈的同一批样本，计算 cached token 总和 / input token 总和；不是全部请求命中率 |
| 网关首增量 | 从请求到达网关到首次非空 content、reasoning 或 tool 增量；包含网关和转发过程，不是后端 TTFT |
| 总耗时 | 网关端完整请求耗时，完成时计入；SSE 返回 HTTP 200 后仍可能 error 或断流，需结合 outcome |
| 分位数与窗口边界 | VM 分位数来自直方图估算，5 秒采样使窗口边界近似；详细分析有独立的 as_of 截止点 |


后续本地审核修复后的口径：画像独立检查 SSE error 和完整结束；详细分析的 pending 仅包含 pending_as_of 时仍在执行的请求，缺失结束记录且不在执行的请求为 unknown。这些修复尚未纳入第 9.6 节的历史线上验收。

2026-09-13 部署验收的真实短请求没有有效 cached-token 反馈，所以该验收窗口缓存比例显示缺失；比例算法另用合成后端验证过。这不代表后端永久不支持缓存反馈。

### 9.4 配置、凭据与数据保留

profile 是主配置的顶层对象，修改后需按第 5.2 节安排网关重启，不支持页面热更新。下面仅是合并到现有主配置的片段，不能用它覆盖后端和管理配置：

```json
{
  "profile": {
    "enabled": true,
    "listen": "127.0.0.1:18082",
    "api_key_env": "AIGATE_PROFILE_KEY",
    "directory": "request-profile",
    "retention_hours": 72,
    "max_disk_bytes": 10737418240,
    "queue_events": 2048
  }
}
```

| 项目  | 当前部署 / 默认值与操作要点 |
|-----|-----------------|
| enabled | 线上已启用；未显式启用时默认关闭 |
| listen | 独立 Profile 端口 18082；默认回环地址。线上绑定的是供采集端访问的主机地址，不能把示例回环地址直接替换到跨主机部署 |
| api_key_env | AIGATE_PROFILE_KEY；独立于 AIGATE_ADMIN_KEY，所有 Profile API 和 /metrics 都需 Bearer 鉴权 |
| directory | 相对路径以主配置所在目录为基准；当前主配置旁的 request-profile 目录保存事件及分段元数据 |
| retention_hours / max_disk_bytes | 当前代码默认 72 小时 / 10 GiB；此前线上核对为 2 GiB，需更新配置并部署后才生效；时间或容量触发清理后，实际可分析历史可能少于 72 小时 |
| queue_events | 默认 2048；异步事件队列，满时丢事件并计入质量指标 |
| VM 保留 | 当前 30 天；短期事件过期不删除已写入 VM 的历史统计 |
| 指标分组 | 最多接纳 64 个后端/模型组，额外组合归入 overflow |

先将独立观察凭据配置到网关服务环境，再给 vmagent 和平台服务端分发对应受限凭据；浏览器无需也不应填写网关观察密钥。平台详细分析使用 GATEWAY_PROFILE_URL 和 GATEWAY_PROFILE_KEY_FILE；网关地址必须从平台服务端可达。vmagent 的抓取地址、鉴权和平台地址需要与 profile.listen 一致。

事件不保存消息正文或工具参数原文；保存请求字节大小、usage 和生命周期字段。原有审计日志的 question_prefix 是另一条日志路径，仍可能包含最后一条用户消息的前 10 个 Unicode 码点，不能把“不保存正文”理解为所有日志都无文本。

当前代码不再生成或读取 hmac.key。旧文件及历史事件不主动删除。事件按分钟或 4 MiB 分段，约每秒同步。正常退出写入不超过 16 MiB 的 counters-metric-v2.json 累计检查点，启动恢复后消费；运行时没有该文件可以是正常状态。它不是指标历史数据库。异常退出仍可能丢失未落盘、未抓取的尾部；旧进程未结束请求记为 unknown。

### 9.5 API 与诊断

以下接口位于独立 Profile 端口，不在业务 10015 或管理 8081 上。均使用独立观察凭据：

| API | 用途  |
|-----|-----|
| GET /metrics | Prometheus 文本格式累计指标，供 vmagent 抓取 |
| GET /profile/v1/status | schema、保留范围、队列、丢弃、写盘错误、索引状态和分析上限 |
| POST /profile/v1/analyses | 提交窗口分析，返回 202 和任务 ID |
| GET /profile/v1/analyses/{id} | 查询 queued / running / completed / failed 及结果 |

分析请求体为 {"start": UTC起始秒数, "end": UTC结束秒数, "backend": "dcu"}；model 可选。窗口采用 [start,end)，单次最多 24 小时，并受当前事件保留范围限制。轮询同一个任务 ID 至结束，不要每次轮询重新 POST。

服务内仅 1 个分析 worker，最多 4 项排队；缓存最多 16 项，任务查询有效期约为创建后 10 分钟；单次执行期限 30 秒，扫描上限 100000 请求 / 256 MiB。超过扫描上限需缩短窗口，不会通过扩大网页超时解除限制。

平台同源代理包括 GET /api/request-profile/metrics、GET /api/request-profile/context、GET /api/request-profile/source-status、POST /api/request-profile/analyses 和 GET /api/request-profile/analyses/{id}。页面已处理代理请求与轮询，无需直接暴露观察端口。

| 现象  | 检查与处理 |
|-----|-------|
| 页面 VM 画像不可用或抓取完整性异常 | 依次检查网关 Profile 状态、vmagent 目标 up、抓取鉴权和连通性、VM 入库、独立查询服务；业务 /health 成功不能证明这条链路正常 |
| 图表正常，详细分析不可用 | 检查平台服务端 GATEWAY_PROFILE_URL、凭据文件及网关事件保留范围；VM 与网关事件是不同数据源 |
| 网关直接访问 401 | 观察凭据缺失或不匹配；不能用管理密钥代替 |
| 分析 400 | 时间无效、超 24 小时、未来窗口或筛选参数无效 |
| 分析 410 | 所选窗口事件未保留或无可用事件；改到保留范围内，历史汇总继续查 VM |
| 分析 429 | 队列或任务缓存满；等待已有任务完成/缓存过期后再试 |
| 分析 404 | 任务不存在或已过期；确认 ID，必要时在事件仍保留时重新提交 |
| 分析 failed，提示扫描上限或超时 | 缩短窗口；后端筛选不保证减少文件扫描量 |
| dropped_events / write_errors 增长 | 检查队列压力、磁盘空间、目录权限及进程日志；本窗口详细分析可能不完整，不能用它补齐 VM 总数 |
| index_ready=false | 索引尚未完整预热或发生淘汰；先看质量说明，不能把低重复率解释为无复用 |
| usage 或缓存比例为 — | 核对 usage 覆盖率和后端反馈，不能手工填零或用后端同期缓存曲线替代 |

运行中的画像解析、排队和写盘问题不应改变业务响应；但启用配置时缺少凭据、目录无法初始化或监听端口被占用会导致网关启动失败，因此仍须先 check，再按重启流程验证。

### 9.6 本次核验与已完成验收

2026-09-13 11:46（Asia/Shanghai）通过只读 SSH MCP 核对：正式服务 active，Profile 已启用，schema=gateway-profile-v1，活动端 dcu、route_version=1；index_ready=true，queue_events=0、dropped_events=0、write_errors=0。这些是瞬时状态，不代表持续无丢失或完整业务验收。

已有 2026-09-13 部署报告记录：网关 race 测试及 vet、监控 23 项测试、平台 38 项回归通过；合成端到端抓取和查询、正常重启累计恢复、浏览器夹具与线上页面展示通过；最终版本真实 JSON/SSE 返回有效文本与 usage。真实请求固定窗口的平台统计为 2 个请求、输入 14 token、输出 4 token，详细分析同为 2 请求。

以上为既有验收记录，本次更新手册未重跑生成、浏览器或全链路测试，未修改配置、重启或切流。首轮升级窗口存在抓取间计数重置不可识别的边界，不能当作完整流量统计；当前证据不覆盖生产长时间压测。具体证据以本地 deploy/profile/REPORT.md 及其引用文件为准。
