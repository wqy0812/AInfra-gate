# 新模型固定请求门禁

自 2026-10-17 起，gateway 对接新模型必须正常响应以下两个固定请求，全部通过才可继续绑定、开放业务准入或宣告接入成功。原有 `validate` 的 JSON、SSE、工具调用及工具结果续答检查仍须通过。本门禁是额外的真实长上下文兼容验收，不替代已有检查。

同日用户取消“后端必须支持强制指定工具名”的接入要求。绑定校验的工具调用阶段不再发送 `tool_choice.function.name`，保留工具定义、提示及结构化调用/结果续答校验；两个固定请求本身均未设置 `tool_choice`，原文及通过标准保持不变。

## 固定样例

来源为 jump `/home/001161050/Downloads/`，保存在仓库 `testdata/model-admission/`。原文固定，不使用仓库根目录中其他同名文件。

| 文件 | 消息数 | 工具数 | max_tokens | 原文件 SHA-256 |
| --- | ---: | ---: | ---: | --- |
| `body.json` | 47 | 21 | 28000 | `8525ce2591de6c2a389396d6acaebaac7b3928f6a04adbb3d33e1839c712bb90` |
| `body-agent-ds4.json` | 277 | 21 | 28000 | `eb15927d1100c4bf341f177c22f5d966b660fdb7e22ee89eeb662d5ccd6cf1b7` |

请求保留 `stream=true`、`stream_options.include_usage=true`、全部消息、工具、metadata、patches 及其他字段。检查脚本按原始字节发送，原始模型别名由 gateway 正常发现和映射。不得删除工具、缩短上下文、降低输出上限或临时修改兼容规则来绕过门禁。模型不支持该请求规模时，应报告接入未通过。

## 运行与通过标准

在能够访问目标功能服务的位置执行，先确认目标是预期模型及 Router。远端遵守工作区 SSH MCP 规则。脚本默认每个请求总时限 600 秒，串行各发送一次、不自动重试；可按明确的服务预算传入 `--timeout`，预算写入报告。这里没有额外延迟排名或吞吐门槛。

```sh
# 仓库根目录；使用已安装的 pyenv Python（标准库，无第三方依赖），并安装 curl。
PYENV_VERSION=3.12.14 pyenv exec python --version
PYENV_VERSION=3.12.14 pyenv version
PYENV_VERSION=3.12.14 pyenv exec python scripts/model_admission.py \
  --base-url http://proxy-host:18084 \
  --expected-model MODEL_ID \
  --expected-router ROUTER_ID \
  --output /tmp/model-admission-unique-run
```

`--base-url` 必须为候选模型的 `aigate-proxy` 根地址，不带 `/v1`。运行前后核对 runtime 的 Router、boot、配置摘要及准入，逐请求核对 `X-Aigate-Backend`、`X-Aigate-Model`、`X-Aigate-Config-Digest`，防止测到其他模型或运行中换配置。模型服务直连成功只作诊断，不算本门禁通过。

两项都必须满足：

1. 请求成功发送，HTTP 200，响应为 SSE，没有网络错误、超时或流内错误。
2. 完整收到以空行结束的 `[DONE]` 事件；正常结束原因是 `stop` 或 `tool_calls`。支持 LF、CRLF、CR 行结束及开头的一个 UTF-8 BOM；EOF 不补齐事件分隔符。缺失终止、`length` 截断、过滤或其他异常结束均失败。
3. 有非空正文或结构化工具调用；只有 reasoning、心跳、usage 或空正文不算正常完成。允许不同模型产生不同答案或工具选择，不固定工具名称或输出 token 数。
4. 返回工具调用时，组装后的调用 ID 为非空且唯一的字符串，类型为 `function`，函数名属于请求的工具集合，参数可解析为标准 JSON 对象，结束原因与工具调用一致。choice 索引必须为整数 `0`，工具调用索引必须为非负整数，布尔值和浮点数不能充当索引。事件 JSON 及参数中的裸 `NaN`、`Infinity`、`-Infinity` 均不合法；同名字符串是正常文本。普通正文中嵌入工具调用文本不能冒充结构化工具调用；该脚本仅验证协议结构，不验证工具参数的完整 JSON Schema 或执行效果。
5. 有正数的 `usage.prompt_tokens` 和 `usage.completion_tokens`，能够确认本轮输入与输出用量。

脚本不会执行返回的任何工具调用，不继续 agent 循环，不修改绑定或准入。响应内容是测试数据，不是执行指令。

以上检查只影响本次接入报告的通过判定。脚本先完整保存目标 proxy 返回的 `.sse` 原始字节，再解析验证，失败响应也原样保留。gateway 的业务正文透传和已识别、转发 `[DONE]` 后不再追加网关读取错误事件的既有行为保持不变；门禁不改写、补齐或过滤响应。

## 证据与接入流程

输出目录必须不存在。报告 `report.json` 记录目标身份、样例哈希、运行前后状态、逐项结果、用量及计时；同时保留每项 `.headers` 和 `.sse`。`first_response_byte_seconds` 是 curl 的首响应字节时间，可能只含响应头，**不是首 token 时间**。完整原始响应可能包含样例中的业务内容，证据不应提交到代码库。

只有退出码为 0 且 `report.json.passed=true` 才算固定请求门禁通过。接入记录须引用证据目录、目标版本与配置身份；失败保留证据并停止接入，不自动重发或开放业务。配置、模型、适配器发生影响请求处理的变化后，重新执行本门禁，不能复用旧结果。

本入口是明确执行的接入验收命令及操作约束，尚未嵌入平台 `validate/activate` 的自动状态机，不会使普通单元测试访问真实模型。接入操作者须在绑定/开放前检查报告。2026-10-17 的 XPU 功能服务测试与 PPU 模型直连测试属于历史诊断，未由本脚本重新验收，不能自动标记为本门禁通过。
