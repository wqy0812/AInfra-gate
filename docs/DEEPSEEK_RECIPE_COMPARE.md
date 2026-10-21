# DeepSeek 离线兼容性对照

已实现的开发诊断工具：`scripts/deepseek_recipe_compare.py` 使用固定的
`deepseek-recipe==0.2.0`，对照仓库中的请求、当前 Go 适配器和 SSE 观察器。
消费者是维护 AIGate 兼容规则、排查工具续答和升级 DeepSeek 后端的开发者。
失败场景包括字段意外丢失、thinking 策略改变、历史 reasoning 缺失、工具参数
分片组装错误和 SSE 终止不完整。只看健康检查或简单问答无法覆盖这些差异。

工具不接入 switch/proxy/replay 的启动、构建或部署路径，不读取线上配置，
不访问模型、执行工具、监听 HTTP、下载 tokenizer 或拉取图片。
它只使用本地文件、官方库的转换/渲染/文本解析，以及测试专用的 Go 文件桥接。
原有[真实模型固定请求门禁](MODEL_ADMISSION.md)继续独立使用。

## 安装与运行

在仓库根目录使用已安装的 pyenv Python。Python 要求 3.10+；以下为本地验证版本：

```sh
PYENV_VERSION=3.12.14 pyenv exec python --version
PYENV_VERSION=3.12.14 pyenv version
PYENV_VERSION=3.12.14 pyenv exec python -m venv .venv
.venv/bin/python -m pip install -r scripts/requirements-deepseek-recipe.txt
.venv/bin/python scripts/deepseek_recipe_compare.py \
  --output dist/deepseek-recipe/first
```

安装阶段可以下载官方 wheel。requirements 固定版本并校验五种已发布平台
wheel 的 SHA-256，仅允许二进制 wheel，不自动源码构建。没有匹配 wheel 的
平台会安装失败；运行命令不会安装依赖或降级版本。Python distribution 与
native 扩展必须都报告 0.2.0。

对照运行需要本机 Go 工具链和当前完整仓库。Python 调用：

```sh
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local \
  go test -mod=vendor -run '^TestDeepseekRecipeOfflineBridge$' -count=1 .
```

输入、输出通过临时 JSON 文件传递；不启动网关或模拟 HTTP 服务。桥接入口
只调用 `parseChat`、`makePayload`、`sseObserver`。普通 Go 测试中，此入口
没有两个文件环境变量时直接跳过。Python 没有重写 Go 兼容规则。

输出目录必须不存在，避免覆盖旧证据：

- `report.json`：全部策略、来源、输入哈希、字段差异、语义摘要和断言。
- `report.md`：便于阅读的案例概览与基线差异路径。

退出码：`0` 为全部断言符合预期且没有基线语义差异；`1` 为断言失败或存在
基线语义差异，仍保存完整报告；`2` 为安装、输入哈希、清单、Go 桥接等运行错误。

## 夹具与策略

`testdata/deepseek-recipe/manifest.json` 是唯一案例清单，包含：

| 类别 | 数量 | 来源与目的 |
| --- | ---: | --- |
| 固定长请求 | 2 | 直接引用 `testdata/model-admission/` 原文件并校验既有 SHA-256，不复制或修改 |
| 已有请求问题 | 5 | mixed metadata、非法 metadata、workspace 局部编码、受保护 overrides、完整工具结果续答 |
| 参数探针 | 8 | 显式 thinking 与 reasoning effort 的优先级、默认值及映射 |
| 原始模型文本 | 7 | 正文/Unicode、reasoning、reasoning-only 截断、DSML、reasoning 后调用工具、length、filter |
| 已有 SSE 问题 | 7 | 工具名/参数分片、reasoning-only、缺失或未闭合 DONE、完成后输出、重复 DONE、NaN 参数 |

已有问题从当前 Go/Python 回归中提取最小案例，每项记录 `source.path`、
`source.test`、`source.note`。`tool_result_continuation` 保留已有
`slowTestBody` 的完整合成请求。参数探针和原始 DeepSeek 文本明确标为合成，
不冒充线上记录。既有 SSE 只送入实际 Go 观察器和 `model_admission.validate_sse`；
它不是原始模型输出，不送入官方 `StreamProcessor`。

清单显式配置六种策略：V4 / V4.1 各有默认 thinking + FillEmpty、默认 thinking
+ Reject、默认关闭 thinking + FillEmpty。这里“默认”指请求没有显式设置时的
回退值；请求中的 thinking / reasoning effort 仍由官方库决定。

每种策略列出模型别名和目标模型版本。报告还记录官方内置模型名映射、token
上限、web search 默认策略及完整特殊 token 拼写。所有别名仅为评估映射，不
据此确认部署中的真实模型身份。固定请求的原始别名映射为相应评估版本，Go
适配结果中的 `recipe-upstream` 也映射为同一版本。

## 报告如何解释

请求报告分别保留 `official.<policy>.original` 与 `adapted`：

- `adaptation_changes` 是当前 Go 适配器实际改变的请求字段，包含 JSON Pointer
  路径和前后值；较长值使用 JSON 摘要。
- `official.<policy>.adaptation_differences` 比较原始/适配后的官方转换结果，
  展示该改动是否影响消息顺序、工具、推理参数、解析选项或 Prompt。
- `policy_differences` 比较各策略的适配后结果，以清单中的第一个策略为参照。
- `checks` 是该案例的预期契约，包含检查路径、期望值、实际值和结果。

官方转换摘要中的消息文本、reasoning、工具参数和 Prompt 使用长度与 SHA-256；
适配字段差异保留短值，较长值使用 JSON 摘要。工具名称、调用关联 ID、顺序、schema 哈希、输出
约束、推理参数可直接比较。报告记录输入原文哈希和相关源码摘要，避免仅凭
Git HEAD 判断未提交代码的身份。

原始文本案例同时按整段和逐字符输入官方解析器，将生成的协议事件交给真实
Go 观察器。规范化结果忽略生成的 ID 值、时间戳和事件分片边界，但校验调用 ID
非空且唯一，并按索引保留调用关联。参数按严格 JSON 值比较，不忽略 Unicode、
正文、reasoning、refusal 或完成原因。规范化要求工具类型为 `function`，拒绝
冲突的调用 ID/类型、非法索引、错误事件及结束后输出，防止归一化掩盖协议损坏。
调用方显式添加 SSE `data:` framing 和 `[DONE]`；
这些是本地测试适配规则，不视为官方库提供 HTTP 传输。计数固定为合成的 0，
不能用于真实 usage 比较。

预期拒绝并不使整项运行失败。例如固定请求没有历史 reasoning，默认 FillEmpty
可以转换，Reject 策略会拒绝；SSE 完整但只有 reasoning 时，业务观察器可以认为
完整，而真实接入门禁的离线解析会拒绝。清单对这些区别分别断言。差异不是
自动归责结果，仍需区分客户端契约、网关配置、后端策略和版本。

## 可复现的基线比较

```sh
.venv/bin/python scripts/deepseek_recipe_compare.py \
  --baseline dist/deepseek-recipe/first/report.json \
  --output dist/deepseek-recipe/second
```

`baseline.differences` 比较完整 `snapshot`，包括增删案例、输入哈希、策略和
各项规范化结果。相同输入和代码的重复运行应为零差异；新增、删除、修改夹具
或策略会明确显示差异，并返回 1，需人工评审后选择新基线。工具不自动更新基线。
显式提供的基线必须是合法报告对象；`null`、数组或缺少案例结构的基线均返回 2，
不会跳过比较。清单字段类型及检查路径也会在执行前验证，非法清单返回 2。

`baseline.provenance_differences` 另列 Python、Go、平台、native 扩展、Git HEAD、
相关源码和清单摘要变化，不直接判为语义回归。跨平台浮点表示等差异仍如实显示；
需要精确复现时使用报告中的相同 Python/native wheel 和工具链。

## 验证与新增案例

```sh
.venv/bin/python -m unittest discover -s scripts -p 'test_deepseek_recipe_compare.py' -v
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go test -mod=vendor \
  -run 'TestDeepseekRecipeOfflineBridge|TestCompatibilityAndOverrides|TestJSONStringMetadataHTTP|TestWorkspaceCompatibilityHTTP|TestSSECompletionBoundaries' .
```

新增案例时选择请求、原始模型文本或已编码 SSE 中对应的一类，填写最小输入、
原问题来源和非空 `checks`，复用或新增明确的转换策略。文件型请求必须声明
仓库相对路径和 SHA-256；工具不会悄悄更新哈希。添加真正新的故障契约时同步
增加断言，并在拥有该行为的 Go/Python 模块保留对应回归。

本阶段未验证实际后端 Prompt/token IDs、真实模型输出、工具执行、上下文容量、
线上 HTTP/SSE 传输或性能；离线通过不改变任何绑定或准入状态。
