# DeepSeek 离线对照夹具

`manifest.json` 包含六种显式转换策略、15 个请求案例、7 个合成原始输出案例和
7 个已编码 SSE 案例。来源和断言随每项案例保存；原始输出与 SSE 分属不同入口。

两个固定请求只通过路径和既有 SHA-256 引用 `../model-admission/`，没有复制或
缩短消息、工具和输出上限。已有问题样例来自 `xpu_compat_test.go`、
`workspace_test.go`、`gateway_test.go`、`slow_requests_test.go`、
`protocol_fixture_test.go`、`sse_completion_test.go` 与
`scripts/test_model_admission.py`。参数探针和 DeepSeek 原始文本明确标为合成。

运行方法、基线语义、报告字段与扩展方法见
[离线对照工具](../../docs/DEEPSEEK_RECIPE_COMPARE.md)。固定请求的真实接入要求见
[模型门禁](../../docs/MODEL_ADMISSION.md)，本工具不替代该门禁。
