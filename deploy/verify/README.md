# 部署 HTTP 检查

纯 Go 验收工具，不写入管理配置。检查业务与管理 HTTP 接口、鉴权、跨域保护、非法请求、模型发现、普通 JSON/SSE 和 32 个并发健康请求。管理密钥从 `AIGATE_ADMIN_KEY` 读取；业务入口启用鉴权时从 `GATEWAY_API_KEY` 读取（若服务使用其他环境变量名，请将其值映射至这个变量）。密钥不写入结果。

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/aigate-verify-linux-amd64 ./deploy/verify
./bin/aigate-verify-linux-amd64 --base http://127.0.0.1:10015 --admin http://127.0.0.1:8081 > http-verification.json
```

正常模式发送两次合成生成请求，包含对象形式的 `metadata.workspace`。失败退出非零；通过仍须另行执行 `aigate warmup dcu --config ...`，验证流式工具调用和工具结果续答。该工具没有覆盖长上下文、生成吞吐、所有模型副本或浏览器表单交互。

下游模型发现已确认不可用时，可增加 `--expect-unavailable` 检查模型发现和生成入口是否正确返回 503。结果标记为 `unavailable-backend-diagnostic`；该模式通过仅证明错误处理符合预期，不能作为上线或模型可用性验收。
