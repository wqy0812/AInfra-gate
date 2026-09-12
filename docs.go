package main

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.json
var openAPI []byte

func serveOpenAPI(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(openAPI)
}

func serveDocs(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AI JSON Gateway</title><style>body{max-width:850px;margin:48px auto;padding:0 24px;font:16px/1.7 system-ui;color:#17212b}code,pre{background:#f2f4f6;border-radius:6px;padding:4px}pre{padding:16px;overflow:auto}h2{margin-top:32px}</style><h1>AI JSON Gateway</h1><p>提供 API 兼容层和 A/B 切换的模型网关，不支持负载均衡。<a href="/openapi.json">下载 OpenAPI 规范</a></p><h2>POST /v1/chat/completions</h2><p>Content-Type: application/json。启用入口鉴权时使用 Authorization: Bearer &lt;key&gt;。</p><pre>{"messages":[{"role":"user","content":"你好"}],"stream":true}</pre><p>messages 必须为非空数组；model 可省略，始终由下游模型发现结果覆盖。支持工具调用、扩展字段和 stream_options。配置 public_model_name 后，普通响应与 SSE 聊天事件使用统一对外 model，下游请求仍使用真实名称。未配置时 SSE 原样转发；模板适配器仅支持非流式请求。</p><h2>GET /v1/models</h2><p>只返回当前活动后端的选定模型；配置 public_model_name 时返回该名称，仍要求模型发现成功。鉴权与聊天入口相同。</p><h2>GET /health</h2><p>返回 {"status":"ok"}，仅表示网关已启动。</p><h2>维护控制台</h2><p>启用 management 后，独立管理监听器提供预热、切流、排空、维护锁和自动回切。默认管理地址为 http://127.0.0.1:8081/，需要独立管理密钥；管理 OpenAPI 位于该监听器的 /openapi.json。原端排空并锁定成功后再重启服务。</p><h2>错误与追踪</h2><p>响应携带 X-Request-ID；错误包含 error.code、error.message、error.request_id。</p><p>400 请求格式或适配错误；401 鉴权失败；429 下游限流；502 下游 HTTP、连接或协议错误；503 路由或模型发现不可用；504 生成超时。流开始后发生错误会发送 error 数据事件并关闭流。</p></html>`))
}
