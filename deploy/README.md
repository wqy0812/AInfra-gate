# 部署源码与本地运行产物

Git 保留网关源码及测试、配置示例、部署/验收/回退脚本、Kubernetes Service 定义和人工维护的运维说明。脚本属于源码，即使只服务于一次发布，也应保留以便审计；日期目录内的脚本仍受当次发布的主机、路径、配置和版本前提约束。

以下文件留在本地或服务器证据目录，不进入新的 Git 提交：

- `bin/`、`dist/` 和发布二进制、源码归档。
- `deploy/**/evidence/`、`deploy/**/source-manifest.json`，以及 `deploy/profile/` 中的 `benchmark.txt`、`e2e-result.json`、`gateway-final-state.json`、`gateway-live.json`。
- 真实配置、`.env` / `*.env` 凭据、管理状态及锁、`.aigate/` socket、`request-profile/`、`slow-requests/` 原始请求、备份和日志。
- Python 字节码、`__pycache__/`、测试产物及系统缓存。

`*.example.json`、`openapi.json`、测试夹具不能被全局 JSON 忽略规则隐藏。自定义运行数据目录放在仓库之外。历史报告中的证据链接指向本地保留文件，干净克隆不包含这些证据。移除 Git 跟踪时保留本地文件，也不重写已有提交历史。

## 指标版本与脚本依赖

[profile_checks.py](profile_checks.py) 是 A3 直连发布、DCU 慢请求发布及实时验收脚本的公共依赖。经 SSH MCP 上传发布工具时，**同时将它上传到执行脚本所在目录**，保持该次工具集合版本一致。仓库内运行时自动从 `deploy/` 导入。不要用当前脚本覆盖历史发布目录中的原版回退工具；历史基线应配套使用当时的脚本。

脚本根据实际指标起点识别三个口径：

| 口径 | 起点标签 | 正常退出检查点 |
| --- | --- | --- |
| legacy | 无 `request_scope` | `counters.json` |
| streaming | `request_scope="streaming"` | `counters-streaming.json` |
| metric-v2 | `request_scope="all"` | `counters-metric-v2.json` |

同口径重启必须保持起点、累计计数与直方图；口径升级必须建立新起点，并完整保留其他口径检查点。历史检查点不会被解释为新版计数。缺失、重复或无效指标使验证失败，缺失在途数不能解释为空闲。阶段验收分别检查旧版 9 个阶段和新版 8 个阶段的完整集合。

本地校验命令：

```sh
python3 -B -m unittest discover -s deploy -p 'test_*.py' -v
go test -race -timeout 60s ./...
go vet ./...
git diff --check
```

本地脚本夹具使用临时文件及模拟指标，不调用服务器、systemd、Docker 或 Kubernetes。线上发布仍须遵守工作区 SSH MCP 约束，并在新增服务器目录前显式告知路径；源码入库和本地测试不代表已部署或已完成真实服务验收。
