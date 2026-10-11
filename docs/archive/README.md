# 历史版本查阅

当前网关使用 [项目说明](../../README.md) 与 [平台控制 v2](../../MANAGEMENT.md)。已退役的 v1 配置、CLI、维护切换方案及旧部署快照通过 Git 查阅，不再在工作树保存旧版正文。

在仓库根目录执行：

```sh
git show f46ea5d289d3:docs/archive/v1/README.md
git show f46ea5d289d3:docs/archive/v1/MANAGEMENT.md
git show f46ea5d289d3:MAINTENANCE_SWITCH_PLAN.md
git show f46ea5d289d3:docs/OUTLINE_GATEWAY_OPERATIONS.md
git show f46ea5d289d3:docs/OUTLINE_XPU_DEPLOYMENT.md
```

上述提交保留清理前的原文；其中日期、状态、旧命令及回退过程只对应当时版本。

历史发布脚本、manifest 和原始证据维持原路径，避免破坏当次版本的审计关系。
