# 独立 systemd 服务

这里是当前源码的待部署模板，不表示生产已替换。`aigate-switch@入口ID` 和 `aigate-proxy@RouterID` 不相互依赖；多个切流入口可转发到一个功能服务。默认仅监听回环地址，跨主机部署时在各实例环境文件中明确填写监听地址。

先用 `scripts/build.sh` 从选定 commit 构建并记录二进制摘要，再将 `aigate-switch`、`aigate-proxy`、`aigate-replay` 三个二进制安装到 `/usr/local/bin/`，按影响范围安装模板。`version` 输出用于识别服务角色与协议，不能代替源码 commit 和构建摘要。模板使用固定系统账号 `aigate-switch`、`aigate-proxy`。部署时先创建缺少的同名系统组与无登录账号，再安装服务；账号创建命令按目标主机既有账号管理方式执行。功能服务可能从导出的配置读取上游 header，因此不能将配置文件设为公开可读。

| 实例 | 所需文件 | 创建的持久目录 |
| --- | --- | --- |
| `aigate-switch@g1` | `/etc/aigate/switch-g1.env` | `/var/lib/aigate-record`（StateDirectory，0700；仅设置 `AIGATE_RECORD_ROOT` 后写入 `<入口ID>/` 子目录） |
| `aigate-proxy@r1` | `/etc/aigate/proxy-r1.json`；可选 `/etc/aigate/proxy-r1.env` | `/var/lib/aigate-proxy-r1`（systemd StateDirectory，归 `aigate-proxy:aigate-proxy`） |
| `aigate-replay` | `/etc/aigate/replay.env`、`/etc/aigate/replay-targets.json` | `/var/lib/aigate-replay`（StateDirectory，0700，归 `aigate-switch`） |

安装文件采用以下明确权限：`/etc/aigate` 为 `root:aigate-proxy`、0751，其他服务账号可以进入目录但不能列出内容，能否读取由各文件自己的属主和权限决定（回放服务以 `aigate-switch` 运行，需要进入该目录读取目标白名单）；`proxy-r1.json` 为 `root:aigate-proxy`、0640；所有实例 `.env` 文件为 `root:root`、0600，由 systemd 以 root 读取后注入环境。切流进程无需直接读取环境文件。二进制为 `root:root`、0755，服务文件为 `root:root`、0644。`StateDirectory` 由 systemd 创建并归属功能服务账号，权限 0700；不要复用旧入口观测目录。

切流实例环境文件提供 `AIGATE_CONTROL_KEY`（至少 32 字节）、`AIGATE_ALLOWED_CIDRS`，以及需要覆盖的 `AIGATE_SWITCH_LISTEN`、`AIGATE_CONTROL_LISTEN`。启用录制时再设置 `AIGATE_RECORD_ROOT=/var/lib/aigate-record`、可选 `AIGATE_RECORD_MAX_BYTES`；录制目录参数为空时不录制。功能实例环境文件可覆盖监听地址、注入本地配置引用的上游环境变量，以及配置下述回放验签密钥；没有入站业务或观测密钥。一个主机启动多个同类实例时，各实例必须使用不同端口和独立数据根目录。

回放服务环境文件提供 `AIGATE_REPLAY_CONTROL_KEY`、`AIGATE_REPLAY_MARK_KEY`（均至少 32 字节），目标白名单文件为 `{"targets":{"r1":"http://127.0.0.1:18084"}}`，名字必须等于该功能服务的 `router_id`（平台按此名字锁定 Router，回放服务开始前核对），权限 `root:aigate-switch`、0640；部署后用 `sudo -u aigate-switch test -r /etc/aigate/replay-targets.json` 确认可读。回放服务与切流使用同一账号读取录制文件，录制目录只读挂载。

## 回放标记与多密钥验签

这是当前源码能力，是否已部署以现场二进制和进程环境为准。回放请求头仍为 `X-Aigate-Replay: <run_id>/<seq>/<hmac>`，使用 HMAC-SHA256，不新增 key ID 或改变旧签名格式。

| 环境变量 | switch / proxy 接收端 | replay 发送端 |
| --- | --- | --- |
| `AIGATE_REPLAY_MARK_KEY` | 默认信任密钥，兼容原单密钥配置 | 唯一签名密钥，仍为必填 |
| `AIGATE_REPLAY_MARK_VERIFY_KEYS` | 可选 JSON 字符串数组，增加信任密钥 | 不读取，不用它选择签名密钥 |

接收端取两项配置的并集，按原始字节去重，最多 8 把不同密钥；每把至少 32 字节，不裁剪空白。数组可以为空，也可以在不配置旧变量时独立使用。未设置或空字符串表示没有附加密钥；`null`、非数组、非字符串成员、短密钥或超限均导致启动失败。两项都未提供密钥时保持旧行为：不识别回放标记，仍按普通流量处理；这不会禁用业务录制。

任一可信密钥验签通过，proxy 即标记 `traffic="replay"`，switch 即排除该请求的业务录制。未知密钥、错误签名或畸形标记均按普通流量处理。验签不改变准入、路由、租约或在途计数规则。

例如在 systemd 的受限 `EnvironmentFile` 中配置以下结构；占位符须替换为真实密钥，不能直接使用：

```ini
AIGATE_REPLAY_MARK_KEY='<existing-key-at-least-32-bytes>'
AIGATE_REPLAY_MARK_VERIFY_KEYS='["<additional-key-at-least-32-bytes>"]'
```

systemd 会去掉值外层的单引号；其他容器环境注入方式应传入原始 JSON 数组，不把外层 shell 引号作为值的一部分。环境文件权限为 0600，密钥只通过受保护通道分发，不写入 Git、命令行、日志或普通证据文件。配置检查与 switch/proxy 启动日志仅在 stderr 输出 `replay_mark_verify_keys=<有效数量>`，不输出密钥或摘要；配置在进程启动时加载，更新文件后需重启受影响进程。

对于使用两组签名密钥的多个回放来源，让目标 switch/proxy 同时信任两组即可；每个 replay 保留自己的签名密钥及目标白名单。轮换顺序为：先部署接收端对新旧密钥的共同信任，再切换发送端签名密钥，确认旧签名请求排空且没有发送端继续使用旧密钥后，再从接收端配置中移除旧密钥。若旧密钥仍在 `AIGATE_REPLAY_MARK_KEY` 中，仅从数组移除它不会撤销信任。

## 配置检查与维护

功能配置参考 [proxy.example.json](../examples/proxy.example.json)。部署前在与正式实例相同的上游及回放验签环境变量下运行：

```sh
/usr/local/bin/aigate-proxy --check-config --config /etc/aigate/proxy-r1.json
```

检查只解析配置并输出脱敏结果，不启动监听器、不连接 Router、不创建数据目录；结果的 `boot_id` 为空、`admission=false`。模板的 `--shutdown-timeout 62m` 为业务优雅收尾上限，systemd `TimeoutStopSec=63min` 为外层上限；超时后保持平台维护并处理当前版本，不恢复旧版本。切流的 `--forward-timeout 62m` 包含上传、上游等待和向连接对端写出，修改业务最长时限时一并核对。

维护窗口和迁移顺序见 [MANAGEMENT.md](../MANAGEMENT.md)。部署操作必须报告新建服务器目录；现有历史观测目录保留原归属，不能把入口历史直接改标为某个 Router 的新数据。
