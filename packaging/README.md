# 独立 systemd 服务

这里是当前源码的待部署模板，不表示生产已替换。`aigate-switch@入口ID` 和 `aigate-proxy@RouterID` 不相互依赖；多个切流入口可转发到一个功能服务。默认仅监听回环地址，跨主机部署时在各实例环境文件中明确填写监听地址。

先用 `scripts/build.sh` 从选定 commit 构建并记录二进制摘要，再将两个二进制安装到 `/usr/local/bin/`，按影响范围安装模板。`version` 输出用于识别服务角色与协议，不能代替源码 commit 和构建摘要。模板使用固定系统账号 `aigate-switch`、`aigate-proxy`。部署时先创建缺少的同名系统组与无登录账号，再安装服务；账号创建命令按目标主机既有账号管理方式执行。功能服务可能从导出的配置读取上游 header，因此不能将配置文件设为公开可读。

| 实例 | 所需文件 | 创建的持久目录 |
| --- | --- | --- |
| `aigate-switch@g1` | `/etc/aigate/switch-g1.env` | `/var/lib/aigate-record`（StateDirectory，0700；仅设置 `AIGATE_RECORD_ROOT` 后写入 `<入口ID>/` 子目录） |
| `aigate-proxy@r1` | `/etc/aigate/proxy-r1.json`；可选 `/etc/aigate/proxy-r1.env` | `/var/lib/aigate-proxy-r1`（systemd StateDirectory，归 `aigate-proxy:aigate-proxy`） |
| `aigate-replay` | `/etc/aigate/replay.env`、`/etc/aigate/replay-targets.json` | `/var/lib/aigate-replay`（StateDirectory，0700，归 `aigate-switch`） |

安装文件采用以下明确权限：`/etc/aigate` 为 `root:aigate-proxy`、0750；`proxy-r1.json` 为 `root:aigate-proxy`、0640；所有实例 `.env` 文件为 `root:root`、0600，由 systemd 以 root 读取后注入环境。切流进程无需直接读取环境文件。二进制为 `root:root`、0755，服务文件为 `root:root`、0644。`StateDirectory` 由 systemd 创建并归属功能服务账号，权限 0700；不要复用旧入口观测目录。

切流实例环境文件提供 `AIGATE_CONTROL_KEY`（至少 32 字节）、`AIGATE_ALLOWED_CIDRS`，以及需要覆盖的 `AIGATE_SWITCH_LISTEN`、`AIGATE_CONTROL_LISTEN`。启用录制时再设置 `AIGATE_RECORD_ROOT=/var/lib/aigate-record`、可选 `AIGATE_RECORD_MAX_BYTES`，以及与功能服务、回放服务相同的 `AIGATE_REPLAY_MARK_KEY`；为空时不录制。回放服务环境文件提供 `AIGATE_REPLAY_CONTROL_KEY`、`AIGATE_REPLAY_MARK_KEY`（均至少 32 字节），目标白名单文件为 `{"targets":{"candidate-a":"http://127.0.0.1:18084"}}`，权限 `root:aigate-switch`、0640。回放服务与切流使用同一账号读取录制文件，录制目录只读挂载。功能服务环境文件配置同一 `AIGATE_REPLAY_MARK_KEY` 后才会为回放请求标 `traffic=replay`。环境文件权限使用 0600，不把密钥写入命令行或日志。功能实例环境文件只需覆盖监听地址或注入本地配置引用的上游环境变量；没有入站业务或观测密钥。一个主机启动多个同类实例时，各实例必须使用不同端口和独立数据根目录。

功能配置参考 [proxy.example.json](../examples/proxy.example.json)。部署前在与正式实例相同的上游环境变量下运行：

```sh
/usr/local/bin/aigate-proxy --check-config --config /etc/aigate/proxy-r1.json
```

检查只解析配置并输出脱敏结果，不启动监听器、不连接 Router、不创建数据目录；结果的 `boot_id` 为空、`admission=false`。模板的 `--shutdown-timeout 62m` 为业务优雅收尾上限，systemd `TimeoutStopSec=63min` 为外层上限；超时后保持平台维护并处理当前版本，不恢复旧版本。切流的 `--forward-timeout 62m` 包含上传、上游等待和向连接对端写出，修改业务最长时限时一并核对。

维护窗口和迁移顺序见 [MANAGEMENT.md](../MANAGEMENT.md)。部署操作必须报告新建服务器目录；现有历史观测目录保留原归属，不能把入口历史直接改标为某个 Router 的新数据。
