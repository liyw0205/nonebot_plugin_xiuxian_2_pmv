# 远端后端冒烟

`docs/refactor_architecture.md` 的 8.3 要求在受控服务器上按“备份 -> 迁移
dry-run -> 隔离旧实例 -> 启动 -> 健康检查 -> 只读命令 -> 可回滚写操作 ->
查看 operation ledger -> 恢复旧实例”的顺序验收。仓库提供
`scripts/remote_smoke.sh`，但不会猜测远端项目目录、服务管理器或进程名。

执行前必须显式设置 `REMOTE_PROJECT_DIR`、`REMOTE_DATA_DIR`、旧实例停止命令、
新实例启动/停止命令、可逆写命令和回滚命令。所有命令通过
`/data/user/0/com.termux/files/home/.ssh/lmm-server.sh` 传输，脚本不会读取、
打印或复制 wrapper 内的密码/token。

远端 Python 默认使用 `python3`（可用 `REMOTE_PYTHON` 覆盖）。hook 在远端项目目录
执行，并可读取脚本注入的 `XIUXIAN_DATA_DIR`、`SMOKE_OPERATION_ID` 和
`BACKUP_PATH` 环境变量。

```sh
SERVER=/data/user/0/com.termux/files/home/.ssh/lmm-server.sh \
REMOTE_PROJECT_DIR=/srv/xiuxian \
REMOTE_DATA_DIR=/srv/xiuxian-smoke-data \
REMOTE_STOP_COMMAND='systemctl stop xiuxian-old.service' \
REMOTE_START_COMMAND='systemctl start xiuxian-new.service' \
REMOTE_STOP_NEW_COMMAND='systemctl stop xiuxian-new.service' \
REMOTE_WRITE_COMMAND='python3 scripts/remote_smoke_write.py' \
REMOTE_ROLLBACK_COMMAND='python3 -m nonebot_plugin_xiuxian_2 restore --backup "$BACKUP_PATH" && systemctl start xiuxian-old.service' \
scripts/remote_smoke.sh
```

`REMOTE_WRITE_COMMAND` 必须只修改专用 smoke 账户/数据，且
`REMOTE_ROLLBACK_COMMAND` 必须能恢复备份并启动旧实例。没有真实远端项目目录
时脚本会明确失败；本机恢复演练使用 `scripts/recovery_smoke.py`，不能替代远端
证据。

若 hook 直接后台启动进程，必须显式关闭其标准输入（例如
`nohup ... </dev/null >service.log 2>&1 &`），否则 SSH wrapper 会等待后台进程
继承的 stdin，冒烟脚本会停在“启动新实例”步骤。

## 受控主机复现

`scripts/remote_smoke_hooks/` 提供一套可直接复现的 hook 与 wrapper 替身，用于在没有
生产 SSH 通道时对**隔离主机**（例如容器）执行同一份 `scripts/remote_smoke.sh` 流程：

- `ssh_wrapper.sh`：以 `docker exec` 作为传输层，替代 `lmm-server.sh` 的角色；它只
  转发命令，不携带凭据。
- `provision.sh`：迁移 smoke 数据目录、复制一份作为“旧部署”、在 5897 端口启动旧实例
  并等待 `/health/ready`，使 stop/rollback hook 有真实服务可停可启。
- `start.sh` / `stop.sh`：以 PID 文件管理实例，后台启动显式 `</dev/null`，停止幂等。
- `rollback.sh`：调用 `restore --backup`（内部逐文件校验 `sha256`）恢复数据库，并撤销
  冒烟自身创建的可逆写入，然后重启旧实例。
- `run.sh`：把上述 hook 接入 `scripts/remote_smoke.sh` 并执行。

```sh
docker run -d --name xiuxian-remote -v "$PWD/..:/srv" python:3.11-slim sleep infinity
sh scripts/remote_smoke_hooks/provision.sh
scripts/remote_smoke_hooks/run.sh
```

该路径产生的是受控主机上的真实运行证据（真实进程、真实端口、真实备份与恢复），
但它**不是**生产发布周期，因此不能作为 P7 关闭兼容层的依据；P7 仍需真实部署实例完成
完整发布周期后的旧 import/旧 URL 零命中日志。
