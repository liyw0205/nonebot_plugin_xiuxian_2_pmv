# 安装、更新与 QQ 连接

新部署优先使用一键脚本。项目使用本地 SQLite，无需单独安装数据库服务。

## Docker

Docker 发布采用低频 base 分片和高频 plugin 单包：

```text
manifest.json
xiuxian2-base-amd64.tar.gz.part00..N
xiuxian2-plugin-latest.tar.gz
```

一键安装：

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh | bash
```

自定义目录：

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh \
  | bash -s -- install /root/xiuxian2-docker
```

更新：

```bash
# 智能判断 base/plugin
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh \
  | bash -s -- update

# 仅插件
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh \
  | bash -s -- update --plugin

# 强制 base + plugin
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh \
  | bash -s -- update --full
```

管理命令需要先将脚本保存到本地：

```bash
curl -fsSLo install_docker.sh https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh
chmod +x install_docker.sh
./install_docker.sh start
./install_docker.sh stop
./install_docker.sh status
./install_docker.sh logs
```

安装后修改 `config/.env.dev` 中的 `SUPERUSERS`。插件进程环境变量写入 `config/runtime.env`。

完整资产、手动导入和发布说明见 [`docker/README.md`](../docker/README.md)。

## Linux 一键安装

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install.sh | bash
```

自定义目录：

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install.sh \
  | bash -s -- install /root/xiuxian
```

更新：

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install.sh \
  | bash -s -- update
```

常用管理命令：

```text
xiu2 start
xiu2 stop
xiu2 restart
xiu2 status
xiu2 update-deps
xiu2 format [log_file]
```

Linux 与 Termux 的插件环境变量保存在项目根 `runtime.env`，Docker 保存在 `config/runtime.env`。例如关闭 Web 面板：

```dotenv
XIUXIAN_WEB_STATUS=false
```

保存后执行 `xiu2 restart`；Docker 执行安装脚本的 `stop` / `start` 或更新命令重建容器。Windows 请设置系统环境变量 `XIUXIAN_WEB_STATUS=false` 后重新运行“启动修仙.bat”。

## Termux 原生安装

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_termux.sh | bash
```

自定义目录：

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_termux.sh \
  | bash -s -- install "$HOME/xiuxian"
```

更新和依赖：

```bash
xiu2 update
xiu2 update-deps
termux-wake-lock
```

若使用 proot 容器，进入容器后使用 Linux 安装脚本，不要在容器中执行原生 Termux 脚本。

## Windows

一键脚本：

https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install.bat

需要 Python 3.11、Git 和可用的 NoneBot 环境。详细配置格式见 [配置与环境变量](configuration.md)。

## 手动安装原则

1. 创建 Python 3.11 虚拟环境。
2. 安装 `nb-cli` 和根目录 `requirements.txt`。
3. 创建启用 OneBot V11 与 QQ 适配器的 NoneBot 项目。
4. 将 `nonebot_plugin_xiuxian_2` 放入项目 `src/plugins/`。
5. 在 `pyproject.toml` 配置 `plugin_dirs = ["src/plugins"]`。
6. 配置 `.env` / `.env.dev` 后执行 `nb run --reload`。

不要依赖临时 `PYTHONPATH` 修复插件发现问题。

## 连接 NapCat / OneBot

NoneBot 默认监听：

```text
ws://127.0.0.1:8080/onebot/v11/ws
```

NapCat WebUI 中新增并启用“WebSocket 客户端”，填入上述地址。若 NapCat 与 xiu2 不在同一主机，将 `127.0.0.1` 替换为 xiu2 主机地址，并检查防火墙。

Web 管理面板使用独立端口 `5888`，不要与 OneBot 的 `8080` 混用。
