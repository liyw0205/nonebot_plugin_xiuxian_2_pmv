# 修仙 2 魔改版

适用于 QQ 群的修仙文字游戏插件，兼容 OneBot V11 与 QQ 官方适配器，并包含 Web 管理、娱乐、媒体解析、定时任务等扩展功能。

当前使用本地 SQLite 数据库，不需要部署 MySQL。

## 能力概览

- 修仙玩法：修炼、突破、宗门、秘境、悬赏、宠物、拍卖、炼丹等
- 双适配器：OneBot V11 / QQ 官方机器人
- Web 管理：配置、数据库、消息、定时任务、备份、日志和更新
- 娱乐功能：链接解析、点歌、番剧、WebDAV、NewAPI、小游戏等
- 多平台部署：Linux、Docker、Windows、Termux

## 快速安装

### Linux

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install.sh | bash
```

### Docker

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh | bash
```

Docker 发布采用 base 分片 + plugin 单包；日常更新通常只下载 plugin：

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh \
  | bash -s -- update

curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh \
  | bash -s -- update --plugin

curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh \
  | bash -s -- update --full
```

### Termux

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_termux.sh | bash
```

Windows 可下载并运行：

```text
https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install.bat
```

完整安装、更新和 NapCat 连接说明见 [安装文档](docs/installation.md)。

## 最小配置

在 NoneBot 项目的 `.env.dev` 中配置：

```dotenv
LOG_LEVEL=INFO
SUPERUSERS = ["管理员ID"]
COMMAND_START = [""]
NICKNAME = ["修仙"]
DEBUG = false
HOST = 127.0.0.1
PORT = 8080
```

- `HOST` / `PORT` 是 NoneBot 与 OneBot WebSocket 端口。
- 修仙 Web 管理面板默认监听 `0.0.0.0:5888`，由 `xiuxian_config.py` / Web 配置页管理。
- 插件环境变量、QQ 官方机器人配置、配置优先级和高级 NoneBot 字段见 [配置文档](docs/configuration.md)。

NapCat 的 WebSocket 客户端默认连接：

```text
ws://127.0.0.1:8080/onebot/v11/ws
```

## Web 管理面板

默认地址：

```text
http://服务器地址:5888/
```

使用 `SUPERUSERS` 中任一 ID 登录。公网开放前请配置 HTTPS、防火墙和 Host 白名单。

主要页面包括：

| 功能 | 路径 |
|:-----|:-----|
| 首页 | `/` |
| 配置管理 | `/config` |
| 消息面板 | `/messages` |
| 数据库 | `/database` |
| 定时任务 | `/scheduler` |
| 备份管理 | `/backups` |
| 日志 | `/logs` |
| 更新 | `/update` |

详见 [Web 管理面板](docs/web_panel.md)。

## 使用入口

```text
修仙帮助
修仙手册
娱乐帮助
小游戏帮助
```

高频玩法见 [常用玩法说明](docs/gameplay_notes.md)，娱乐扩展见 [娱乐功能](docs/entertainment.md)。

## 文档

### 部署与运维

| 文档 | 说明 |
|:-----|:-----|
| [安装、更新与 QQ 连接](docs/installation.md) | Linux / Docker / Windows / Termux / NapCat |
| [配置与环境变量](docs/configuration.md) | NoneBot、插件配置、环境变量与优先级 |
| [Docker](docker/README.md) | base + plugin 发布结构、安装和更新 |
| [Web 管理面板](docs/web_panel.md) | 页面、权限、安全和定时任务 |
| [链接解析](docs/media_parser.md) | 支持平台、发送策略、代理与排错 |

### 玩法与数据

| 文档 | 说明 |
|:-----|:-----|
| [常用玩法](docs/gameplay_notes.md) | 高频玩法与服主注意事项 |
| [娱乐功能](docs/entertainment.md) | 趣味接口、WebDAV、番剧、NewAPI |
| [物品 ID](docs/items.md) | 类型、ID 范围和品阶 |
| [物品系统](docs/buff.md) | 功法、神通、装备、丹药和材料 |

### 贡献与架构

| 文档 | 说明 |
|:-----|:-----|
| [开发与交付](CONTRIBUTING.md) | 测试、提交边界和发布要求 |
| [插件模块索引](nonebot_plugin_xiuxian_2/xiuxian/README.md) | 目录职责 |
| [跨适配器兼容层](docs/adapter_compat.md) | 消息段、事件和发送门面 |
| [Matcher 路由兼容层](docs/on_compat.md) | 路由机制和注册约束 |
| [消息通道约定](docs/message_channel.md) | 文案与发送通道 |
| [数据与 Web 治理](docs/database_web_governance.md) | SQLite、迁移和 Web 安全 |

## 数据目录

运行数据默认位于：

```text
data/xiuxian/
```

包括 SQLite 数据库、消息记录、缓存、备份和运行期 JSON。数据库、缓存、日志、备份、Bot token、secret、用户 ID 和群 ID 均不得提交到 Git。

## 依赖与更新

启动时会按根目录 `requirements.txt` 检查缺失依赖。手动更新：

```bash
source ~/myenv/bin/activate
python -m pip install -r requirements.txt
```

一键安装用户也可执行：

```text
xiu2 update-deps
```

## 鸣谢

- [NoneBot2](https://github.com/nonebot/nonebot2)
- [nonebot/adapter-qq](https://github.com/nonebot/adapter-qq)
- [nonebot_plugin_xiuxian](https://github.com/s52047qwas/nonebot_plugin_xiuxian)
- [nonebot_plugin_xiuxian_2](https://github.com/QingMuCat/nonebot_plugin_xiuxian_2)
- [nonebot_plugin_xiuxian_2_pmv](https://github.com/MyXiaoNan/nonebot_plugin_xiuxian_2_pmv)
- [yt-dlp](https://github.com/yt-dlp/yt-dlp)

## 许可证

本项目采用 [MIT License](https://choosealicense.com/licenses/mit/)。
