# 配置与环境变量

本文区分 NoneBot 配置、插件持久配置和进程环境变量。不要把三类配置混在同一个文件里。

## NoneBot 配置

NoneBot 配置写在运行项目的 `.env` / `.env.dev`，常用项：

```dotenv
LOG_LEVEL=INFO
SUPERUSERS = ["管理员ID"]
COMMAND_START = [""]
NICKNAME = ["修仙"]
DEBUG = false
HOST = 127.0.0.1
PORT = 8080
```

`HOST` / `PORT` 是 NoneBot 与 OneBot WebSocket 端口，不是修仙 Web 管理面板端口。

QQ 官方机器人使用 NoneBot 的 `QQ_BOTS` 配置。Web 面板 `/config` 同时提供二维码和“快捷绑定”：快捷按钮在用户点击事件中打开 QQ 官方 connect 页面并申请跳转 QQ；QQ 返回 `status=2` 后面板立即确认完成、停止轮询并原子写入配置。面板会将 `QQ_BOTS` 替换为本次扫码机器人，固定 `use_websocket=true`，并自动补全 `c2c_group_at_messages`、`direct_message` 事件订阅，避免配置为空导致 READY 后收不到群聊艾特或私聊事件。原 `.env.dev` 会备份，凭据不会显示在页面或日志中。完成后显示常驻“立即重启”按钮，由管理员主动点击；不依赖浏览器弹窗。凭据只能写在本地配置中，禁止提交到 Git：

```dotenv
QQ_BOTS='
[
  {
    "id": "应用ID",
    "token": "令牌",
    "secret": "密钥",
    "intent": {
      "c2c_group_at_messages": true,
      "direct_message": true
    },
    "use_websocket": true
  }
]
'
```

## NoneBot 可注入的插件高级配置

以下字段通过 `get_driver().config` 读取，因此可以按 NoneBot 规则写入 `.env.dev`。它们不是系统 `os.environ` 直读变量，但环境文件键名通常使用大写形式。修改后需重启：

| `.env.dev` 键 | 默认值 | 用途 |
|:---------------|:------:|:-----|
| `XIUXIAN_ON_COMPAT_GATE` | `true` | 是否启用 matcher 路由索引；仅排错时关闭 |
| `XIUXIAN_AUTO_INSTALL_DEPENDENCIES` | `false` | 启动时是否执行缺失依赖安装 |
| `XIUXIAN_AUTO_DOWNLOAD_RESOURCES` | `true` | 启动时是否检查/下载资源 |
| `XIUXIAN_STARTUP_DATABASE_MAINTENANCE` | `true` | 启动时是否整理数据库 |
| `XIUXIAN_QQ_CAPABILITIES` | 空 | 按 AppID 声明 Markdown、键盘等 QQ 能力；支持 JSON |
| `XIUXIAN_USER_COMMAND_RATE_WINDOW` | `60` | 单用户指令限流窗口秒数 |
| `XIUXIAN_USER_COMMAND_RATE_LIMIT` | `1000` | 单用户窗口内上限 |
| `XIUXIAN_USER_COMMAND_RATE_LOG_INTERVAL` | `10` | 单用户限流日志间隔 |
| `XIUXIAN_USER_COMMAND_RATE_CACHE_CLEAN_INTERVAL` | `60` | 单用户限流缓存清理间隔 |
| `XIUXIAN_GLOBAL_COMMAND_RATE_WINDOW` | `1` | 全局指令限流窗口秒数 |
| `XIUXIAN_GLOBAL_COMMAND_RATE_LIMIT` | `1000` | 全局窗口内上限 |
| `XIUXIAN_GLOBAL_COMMAND_RATE_LOG_INTERVAL` | `5` | 全局限流日志间隔 |
| `XIUXIAN_GLOBAL_COMMAND_OVERLOAD_NOTICE` | 内置繁忙提示 | 全局过载时的提示文案 |
| `XIUXIAN_GLOBAL_COMMAND_OVERLOAD_NOTICE_INTERVAL` | `30` | 过载提示最小间隔 |
| `XIUXIAN_GLOBAL_COMMAND_OVERLOAD_NOTICE_RATE_WINDOW` | `1` | 过载提示频率窗口 |
| `XIUXIAN_GLOBAL_COMMAND_OVERLOAD_NOTICE_RATE_LIMIT` | `5` | 过载提示窗口内上限 |
| `XIUXIAN_QQ_EVENT_DEDUP_ENABLED` | `true` | 是否启用 QQ 事件去重 |
| `XIUXIAN_QQ_EVENT_DEDUP_TTL` | `300` | QQ 事件去重保留秒数 |
| `XIUXIAN_QQ_EVENT_DEDUP_MAX_SIZE` | `5000` | QQ 事件去重缓存上限 |

普通部署无需设置这些高级字段。限流值在模块导入时固化，Web 热更新不会立即生效。

## 插件配置

插件行为配置位于：

```text
nonebot_plugin_xiuxian_2/xiuxian/xiuxian_config.py
```

也可在 Web 面板 `/config` 修改。保存后需要重启 NoneBot 的配置，页面会提示。

常用项：

| 配置 | 默认值 | 说明 |
|:-----|:------:|:-----|
| `web_status` | `True` | 是否启动 Web 管理面板 |
| `adapter_source` | `vendor` | `vendor` / `installed` / `auto` |
| `reference_reply` | `False` | QQ 官方普通群/C2C 是否优先引用回复 |
| `shield_group` | `[]` | 屏蔽群列表 |
| `layout_bot_dict` | `{}` | 群与发送 Bot 的映射 |
| `custom_proxy_enabled` | `False` | 是否为境外平台启用代理 |
| `custom_proxy` | `""` | HTTP/SOCKS 代理地址 |

`layout_bot_dict` 示例：

```python
self.layout_bot_dict = {
    "111": "bot-a",
    "222": ["bot-b", "bot-c"],
}
```

管理面板 host 复用 NoneBot `HOST`，端口由 `XIUXIAN_WEB_PORT` 环境变量控制，缺失时进程内自动补入默认 `5888`。该默认值不会改写用户维护的 `.env` 文件。

## 插件环境变量

仅保留适合部署、密钥和底层性能调优的变量：

| 环境变量 | 默认值 | 用途 | 修改后 |
|:---------|:-------|:-----|:-------|
| `XIUXIAN_DATA_DIR` | `./data/xiuxian` | 数据库、缓存和持久文件目录 | 重启 |
| `XIUXIAN_WEB_PORT` | `5888` | Web 管理面板端口（host 复用 NoneBot `HOST`；缺失时启动自动补入默认值） | 重启 |
| `XIUXIAN_WEB_SECRET_KEY` | 自动生成并保存 | Web 会话签名密钥 | 重启并重新登录 |
| `XIUXIAN_PROJECT_DIR` | 自动探测 | Web 日志页项目根目录 | 重启 |
| `XIUXIAN_PIP_INDEX` | 清华 PyPI 镜像 | 自动依赖安装的 pip 源 | 下次安装 |
| `XIUXIAN_SKIP_AUTO_PIP` | 空 | `1` / `true` / `yes` 时跳过自动 pip | 重启 |
| `XIUXIAN_FAST_DB_POOL_SIZE` | `64` | SQLite 快速连接池上限 | 重启 |
| `XIUXIAN_READ_CACHE_TTL` | `2` | 高频读取缓存秒数，`0` 关闭 | 重启 |
| `XIUXIAN_STAMINA_RECOVERY_BATCH_SIZE` | `1000` | 体力恢复单批人数 | 重启 |
| `XIUXIAN_MESSAGE_DB_QUEUE_MAXSIZE` | `100000` | 消息异步写队列容量 | 重启 |
| `XIUXIAN_MESSAGE_DB_BATCH_SIZE` | `200` | 消息单次批写数量 | 重启 |

`PREFIX` 是 Termux 提供的系统变量，只用于识别 Termux，不需要手动设置。

以下旧变量不再生效，应从本地 `.env` 删除：

```text
XIUXIAN_WEB_HOST
XIUXIAN_WEB_STATUS
XIUXIAN_ADAPTER_SOURCE
XIUXIAN_MESSAGE_DB_MAX_SIZE_MB
XIUXIAN_MESSAGE_GROUP_KEEP_DAYS
XIUXIAN_MESSAGE_PRIVATE_KEEP_DAYS
XIUXIAN_LOG_DIR
XIUXIAN_BOT_DIR
XIUXIAN_ROOT
PROJECT_DIR
```

## Docker 配置

Docker 安装器生成：

```text
config/.env
config/.env.dev
config/runtime.env
```

- `.env` / `.env.dev`：NoneBot 配置。
- `runtime.env`：通过 Docker `--env-file` 注入的插件环境变量。

完整 Docker 流程见 [`docker/README.md`](../docker/README.md)。

## 依赖自检

启动时会按根目录 `requirements.txt` 检查缺失依赖，并使用当前 NoneBot 解释器安装。关闭自动 pip：

```dotenv
XIUXIAN_SKIP_AUTO_PIP=1
```

手动安装：

```bash
source ~/myenv/bin/activate
python -m pip install -r requirements.txt
```
