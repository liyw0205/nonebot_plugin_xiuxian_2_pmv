# NoneBot 跨适配器兼容层

本文档对应 `nonebot_plugin_xiuxian_2/xiuxian/adapter_compat.py`，说明本项目在 OneBot v11 与 QQ 官方适配器之间使用的统一接口。

项目内大量模块直接依赖该兼容层：业务代码主要使用统一导出的 `Bot`、`Message`、`MessageSegment`、`GroupMessageEvent`、`PrivateMessageEvent`、`MessageEvent`，避免在每个功能模块里重复判断适配器类型。

## 支持范围

- OneBot v11：普通群聊、普通私聊、主动发送、撤回、消息记录。
- QQ 官方适配器：普通群、C2C 私聊、频道公域消息、频道私信、Markdown、自定义键盘、媒体上传、消息序列重试。
- 频道公域消息在业务语义中归入“群聊”，频道私信归入“私聊”。
- 未安装某个适配器时，兼容层会回退到可用类型，不阻塞插件加载。

## 统一导出

```python
from .adapter_compat import (
    Bot,
    GROUP,
    Message,
    MessageSegment,
    CompatSender,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageEvent,
    is_group_event,
    is_private_event,
    is_channel_event,
    get_chat_scene,
    get_user_id,
    get_group_id,
    patch_bot_inplace,
    patch_event_inplace,
    patch_context,
)
```

`Bot` 是 `nonebot.adapters.Bot` 的统一别名；事件类型会根据当前安装的适配器动态组合。项目里的 handler 可以继续写成：

```python
async def handle(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, event = patch_context(bot, event)
```

## 消息段

`MessageSegment` 实际导出的是 `CompatMessageSegment`。它会根据 `bot` 类型构造对应适配器消息段。

常用方法：

- `text(bot, text)`：文本。
- `image(bot, file)`：图片，支持 URL、`bytes`、`BytesIO`、`Path`。
- `audio(bot, file)` / `video(bot, file)` / `file(bot, file)`：媒体和文件。
- `at(bot, user_id)`：OneBot v11 返回 at 段；QQ 官方适配器返回空文本，避免错误 at。
- `markdown(bot, content, button_id="")`：Markdown 内容。
- `markdown_template(bot, md_id, msg_body, button_id="")`：Markdown 模板。
- `markdown_param(key, value)`：模板参数构造器。
- `markdown_keyboard(bot, content, keyboard)`：QQ Markdown + 自定义键盘。
- `upload_image_and_get_url(...)`：QQ 图片上传并返回可用于 Markdown 的 URL。

示例：

```python
msg = MessageSegment.markdown_keyboard(
    bot,
    " ",
    [[("查看背包", "我的背包"), ("修仙帮助", "修仙帮助")]],
)
await bot.send(event=event, message=msg)
```

## 事件语义

兼容层用 `get_chat_scene(event)` 统一会话类型：

| 返回值 | 含义 |
|:--|:--|
| `group` | OneBot v11 群聊或 QQ 普通群 |
| `private` | OneBot v11 私聊或 QQ C2C 私聊 |
| `channel_group` | QQ 频道公域消息，业务上按群聊处理 |
| `channel_private` | QQ 频道私信，业务上按私聊处理 |
| `unknown` | 无法识别的事件 |

相关辅助函数：

```python
is_group_event(event)
is_private_event(event)
is_channel_event(event)
get_user_id(event)
get_group_id(event)
```

`GROUP` 权限等价于“群语义事件”，因此包含 OneBot v11 群聊、QQ 普通群、QQ 频道公域消息。

## patch_context

推荐在 handler 开头调用：

```python
bot, event = patch_context(bot, event)
```

它会依次执行：

- `patch_bot_inplace(bot)`
- `patch_event_inplace(event, bot)`

这样业务代码可以稳定访问 `event.user_id`、`event.group_id`、`event.message_type`、`event.sender`、`event.raw_message`、`event.to_me` 等常用字段。

## patch_event_inplace

对 QQ 官方适配器事件补齐 OneBot v11 风格字段：

- `message_type`：`group` / `private`。
- `user_id`：用户 openid 或频道用户 id。
- `group_id`：普通群 openid 或频道 id。
- `message_id`：当前事件消息 id。
- `raw_message` / `plaintext`：纯文本缓存。
- `sender`：统一的 `CompatSender`，支持属性访问、`dict()`、`model_dump()`。
- `to_me`：QQ 群消息会结合 at、reply、mention 数据判断是否指向当前 bot。

补丁会设置 `__compat_patched__ = True`，重复调用不会重复处理。

## patch_bot_inplace

对 bot 注入或包装统一发送能力。

OneBot v11：

- 包装 `send`、`send_group_msg`、`send_private_msg`、`call_api`。
- 记录发送消息到 `message.db`。
- 支持 `revoke_time` / `revoke_after` 自动撤回。

QQ 官方适配器：

- `bot.send(event, message, **kwargs)` 会按事件类型分发到 `send_to_group`、`send_to_c2c`、`send_to_channel`、`send_to_dms`。
- `bot.send_group_msg(group_id=..., message=...)` 映射到 QQ 群主动发送。
- `bot.send_private_msg(user_id=..., message=...)` 映射到 QQ C2C 主动发送。
- `bot.delete_msg(...)` 提供统一撤回入口。
- 内置 `msg_seq` 分配与 `40054005` 去重冲突重试。

## 消息记录

兼容层内置 Web 面板使用的消息记录能力：

- 默认数据库：`message.db`。
- 接收消息：`patch_event_inplace` 后会记录 recv。
- 发送消息：包装后的发送接口会记录 send。
- Web 主动发送：使用 `record_web_send_message(...)`。
- 回复计数：使用 `increase_recv_reply_used_count(...)`。
- 撤回：使用 `delete_message_compat(...)` 或 `schedule_delete_message(...)`。

导出的 Web 公共接口：

```python
init_message_db()
get_message_db_path()
extract_result_message_id(result)
get_bot_id(bot)
record_web_send_message(...)
increase_recv_reply_used_count(...)
```

消息清理配置来自 NoneBot 配置或修仙配置对象：

- `message_db_max_size_mb`
- `message_group_keep_days`
- `message_private_keep_days`

## Web 与广播

`xiuxian_web/messages.py` 和 `broadcast_manager.py` 会复用本兼容层：

- Web 消息列表按 `adapter`、`bot_id`、`scene`、`group_id`、`user_id` 记录会话。
- OneBot v11 广播可直接主动发送。
- QQ 官方适配器广播默认通过会话事件补发，避免依赖主动消息授权。
- Web 主动发送会按 `scene` 选择 QQ 群、C2C、频道、公域私信的实际接口。

## 与 on_compat.py 的关系

`adapter_compat.py` 解决“事件和发送接口跨适配器统一”的问题；`on_compat.py` 解决“本项目大量空前缀 `on_command` matcher 的路由压力”的问题。

当前项目约有 500 个 `on_command` matcher，且默认配置示例使用：

```dotenv
COMMAND_START = [""]
```

因此 `on_compat.py` 会为修仙模块创建命令索引，只让可能命中的 matcher 进入后续规则检查；带明确字面前缀的 `on_regex` 会进入前缀索引，无法安全提取前缀的正则和 `on_message` 会保留通用 matcher 语义，避免误过滤。

## 使用范式

```python
from .adapter_compat import MessageSegment, get_chat_scene, patch_context
from .on_compat import on_command

help_cmd = on_command("修仙帮助", priority=12, block=True)

@help_cmd.handle()
async def _(bot, event):
    bot, event = patch_context(bot, event)
    scene = get_chat_scene(event)
    await bot.send(event, MessageSegment.text(bot, f"当前会话: {scene}"))
```

## 注意事项

1. QQ 官方适配器的 at、Markdown、键盘、主动消息授权与 OneBot v11 行为不同，业务代码应优先通过 `MessageSegment` 和 `patch_context` 访问。
2. `MessageSegment.file()` 在 OneBot v11 下可能按图片兼容处理，具体能力取决于协议端。
3. QQ `msg_seq` 冲突会自动重试，但业务侧仍应避免短时间大量重复发送。
4. `patch_bot_inplace` 会替换或包装 bot 的发送方法；如果外部也包装发送接口，需要注意加载顺序。
5. `adapter_compat.py` 属于运行时代码，本文档只记录项目约定，实际行为以源码为准。
