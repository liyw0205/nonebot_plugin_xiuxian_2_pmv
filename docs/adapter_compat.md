# NoneBot 跨适配器兼容层与消息模块

本文档对应 `nonebot_plugin_xiuxian_2/xiuxian/adapter_compat.py` 及其配套消息模块，说明本项目在 OneBot v11 与 QQ 官方适配器之间使用的统一接口。

兼容层只负责适配器差异：类型别名、消息段构造、事件字段补齐、会话语义和事件上下文发送。主动群聊/私聊发送、消息记录、撤回调度、Web 面板消息接口、广播等功能放在独立模块中，避免后续新增适配器时继续扩大 `adapter_compat.py`。

项目内大量模块直接依赖该兼容层：业务代码主要使用统一导出的 `Bot`、`Message`、`MessageSegment`、`GroupMessageEvent`、`PrivateMessageEvent`、`MessageEvent`，避免在每个功能模块里重复判断适配器类型。

## 支持范围

- OneBot v11：普通群聊、普通私聊、事件回复发送、主动群聊/私聊发送、消息记录钩子。
- QQ 官方适配器：普通群、C2C 私聊、频道公域消息、频道私信、事件回复发送、主动群聊/C2C 发送、Markdown、自定义键盘、媒体上传、消息序列重试。
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
    get_message_reference_id,
    build_reference_reply,
    send_reference_reply,
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
- `reference(bot, message_id, ignore_error=True)`：QQ 引用回复段。
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
- `message_reference_id` / `reference_id`：QQ 普通群与 C2C 会从 `message_scene.ext` 提取可引用的 `REFIDX`。

补丁会设置 `__compat_patched__ = True`，重复调用不会重复处理。

## patch_bot_inplace

对 bot 包装事件上下文发送能力。

OneBot v11：

- 包装适配器已有的 `send`、`send_group_msg`、`send_private_msg`、`call_api`。
- 记录发送消息到本地 SQLite 文件 `message.db`。
- 支持 `revoke_time` / `revoke_after` 自动撤回。

QQ 官方适配器：

- `bot.send(event, message, **kwargs)` 会按事件类型分发到 `send_to_group`、`send_to_c2c`、`send_to_channel`、`send_to_dms`。
- 普通 QQ 群与 C2C 可传 `auto_reference=True`，或使用 `send_reference_reply(...)`，以当前事件的 `REFIDX` 做引用回复。
- 显式引用可传 `reference_id`、`message_reference_id`、`reference_message_id`、`quote_message_id`、`message_reference` 或 `msg_ref_id`。
- 内置 `msg_seq` 分配与 `40054005` 去重冲突重试。
- 不再向 QQ bot 注入 `send_group_msg`、`send_private_msg`、`delete_msg` 这类 OneBot 风格主动接口；主动发送和撤回应使用专职模块。

## 主动发送

主动群聊/私聊发送位于 `adapter_message_sender.py`：

```python
from .adapter_message_sender import send_group_message, send_private_message

await send_group_message(bot, group_id=group_id, message="通知内容")
await send_private_message(bot, user_id=user_id, message="私聊内容")
```

行为约定：

- OneBot v11：调用 `bot.call_api("send_group_msg" / "send_private_msg", ...)`。
- QQ 官方适配器：调用 `send_to_group` / `send_to_c2c`。
- 会写入发送消息记录，并支持 `revoke_time` / `revoke_after`。
- QQ 普通群/C2C 支持 `reference_id`、`msg_ref_id` 等引用参数。
- 新业务代码不要再直接依赖 `bot.send_group_msg(...)` / `bot.send_private_msg(...)` 作为跨适配器接口。

## 统一消息投递门面

新增业务和 Web 面板发送优先使用 `messaging.delivery_service`。该门面复用现有
`adapter_message_sender.py`、`adapter_message_actions.py` 和
`adapter_message_records.py`，不直接实现第二套 Adapter 发送逻辑：

```python
from .messaging import SendRequest, delivery_service

result = await delivery_service.send(
    bot,
    SendRequest(
        scene="group",
        target_id=group_id,
        message="通知内容",
        reference_id=reference_id,
        source_message_id=source_message_id,
    ),
)
```

当前支持 `group`、`private`、`channel_group` 和 `channel_private`。返回值统一为
`SendResult`，包含 `message_id`、`reference_id` 和原始 Adapter 响应。Web 消息发送
与撤回，以及帮助、状态等 Presenter 的普通文本出口已经接入该门面；Markdown、
keyboard 仍由能力适配层构建后交给该出口发送，OneBot 合并转发继续作为平台特有
能力保留专用路径。

`source_message_id` 用于 QQ 回复窗口和消息记录回复计数，`reference_id` 用于引用
回复。两者语义不同，不应互相替代。

QQ 普通群和 C2C 的 `msg_seq` 默认由投递门面按 Bot、场景和目标生成；发生平台
去重冲突时会在门面内换号重试。调用方显式传入 `msg_seq` 时保持原值且不自动换号。
发送错误统一包装为 `DeliveryError` 并标记是否可重试；触发消息审核时
`SendResult.status` 为 `pending_audit`，可通过 `audit_id` 继续跟踪。需要同步等待审核
结果时，在 `SendRequest.audit_timeout` 中设置正数超时。

## 引用回复

QQ 官方普通群与 C2C 的引用回复必须使用平台返回的 `REFIDX`，不是普通消息 ID。兼容层会在 `patch_event_inplace()` 时把它补到 `event.message_reference_id` / `event.reference_id`。通用业务发送可通过配置 `reference_reply=True` 走 `send_reference_reply(...)`；开启后 `send_msg_handler` 会避开合并转发 API，改走普通消息发送。底层仍支持显式传 `auto_reference=True` 或引用参数。

常用接口：

```python
from .adapter_compat import (
    build_reference_reply,
    get_message_reference_id,
    send_reference_reply,
)

ref_id = get_message_reference_id(event)
msg = build_reference_reply(bot, "已处理", ref_id)
await bot.send(event=event, message=msg, msg_ref_id=ref_id)

await send_reference_reply(bot, event, "已处理")
```

主动发送或 Web 面板发送时，应优先使用专职发送模块或对应适配器的实际接口，并传入已记录的 `reference_id`：

```python
from .adapter_message_sender import send_group_message

await send_group_message(
    bot,
    group_id=group_openid,
    message="已处理",
    reference_id=source_reference_id,
)
```

如果消息本身已经带 `MessageSegment.reference(...)`，兼容层不会重复插入引用段；`msg_ref_id` 仍会传给底层适配器，用于把非 `REFIDX` 的引用段修正为官方可用的 `REFIDX`。

## 模块边界

当前相关模块按职责拆分：

- `adapter_compat.py`：跨适配器核心兼容层，业务 handler 优先从这里导入 `Bot`、`MessageSegment`、事件类型、`patch_context` 等。
- `adapter_message_records.py`：消息展示文本抽取、`message.db` 收发记录、Web 主动发送记录、回复计数、`bot_id` 和发送结果消息 ID 提取。
- `adapter_message_actions.py`：通用撤回和定时撤回调度。
- `adapter_message_sender.py`：主动群聊和私聊发送入口，按适配器调用 OneBot `call_api` 或 QQ 官方 `send_to_*`。
- `broadcast_manager.py`：广播任务生命周期、广播目标记忆、不同适配器的广播发送策略。

记录、撤回、主动发送、广播等专职能力不再通过 `adapter_compat.py` 兼容导出，业务代码应直接依赖对应模块。

## 消息记录

消息记录能力位于 `adapter_message_records.py`：

- 默认逻辑库：本地 SQLite 文件 `message.db`。
- 接收消息：`patch_event_inplace` 后会记录 recv。
- 发送消息：包装后的事件发送和 `adapter_message_sender.py` 会记录 send。
- Web 主动发送：使用 `record_web_send_message(...)`。
- 回复计数：使用 `increase_recv_reply_used_count(...)`。

常用公共接口：

```python
from .adapter_message_records import (
    extract_result_message_id,
    extract_result_reference_id,
    get_bot_id,
    get_message_db_path,
    increase_recv_reply_used_count,
    init_message_db,
    record_web_send_message,
)
```

消息清理配置来自 NoneBot 配置或修仙配置对象：

- `message_db_max_size_mb`
- `message_group_keep_days`
- `message_private_keep_days`

## 撤回调度

撤回能力位于 `adapter_message_actions.py`：

```python
from .adapter_message_actions import delete_message_compat, schedule_delete_message
```

`delete_message_compat(...)` 根据 `scene` 调用 OneBot v11 或 QQ 官方适配器对应的撤回接口；`schedule_delete_message(...)` 只负责按 `revoke_time` 延迟调用撤回。

新代码直接从 `adapter_message_actions.py` 导入，不要从 `adapter_compat.py` 导入撤回接口。

## Web 与广播

`xiuxian_web/messages.py`、`adapter_message_records.py`、`adapter_message_actions.py`、`adapter_message_sender.py` 和 `broadcast_manager.py` 会复用兼容层的适配器语义：

- Web 消息列表按 `adapter`、`bot_id`、`scene`、`group_id`、`user_id` 记录会话。
- OneBot v11 广播通过 `call_api` 主动发送。
- QQ 官方适配器广播默认通过会话事件补发，避免依赖主动消息授权。
- Web 主动发送会按 `scene` 选择 QQ 群、C2C、频道、公域私信的实际接口。

## 与 on_compat.py 的关系

`adapter_compat.py` 解决“事件语义、消息段和事件上下文发送跨适配器统一”的问题；`on_compat.py` 解决“本项目大量空前缀 `on_command` matcher 的路由压力”的问题。

当前项目约有 500 个 `on_command` matcher，且默认配置示例使用：

```dotenv
COMMAND_START = [""]
```

因此 `on_compat.py` 会为修仙模块创建命令索引，只让可能命中的 matcher 进入后续规则检查；带明确字面前缀的 `on_regex` 会进入前缀索引，无法安全提取前缀的正则和 `on_message` 会保留通用 matcher 语义，避免误过滤。

## 使用范式

```python
from .adapter_compat import MessageSegment, get_chat_scene, patch_context
from .adapter_message_sender import send_group_message
from .on_compat import on_command

help_cmd = on_command("修仙帮助", priority=12, block=True)

@help_cmd.handle()
async def _(bot, event):
    bot, event = patch_context(bot, event)
    scene = get_chat_scene(event)
    await bot.send(event, MessageSegment.text(bot, f"当前会话: {scene}"))

async def notify_group(bot, group_id: str):
    await send_group_message(bot, group_id=group_id, message="主动通知")
```

## 内置上游适配器源码策略

上游适配器源码内置在独立 vendored 命名空间中，不直接改写上游文件：

- 上游来源固定为 `https://github.com/nonebot/adapter-qq` 与 `https://github.com/nonebot/adapter-onebot`。
- 两个上游仓库均为 MIT 许可证，内置时必须保留对应 `LICENSE`、上游仓库地址、锁定的 tag 或 commit。
- vendored 代码放在 `nonebot_plugin_xiuxian_2/xiuxian/xiuxian_adapter/`，避免把上游实现散落到兼容层。
- `adapter_compat.py` 继续作为事件、消息段与会话语义的统一入口；本项目内优先通过 `xiuxian_adapter.qq` 和 `xiuxian_adapter.onebot` 保留 vendored 行为，独立复用时可回退到 `nonebot.adapters.qq` 与 `nonebot.adapters.onebot.v11` 标准路径。
- 当前内置范围是上游运行时源码、`LICENSE` 和 `UPSTREAM` 记录；不提交 tests、website、CI 等开发文件。
- 更新流程必须是“记录上游版本 -> 更新 vendored 目录 -> 跑编译和消息发送回归 -> 更新本节版本记录”，不能手工零散复制文件。

当前目录边界：

```text
nonebot_plugin_xiuxian_2/xiuxian/xiuxian_adapter/
  README.md
  __init__.py
  onebot.py
  qq.py
  vendor/
    adapter_qq/
      UPSTREAM
      LICENSE
      nonebot/adapters/qq/
    adapter_onebot/
      UPSTREAM
      LICENSE
      nonebot/adapters/onebot/
```

`UPSTREAM` 文件至少记录：

- upstream repository
- upstream tag or commit
- vendored date
- local changes
- update command

当前状态：已内置 `adapter-qq 1.7.1` 与 `adapter-onebot 2.4.6` 的运行时源码。`xiuxian_adapter.ensure_vendored_adapters()` 会扩展 `nonebot.adapters.__path__`，使上游原始包路径 `nonebot.adapters.qq` 与 `nonebot.adapters.onebot` 可以解析到内置源码。

## 注意事项

1. QQ 官方适配器的 at、Markdown、键盘、主动消息授权与 OneBot v11 行为不同，业务代码应优先通过 `MessageSegment` 和 `patch_context` 访问。
2. `MessageSegment.file()` 在 OneBot v11 下可能按图片兼容处理，具体能力取决于协议端。
3. QQ `msg_seq` 冲突会自动重试，但业务侧仍应避免短时间大量重复发送。
4. `patch_bot_inplace` 会替换或包装 bot 的事件发送方法；如果外部也包装发送接口，需要注意加载顺序。
5. `adapter_compat.py` 属于运行时代码，本文档只记录项目约定，实际行为以源码为准。
6. 新增 Web、广播、统计、审计类功能时优先新增独立模块，不要把业务功能继续塞入 `adapter_compat.py`。
