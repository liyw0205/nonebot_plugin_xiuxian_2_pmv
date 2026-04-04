```markdown
# NoneBot 跨适配器兼容层文档（OneBot v11 + QQ）

本文档介绍 `adapter_compat.py` 的设计目标、能力范围、关键 API 与使用方式。  
目标是：**让插件尽量以统一方式处理 QQ 适配器与 OneBot v11 适配器的消息发送、事件识别与上下文字段。**

---

## 1. 设计目标

`adapter_compat.py` 提供了三类兼容能力：

1. **消息段兼容构造**（`CompatMessageSegment`）  
   统一构造 text/image/audio/video/file/markdown 等消息段。

2. **事件语义兼容**  
   将 QQ 的频道消息并入“群语义”，频道私信并入“私聊语义”，并提供统一判断函数。

3. **Bot/Event 补丁（patch）**  
   通过 `patch_bot_inplace` 与 `patch_event_inplace`，补齐常用字段与发送接口，降低上层插件分支判断复杂度。

---

## 2. 适配器探测与兼容导出

模块会尝试可选导入：

- `nonebot.adapters.onebot.v11`（OB11）
- `nonebot.adapters.qq`（QQ）

根据安装情况动态启用。

对外导出：

- `Bot = BaseBot`
- `Message = Union[OB11Message, QQMessage, str]`
- `MessageSegment = CompatMessageSegment`
- `GroupMessageEvent` / `PrivateMessageEvent` / `MessageEvent`（按环境动态 Union）

---

## 3. CompatMessageSegment（统一消息段工厂）

### 3.1 文本

```python
MessageSegment.text(bot, "hello")
# 或
MessageSegment.text("hello")  # 无 bot 时按已安装适配器兜底
```

### 3.2 图片 / 音频 / 视频 / 文件

支持两类输入：

- URL（http/https）
- 本地内容（`bytes` / `BytesIO` / `Path`）

```python
MessageSegment.image(bot, "https://example.com/a.jpg")
MessageSegment.image(bot, Path("a.jpg"))
MessageSegment.audio(bot, BytesIO(b"..."))
MessageSegment.video(bot, b"...")
MessageSegment.file(bot, Path("doc.pdf"))
```

> 注：OB11 的 `file()` 在当前实现中回退为 `image()` 语义（协议能力差异导致）。

### 3.3 @用户

```python
MessageSegment.at(bot, user_id)
```

- OB11：返回 `at` 段
- QQ：当前返回空文本（QQ 侧 at 语义与 OB11 不同，避免错误 at）

### 3.4 Markdown（QQ/OB11 统一入口）

- 原生 markdown 内容
- markdown 模板 + 参数 + 可选按钮

```python
params = [
    MessageSegment.markdown_param("title", "标题"),
    MessageSegment.markdown_param("text", "内容"),
]

seg = MessageSegment.markdown_template(bot, "tpl_id", params, button_id="kb_id")
# 或
seg2 = MessageSegment.markdown(bot, "# Hello", button_id="")
```

---

## 4. QQ 图片上传并获取 URL

`upload_image_and_get_url` 仅对 QQBot 生效：

```python
url = await MessageSegment.upload_image_and_get_url(
    bot=bot,
    channel_id="123456",
    image=Path("a.png"),
    mode="md5",          # "md5" | "link"
    fallback_url=None,
    audit_timeout=30.0,
)
```

### 参数说明

- `mode="md5"`：立即返回基于图片 MD5 的 CDN 链接（默认，快）
- `mode="link"`：等待审核通过并回查真实附件链接（慢，但真实）
- `fallback_url`：失败时兜底

### 审核逻辑

- 遇到 `AuditException`：
  - `md5` 模式：后台异步跟踪审核结果并写日志，前台先返回 md5 链接
  - `link` 模式：同步等待审核事件并回查真实 URL

---

## 5. 事件语义统一

### 5.1 统一判定函数

```python
is_group_event(event)
is_private_event(event)
is_channel_event(event)
scene = get_chat_scene(event)
```

`get_chat_scene` 返回值：

- `group`：普通群
- `private`：普通私聊
- `channel_group`：频道公域消息（按群语义）
- `channel_private`：频道私信（按私聊语义）
- `unknown`

### 5.2 GROUP 权限

模块定义了：

```python
GROUP: Permission
```

逻辑等价于“匹配群语义事件（含频道公域）”。

---

## 6. QQ 发送去重冲突（msg_seq）处理

QQ 常见错误：`40054005`（消息被去重 / msg_seq 冲突）

模块实现了：

- `_next_group_seq` / `_next_c2c_seq`：按会话维度生成递增序列
- `_send_with_retry`：仅对冲突错误重试，带抖动退避

重试策略：

- 默认最多 3 次
- 每次延迟：`base_delay * (i+1) + random_jitter`

---

## 7. patch_event_inplace（事件补丁）

对 QQ 事件补齐 OB11 常用字段，减少上层兼容判断：

补齐/统一字段示例：

- `message_type`（`group` / `private`）
- `user_id`
- `group_id`（频道消息映射 `channel_id -> group_id`）
- `message_id`
- `raw_message` / `message` / `plaintext`
- `sender`（`user_id/nickname/card/role`）

并打标记：`__compat_patched__ = True`，避免重复 patch。

---

## 8. patch_bot_inplace（Bot 补丁）

给 QQBot 注入统一接口：

- `bot.send(event, message, **kwargs)`（按事件类型路由）
- `bot.send_private_msg(user_id=..., message=..., **kwargs)`
- `bot.send_group_msg(group_id=..., message=..., **kwargs)`
- `bot.delete_msg(message_id=..., group_id=...|user_id=...)`

路由规则：

- QQ群消息 -> `send_to_group`
- QQ私聊(C2C) -> `send_to_c2c`
- 频道公域 -> `send_to_channel`
- 频道私信 -> `send_to_dms`

并内建 `msg_seq` 自动分配与冲突重试。

---

## 9. patch_context（推荐入口）

```python
bot, event = patch_context(bot, event)
```

等价于同时执行：

- `patch_bot_inplace(bot)`
- `patch_event_inplace(event)`

建议在 matcher/handler 入口尽早调用一次。

---

## 10. 常用工具函数

```python
uid = get_user_id(event)
gid = get_group_id(event)
```

- `get_group_id` 会优先取 `group_id`，其次 `group_openid`，再其次 `channel_id`（统一群语义）。

---

## 11. 推荐使用范式

```python
from .adapter_compat import patch_context, MessageSegment, get_chat_scene

@matcher.handle()
async def _(bot, event):
    bot, event = patch_context(bot, event)

    scene = get_chat_scene(event)
    if scene in ("group", "channel_group"):
        await bot.send(event, MessageSegment.text(bot, "群语义收到"))
    else:
        await bot.send(event, MessageSegment.text(bot, "私聊语义收到"))
```

---

## 12. 注意事项

1. **QQ 的 at 段**与 OB11 不同，`MessageSegment.at` 在 QQ 下返回空文本，避免误行为。  
2. **OB11 的 file 语义**当前实现回退到 image，请按业务评估。  
3. `patch_bot_inplace` 会替换 QQBot 的 `send` 行为；若你已有自定义 send，请注意调用顺序。  
4. 上传图片真实链接（`mode="link"`）依赖审核与回查接口，时延更高。  

---

## 13. 导出清单（`__all__`）

- `Bot`
- `GROUP`
- `Message`
- `MessageSegment`
- `GroupMessageEvent`
- `PrivateMessageEvent`
- `MessageEvent`
- `is_group_event`
- `is_private_event`
- `is_channel_event`
- `get_chat_scene`
- `get_user_id`
- `get_group_id`
- `patch_bot_inplace`
- `patch_event_inplace`
- `patch_context`

---
```