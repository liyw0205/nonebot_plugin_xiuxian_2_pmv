from __future__ import annotations

import random
from typing import Optional, Union, Any

from nonebot.permission import Permission
from nonebot.adapters import Bot as BaseBot
from nonebot.adapters import Event as BaseEvent

# =========================
# 可选导入：onebot v11
# =========================
try:
    from nonebot.adapters.onebot.v11 import (
        Bot as OB11Bot,
        Message as OB11Message,
        MessageSegment as OB11MessageSegment,
    )
    from nonebot.adapters.onebot.v11.event import (
        GroupMessageEvent as OB11GroupMessageEvent,
        PrivateMessageEvent as OB11PrivateMessageEvent,
    )

    HAS_OB11 = True
except Exception:
    HAS_OB11 = False
    OB11Bot = None  # type: ignore
    OB11Message = str  # type: ignore
    OB11MessageSegment = None  # type: ignore
    OB11GroupMessageEvent = tuple()  # type: ignore
    OB11PrivateMessageEvent = tuple()  # type: ignore

# =========================
# 可选导入：qq
# =========================
try:
    from nonebot.adapters.qq import (
        Bot as QQBot,
        Message as QQMessage,
        MessageSegment as QQMessageSegment,
    )
    from nonebot.adapters.qq.event import (
        C2CMessageCreateEvent as QQPrivateMessageEvent,
        GroupAtMessageCreateEvent as QQGroupMessageEvent,
    )
    from nonebot.adapters.qq.models import MessageMarkdown, MessageKeyboard

    HAS_QQ = True
except Exception:
    HAS_QQ = False
    QQBot = None  # type: ignore
    QQMessage = str  # type: ignore
    QQMessageSegment = None  # type: ignore
    QQPrivateMessageEvent = tuple()  # type: ignore
    QQGroupMessageEvent = tuple()  # type: ignore
    MessageMarkdown = None  # type: ignore
    MessageKeyboard = None  # type: ignore


# =========================
# 跨适配器 MessageSegment 工具
# =========================
class CompatMessageSegment:
    """跨适配器消息段工厂"""

    @staticmethod
    def markdown_param(key: str, value: str) -> dict[str, list[str]]:
        return {"key": key, "values": [value]}

    @staticmethod
    def markdown_template(
        bot: Any,
        md_id: str,
        msg_body: list[dict[str, Any]],
        button_id: str = "",
    ):
        # QQ：支持 markdown + keyboard
        if HAS_QQ and QQBot is not None and isinstance(bot, QQBot):
            md_seg = QQMessageSegment.markdown(  # type: ignore[union-attr]
                MessageMarkdown(custom_template_id=md_id, params=msg_body)  # type: ignore[misc]
            )
            if button_id:
                kb_seg = QQMessageSegment.keyboard(MessageKeyboard(id=button_id))  # type: ignore[union-attr,misc]
                return QQMessage(md_seg) + kb_seg  # type: ignore[call-arg]
            return md_seg

        # OB11：走扩展 markdown 段
        if HAS_OB11 and OB11MessageSegment is not None:
            data: dict[str, Any] = {
                "markdown": {
                    "custom_template_id": md_id,
                    "params": msg_body,
                }
            }
            if button_id:
                data["keyboard"] = {"id": button_id}
            return OB11MessageSegment("markdown", {"data": data})

        raise RuntimeError("当前环境未安装 QQ 或 OB11 适配器，无法构造 markdown_template")

    @staticmethod
    def markdown(
        bot: Any,
        msg: str,
        button_id: str = "",
    ):
        # QQ：支持 markdown + keyboard
        if HAS_QQ and QQBot is not None and isinstance(bot, QQBot):
            md_seg = QQMessageSegment.markdown(MessageMarkdown(content=msg))  # type: ignore[union-attr,misc]
            if button_id:
                kb_seg = QQMessageSegment.keyboard(MessageKeyboard(id=button_id))  # type: ignore[union-attr,misc]
                return QQMessage(md_seg) + kb_seg  # type: ignore[call-arg]
            return md_seg

        # OB11：扩展 markdown 段
        if HAS_OB11 and OB11MessageSegment is not None:
            data: dict[str, Any] = {"markdown": {"content": msg}}
            if button_id:
                data["keyboard"] = {"id": button_id}
            return OB11MessageSegment("markdown", {"data": data})

        raise RuntimeError("当前环境未安装 QQ 或 OB11 适配器，无法构造 markdown")

    @staticmethod
    def text(text: str):
        # 优先 OB11，再 QQ，最后回退字符串
        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.text(text)
        if HAS_QQ and QQMessageSegment is not None:
            return QQMessageSegment.text(text)
        return text

    @staticmethod
    def image(file: Any):
        # 优先 OB11，再 QQ
        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.image(file)
        if HAS_QQ and QQMessageSegment is not None:
            return QQMessageSegment.image(file)
        raise RuntimeError("当前环境未安装可用适配器，无法构造 image 消息段")

    @staticmethod
    def at(user_id: str):
        """
        @消息兼容方法
        - OB11: 返回正常的at消息段
        - QQ适配器: 不支持at，返回空文本
        """
        # OB11支持at
        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.at(str(user_id))

        # QQ适配器不支持at，返回空文本避免报错
        if HAS_QQ and QQMessageSegment is not None:
            return QQMessageSegment.text("")

        # 兜底返回空字符串
        return ""

# =========================
# 对外导出的兼容类型
# =========================
Bot = BaseBot
Message = Union[OB11Message, QQMessage, str]
MessageSegment = CompatMessageSegment

GroupMessageEvent = Union[OB11GroupMessageEvent, QQGroupMessageEvent]
PrivateMessageEvent = Union[OB11PrivateMessageEvent, QQPrivateMessageEvent]
MessageEvent = Union[GroupMessageEvent, PrivateMessageEvent]


def is_group_event(event: BaseEvent) -> bool:
    types: list[type] = []
    if HAS_OB11:
        types.append(OB11GroupMessageEvent)  # type: ignore[arg-type]
    if HAS_QQ:
        types.append(QQGroupMessageEvent)  # type: ignore[arg-type]
    return isinstance(event, tuple(types)) if types else False


def is_private_event(event: BaseEvent) -> bool:
    types: list[type] = []
    if HAS_OB11:
        types.append(OB11PrivateMessageEvent)  # type: ignore[arg-type]
    if HAS_QQ:
        types.append(QQPrivateMessageEvent)  # type: ignore[arg-type]
    return isinstance(event, tuple(types)) if types else False


_group_seq_cache: dict[str, int] = {}


def _next_group_seq(group_openid: str) -> int:
    current = _group_seq_cache.get(group_openid)
    if current is None:
        # 按需求：随机1-10000减随机1-1000
        current = random.randint(1, 10000) - random.randint(1, 1000)

    current += 1

    # 兜底为正整数
    if current <= 0:
        current = 1
    if current > 1_000_000:
        current = 1

    _group_seq_cache[group_openid] = current
    return current


async def _group_checker(event: BaseEvent) -> bool:
    return is_group_event(event)


GROUP: Permission = Permission(_group_checker)


def get_user_id(event: BaseEvent) -> Optional[str]:
    if hasattr(event, "user_id"):
        uid = getattr(event, "user_id")
        return str(uid) if uid is not None else None
    try:
        return str(event.get_user_id())
    except Exception:
        return None


def get_group_id(event: BaseEvent) -> Optional[str]:
    if hasattr(event, "group_id"):
        gid = getattr(event, "group_id")
        return str(gid) if gid is not None else None
    if hasattr(event, "group_openid"):
        gid = getattr(event, "group_openid")
        return str(gid) if gid is not None else None
    return None


def patch_event_inplace(event: BaseEvent) -> BaseEvent:
    if getattr(event, "__compat_patched__", False):
        return event

    if HAS_QQ and isinstance(event, QQPrivateMessageEvent):
        raw = event.content or ""
        setattr(event, "message_type", "private")
        setattr(event, "user_id", str(event.author.user_openid))
        setattr(event, "group_id", None)
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)

        # 兼容 OB11 习惯：补 message / plaintext
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)

    elif HAS_QQ and isinstance(event, QQGroupMessageEvent):
        raw = event.content or ""
        setattr(event, "message_type", "group")
        setattr(event, "user_id", str(event.author.member_openid))
        setattr(event, "group_id", str(event.group_openid))
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)

        # 兼容 OB11 习惯：补 message / plaintext
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)

    if not hasattr(event, "user_id"):
        uid = get_user_id(event)
        if uid is not None:
            setattr(event, "user_id", uid)

    setattr(event, "__compat_patched__", True)
    return event


def patch_bot_inplace(bot: BaseBot) -> BaseBot:
    if getattr(bot, "__compat_patched__", False):
        return bot

    if HAS_QQ and QQBot is not None and isinstance(bot, QQBot):
        _origin_send = bot.send

        async def send(event, message, **kwargs):
            # QQ 群聊：显式 msg_seq
            if HAS_QQ and isinstance(event, QQGroupMessageEvent):
                group_openid = str(event.group_openid)
                msg_seq = kwargs.pop("msg_seq", _next_group_seq(group_openid))
                return await bot.send_to_group(
                    group_openid=group_openid,
                    message=message,
                    msg_id=str(event.id),
                    msg_seq=int(msg_seq),
                    event_id=kwargs.pop("event_id", None),
                )

            # QQ 私聊
            if HAS_QQ and isinstance(event, QQPrivateMessageEvent):
                return await _origin_send(event=event, message=message, **kwargs)

            return await _origin_send(event=event, message=message, **kwargs)

        async def send_private_msg(*, user_id, message, **kwargs):
            return await bot.send_to_c2c(openid=str(user_id), message=message, **kwargs)

        async def send_group_msg(*, group_id, message, **kwargs):
            # 兼容 OB11 的 send_group_msg 形态
            msg_seq = kwargs.pop("msg_seq", _next_group_seq(str(group_id)))
            return await bot.send_to_group(
                group_openid=str(group_id),
                message=message,
                msg_id=kwargs.pop("msg_id", None),
                msg_seq=int(msg_seq),
                event_id=kwargs.pop("event_id", None),
            )

        async def delete_msg(*, message_id, group_id=None, user_id=None):
            if group_id is not None:
                return await bot.delete_group_message(
                    group_openid=str(group_id), message_id=str(message_id)
                )
            if user_id is not None:
                return await bot.delete_c2c_message(
                    openid=str(user_id), message_id=str(message_id)
                )
            raise ValueError("QQ delete_msg 需要 group_id 或 user_id")

        setattr(bot, "send", send)
        setattr(bot, "send_private_msg", send_private_msg)
        setattr(bot, "send_group_msg", send_group_msg)
        setattr(bot, "delete_msg", delete_msg)

    setattr(bot, "__compat_patched__", True)
    return bot


def patch_context(bot: BaseBot, event: BaseEvent) -> tuple[BaseBot, BaseEvent]:
    return patch_bot_inplace(bot), patch_event_inplace(event)


__all__ = [
    "Bot",
    "GROUP",
    "Message",
    "MessageSegment",
    "GroupMessageEvent",
    "PrivateMessageEvent",
    "MessageEvent",
    "is_group_event",
    "is_private_event",
    "get_user_id",
    "get_group_id",
    "patch_bot_inplace",
    "patch_event_inplace",
    "patch_context",
]