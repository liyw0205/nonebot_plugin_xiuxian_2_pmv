from __future__ import annotations

import random
from io import BytesIO
from pathlib import Path
from typing import Optional, Union, Any
from urllib.parse import urlparse

from nonebot.permission import Permission
from nonebot.adapters import Bot as BaseBot
from nonebot.adapters import Event as BaseEvent
from nonebot.log import logger

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
        AtMessageCreateEvent as QQAtChannelMessageEvent,  # 频道@消息
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
    QQAtChannelMessageEvent = tuple()  # type: ignore
    MessageMarkdown = None  # type: ignore
    MessageKeyboard = None  # type: ignore


# =========================
# 跨适配器 MessageSegment 工具
# =========================
class CompatMessageSegment:
    """跨适配器消息段工厂"""

    @staticmethod
    def _is_url(value: Any) -> bool:
        # 只有字符串才可能是 URL
        if not isinstance(value, str):
            return False
        value = value.strip()
        if not value:
            return False
        try:
            parsed = urlparse(value)
            return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
        except Exception:
            return False

    @staticmethod
    def _to_bytes(data: Any) -> bytes:
        # 统一转换为 bytes，供 QQ 的 file_* 使用
        if isinstance(data, bytes):
            return data
        if isinstance(data, BytesIO):
            return data.getvalue()
        if isinstance(data, Path):
            return data.read_bytes()
        raise TypeError(f"不支持的本地文件类型: {type(data)}")

    @staticmethod
    def _is_qq_bot(bot: Any) -> bool:
        return HAS_QQ and QQBot is not None and isinstance(bot, QQBot)

    @staticmethod
    def _is_ob11_bot(bot: Any) -> bool:
        return HAS_OB11 and OB11Bot is not None and isinstance(bot, OB11Bot)

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
        if CompatMessageSegment._is_qq_bot(bot):
            md_seg = QQMessageSegment.markdown(  # type: ignore[union-attr]
                MessageMarkdown(custom_template_id=md_id, params=msg_body)  # type: ignore[misc]
            )
            if button_id:
                kb_seg = QQMessageSegment.keyboard(MessageKeyboard(id=button_id))  # type: ignore[union-attr,misc]
                return QQMessage(md_seg) + kb_seg  # type: ignore[call-arg]
            return md_seg

        if CompatMessageSegment._is_ob11_bot(bot):
            data: dict[str, Any] = {
                "markdown": {"custom_template_id": md_id, "params": msg_body}
            }
            if button_id:
                data["keyboard"] = {"id": button_id}
            return OB11MessageSegment("markdown", {"data": data})

        raise RuntimeError("无法根据 bot 判断适配器类型，markdown_template 构造失败")

    @staticmethod
    def markdown(bot: Any, msg: str, button_id: str = ""):
        if CompatMessageSegment._is_qq_bot(bot):
            md_seg = QQMessageSegment.markdown(MessageMarkdown(content=msg))  # type: ignore[union-attr,misc]
            if button_id:
                kb_seg = QQMessageSegment.keyboard(MessageKeyboard(id=button_id))  # type: ignore[union-attr,misc]
                return QQMessage(md_seg) + kb_seg  # type: ignore[call-arg]
            return md_seg

        if CompatMessageSegment._is_ob11_bot(bot):
            data: dict[str, Any] = {"markdown": {"content": msg}}
            if button_id:
                data["keyboard"] = {"id": button_id}
            return OB11MessageSegment("markdown", {"data": data})

        raise RuntimeError("无法根据 bot 判断适配器类型，markdown 构造失败")

    @staticmethod
    def text(bot_or_text: Any, text: Optional[str] = None):
        # 兼容两种调用
        # 新: text(bot, "xxx")
        # 旧: text("xxx")
        if text is None:
            pure_text = str(bot_or_text)
            if HAS_OB11 and OB11MessageSegment is not None:
                return OB11MessageSegment.text(pure_text)
            if HAS_QQ and QQMessageSegment is not None:
                return QQMessageSegment.text(pure_text)
            return pure_text

        bot = bot_or_text
        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.text(text)
        if CompatMessageSegment._is_qq_bot(bot):
            return QQMessageSegment.text(text)
        return text

    @staticmethod
    def image(bot: Any, file: Any):
        # QQ：URL -> image；本地 -> file_image
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.image(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_image(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        # OB11：原生 image
        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.image(file)  # type: ignore[union-attr]

        # 兜底
        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.image(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.image(file)
            return QQMessageSegment.file_image(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 image 消息段")

    @staticmethod
    def audio(bot: Any, file: Any):
        # QQ：URL -> audio；本地 -> file_audio
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.audio(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_audio(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        # OB11：record
        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.record(file)  # type: ignore[union-attr]

        # 兜底
        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.record(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.audio(file)
            return QQMessageSegment.file_audio(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 audio 消息段")

    @staticmethod
    def video(bot: Any, file: Any):
        # QQ：URL -> video；本地 -> file_video
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.video(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_video(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        # OB11：video
        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.video(file)  # type: ignore[union-attr]

        # 兜底
        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.video(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.video(file)
            return QQMessageSegment.file_video(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 video 消息段")

    @staticmethod
    def file(bot: Any, file: Any):
        # QQ：URL -> file；本地 -> file_file
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.file(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_file(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        # OB11：无严格对等段，这里保底走 image
        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.image(file)  # type: ignore[union-attr]

        # 兜底
        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.image(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.file(file)
            return QQMessageSegment.file_file(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 file 消息段")

    @staticmethod
    def at(bot: Any, user_id: str):
        # OB11 支持 at
        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.at(str(user_id))  # type: ignore[union-attr]

        # QQ 不支持 at，返回空文本
        if CompatMessageSegment._is_qq_bot(bot):
            return QQMessageSegment.text("")  # type: ignore[union-attr]

        # 兜底
        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.at(str(user_id))
        if HAS_QQ and QQMessageSegment is not None:
            return QQMessageSegment.text("")
        return ""


# =========================
# 对外导出的兼容类型
# =========================
Bot = BaseBot
Message = Union[OB11Message, QQMessage, str]
MessageSegment = CompatMessageSegment

if HAS_QQ:
    GroupMessageEvent = Union[
        OB11GroupMessageEvent,
        QQGroupMessageEvent,
        QQAtChannelMessageEvent,  # 频道事件并入“群语义”
    ]
else:
    GroupMessageEvent = Union[OB11GroupMessageEvent]

PrivateMessageEvent = Union[OB11PrivateMessageEvent, QQPrivateMessageEvent]
MessageEvent = Union[GroupMessageEvent, PrivateMessageEvent]


def is_group_event(event: BaseEvent) -> bool:
    types: list[type] = []
    if HAS_OB11:
        types.append(OB11GroupMessageEvent)  # type: ignore[arg-type]
    if HAS_QQ:
        types.append(QQGroupMessageEvent)  # type: ignore[arg-type]
        types.append(QQAtChannelMessageEvent)  # type: ignore[arg-type]
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
    if hasattr(event, "channel_id"):  # 频道兜底
        gid = getattr(event, "channel_id")
        return str(gid) if gid is not None else None
    return None


def patch_event_inplace(event: BaseEvent) -> BaseEvent:
    if getattr(event, "__compat_patched__", False):
        return event

    # 统一获取昵称，优先级 username > id > user_id
    def _resolve_sender_name(e: BaseEvent, fallback_user_id: Optional[str] = None) -> str:
        author = getattr(e, "author", None)
        username = getattr(author, "username", None)
        if username:
            return str(username)

        author_id = getattr(author, "id", None)
        if author_id:
            return str(author_id)

        if fallback_user_id:
            return str(fallback_user_id)

        uid = get_user_id(e)
        return str(uid) if uid is not None else ""

    if HAS_QQ and isinstance(event, QQPrivateMessageEvent):
        raw = event.content or ""
        setattr(event, "message_type", "private")
        setattr(event, "user_id", str(event.author.user_openid))
        setattr(event, "group_id", None)
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)

        # 补齐 sender，供业务统一访问 event.sender.nickname/card/role
        sender = type("CompatSender", (), {})()
        sender.user_id = str(event.author.user_openid)
        sender_name = _resolve_sender_name(event, fallback_user_id=sender.user_id)
        sender.nickname = sender_name
        sender.card = sender_name
        sender.role = "member"
        setattr(event, "sender", sender)

    elif HAS_QQ and isinstance(event, QQGroupMessageEvent):
        raw = event.content or ""
        setattr(event, "message_type", "group")
        setattr(event, "user_id", str(event.author.member_openid))
        setattr(event, "group_id", str(event.group_openid))
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)

        # 补齐 sender，供业务统一访问 event.sender.nickname/card/role
        sender = type("CompatSender", (), {})()
        sender.user_id = str(event.author.member_openid)
        sender_name = _resolve_sender_name(event, fallback_user_id=sender.user_id)
        sender.nickname = sender_name
        sender.card = sender_name
        sender.role = "member"
        setattr(event, "sender", sender)

    # 频道 AT 消息按“群语义”处理，关键是映射 channel_id -> group_id
    elif HAS_QQ and isinstance(event, QQAtChannelMessageEvent):
        raw = event.content or ""
        setattr(event, "message_type", "group")
        # 频道场景下统一 user_id
        uid = getattr(getattr(event, "author", None), "id", None) or get_user_id(event) or ""
        setattr(event, "user_id", str(uid))
        # 把频道ID映射到group_id，复用现有群逻辑
        setattr(event, "group_id", str(event.channel_id))
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)

        # 补齐 sender，避免 event.sender 访问报错
        sender = type("CompatSender", (), {})()
        sender.user_id = str(getattr(event, "user_id", ""))
        sender_name = _resolve_sender_name(event, fallback_user_id=sender.user_id)
        sender.nickname = sender_name
        sender.card = sender_name
        sender.role = "member"
        setattr(event, "sender", sender)

    # 兜底补齐 user_id
    if not hasattr(event, "user_id"):
        uid = get_user_id(event)
        if uid is not None:
            setattr(event, "user_id", uid)

    # 兜底补齐 sender，避免外部直接访问 event.sender 报错
    if not hasattr(event, "sender"):
        sender = type("CompatSender", (), {})()
        sender.user_id = getattr(event, "user_id", None)
        sender_name = _resolve_sender_name(event, fallback_user_id=str(sender.user_id or ""))
        sender.nickname = sender_name
        sender.card = sender_name
        sender.role = "member"
        setattr(event, "sender", sender)

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

            # QQ 频道消息：走频道发送
            if HAS_QQ and isinstance(event, QQAtChannelMessageEvent):
                return await bot.send_to_channel(
                    channel_id=str(event.channel_id),
                    message=message,
                    msg_id=str(event.id),
                    event_id=kwargs.pop("event_id", None),
                )

            return await _origin_send(event=event, message=message, **kwargs)

        async def send_private_msg(*, user_id, message, **kwargs):
            return await bot.send_to_c2c(openid=str(user_id), message=message, **kwargs)

        async def send_group_msg(*, group_id, message, **kwargs):
            # 兼容 OB11 的 send_group_msg 形态（QQ 中按 group_openid 发送）
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