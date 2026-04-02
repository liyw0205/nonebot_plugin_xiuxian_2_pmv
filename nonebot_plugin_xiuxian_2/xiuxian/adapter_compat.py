from __future__ import annotations

import random
from io import BytesIO
from pathlib import Path
import hashlib
import asyncio
from dataclasses import dataclass, asdict
from typing import Optional, Union, Any, Literal, Iterator
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
        AtMessageCreateEvent as QQAtChannelMessageEvent,      # 频道 @ 消息 -> 群语义
        DirectMessageCreateEvent as QQChannelPrivateMessageEvent,  # 频道私信 -> 私聊语义
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
    QQChannelPrivateMessageEvent = tuple()  # type: ignore
    MessageMarkdown = None  # type: ignore
    MessageKeyboard = None  # type: ignore


@dataclass
class CompatSender:
    """兼容 sender 对象，支持属性访问与序列化"""

    user_id: Optional[str] = None
    nickname: Optional[str] = None
    card: Optional[str] = None
    role: Optional[str] = "member"

    def dict(self) -> dict[str, Any]:
        return asdict(self)

    def model_dump(self) -> dict[str, Any]:
        return asdict(self)

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        yield from asdict(self).items()


# =========================
# 跨适配器 MessageSegment 工具
# =========================
class CompatMessageSegment:
    """跨适配器消息段工厂"""

    @staticmethod
    def _is_url(value: Any) -> bool:
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
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.image(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_image(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.image(file)  # type: ignore[union-attr]

        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.image(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.image(file)
            return QQMessageSegment.file_image(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 image 消息段")

    @staticmethod
    def audio(bot: Any, file: Any):
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.audio(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_audio(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.record(file)  # type: ignore[union-attr]

        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.record(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.audio(file)
            return QQMessageSegment.file_audio(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 audio 消息段")

    @staticmethod
    def video(bot: Any, file: Any):
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.video(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_video(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.video(file)  # type: ignore[union-attr]

        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.video(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.video(file)
            return QQMessageSegment.file_video(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 video 消息段")

    @staticmethod
    def file(bot: Any, file: Any):
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.file(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_file(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.image(file)  # type: ignore[union-attr]

        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.image(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.file(file)
            return QQMessageSegment.file_file(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 file 消息段")

    @staticmethod
    def at(bot: Any, user_id: str):
        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.at(str(user_id))  # type: ignore[union-attr]

        if CompatMessageSegment._is_qq_bot(bot):
            return QQMessageSegment.text("")  # type: ignore[union-attr]

        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.at(str(user_id))
        if HAS_QQ and QQMessageSegment is not None:
            return QQMessageSegment.text("")
        return ""

    @staticmethod
    async def upload_image_and_get_url(
        bot: BaseBot,
        channel_id: str,
        image: Union[str, Path, bytes, BytesIO],
        *,
        mode: Literal["md5", "link"] = "md5",
        fallback_url: Optional[str] = None,
        audit_timeout: float = 30.0,
    ) -> Optional[str]:
        """
        mode:
        - "md5": 立即返回 md5 链接（默认）
        - "link": 等待审核并回查真实附件链接后返回
        """

        try:
            from nonebot.adapters.qq.exception import AuditException
        except Exception:
            AuditException = Exception  # type: ignore

        if not (HAS_QQ and QQBot is not None and isinstance(bot, QQBot)):
            logger.warning("upload_image_and_get_url: 当前 bot 不是 QQBot")
            return fallback_url

        if not channel_id:
            logger.warning("upload_image_and_get_url: channel_id为空，跳过上传")
            return fallback_url

        try:
            if isinstance(image, str):
                image_bytes = Path(image).read_bytes()
            elif isinstance(image, Path):
                image_bytes = image.read_bytes()
            elif isinstance(image, BytesIO):
                image_bytes = image.getvalue()
            elif isinstance(image, bytes):
                image_bytes = image
            else:
                raise TypeError(f"不支持的 image 类型: {type(image)}")
        except Exception as e:
            logger.error(f"upload_image_and_get_url: 读取图片失败: {e}")
            return fallback_url

        md5_hex = hashlib.md5(image_bytes).hexdigest().upper()
        md5_url = f"https://gchat.qpic.cn/qmeetpic/0/0-0-{md5_hex}/0"

        async def _resolve_real_url_from_message(message_id: str) -> Optional[str]:
            msg_obj = await bot.get_message_of_id(
                channel_id=str(channel_id),
                message_id=str(message_id),
            )
            atts = getattr(msg_obj, "attachments", None) or []
            for att in atts:
                url = getattr(att, "url", None)
                if url:
                    return str(url)
            return None

        async def _audit_followup(audit_exc: Exception):
            try:
                audit_id = getattr(audit_exc, "audit_id", None)
                logger.warning(f"upload_image_and_get_url: 触发审核 audit_id={audit_id}，先返回md5链接")
                audit_res = await audit_exc.get_audit_result(timeout=audit_timeout)  # type: ignore[attr-defined]
                event_name = str(getattr(audit_res, "__type__", ""))
                message_id = getattr(audit_res, "message_id", None)
                if "MESSAGE_AUDIT_PASS" not in event_name or not message_id:
                    logger.warning(f"upload_image_and_get_url: 审核未通过或缺少message_id, event={event_name}, message_id={message_id}")
                    return
                real_url = await _resolve_real_url_from_message(str(message_id))
                if real_url:
                    logger.info(f"upload_image_and_get_url: 审核回查真实链接={real_url}")
            except Exception as ex:
                logger.warning(f"upload_image_and_get_url: 审核后台任务异常: {ex}")

        try:
            result = await bot.post_messages(
                channel_id=str(channel_id),
                content=" ",
                file_image=image_bytes,
            )
            if mode == "link":
                message_id = getattr(result, "id", None)
                if message_id:
                    real_url = await _resolve_real_url_from_message(str(message_id))
                    if real_url:
                        return real_url
                return fallback_url or md5_url

            return md5_url

        except AuditException as e:
            if mode == "md5":
                asyncio.create_task(_audit_followup(e))
                return md5_url

            try:
                audit_res = await e.get_audit_result(timeout=audit_timeout)  # type: ignore[attr-defined]
                event_name = str(getattr(audit_res, "__type__", ""))
                message_id = getattr(audit_res, "message_id", None)

                if "MESSAGE_AUDIT_PASS" not in event_name or not message_id:
                    logger.warning(
                        f"upload_image_and_get_url(link): 审核未通过或缺少message_id, "
                        f"event={event_name}, message_id={message_id}"
                    )
                    return fallback_url

                real_url = await _resolve_real_url_from_message(str(message_id))
                return real_url or fallback_url
            except Exception as ex:
                logger.warning(f"upload_image_and_get_url(link): 等待审核失败: {ex}")
                return fallback_url

        except Exception as e:
            logger.error(f"upload_image_and_get_url: 上传失败: {e}")
            return fallback_url


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
        QQAtChannelMessageEvent,
    ]
else:
    GroupMessageEvent = Union[OB11GroupMessageEvent]

if HAS_QQ:
    PrivateMessageEvent = Union[
        OB11PrivateMessageEvent,
        QQPrivateMessageEvent,
        QQChannelPrivateMessageEvent,
    ]
else:
    PrivateMessageEvent = Union[OB11PrivateMessageEvent]

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
        types.append(QQChannelPrivateMessageEvent)  # type: ignore[arg-type]
    return isinstance(event, tuple(types)) if types else False


def is_channel_event(event: BaseEvent) -> bool:
    """是否为频道来源事件（包括频道公域消息、频道私信）"""
    if HAS_QQ:
        if get_chat_scene(event) == "channel_group" or get_chat_scene(event) == "channel_private":
            return True
    return False


def get_chat_scene(event: BaseEvent) -> str:
    """
    获取会话场景：
    - group: 普通群
    - private: 普通私聊
    - channel_group: 频道内消息（@机器人消息，按群语义处理）
    - channel_private: 频道私信（按私聊语义处理）
    - unknown: 未识别
    """
    if HAS_QQ and isinstance(event, QQAtChannelMessageEvent):
        return "channel_group"
    if HAS_QQ and isinstance(event, QQChannelPrivateMessageEvent):
        return "channel_private"
    if is_group_event(event):
        return "group"
    if is_private_event(event):
        return "private"
    return "unknown"


_group_seq_cache: dict[str, int] = {}
_c2c_seq_cache: dict[str, int] = {}


def _next_group_seq(group_openid: str) -> int:
    current = _group_seq_cache.get(group_openid)
    if current is None:
        current = random.randint(1000, 900000)

    current += random.randint(1, 3)

    if current <= 0:
        current = 1
    if current > 1_000_000:
        current = random.randint(1, 10000)

    _group_seq_cache[group_openid] = current
    return current


def _next_c2c_seq(user_openid: str) -> int:
    current = _c2c_seq_cache.get(user_openid)
    if current is None:
        current = random.randint(1000, 900000)

    current += random.randint(1, 3)

    if current <= 0:
        current = 1
    if current > 1_000_000:
        current = random.randint(1, 10000)

    _c2c_seq_cache[user_openid] = current
    return current


def _is_msgseq_conflict_error(exc: Exception) -> bool:
    try:
        code = getattr(exc, "retcode", None)
        if code == 40054005:
            return True
    except Exception:
        pass

    s = str(exc)
    return ("40054005" in s) or ("消息被去重" in s) or ("msgseq" in s.lower())


async def _send_with_retry(
    send_coro_factory,
    *,
    get_new_seq=None,
    max_retry: int = 3,
    base_delay: float = 0.05,
):
    last_exc = None
    for i in range(max_retry + 1):
        try:
            kwargs = {}
            if get_new_seq is not None:
                kwargs["msg_seq"] = int(get_new_seq())
            return await send_coro_factory(**kwargs)
        except Exception as e:
            last_exc = e
            if (not _is_msgseq_conflict_error(e)) or i >= max_retry:
                raise
            delay = base_delay * (i + 1) + random.uniform(0.01, 0.08)
            logger.warning(
                f"[QQ发送重试] 检测到msg_seq冲突，准备重试 {i+1}/{max_retry}，"
                f"等待 {delay:.3f}s，错误: {e}"
            )
            await asyncio.sleep(delay)

    raise last_exc


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
    if hasattr(event, "channel_id"):
        gid = getattr(event, "channel_id")
        return str(gid) if gid is not None else None
    return None


def _resolve_sender_name(
    e: BaseEvent, fallback_user_id: Optional[str] = None
) -> str:
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


def _build_compat_sender(
    *,
    user_id: Optional[str],
    nickname: Optional[str],
    card: Optional[str],
    role: Optional[str] = "member",
) -> CompatSender:
    return CompatSender(
        user_id=str(user_id) if user_id is not None else None,
        nickname=nickname,
        card=card,
        role=role,
    )


def patch_event_inplace(event: BaseEvent) -> BaseEvent:
    if getattr(event, "__compat_patched__", False):
        return event

    if HAS_QQ and isinstance(event, QQPrivateMessageEvent):
        raw = event.content or ""
        sender_id = str(event.author.user_openid)
        sender_name = _resolve_sender_name(event, fallback_user_id=sender_id)

        setattr(event, "message_type", "private")
        setattr(event, "user_id", sender_id)
        setattr(event, "group_id", None)
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)
        setattr(
            event,
            "sender",
            _build_compat_sender(
                user_id=sender_id,
                nickname=sender_name,
                card=sender_name,
                role="member",
            ),
        )

    elif HAS_QQ and isinstance(event, QQChannelPrivateMessageEvent):
        raw = event.content or ""
        uid = getattr(getattr(event, "author", None), "id", None) or get_user_id(event) or ""
        uid = str(uid)
        sender_name = _resolve_sender_name(event, fallback_user_id=uid)

        setattr(event, "message_type", "private")
        setattr(event, "user_id", uid)
        setattr(event, "group_id", None)
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)
        setattr(
            event,
            "sender",
            _build_compat_sender(
                user_id=uid,
                nickname=sender_name,
                card=sender_name,
                role="member",
            ),
        )

    elif HAS_QQ and isinstance(event, QQGroupMessageEvent):
        raw = event.content or ""
        sender_id = str(event.author.member_openid)
        sender_name = _resolve_sender_name(event, fallback_user_id=sender_id)

        setattr(event, "message_type", "group")
        setattr(event, "user_id", sender_id)
        setattr(event, "group_id", str(event.group_openid))
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)
        setattr(
            event,
            "sender",
            _build_compat_sender(
                user_id=sender_id,
                nickname=sender_name,
                card=sender_name,
                role="member",
            ),
        )

    elif HAS_QQ and isinstance(event, QQAtChannelMessageEvent):
        raw = event.content or ""
        uid = getattr(getattr(event, "author", None), "id", None) or get_user_id(event) or ""
        uid = str(uid)
        sender_name = _resolve_sender_name(event, fallback_user_id=uid)

        setattr(event, "message_type", "group")
        setattr(event, "user_id", uid)
        setattr(event, "group_id", str(event.channel_id))
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)
        setattr(
            event,
            "sender",
            _build_compat_sender(
                user_id=uid,
                nickname=sender_name,
                card=sender_name,
                role="member",
            ),
        )

    if not hasattr(event, "user_id"):
        uid = get_user_id(event)
        if uid is not None:
            setattr(event, "user_id", uid)

    if not hasattr(event, "group_id"):
        gid = get_group_id(event)
        if gid is not None and is_group_event(event):
            setattr(event, "group_id", gid)

    if not hasattr(event, "sender"):
        sender_id = getattr(event, "user_id", None)
        sender_name = _resolve_sender_name(
            event, fallback_user_id=str(sender_id or "")
        )
        setattr(
            event,
            "sender",
            _build_compat_sender(
                user_id=sender_id,
                nickname=sender_name,
                card=sender_name,
                role="member",
            ),
        )

    setattr(event, "__compat_patched__", True)
    return event


def patch_bot_inplace(bot: BaseBot) -> BaseBot:
    if getattr(bot, "__compat_patched__", False):
        return bot

    if HAS_QQ and QQBot is not None and isinstance(bot, QQBot):
        _origin_send = bot.send

        async def send(event, message, **kwargs):
            # ===== 普通群 =====
            if HAS_QQ and isinstance(event, QQGroupMessageEvent):
                group_openid = str(event.group_openid)

                async def _do_send(msg_seq: int):
                    return await bot.send_to_group(
                        group_openid=group_openid,
                        message=message,
                        msg_id=str(event.id),
                        msg_seq=int(msg_seq),
                        event_id=kwargs.pop("event_id", None),
                    )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    return await _do_send(msg_seq)
                return await _send_with_retry(
                    _do_send,
                    get_new_seq=lambda: _next_group_seq(group_openid),
                    max_retry=3,
                )

            # ===== 私聊 C2C =====
            if HAS_QQ and isinstance(event, QQPrivateMessageEvent):
                user_openid = str(event.author.user_openid)

                async def _do_send(msg_seq: int):
                    return await bot.send_to_c2c(
                        openid=user_openid,
                        message=message,
                        msg_id=str(event.id),
                        msg_seq=int(msg_seq),
                        event_id=kwargs.pop("event_id", None),
                    )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    return await _do_send(msg_seq)
                return await _send_with_retry(
                    _do_send,
                    get_new_seq=lambda: _next_c2c_seq(user_openid),
                    max_retry=3,
                )

            # ===== 频道公域消息（群语义）=====
            if HAS_QQ and isinstance(event, QQAtChannelMessageEvent):
                channel_id = str(event.channel_id)

                async def _do_send(msg_seq: int):
                    try:
                        return await bot.send_to_channel(
                            channel_id=channel_id,
                            message=message,
                            msg_id=str(event.id),
                            msg_seq=int(msg_seq),
                            event_id=kwargs.pop("event_id", None),
                        )
                    except TypeError:
                        return await bot.send_to_channel(
                            channel_id=channel_id,
                            message=message,
                            msg_id=str(event.id),
                            event_id=kwargs.pop("event_id", None),
                        )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    return await _do_send(msg_seq)
                return await _send_with_retry(
                    _do_send,
                    get_new_seq=lambda: _next_group_seq(channel_id),
                    max_retry=3,
                )

            # ===== 频道私信 DMS（私聊语义）=====
            if HAS_QQ and isinstance(event, QQChannelPrivateMessageEvent):
                guild_id = str(event.guild_id)
                uid = str(getattr(getattr(event, "author", None), "id", "") or "")
                seq_key = f"{guild_id}:{uid}" if uid else guild_id

                async def _do_send(msg_seq: int):
                    try:
                        return await bot.send_to_dms(
                            guild_id=guild_id,
                            message=message,
                            msg_id=str(event.id),
                            msg_seq=int(msg_seq),
                            event_id=kwargs.pop("event_id", None),
                        )
                    except TypeError:
                        return await bot.send_to_dms(
                            guild_id=guild_id,
                            message=message,
                            msg_id=str(event.id),
                            event_id=kwargs.pop("event_id", None),
                        )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    return await _do_send(msg_seq)
                return await _send_with_retry(
                    _do_send,
                    get_new_seq=lambda: _next_c2c_seq(seq_key),
                    max_retry=3,
                )

            return await _origin_send(event=event, message=message, **kwargs)

        async def send_private_msg(*, user_id, message, **kwargs):
            openid = str(user_id)

            async def _do_send(msg_seq: int):
                return await bot.send_to_c2c(
                    openid=openid,
                    message=message,
                    msg_seq=int(msg_seq),
                    **kwargs
                )

            if "msg_seq" in kwargs:
                msg_seq = int(kwargs.pop("msg_seq"))
                return await _do_send(msg_seq)
            return await _send_with_retry(
                _do_send,
                get_new_seq=lambda: _next_c2c_seq(openid),
                max_retry=3,
            )

        async def send_group_msg(*, group_id, message, **kwargs):
            group_openid = str(group_id)

            async def _do_send(msg_seq: int):
                return await bot.send_to_group(
                    group_openid=group_openid,
                    message=message,
                    msg_id=kwargs.pop("msg_id", None),
                    msg_seq=int(msg_seq),
                    event_id=kwargs.pop("event_id", None),
                )

            if "msg_seq" in kwargs:
                msg_seq = int(kwargs.pop("msg_seq"))
                return await _do_send(msg_seq)
            return await _send_with_retry(
                _do_send,
                get_new_seq=lambda: _next_group_seq(group_openid),
                max_retry=3,
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
    "CompatSender",
    "GroupMessageEvent",
    "PrivateMessageEvent",
    "MessageEvent",
    "is_group_event",
    "is_private_event",
    "is_channel_event",
    "get_chat_scene",
    "get_user_id",
    "get_group_id",
    "patch_bot_inplace",
    "patch_event_inplace",
    "patch_context",
]