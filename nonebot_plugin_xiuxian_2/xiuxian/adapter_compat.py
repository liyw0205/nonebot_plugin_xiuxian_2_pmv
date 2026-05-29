from __future__ import annotations

import asyncio
import hashlib
import json
import random
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Iterator, Literal, Optional, Union
from urllib.parse import urlparse

from nonebot.adapters import Bot as BaseBot
from nonebot.adapters import Event as BaseEvent
from nonebot.log import logger
from nonebot.permission import Permission

from .xiuxian_utils.message_db import (
    get_message_db_path as _get_message_db_path,
    increase_recv_reply_used_count as _increase_recv_reply_used_count,
    init_message_db as _init_message_db,
    insert_message_record as _insert_message_record,
    record_group_user_nickname as _record_group_user_nickname,
)

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
        AtMessageCreateEvent as QQAtChannelMessageEvent,
        DirectMessageCreateEvent as QQChannelPrivateMessageEvent,
    )
    try:
        from nonebot.adapters.qq.event import (
            GroupMessageCreateEvent as QQGroupMessageCreateEvent,
        )
    except Exception:
        QQGroupMessageCreateEvent = None  # type: ignore

    try:
        from nonebot.adapters.qq.event import (
            GroupAtMessageCreateEvent as QQGroupAtMessageEvent,
        )
    except Exception:
        QQGroupAtMessageEvent = None  # type: ignore

    if QQGroupMessageCreateEvent is None and QQGroupAtMessageEvent is None:
        raise ImportError("QQ adapter has no group message event class")
    if QQGroupMessageCreateEvent is None:
        QQGroupMessageCreateEvent = QQGroupAtMessageEvent  # type: ignore
    if QQGroupAtMessageEvent is None:
        QQGroupAtMessageEvent = QQGroupMessageCreateEvent  # type: ignore

    QQGroupMessageEvent = QQGroupMessageCreateEvent
    from nonebot.adapters.qq.models import (
        Action as QQKeyboardAction,
        Button as QQKeyboardButton,
        InlineKeyboard as QQInlineKeyboard,
        InlineKeyboardRow as QQInlineKeyboardRow,
        MessageMarkdown,
        MessageKeyboard,
        Permission as QQKeyboardPermission,
        RenderData as QQKeyboardRenderData,
    )

    HAS_QQ = True
except Exception:
    HAS_QQ = False
    QQBot = None  # type: ignore
    QQMessage = str  # type: ignore
    QQMessageSegment = None  # type: ignore
    QQPrivateMessageEvent = tuple()  # type: ignore
    QQGroupMessageCreateEvent = tuple()  # type: ignore
    QQGroupAtMessageEvent = tuple()  # type: ignore
    QQGroupMessageEvent = tuple()  # type: ignore
    QQAtChannelMessageEvent = tuple()  # type: ignore
    QQChannelPrivateMessageEvent = tuple()  # type: ignore
    QQKeyboardAction = None  # type: ignore
    QQKeyboardButton = None  # type: ignore
    QQInlineKeyboard = None  # type: ignore
    QQInlineKeyboardRow = None  # type: ignore
    MessageMarkdown = None  # type: ignore
    MessageKeyboard = None  # type: ignore
    QQKeyboardPermission = None  # type: ignore
    QQKeyboardRenderData = None  # type: ignore


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


def _normalize_logged_http_url(url: str) -> str:
    return re.sub(r"\s+", "", str(url or "").strip())


def _extract_attachment_url_from_repr_data(data_body: str) -> str:
    data_body = str(data_body or "")

    try:
        data = json.loads(data_body.replace("'", '"'))
        if isinstance(data, dict):
            for key in ("url", "file", "path", "src"):
                value = data.get(key)
                if isinstance(value, str) and value.startswith(("http://", "https://")):
                    return _normalize_logged_http_url(value)
    except Exception:
        pass

    m = re.search(
        r"['\"](?:url|file|path|src)['\"]\s*:\s*(['\"])(?P<url>https?://[\s\S]*?)\1",
        data_body,
        re.S,
    )
    return _normalize_logged_http_url(m.group("url")) if m else ""


def _normalize_attachment_repr_text(text: str) -> str:
    def replace_attachment(match):
        media_type = str(match.group("type") or "attachment").strip().lower()
        if media_type in ("record", "voice"):
            media_type = "audio"

        url = _extract_attachment_url_from_repr_data(match.group("data"))
        return f"<attachment[{media_type}]:{url}>" if url else match.group(0)

    return re.sub(
        r"Attachment\(\s*type=['\"](?P<type>[^'\"]+)['\"]\s*,\s*data=(?P<data>\{[\s\S]*?\})\s*\)",
        replace_attachment,
        str(text or ""),
        flags=re.S,
    )


def _extract_text_from_message_obj(message: Any) -> str:
    """
    提取消息展示内容：
    - 文本正常保留
    - QQ Attachment 转成统一标记：
      <attachment[image]:url>
      <attachment[audio]:url>
      <attachment[video]:url>
      <attachment[file]:url>
    - 图文混合时保留文本 + 图片标记，避免 extract_plain_text 丢图
    """
    try:
        if message is None:
            return ""

        if isinstance(message, str):
            return message

        parts: list[str] = []

        # nonebot Message 通常可迭代，每个 seg 有 type/data
        try:
            for seg in message:
                seg_type = str(getattr(seg, "type", "") or "")
                data = getattr(seg, "data", {}) or {}

                # 兼容 data 不是 dict 的情况
                if not isinstance(data, dict):
                    try:
                        data = dict(data)
                    except Exception:
                        data = {}

                if seg_type == "text":
                    text = data.get("text", "")
                    if text:
                        parts.append(str(text))

                elif seg_type in ("image", "audio", "record", "voice", "video", "file", "attachment"):
                    url = (
                        data.get("url")
                        or data.get("file")
                        or data.get("path")
                        or data.get("src")
                        or ""
                    )

                    # QQ 官方适配器 Attachment 可能 type=attachment，data 里带 url
                    media_type = seg_type
                    if seg_type == "record" or seg_type == "voice":
                        media_type = "audio"

                    # 尝试从字符串里识别 attachment[image]
                    seg_str = str(seg)
                    m = None
                    try:
                        import re
                        m = re.search(r"<attachment\[(?P<t>[^\]]+)\]:(?P<u>https?://[^>]+)>", seg_str)
                    except Exception:
                        m = None

                    if m:
                        media_type = m.group("t")
                        url = m.group("u")

                    if url:
                        parts.append(f"<attachment[{media_type}]:{_normalize_logged_http_url(url)}>")
                    else:
                        parts.append(_normalize_attachment_repr_text(seg_str))

                else:
                    # 兜底：保留非文本段字符串，避免信息丢失
                    seg_str = _normalize_attachment_repr_text(str(seg))
                    if seg_str:
                        parts.append(seg_str)

            if parts:
                return "".join(parts)

        except TypeError:
            # 不可迭代，继续走下面兜底
            pass
        except Exception:
            pass

        # 兜底：如果能提取纯文本就用纯文本
        if hasattr(message, "extract_plain_text"):
            text = message.extract_plain_text()
            if text:
                return str(text)

        if hasattr(message, "extract_content"):
            text = message.extract_content()
            if text:
                return str(text)

        return _normalize_attachment_repr_text(str(message))

    except Exception:
        return ""

def _get_event_plaintext(event: BaseEvent, fallback: str = "") -> str:
    try:
        msg = event.get_message()
        text = _extract_text_from_message_obj(msg)
        if text:
            return text
    except Exception:
        pass
    return str(fallback or "")


def _to_nonempty_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        text = str(value)
    except Exception:
        return None
    return text if text != "" else None


def _copy_message_obj(message: Any) -> Any:
    if message is None:
        return None

    try:
        return message.__class__(message)
    except Exception:
        pass

    try:
        return message.copy()
    except Exception:
        return message


def _timestamp_to_epoch(value: Any) -> int:
    try:
        if isinstance(value, datetime):
            return int(value.timestamp())

        if isinstance(value, (int, float)):
            return int(value)

        text = str(value or "").strip()
        if text:
            try:
                return int(float(text))
            except Exception:
                pass

            try:
                return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
            except Exception:
                pass
    except Exception:
        pass

    return int(datetime.now().timestamp())


def _set_event_text_cache(event: BaseEvent, raw: str) -> None:
    """
    更新本插件兼容字段，但不覆盖 adapter-qq 1.7.1 起使用的 event.message 缓存。

    新版 adapter-qq 的 get_message() 会读写 event.message；如果这里把它设成 str，
    后续命令解析会拿不到真正的 Message 对象。
    """
    raw = str(raw or "")
    setattr(event, "raw_message", raw)
    setattr(event, "plaintext", raw)

    try:
        msg = event.get_message()
    except Exception:
        msg = None

    if msg is not None:
        if not hasattr(event, "original_message"):
            try:
                setattr(event, "original_message", _copy_message_obj(msg))
            except Exception:
                pass

        try:
            setattr(event, "message", msg)
        except Exception:
            pass
    elif not (
        HAS_QQ
        and isinstance(
            event,
            (QQPrivateMessageEvent, QQChannelPrivateMessageEvent)
            + _QQ_GROUP_MESSAGE_EVENT_TYPES,
        )
    ):
        try:
            setattr(event, "message", raw)
        except Exception:
            pass


def _ensure_message_common_fields(
    event: BaseEvent,
    *,
    message_type: str,
    user_id: Any,
    group_id: Any = None,
    message_id: Any = None,
    sub_type: str = "normal",
    set_group_id: bool = True,
) -> None:
    setattr(event, "post_type", "message")
    setattr(event, "message_type", message_type)
    setattr(event, "sub_type", sub_type)
    setattr(event, "user_id", str(user_id or ""))
    if set_group_id:
        setattr(event, "group_id", str(group_id) if group_id is not None else None)
    setattr(event, "message_id", str(message_id or getattr(event, "id", "") or ""))

    if not hasattr(event, "time"):
        setattr(event, "time", _timestamp_to_epoch(getattr(event, "timestamp", None)))
    if not hasattr(event, "font"):
        setattr(event, "font", 0)
    if message_type == "group" and not hasattr(event, "anonymous"):
        setattr(event, "anonymous", None)


def _get_qq_message_ref_id(event: BaseEvent) -> Optional[str]:
    try:
        message_scene = getattr(event, "message_scene", None)
        for ext in getattr(message_scene, "ext", None) or []:
            ext_text = str(ext)
            if ext_text.startswith("msg_idx="):
                value = ext_text.partition("=")[-1]
                return value or None
    except Exception:
        pass

    return _to_nonempty_str(getattr(event, "msg_idx", None))


def _get_adapter_name(bot: Any) -> str:
    try:
        return str(bot.adapter.get_name())
    except Exception:
        return ""


def _get_bot_id(bot: Any) -> str:
    try:
        return str(bot.self_id)
    except Exception:
        return ""


def _extract_result_message_id(result: Any) -> str:
    try:
        if result is None:
            return ""

        if isinstance(result, dict):
            return str(
                result.get("message_id")
                or result.get("msg_id")
                or result.get("id")
                or ""
            )

        return str(
            getattr(result, "message_id", "")
            or getattr(result, "msg_id", "")
            or getattr(result, "id", "")
            or ""
        )
    except Exception:
        return ""


def _get_author_username_avatar(event: BaseEvent) -> tuple[str, str]:
    username = ""
    avatar = ""

    try:
        author = getattr(event, "author", None)
        if author is not None:
            username = str(getattr(author, "username", "") or "")
            avatar = str(getattr(author, "avatar", "") or "")
    except Exception:
        pass

    return username, avatar

def _record_recv_message(bot: Any, event: BaseEvent):
    """记录收到的消息"""
    try:
        if getattr(event, "__message_db_recv_recorded__", False):
            return

        if event.get_type() != "message":
            return

        scene = get_chat_scene(event)
        adapter = _get_adapter_name(bot) if bot is not None else ""
        bot_id = _get_bot_id(bot) if bot is not None else ""

        message_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "")
        user_id = str(getattr(event, "user_id", "") or get_user_id(event) or "")
        group_id = str(getattr(event, "group_id", "") or get_group_id(event) or "")

        raw_message = str(
            getattr(event, "raw_message", "")
            or getattr(event, "content", "")
            or ""
        )

        content = raw_message
        try:
            msg = event.get_message()
            content = _extract_text_from_message_obj(msg) or raw_message
        except Exception:
            pass

        sender = getattr(event, "sender", None)
        nickname = ""
        username = ""
        avatar = ""

        if sender is not None:
            nickname = str(getattr(sender, "nickname", "") or "")
            username = str(getattr(sender, "card", "") or nickname or "")

        author_username, author_avatar = _get_author_username_avatar(event)
        username = author_username or username
        avatar = author_avatar or avatar

        group_name = str(getattr(event, "group_name", "") or "")

        _insert_message_record(
            adapter=adapter,
            bot_id=bot_id,
            direction="recv",
            scene=scene,
            message_id=message_id,
            group_id=group_id,
            group_name=group_name,
            user_id=user_id,
            username=username,
            nickname=nickname,
            avatar=avatar,
            content=content,
        )
        
        _record_group_user_nickname(
            adapter=adapter,
            bot_id=bot_id,
            scene=scene,
            group_id=group_id,
            user_id=user_id,
            username=username,
        )

        setattr(event, "__message_db_recv_recorded__", True)
    except Exception as e:
        logger.warning(f"[message.db] 记录接收消息失败: {e}")


def _record_send_message(
    bot: Any,
    *,
    scene: str,
    message: Any,
    message_id: str = "",
    source_message_id: str = "",
    group_id: str = "",
    user_id: str = "",
    raw_result: Any = None,
):
    """
    记录发送消息。

    注意：
    - message_id：本次发送出去的消息ID
    - source_message_id：被回复的 recv.message_id
    - 如果 source_message_id 存在，说明这次发送消耗了一次被回复消息的回复次数，
      需要更新对应 recv 记录的 reply_used_count。
    """
    try:
        adapter = _get_adapter_name(bot)
        bot_id = _get_bot_id(bot)
        content = _extract_text_from_message_obj(message)

        _insert_message_record(
            adapter=adapter,
            bot_id=bot_id,
            direction="send",
            scene=scene,
            message_id=str(message_id or ""),
            source_message_id=str(source_message_id or ""),
            group_id=str(group_id or ""),
            user_id=str(user_id or ""),
            username="Bot",
            nickname="Bot",
            content=content,
        )

        if source_message_id:
            _increase_recv_reply_used_count(
                source_message_id=str(source_message_id),
                adapter=adapter,
                bot_id=bot_id,
                scene=scene,
                group_id=str(group_id or ""),
                user_id=str(user_id or ""),
            )

    except Exception as e:
        logger.warning(f"[message.db] 记录发送消息失败: {e}")

async def delete_message_compat(
    bot: Any,
    *,
    scene: str,
    message_id: str,
    group_id: str = "",
    user_id: str = "",
):
    """
    跨适配器通用撤回接口。

    scene:
    - group
    - private
    - channel_group
    - channel_private
    """
    if not message_id:
        raise ValueError("message_id 不能为空")

    adapter = _get_adapter_name(bot)

    # =========================
    # OneBot V11
    # =========================
    if HAS_OB11 and OB11Bot is not None and isinstance(bot, OB11Bot):
        mid = int(message_id) if str(message_id).isdigit() else message_id

        if hasattr(bot, "delete_msg"):
            return await bot.delete_msg(message_id=mid)

        return await bot.call_api("delete_msg", message_id=mid)

    # =========================
    # QQ 官方适配器
    # =========================
    if HAS_QQ and QQBot is not None and isinstance(bot, QQBot):
        if scene == "group":
            if not group_id:
                raise ValueError("QQ 群聊撤回需要 group_id/group_openid")

            return await bot.delete_group_message(
                group_openid=str(group_id),
                message_id=str(message_id),
            )

        if scene == "private":
            if not user_id:
                raise ValueError("QQ 私聊撤回需要 user_id/openid")

            return await bot.delete_c2c_message(
                openid=str(user_id),
                message_id=str(message_id),
            )

        if scene == "channel_group":
            if not group_id:
                raise ValueError("QQ 频道群聊撤回需要 channel_id")

            return await bot.delete_message(
                channel_id=str(group_id),
                message_id=str(message_id),
            )

        if scene == "channel_private":
            guild_id = group_id or user_id
            if not guild_id:
                raise ValueError("QQ 频道私信撤回需要 guild_id")

            return await bot.delete_dms_message(
                guild_id=str(guild_id),
                message_id=str(message_id),
            )

    raise RuntimeError(f"当前适配器不支持通用撤回: {adapter}")


def schedule_delete_message(
    bot: Any,
    *,
    scene: str,
    message_id: str,
    group_id: str = "",
    user_id: str = "",
    revoke_time: int | float = 0,
):
    """
    定时撤回消息。

    revoke_time:
    - <= 0 不撤回
    - > 0 按秒延迟撤回
    """
    try:
        revoke_time = float(revoke_time or 0)
    except Exception:
        revoke_time = 0

    if revoke_time <= 0 or not message_id:
        return

    async def _job():
        try:
            await asyncio.sleep(revoke_time)

            await delete_message_compat(
                bot,
                scene=scene,
                message_id=message_id,
                group_id=group_id,
                user_id=user_id,
            )

            logger.info(
                f"[自动撤回] 已撤回消息 scene={scene}, message_id={message_id}, "
                f"group_id={group_id}, user_id={user_id}"
            )

        except Exception as e:
            logger.warning(
                f"[自动撤回] 撤回失败 scene={scene}, message_id={message_id}, "
                f"group_id={group_id}, user_id={user_id}: {e}"
            )

    try:
        asyncio.create_task(_job())
    except RuntimeError:
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_job())
        except Exception as e:
            logger.warning(f"[自动撤回] 创建撤回任务失败: {e}")


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
    def qq_inline_command_button(
        label: str,
        command: Optional[str] = None,
        *,
        button_id: Optional[str] = None,
        action_type: Optional[int] = None,
        style: int = 1,
        enter: bool = False,
        reply: bool = False,
        permission_type: int = 2,
        specify_role_ids: Optional[list[str]] = None,
        specify_user_ids: Optional[list[str]] = None,
    ):
        if not (
            HAS_QQ
            and QQKeyboardButton is not None
            and QQKeyboardRenderData is not None
            and QQKeyboardAction is not None
            and QQKeyboardPermission is not None
        ):
            raise RuntimeError("当前环境未安装可用 QQ 适配器，无法构造 keyboard 按钮")

        label_text = str(label or " ").replace("\r", " ").replace("\n", " ").strip() or " "
        command_text = str(command if command is not None else label).replace("\r", " ").replace("\n", " ").strip()
        if not command_text:
            command_text = label_text

        if action_type is None:
            parsed = urlparse(command_text)
            if parsed.scheme in {"http", "https"}:
                action_type = 0
            elif parsed.scheme == "mqqapi":
                action_type = 3
            else:
                action_type = 2

        if not button_id:
            digest = hashlib.md5(f"{label_text}:{command_text}".encode("utf-8")).hexdigest()[:12]
            button_id = f"btn_{digest}"

        return QQKeyboardButton(  # type: ignore[misc]
            id=str(button_id),
            render_data=QQKeyboardRenderData(  # type: ignore[misc]
                label=label_text,
                visited_label=label_text,
                style=style,
            ),
            action=QQKeyboardAction(  # type: ignore[misc]
                type=action_type,
                permission=QQKeyboardPermission(  # type: ignore[misc]
                    type=permission_type,
                    specify_role_ids=specify_role_ids,
                    specify_user_ids=specify_user_ids,
                ),
                data=command_text,
                enter=enter,
                reply=reply,
            ),
        )

    @staticmethod
    def qq_inline_keyboard(
        rows: list[list[tuple[str, str] | Any]],
        *,
        action_type: Optional[int] = None,
        style: int = 1,
        enter: bool = False,
        reply: bool = False,
        permission_type: int = 2,
        specify_role_ids: Optional[list[str]] = None,
        specify_user_ids: Optional[list[str]] = None,
    ):
        if not (
            HAS_QQ
            and QQInlineKeyboard is not None
            and QQInlineKeyboardRow is not None
            and MessageKeyboard is not None
        ):
            raise RuntimeError("当前环境未安装可用 QQ 适配器，无法构造 keyboard")

        keyboard_rows = []
        for row in rows:
            buttons = []
            for index, item in enumerate(row):
                if HAS_QQ and QQKeyboardButton is not None and isinstance(item, QQKeyboardButton):
                    buttons.append(item)
                    continue

                label, command = item
                buttons.append(
                    CompatMessageSegment.qq_inline_command_button(
                        str(label),
                        str(command),
                        button_id=f"btn_{len(keyboard_rows)}_{index}",
                        action_type=action_type,
                        style=style,
                        enter=enter,
                        reply=reply,
                        permission_type=permission_type,
                        specify_role_ids=specify_role_ids,
                        specify_user_ids=specify_user_ids,
                    )
                )

            if buttons:
                keyboard_rows.append(QQInlineKeyboardRow(buttons=buttons))  # type: ignore[misc]

        return MessageKeyboard(content=QQInlineKeyboard(rows=keyboard_rows))  # type: ignore[misc]

    @staticmethod
    def markdown_keyboard(
        bot: Any,
        msg: str,
        rows: list[list[tuple[str, str] | Any]],
        *,
        action_type: Optional[int] = None,
        style: int = 1,
        enter: bool = False,
        reply: bool = False,
        permission_type: int = 2,
        specify_role_ids: Optional[list[str]] = None,
        specify_user_ids: Optional[list[str]] = None,
    ):
        if not CompatMessageSegment._is_qq_bot(bot):
            raise RuntimeError("自定义 keyboard 仅支持 QQ 官方适配器")

        md_seg = QQMessageSegment.markdown(MessageMarkdown(content=msg or " "))  # type: ignore[union-attr,misc]
        kb_seg = QQMessageSegment.keyboard(  # type: ignore[union-attr]
            CompatMessageSegment.qq_inline_keyboard(
                rows,
                action_type=action_type,
                style=style,
                enter=enter,
                reply=reply,
                permission_type=permission_type,
                specify_role_ids=specify_role_ids,
                specify_user_ids=specify_user_ids,
            )
        )
        return QQMessage(md_seg) + kb_seg  # type: ignore[call-arg]

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
    def reference(bot: Any, message_id: str, ignore_error: bool = True):
        if CompatMessageSegment._is_qq_bot(bot):
            if hasattr(QQMessageSegment, "reference"):
                try:
                    return QQMessageSegment.reference(  # type: ignore[union-attr]
                        str(message_id),
                        ignore_get_message_error=bool(ignore_error),
                    )
                except TypeError:
                    try:
                        return QQMessageSegment.reference(str(message_id), bool(ignore_error))  # type: ignore[union-attr]
                    except TypeError:
                        return QQMessageSegment.reference(str(message_id))  # type: ignore[union-attr]

        return ""

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
        - "md5": 立即返回 md5 链接
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
                    logger.warning(
                        f"upload_image_and_get_url: 审核未通过或缺少message_id, "
                        f"event={event_name}, message_id={message_id}"
                    )
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


def _unique_event_types(*event_types: Any) -> tuple[type, ...]:
    types: list[type] = []
    for event_type in event_types:
        if isinstance(event_type, type) and event_type not in types:
            types.append(event_type)
    return tuple(types)


def _event_type_union(event_types: tuple[type, ...]) -> Any:
    if not event_types:
        return BaseEvent
    if len(event_types) == 1:
        return event_types[0]
    return Union[event_types]  # type: ignore[index]


_QQ_GROUP_MESSAGE_EVENT_TYPES = _unique_event_types(
    QQGroupMessageCreateEvent,
    QQGroupAtMessageEvent,
)

_GROUP_MESSAGE_EVENT_TYPES = _unique_event_types(
    OB11GroupMessageEvent,
    QQGroupMessageCreateEvent,
    QQGroupAtMessageEvent,
    QQAtChannelMessageEvent,
)
_PRIVATE_MESSAGE_EVENT_TYPES = _unique_event_types(
    OB11PrivateMessageEvent,
    QQPrivateMessageEvent,
    QQChannelPrivateMessageEvent,
)
_MESSAGE_EVENT_TYPES = _unique_event_types(
    *_GROUP_MESSAGE_EVENT_TYPES,
    *_PRIVATE_MESSAGE_EVENT_TYPES,
)

GroupMessageEvent = _event_type_union(_GROUP_MESSAGE_EVENT_TYPES)
PrivateMessageEvent = _event_type_union(_PRIVATE_MESSAGE_EVENT_TYPES)
MessageEvent = _event_type_union(_MESSAGE_EVENT_TYPES)


def is_group_event(event: BaseEvent) -> bool:
    return isinstance(event, _GROUP_MESSAGE_EVENT_TYPES) if _GROUP_MESSAGE_EVENT_TYPES else False


def is_private_event(event: BaseEvent) -> bool:
    return isinstance(event, _PRIVATE_MESSAGE_EVENT_TYPES) if _PRIVATE_MESSAGE_EVENT_TYPES else False


def is_channel_event(event: BaseEvent) -> bool:
    if HAS_QQ:
        if get_chat_scene(event) in ("channel_group", "channel_private"):
            return True
    return False


def get_chat_scene(event: BaseEvent) -> str:
    """
    获取会话场景：
    - group
    - private
    - channel_group
    - channel_private
    - unknown
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
                f"[QQ发送重试] 检测到msg_seq冲突，准备重试 {i + 1}/{max_retry}，"
                f"等待 {delay:.3f}s，错误: {e}"
            )
            await asyncio.sleep(delay)

    raise last_exc


async def _group_checker(event: BaseEvent) -> bool:
    return is_group_event(event)


GROUP: Permission = Permission(_group_checker)


def get_user_id(event: BaseEvent) -> Optional[str]:
    uid = _to_nonempty_str(getattr(event, "user_id", None))
    if uid is not None:
        return uid

    try:
        uid = _to_nonempty_str(event.get_user_id())
        if uid is not None:
            return uid
    except Exception:
        pass

    author = getattr(event, "author", None)
    for attr in ("user_openid", "member_openid", "id"):
        uid = _to_nonempty_str(getattr(author, attr, None))
        if uid is not None:
            return uid

    return None


def get_group_id(event: BaseEvent) -> Optional[str]:
    if HAS_QQ and isinstance(event, _QQ_GROUP_MESSAGE_EVENT_TYPES):
        for attr in ("group_openid", "group_id"):
            gid = _to_nonempty_str(getattr(event, attr, None))
            if gid is not None:
                return gid

    if HAS_QQ and isinstance(event, QQAtChannelMessageEvent):
        gid = _to_nonempty_str(getattr(event, "channel_id", None))
        if gid is not None:
            return gid

    for attr in ("group_id", "group_openid", "channel_id"):
        gid = _to_nonempty_str(getattr(event, attr, None))
        if gid is not None:
            return gid

    return None


def _resolve_sender_name(
    e: BaseEvent,
    fallback_user_id: Optional[str] = None,
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


def _collect_bot_self_ids(bot: Optional[BaseBot]) -> set[str]:
    ids: set[str] = set()
    if bot is None:
        return ids

    def add(value: Any):
        if value is not None:
            ids.add(str(value))

    try:
        add(getattr(bot, "self_id", None))
    except Exception:
        pass

    try:
        bot_info = getattr(bot, "bot_info", None)
        for attr in ("id", "app_id"):
            add(getattr(bot_info, attr, None))
    except Exception:
        pass

    self_info = None
    try:
        self_info = getattr(bot, "_self_info", None)
    except Exception:
        pass
    if self_info is None:
        try:
            self_info = getattr(bot, "self_info", None)
        except Exception:
            self_info = None

    for attr in ("id", "user_id", "openid", "user_openid", "member_openid", "union_openid", "union_user_account"):
        try:
            add(getattr(self_info, attr, None))
        except Exception:
            pass

    return ids


def _mention_data_is_bot(data: Any, bot_ids: set[str]) -> bool:
    if not isinstance(data, dict):
        return False

    if bool(data.get("is_you", False)):
        return True
    if bool(data.get("is_bot", False)):
        return True

    if not bot_ids:
        return False

    for key in ("user_id", "id", "member_openid", "openid", "user_openid"):
        value = data.get(key)
        if value is not None and str(value) in bot_ids:
            return True

    return False


def _mention_user_is_bot(user: Any, bot_ids: set[str]) -> bool:
    if isinstance(user, dict):
        return _mention_data_is_bot(user, bot_ids)

    if bool(getattr(user, "is_you", False)):
        return True

    if not bot_ids:
        return False

    for attr in ("id", "user_id", "member_openid", "openid", "user_openid"):
        value = getattr(user, attr, None)
        if value is not None and str(value) in bot_ids:
            return True

    return False


_MENTION_SEGMENT_TYPES = {"at", "mention_user", "group_mention_user"}
_MENTION_USER_ID_KEYS = (
    "qq",
    "user_id",
    "member_openid",
    "id",
    "openid",
    "user_openid",
    "union_openid",
    "union_user_account",
)


def _mention_user_id_from_data(data: Any) -> Optional[str]:
    if isinstance(data, dict):
        for key in _MENTION_USER_ID_KEYS:
            uid = _to_nonempty_str(data.get(key))
            if uid is not None and uid.lower() != "all":
                return uid
        return None

    for attr in _MENTION_USER_ID_KEYS:
        uid = _to_nonempty_str(getattr(data, attr, None))
        if uid is not None and uid.lower() != "all":
            return uid

    return None


def _mention_user_id_from_segment(seg: Any) -> Optional[str]:
    if isinstance(seg, dict):
        seg_type = str(seg.get("type", "") or "")
        data = seg.get("data", {}) or {}
    else:
        seg_type = str(getattr(seg, "type", "") or "")
        data = getattr(seg, "data", {}) or {}

    if seg_type not in _MENTION_SEGMENT_TYPES:
        return None

    return _mention_user_id_from_data(data)


def _iter_message_segments(message_or_event: Any):
    message = message_or_event
    try:
        get_message = getattr(message_or_event, "get_message", None)
        if callable(get_message):
            message = get_message()
    except Exception:
        message = message_or_event

    if isinstance(message, dict):
        yield message
        return

    if isinstance(message, (str, bytes)):
        return

    if getattr(message, "type", None) is not None and hasattr(message, "data"):
        yield message
        return

    try:
        yield from message
    except TypeError:
        return


def get_at_user_ids(message_or_event: Any) -> list[str]:
    """提取消息中的被艾特用户 ID，兼容 OneBot v11 与 QQ 全量消息。"""
    user_ids: list[str] = []

    def add(uid: Optional[str]) -> None:
        if uid is not None and uid not in user_ids:
            user_ids.append(uid)

    for seg in _iter_message_segments(message_or_event):
        add(_mention_user_id_from_segment(seg))

    try:
        mentions = getattr(message_or_event, "mentions", None) or []
    except Exception:
        mentions = []

    for user in mentions:
        add(_mention_user_id_from_data(user))

    return user_ids


def get_at_user_id(message_or_event: Any) -> Optional[str]:
    """返回消息中的第一个被艾特用户 ID。"""
    user_ids = get_at_user_ids(message_or_event)
    return user_ids[0] if user_ids else None


def has_at_user(message_or_event: Any) -> bool:
    return bool(get_at_user_ids(message_or_event))


def _qq_reply_is_to_bot(event: BaseEvent, bot: Optional[BaseBot] = None) -> bool:
    if not (HAS_QQ and isinstance(event, (QQPrivateMessageEvent,) + _QQ_GROUP_MESSAGE_EVENT_TYPES)):
        return False

    bot_names: set[str] = set()
    if bot is not None:
        for obj in (
            getattr(bot, "self_info", None),
            getattr(bot, "_self_info", None),
            getattr(bot, "bot_info", None),
        ):
            if obj is None:
                continue
            for attr in ("username", "name", "nickname"):
                value = getattr(obj, attr, None)
                if value:
                    bot_names.add(str(value))

    replies = []
    direct_reply = getattr(event, "reply", None)
    if direct_reply is not None:
        replies.append(direct_reply)

    msg_elements = getattr(event, "msg_elements", None) or []
    if msg_elements:
        replies.extend(msg_elements)

    for reply in replies:
        author = getattr(reply, "author", None)
        if author is None:
            continue

        if not bool(getattr(author, "bot", False)):
            continue

        username = getattr(author, "username", None)
        if username and str(username) in bot_names:
            try:
                setattr(event, "reply", reply)
            except Exception:
                pass
            return True

    return False


def _strip_qq_group_at_me(event: BaseEvent, bot: Optional[BaseBot] = None) -> None:
    bot_ids = _collect_bot_self_ids(bot)

    def is_at_me_seg(seg: Any) -> bool:
        if getattr(seg, "type", "") not in ("group_mention_user", "mention_user"):
            return False
        data = getattr(seg, "data", {}) or {}
        return _mention_data_is_bot(data, bot_ids)

    try:
        message = event.get_message()
    except Exception:
        return

    if not message:
        return

    if is_at_me_seg(message[0]):
        message.pop(0)
        setattr(event, "to_me", True)
        if message and getattr(message[0], "type", "") == "text":
            data = getattr(message[0], "data", {}) or {}
            if isinstance(data, dict):
                data["text"] = str(data.get("text", "")).lstrip("\xa0").lstrip()
                if not data["text"]:
                    del message[0]
        return

    index = -1
    last_seg = message[index]
    last_data = getattr(last_seg, "data", {}) or {}
    if (
        getattr(last_seg, "type", "") == "text"
        and isinstance(last_data, dict)
        and not str(last_data.get("text", "")).strip()
        and len(message) >= 2
    ):
        index -= 1
        last_seg = message[index]

    if is_at_me_seg(last_seg):
        setattr(event, "to_me", True)
        del message[index:]


def _is_qq_group_message_to_me(
    event: BaseEvent,
    bot: Optional[BaseBot] = None,
) -> bool:
    if bool(getattr(event, "to_me", False)):
        return True

    if _qq_reply_is_to_bot(event, bot):
        return True

    bot_ids = _collect_bot_self_ids(bot)

    try:
        mentions = getattr(event, "mentions", None) or []
        if any(_mention_user_is_bot(user, bot_ids) for user in mentions):
            return True
    except Exception:
        pass

    try:
        for seg in event.get_message():
            if getattr(seg, "type", "") not in ("group_mention_user", "mention_user"):
                continue

            data = getattr(seg, "data", {}) or {}
            if _mention_data_is_bot(data, bot_ids):
                return True
    except Exception:
        pass

    return False


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


def patch_event_inplace(
    event: BaseEvent,
    bot: Optional[BaseBot] = None,
) -> BaseEvent:
    if getattr(event, "__compat_patched__", False):
        if (
            bot is not None
            and HAS_QQ
            and isinstance(event, _QQ_GROUP_MESSAGE_EVENT_TYPES)
            and _is_qq_group_message_to_me(event, bot)
        ):
            setattr(event, "to_me", True)
            _strip_qq_group_at_me(event, bot)
        return event

    if HAS_QQ and isinstance(event, QQPrivateMessageEvent):
        raw = _get_event_plaintext(event, getattr(event, "content", "") or "")
        author = getattr(event, "author", None)
        sender_id = (
            _to_nonempty_str(getattr(author, "user_openid", None))
            or _to_nonempty_str(getattr(author, "id", None))
            or ""
        )
        openid = _to_nonempty_str(getattr(author, "id", None)) or sender_id
        sender_name = _resolve_sender_name(event, fallback_user_id=sender_id)

        _ensure_message_common_fields(
            event,
            message_type="private",
            user_id=sender_id,
            group_id=None,
            message_id=getattr(event, "id", ""),
            sub_type="friend",
        )
        setattr(event, "user_openid", sender_id)
        setattr(event, "openid", openid)
        _set_event_text_cache(event, raw)
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
        raw = _get_event_plaintext(event, getattr(event, "content", "") or "")
        author = getattr(event, "author", None)
        uid = _to_nonempty_str(getattr(author, "id", None)) or get_user_id(event) or ""
        sender_name = _resolve_sender_name(event, fallback_user_id=uid)

        _ensure_message_common_fields(
            event,
            message_type="private",
            user_id=uid,
            group_id=None,
            message_id=getattr(event, "id", ""),
            sub_type="guild",
        )
        _set_event_text_cache(event, raw)
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

    elif HAS_QQ and isinstance(event, _QQ_GROUP_MESSAGE_EVENT_TYPES):
        raw = _get_event_plaintext(event, getattr(event, "content", "") or "")
        author = getattr(event, "author", None)
        sender_id = (
            _to_nonempty_str(getattr(author, "member_openid", None))
            or get_user_id(event)
            or ""
        )
        original_group_id = _to_nonempty_str(getattr(event, "group_id", None))
        group_openid = (
            _to_nonempty_str(getattr(event, "group_openid", None))
            or original_group_id
            or ""
        )
        sender_name = _resolve_sender_name(event, fallback_user_id=sender_id)

        if original_group_id and original_group_id != group_openid:
            setattr(event, "qq_group_id", original_group_id)
        setattr(event, "group_openid", group_openid)
        _ensure_message_common_fields(
            event,
            message_type="group",
            user_id=sender_id,
            group_id=group_openid,
            message_id=getattr(event, "id", ""),
            sub_type="normal",
        )
        _set_event_text_cache(event, raw)
        setattr(event, "to_me", _is_qq_group_message_to_me(event, bot))
        if bool(getattr(event, "to_me", False)):
            _strip_qq_group_at_me(event, bot)
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
        raw = _get_event_plaintext(event, getattr(event, "content", "") or "")
        author = getattr(event, "author", None)
        uid = _to_nonempty_str(getattr(author, "id", None)) or get_user_id(event) or ""
        channel_id = _to_nonempty_str(getattr(event, "channel_id", None)) or ""
        sender_name = _resolve_sender_name(event, fallback_user_id=uid)

        _ensure_message_common_fields(
            event,
            message_type="group",
            user_id=uid,
            group_id=channel_id,
            message_id=getattr(event, "id", ""),
            sub_type="channel",
        )
        setattr(event, "to_me", True)
        _set_event_text_cache(event, raw)
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
        sender_name = _resolve_sender_name(event, fallback_user_id=str(sender_id or ""))
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


def _patch_ob11_send_record(bot: BaseBot):
    """给 OneBot V11 尝试添加发送记录包装，并支持 revoke_time 自动撤回"""
    if not (HAS_OB11 and OB11Bot is not None and isinstance(bot, OB11Bot)):
        return

    if getattr(bot, "__message_db_ob11_send_patched__", False):
        return

    # 包装 send_group_msg
    if hasattr(bot, "send_group_msg"):
        try:
            _origin_send_group_msg = bot.send_group_msg

            async def send_group_msg(*args, **kwargs):
                revoke_time = kwargs.pop("revoke_time", kwargs.pop("revoke_after", 0))

                result = await _origin_send_group_msg(*args, **kwargs)

                group_id = kwargs.get("group_id", "")
                message = kwargs.get("message", "")
                message_id = _extract_result_message_id(result)

                _record_send_message(
                    bot,
                    scene="group",
                    message=message,
                    message_id=message_id,
                    group_id=str(group_id or ""),
                    raw_result=result,
                )

                schedule_delete_message(
                    bot,
                    scene="group",
                    message_id=message_id,
                    group_id=str(group_id or ""),
                    revoke_time=revoke_time,
                )

                return result

            setattr(bot, "send_group_msg", send_group_msg)

        except Exception as e:
            logger.debug(f"[message.db] OB11 send_group_msg 包装失败: {e}")

    # 包装 send_private_msg
    if hasattr(bot, "send_private_msg"):
        try:
            _origin_send_private_msg = bot.send_private_msg

            async def send_private_msg(*args, **kwargs):
                revoke_time = kwargs.pop("revoke_time", kwargs.pop("revoke_after", 0))

                result = await _origin_send_private_msg(*args, **kwargs)

                user_id = kwargs.get("user_id", "")
                message = kwargs.get("message", "")
                message_id = _extract_result_message_id(result)

                _record_send_message(
                    bot,
                    scene="private",
                    message=message,
                    message_id=message_id,
                    user_id=str(user_id or ""),
                    raw_result=result,
                )

                schedule_delete_message(
                    bot,
                    scene="private",
                    message_id=message_id,
                    user_id=str(user_id or ""),
                    revoke_time=revoke_time,
                )

                return result

            setattr(bot, "send_private_msg", send_private_msg)

        except Exception as e:
            logger.debug(f"[message.db] OB11 send_private_msg 包装失败: {e}")

    # 包装 send(event, message)
    if hasattr(bot, "send"):
        try:
            _origin_send = bot.send

            async def send(event, message, **kwargs):
                revoke_time = kwargs.pop("revoke_time", kwargs.pop("revoke_after", 0))

                result = await _origin_send(event=event, message=message, **kwargs)

                try:
                    patched_event = patch_event_inplace(event, bot)
                    scene = get_chat_scene(patched_event)

                    message_id = _extract_result_message_id(result)
                    group_id = str(getattr(patched_event, "group_id", "") or "")
                    user_id = str(getattr(patched_event, "user_id", "") or "")
                    source_message_id = str(
                        getattr(patched_event, "message_id", "") or ""
                    )

                    _record_send_message(
                        bot,
                        scene=scene,
                        message=message,
                        message_id=message_id,
                        source_message_id=source_message_id,
                        group_id=group_id,
                        user_id=user_id,
                        raw_result=result,
                    )

                    schedule_delete_message(
                        bot,
                        scene=scene,
                        message_id=message_id,
                        group_id=group_id,
                        user_id=user_id,
                        revoke_time=revoke_time,
                    )

                except Exception:
                    pass

                return result

            setattr(bot, "send", send)

        except Exception as e:
            logger.debug(f"[message.db] OB11 send 包装失败: {e}")

    # 包装 call_api 兜底
    if hasattr(bot, "call_api"):
        try:
            _origin_call_api = bot.call_api

            async def call_api(api: str, **data):
                revoke_time = data.pop("revoke_time", data.pop("revoke_after", 0))
                api_lower = str(api).lower()

                result = await _origin_call_api(api, **data)

                try:
                    if api_lower in ("send_group_msg", "send_group_message"):
                        group_id = str(data.get("group_id", "") or "")
                        message = data.get("message", "")
                        message_id = _extract_result_message_id(result)

                        _record_send_message(
                            bot,
                            scene="group",
                            message=message,
                            message_id=message_id,
                            group_id=group_id,
                            raw_result=result,
                        )

                        schedule_delete_message(
                            bot,
                            scene="group",
                            message_id=message_id,
                            group_id=group_id,
                            revoke_time=revoke_time,
                        )

                    elif api_lower in ("send_private_msg", "send_private_message"):
                        user_id = str(data.get("user_id", "") or "")
                        message = data.get("message", "")
                        message_id = _extract_result_message_id(result)

                        _record_send_message(
                            bot,
                            scene="private",
                            message=message,
                            message_id=message_id,
                            user_id=user_id,
                            raw_result=result,
                        )

                        schedule_delete_message(
                            bot,
                            scene="private",
                            message_id=message_id,
                            user_id=user_id,
                            revoke_time=revoke_time,
                        )

                except Exception:
                    pass

                return result

            setattr(bot, "call_api", call_api)

        except Exception as e:
            logger.debug(f"[message.db] OB11 call_api 包装失败: {e}")

    setattr(bot, "__message_db_ob11_send_patched__", True)


def patch_bot_inplace(bot: BaseBot) -> BaseBot:
    if getattr(bot, "__compat_patched__", False):
        _patch_ob11_send_record(bot)
        return bot

    # OB11 尝试包装发送记录和自动撤回
    _patch_ob11_send_record(bot)

    if HAS_QQ and QQBot is not None and isinstance(bot, QQBot):
        _origin_send = bot.send

        async def send(event, message, **kwargs):
            revoke_time = kwargs.pop("revoke_time", kwargs.pop("revoke_after", 0))

            # ===== 普通 QQ 群 =====
            if HAS_QQ and isinstance(event, _QQ_GROUP_MESSAGE_EVENT_TYPES):
                group_openid = str(get_group_id(event) or getattr(event, "group_openid", "") or "")
                event_id = kwargs.pop("event_id", None)
                msg_ref_id = _get_qq_message_ref_id(event)

                async def _do_send(msg_seq: int):
                    try:
                        return await bot.send_to_group(
                            group_openid=group_openid,
                            message=message,
                            msg_id=str(event.id),
                            msg_seq=int(msg_seq),
                            event_id=event_id,
                            msg_ref_id=msg_ref_id,
                        )
                    except TypeError:
                        return await bot.send_to_group(
                            group_openid=group_openid,
                            message=message,
                            msg_id=str(event.id),
                            msg_seq=int(msg_seq),
                            event_id=event_id,
                        )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    result = await _do_send(msg_seq)
                else:
                    result = await _send_with_retry(
                        _do_send,
                        get_new_seq=lambda: _next_group_seq(group_openid),
                        max_retry=3,
                    )

                message_id = _extract_result_message_id(result)

                _record_send_message(
                    bot,
                    scene="group",
                    message=message,
                    message_id=message_id,
                    source_message_id=str(event.id),
                    group_id=group_openid,
                    raw_result=result,
                )

                schedule_delete_message(
                    bot,
                    scene="group",
                    message_id=message_id,
                    group_id=group_openid,
                    revoke_time=revoke_time,
                )

                return result

            # ===== QQ 私聊 C2C =====
            if HAS_QQ and isinstance(event, QQPrivateMessageEvent):
                author = getattr(event, "author", None)
                user_id = (
                    _to_nonempty_str(getattr(author, "user_openid", None))
                    or get_user_id(event)
                    or ""
                )
                openid = (
                    _to_nonempty_str(getattr(author, "id", None))
                    or _to_nonempty_str(getattr(event, "openid", None))
                    or user_id
                )
                event_id = kwargs.pop("event_id", None)
                msg_ref_id = _get_qq_message_ref_id(event)

                async def _do_send(msg_seq: int):
                    try:
                        return await bot.send_to_c2c(
                            openid=openid,
                            message=message,
                            msg_id=str(event.id),
                            msg_seq=int(msg_seq),
                            event_id=event_id,
                            msg_ref_id=msg_ref_id,
                        )
                    except TypeError:
                        return await bot.send_to_c2c(
                            openid=openid,
                            message=message,
                            msg_id=str(event.id),
                            msg_seq=int(msg_seq),
                            event_id=event_id,
                        )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    result = await _do_send(msg_seq)
                else:
                    result = await _send_with_retry(
                        _do_send,
                        get_new_seq=lambda: _next_c2c_seq(openid),
                        max_retry=3,
                    )

                message_id = _extract_result_message_id(result)

                _record_send_message(
                    bot,
                    scene="private",
                    message=message,
                    message_id=message_id,
                    source_message_id=str(event.id),
                    user_id=user_id,
                    raw_result=result,
                )

                schedule_delete_message(
                    bot,
                    scene="private",
                    message_id=message_id,
                    user_id=openid,
                    revoke_time=revoke_time,
                )

                return result

            # ===== QQ 频道公域消息 =====
            if HAS_QQ and isinstance(event, QQAtChannelMessageEvent):
                channel_id = str(event.channel_id)
                event_id = kwargs.pop("event_id", None)

                async def _do_send(msg_seq: int):
                    try:
                        return await bot.send_to_channel(
                            channel_id=channel_id,
                            message=message,
                            msg_id=str(event.id),
                            msg_seq=int(msg_seq),
                            event_id=event_id,
                        )
                    except TypeError:
                        return await bot.send_to_channel(
                            channel_id=channel_id,
                            message=message,
                            msg_id=str(event.id),
                            event_id=event_id,
                        )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    result = await _do_send(msg_seq)
                else:
                    result = await _send_with_retry(
                        _do_send,
                        get_new_seq=lambda: _next_group_seq(channel_id),
                        max_retry=3,
                    )

                message_id = _extract_result_message_id(result)

                _record_send_message(
                    bot,
                    scene="channel_group",
                    message=message,
                    message_id=message_id,
                    source_message_id=str(event.id),
                    group_id=channel_id,
                    raw_result=result,
                )

                schedule_delete_message(
                    bot,
                    scene="channel_group",
                    message_id=message_id,
                    group_id=channel_id,
                    revoke_time=revoke_time,
                )

                return result

            # ===== QQ 频道私信 DMS =====
            if HAS_QQ and isinstance(event, QQChannelPrivateMessageEvent):
                guild_id = str(event.guild_id)
                uid = str(getattr(getattr(event, "author", None), "id", "") or "")
                seq_key = f"{guild_id}:{uid}" if uid else guild_id
                event_id = kwargs.pop("event_id", None)

                async def _do_send(msg_seq: int):
                    try:
                        return await bot.send_to_dms(
                            guild_id=guild_id,
                            message=message,
                            msg_id=str(event.id),
                            msg_seq=int(msg_seq),
                            event_id=event_id,
                        )
                    except TypeError:
                        return await bot.send_to_dms(
                            guild_id=guild_id,
                            message=message,
                            msg_id=str(event.id),
                            event_id=event_id,
                        )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    result = await _do_send(msg_seq)
                else:
                    result = await _send_with_retry(
                        _do_send,
                        get_new_seq=lambda: _next_c2c_seq(seq_key),
                        max_retry=3,
                    )

                message_id = _extract_result_message_id(result)

                _record_send_message(
                    bot,
                    scene="channel_private",
                    message=message,
                    message_id=message_id,
                    source_message_id=str(event.id),
                    user_id=uid,
                    group_id=guild_id,
                    raw_result=result,
                )

                schedule_delete_message(
                    bot,
                    scene="channel_private",
                    message_id=message_id,
                    group_id=guild_id,
                    user_id=uid,
                    revoke_time=revoke_time,
                )

                return result

            # ===== 兜底 =====
            result = await _origin_send(event=event, message=message, **kwargs)

            scene = get_chat_scene(event)
            message_id = _extract_result_message_id(result)
            group_id = str(get_group_id(event) or "")
            user_id = str(get_user_id(event) or "")
            source_message_id = str(
                getattr(event, "id", "") or getattr(event, "message_id", "") or ""
            )

            _record_send_message(
                bot,
                scene=scene,
                message=message,
                message_id=message_id,
                source_message_id=source_message_id,
                group_id=group_id,
                user_id=user_id,
                raw_result=result,
            )

            schedule_delete_message(
                bot,
                scene=scene,
                message_id=message_id,
                group_id=group_id,
                user_id=user_id,
                revoke_time=revoke_time,
            )

            return result

        async def send_private_msg(*, user_id, message, **kwargs):
            revoke_time = kwargs.pop("revoke_time", kwargs.pop("revoke_after", 0))
            openid = str(user_id)

            async def _do_send(msg_seq: int):
                return await bot.send_to_c2c(
                    openid=openid,
                    message=message,
                    msg_seq=int(msg_seq),
                    **kwargs,
                )

            if "msg_seq" in kwargs:
                msg_seq = int(kwargs.pop("msg_seq"))
                result = await _do_send(msg_seq)
            else:
                result = await _send_with_retry(
                    _do_send,
                    get_new_seq=lambda: _next_c2c_seq(openid),
                    max_retry=3,
                )

            message_id = _extract_result_message_id(result)

            _record_send_message(
                bot,
                scene="private",
                message=message,
                message_id=message_id,
                user_id=openid,
                raw_result=result,
            )

            schedule_delete_message(
                bot,
                scene="private",
                message_id=message_id,
                user_id=openid,
                revoke_time=revoke_time,
            )

            return result

        async def send_group_msg(*, group_id, message, **kwargs):
            revoke_time = kwargs.pop("revoke_time", kwargs.pop("revoke_after", 0))
            group_openid = str(group_id)
            msg_id = kwargs.pop("msg_id", None)
            event_id = kwargs.pop("event_id", None)

            async def _do_send(msg_seq: int):
                return await bot.send_to_group(
                    group_openid=group_openid,
                    message=message,
                    msg_id=msg_id,
                    msg_seq=int(msg_seq),
                    event_id=event_id,
                )

            if "msg_seq" in kwargs:
                msg_seq = int(kwargs.pop("msg_seq"))
                result = await _do_send(msg_seq)
            else:
                result = await _send_with_retry(
                    _do_send,
                    get_new_seq=lambda: _next_group_seq(group_openid),
                    max_retry=3,
                )

            message_id = _extract_result_message_id(result)

            _record_send_message(
                bot,
                scene="group",
                message=message,
                message_id=message_id,
                group_id=group_openid,
                raw_result=result,
            )

            schedule_delete_message(
                bot,
                scene="group",
                message_id=message_id,
                group_id=group_openid,
                revoke_time=revoke_time,
            )

            return result

        async def delete_msg(*, message_id, group_id=None, user_id=None, scene: str = ""):
            """
            兼容 delete_msg。

            OB11:
            - delete_msg(message_id=xxx)

            QQ:
            - 群聊需要 group_id
            - 私聊需要 user_id
            """
            if not scene:
                if group_id is not None:
                    scene = "group"
                elif user_id is not None:
                    scene = "private"

            return await delete_message_compat(
                bot,
                scene=scene,
                message_id=str(message_id),
                group_id=str(group_id or ""),
                user_id=str(user_id or ""),
            )

        setattr(bot, "send", send)
        setattr(bot, "send_private_msg", send_private_msg)
        setattr(bot, "send_group_msg", send_group_msg)
        setattr(bot, "delete_msg", delete_msg)

    setattr(bot, "__compat_patched__", True)
    return bot


def patch_context(bot: BaseBot, event: BaseEvent) -> tuple[BaseBot, BaseEvent]:
    bot = patch_bot_inplace(bot)
    event = patch_event_inplace(event, bot)

    # 收消息入库
    _record_recv_message(bot, event)

    return bot, event

# =========================
# 对 Web 管理面板开放的公共接口
# =========================

def init_message_db():
    """初始化 message.db，供 Web 面板调用"""
    return _init_message_db()


def get_message_db_path() -> Path:
    """获取消息数据库路径"""
    return _get_message_db_path()


def extract_result_message_id(result: Any) -> str:
    """从不同适配器发送结果中提取 message_id"""
    return _extract_result_message_id(result)


def get_bot_id(bot: Any) -> str:
    """安全获取 bot_id/self_id"""
    return _get_bot_id(bot)


def record_web_send_message(
    bot: Any,
    *,
    scene: str,
    message: Any,
    message_id: str = "",
    source_message_id: str = "",
    group_id: str = "",
    user_id: str = "",
    raw_result: Any = None,
):
    """
    Web 面板主动发送消息后记录发送消息。

    会自动：
    - 写入 messages 表 direction='send'
    - 如果 source_message_id 存在，增加对应 recv.reply_used_count
    """
    return _record_send_message(
        bot,
        scene=scene,
        message=message,
        message_id=message_id,
        source_message_id=source_message_id,
        group_id=group_id,
        user_id=user_id,
        raw_result=raw_result,
    )


def increase_recv_reply_used_count(
    *,
    source_message_id: str,
    adapter: str = "",
    bot_id: str = "",
    scene: str = "",
    group_id: str = "",
    user_id: str = "",
):
    """增加被回复 recv 消息的 reply_used_count"""
    return _increase_recv_reply_used_count(
        source_message_id=source_message_id,
        adapter=adapter,
        bot_id=bot_id,
        scene=scene,
        group_id=group_id,
        user_id=user_id,
    )

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
    "get_at_user_id",
    "get_at_user_ids",
    "has_at_user",
    "get_group_id",
    "patch_bot_inplace",
    "patch_event_inplace",
    "patch_context",

    # Web 公共接口
    "init_message_db",
    "get_message_db_path",
    "extract_result_message_id",
    "get_bot_id",
    "record_web_send_message",
    "increase_recv_reply_used_count",

    # 通用撤回
    "delete_message_compat",
    "schedule_delete_message",
]
