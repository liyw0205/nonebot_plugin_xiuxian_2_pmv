from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from nonebot.adapters import Event as BaseEvent
from nonebot.log import logger

from .xiuxian_utils.message_db import (
    get_message_db_path as _get_message_db_path,
    increase_recv_reply_used_count as _increase_recv_reply_used_count,
    init_message_db as _init_message_db,
    insert_message_record as _insert_message_record,
    record_group_user_nickname as _record_group_user_nickname,
)


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


def extract_text_from_message_obj(message: Any) -> str:
    """
    提取消息展示内容，保留文本和常见媒体附件标记。
    """
    try:
        if message is None:
            return ""

        if isinstance(message, str):
            return message

        parts: list[str] = []

        try:
            for seg in message:
                seg_type = str(getattr(seg, "type", "") or "")
                data = getattr(seg, "data", {}) or {}

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

                    media_type = seg_type
                    if seg_type in ("record", "voice"):
                        media_type = "audio"

                    seg_str = str(seg)
                    m = None
                    try:
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
                    seg_str = _normalize_attachment_repr_text(str(seg))
                    if seg_str:
                        parts.append(seg_str)

            if parts:
                return "".join(parts)

        except TypeError:
            pass
        except Exception:
            pass

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


def get_adapter_name(bot: Any) -> str:
    try:
        return str(bot.adapter.get_name())
    except Exception:
        return ""


def get_bot_id(bot: Any) -> str:
    try:
        return str(bot.self_id)
    except Exception:
        return ""


def extract_result_message_id(result: Any) -> str:
    return _extract_field_from_any(result, ("message_id", "msg_id", "id"))


def _extract_field_from_any(
    value: Any,
    keys: tuple[str, ...],
    *,
    _depth: int = 0,
    _seen: set[int] | None = None,
) -> str:
    if value is None or _depth > 6:
        return ""

    if _seen is None:
        _seen = set()
    if not isinstance(value, (str, bytes, int, float, bool)):
        value_id = id(value)
        if value_id in _seen:
            return ""
        _seen.add(value_id)

    if isinstance(value, (list, tuple)):
        for item in value:
            result = _extract_field_from_any(
                item,
                keys,
                _depth=_depth + 1,
                _seen=_seen,
            )
            if result:
                return result
        return ""

    if isinstance(value, dict):
        for key in keys:
            item = value.get(key)
            if item is not None and item != "":
                return str(item)
        for nested_key in ("data", "result", "response", "ext_info"):
            if nested_key in value:
                result = _extract_field_from_any(
                    value[nested_key],
                    keys,
                    _depth=_depth + 1,
                    _seen=_seen,
                )
                if result:
                    return result
        return ""

    for key in keys:
        try:
            item = getattr(value, key, None)
        except Exception:
            item = None
        if item is not None and item != "":
            return str(item)

    for method_name in ("model_dump", "dict"):
        try:
            method = getattr(value, method_name)
            data = method(exclude_none=True)
        except Exception:
            continue
        result = _extract_field_from_any(
            data,
            keys,
            _depth=_depth + 1,
            _seen=_seen,
        )
        if result:
            return result

    return ""


_MSG_IDX_RE = re.compile(r"(?:^|[?&])msg_idx=([^&\s]+)")


def _extract_reference_from_ext(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        for item in value:
            result = _extract_reference_from_ext(item)
            if result:
                return result
        return ""
    if isinstance(value, dict):
        for nested in value.values():
            result = _extract_reference_from_ext(nested)
            if result:
                return result
        return ""
    if not isinstance(value, str):
        for attr in ("ext", "message_scene", "ext_info", "data", "result", "response"):
            result = _extract_reference_from_ext(getattr(value, attr, None))
            if result:
                return result
        return ""
    match = _MSG_IDX_RE.search(value)
    return unquote(match.group(1)) if match else ""


def extract_result_reference_id(result: Any) -> str:
    try:
        if result is None:
            return ""

        keys = ("reference_id", "message_reference_id", "ref_idx", "msg_idx")
        ext_info = None
        if isinstance(result, dict):
            ext_info = result.get("ext_info")
        else:
            ext_info = getattr(result, "ext_info", None)

        ref_id = _extract_field_from_any(ext_info, keys)
        if ref_id:
            return ref_id

        ref_id = _extract_field_from_any(result, keys)
        if ref_id:
            return ref_id
        return _extract_reference_from_ext(result)

    except Exception:
        return ""


def _extract_event_reference_id(event: BaseEvent) -> str:
    for attr in ("message_reference_id", "reference_id", "msg_idx"):
        try:
            value = getattr(event, attr, None)
        except Exception:
            value = None
        if value:
            return str(value)

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


def record_recv_message(bot: Any, event: BaseEvent):
    """记录收到的消息。"""
    try:
        if getattr(event, "__message_db_recv_recorded__", False):
            return

        if event.get_type() != "message":
            return

        from .adapter_compat import get_chat_scene, get_group_id, get_user_id

        scene = get_chat_scene(event)
        adapter = get_adapter_name(bot) if bot is not None else ""
        bot_id = get_bot_id(bot) if bot is not None else ""

        message_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "")
        reference_id = _extract_event_reference_id(event)
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
            content = extract_text_from_message_obj(msg) or raw_message
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
            reference_id=reference_id,
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


def record_send_message(
    bot: Any,
    *,
    scene: str,
    message: Any,
    message_id: str = "",
    reference_id: str = "",
    source_message_id: str = "",
    group_id: str = "",
    user_id: str = "",
    raw_result: Any = None,
):
    """
    记录发送消息。

    source_message_id 存在时，会增加对应接收消息的回复计数。
    """
    try:
        adapter = get_adapter_name(bot)
        bot_id = get_bot_id(bot)
        content = extract_text_from_message_obj(message)
        reference_id = str(reference_id or extract_result_reference_id(raw_result) or "")

        _insert_message_record(
            adapter=adapter,
            bot_id=bot_id,
            direction="send",
            scene=scene,
            message_id=str(message_id or ""),
            reference_id=reference_id,
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


def init_message_db():
    """初始化 message.db，供 Web 面板调用。"""
    return _init_message_db()


def get_message_db_path() -> Path:
    """获取消息数据库路径。"""
    return _get_message_db_path()


def record_web_send_message(
    bot: Any,
    *,
    scene: str,
    message: Any,
    message_id: str = "",
    reference_id: str = "",
    source_message_id: str = "",
    group_id: str = "",
    user_id: str = "",
    raw_result: Any = None,
):
    """Web 面板主动发送消息后的发送记录入口。"""
    return record_send_message(
        bot,
        scene=scene,
        message=message,
        message_id=message_id,
        reference_id=reference_id,
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
    """增加被回复 recv 消息的 reply_used_count。"""
    return _increase_recv_reply_used_count(
        source_message_id=source_message_id,
        adapter=adapter,
        bot_id=bot_id,
        scene=scene,
        group_id=group_id,
        user_id=user_id,
    )


__all__ = [
    "extract_result_message_id",
    "extract_result_reference_id",
    "extract_text_from_message_obj",
    "get_adapter_name",
    "get_bot_id",
    "get_message_db_path",
    "increase_recv_reply_used_count",
    "init_message_db",
    "record_recv_message",
    "record_send_message",
    "record_web_send_message",
]
