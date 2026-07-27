"""超管调试：消息信息 / 取链接。

取链接不依赖固定字段名：把触发 event 摊成文本后正则抽链。
"""

from __future__ import annotations

try:
    import ujson as json
except ImportError:
    import json

import re
from typing import Any
from urllib.parse import unquote

from nonebot.compat import model_dump
from nonebot.log import logger
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, GroupMessageEvent, MessageSegment, PrivateMessageEvent
from ..messaging.delivery import delivery_service
from ..on_compat import on_command
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import handle_send, send_msg_handler

parse_event_cmd = on_command("消息信息", permission=SUPERUSER, priority=100, block=True)
fetch_link_cmd = on_command("取链接", aliases={"提取链接", "获取链接"}, permission=SUPERUSER, priority=100, block=True)

# 宽松：兼容 https:// 与 https:\/\/
_LOOSE_URL_RE = re.compile(
    r"""(?i)https?:\\?/\\?/[^\s"'<>\\\]\)\}\,，。；]*"""
)
_MD_URL_RE = re.compile(r"\[[^\]]*\]\((https?:\\?/\\?/[^)\s]+)\)", re.I)


def _safe_str(obj: Any) -> str:
    try:
        return str(obj)
    except Exception:
        try:
            return repr(obj)
        except Exception:
            return "<无法转为字符串>"


def _unescape_slashes(text: str) -> str:
    if not isinstance(text, str):
        text = _safe_str(text)
    prev = None
    while prev != text:
        prev = text
        text = text.replace("\\/", "/")
    return text


def _truncate(text: str, limit: int = 10000) -> str:
    text = text or ""
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n\n......\n（内容过长，已截断，原长度：{len(text)}）"


def _segment_to_simple(seg: Any) -> Any:
    try:
        return {
            "type": getattr(seg, "type", None),
            "data": getattr(seg, "data", None) if getattr(seg, "data", None) is not None else _safe_str(seg),
        }
    except Exception:
        return _safe_str(seg)


def _message_to_simple(msg: Any) -> Any:
    if msg is None:
        return None
    if isinstance(msg, str):
        return msg
    try:
        return [_segment_to_simple(seg) for seg in msg]
    except Exception:
        return _safe_str(msg)


def _extract_plain_from_message(msg: Any) -> str:
    if msg is None:
        return ""
    try:
        if hasattr(msg, "extract_plain_text"):
            return msg.extract_plain_text() or ""
    except Exception:
        pass
    return _safe_str(msg)


def _object_to_search_text(obj: Any, *, _depth: int = 0, _seen: set[int] | None = None) -> str:
    """任意对象摊成可正则抽链的文本，不依赖固定字段路径。"""
    if obj is None or _depth > 12:
        return ""
    if _seen is None:
        _seen = set()

    if isinstance(obj, str):
        return _unescape_slashes(obj)
    if isinstance(obj, (bytes, bytearray)):
        try:
            return _unescape_slashes(obj.decode("utf-8", "ignore"))
        except Exception:
            return ""
    if isinstance(obj, (int, float, bool)):
        return ""

    try:
        oid = id(obj)
        if oid in _seen:
            return ""
        if not isinstance(obj, (str, bytes, int, float, bool, dict, list, tuple, set)):
            _seen.add(oid)
    except Exception:
        pass

    chunks: list[str] = []

    for dumper in (
        lambda: obj.model_dump() if hasattr(obj, "model_dump") else None,
        lambda: obj.dict() if hasattr(obj, "dict") else None,
    ):
        try:
            dumped = dumper()
        except Exception:
            dumped = None
        if isinstance(dumped, dict):
            try:
                chunks.append(json.dumps(dumped, ensure_ascii=False, default=str))
            except Exception:
                chunks.append(_safe_str(dumped))
            for v in dumped.values():
                chunks.append(_object_to_search_text(v, _depth=_depth + 1, _seen=_seen))
            break
        if isinstance(dumped, (list, tuple)):
            for v in dumped:
                chunks.append(_object_to_search_text(v, _depth=_depth + 1, _seen=_seen))
            break

    if isinstance(obj, dict):
        try:
            chunks.append(json.dumps(obj, ensure_ascii=False, default=str))
        except Exception:
            pass
        for v in obj.values():
            chunks.append(_object_to_search_text(v, _depth=_depth + 1, _seen=_seen))
    elif isinstance(obj, (list, tuple, set)):
        for v in obj:
            chunks.append(_object_to_search_text(v, _depth=_depth + 1, _seen=_seen))
    else:
        # 不假设字段名：盲扫属性 + str/repr（Attachment 链接常只出现在 str）
        try:
            for name in dir(obj):
                if name.startswith("_"):
                    continue
                try:
                    value = getattr(obj, name, None)
                except Exception:
                    continue
                if callable(value):
                    continue
                if isinstance(value, (str, bytes, dict, list, tuple, set)) or hasattr(value, "model_dump") or hasattr(value, "url"):
                    chunks.append(_object_to_search_text(value, _depth=_depth + 1, _seen=_seen))
        except Exception:
            pass
        chunks.append(_safe_str(obj))
        try:
            chunks.append(repr(obj))
        except Exception:
            pass

    return _unescape_slashes("\n".join(c for c in chunks if c))


def _normalize_url(raw: str) -> str:
    u = _unescape_slashes(unquote((raw or "").strip()))
    u = u.rstrip("\\").rstrip(").,;，。；'\"")
    return u


def _collect_urls_from_text(text: str, out: list[str], seen: set[str]) -> None:
    if not text:
        return
    text = _unescape_slashes(text)
    for m in _MD_URL_RE.finditer(text):
        u = _normalize_url(m.group(1))
        if u and u not in seen and u.lower().startswith(("http://", "https://")):
            seen.add(u)
            out.append(u)
    for m in _LOOSE_URL_RE.finditer(text):
        u = _normalize_url(m.group(0))
        if u and u not in seen and u.lower().startswith(("http://", "https://")):
            seen.add(u)
            out.append(u)


def _extract_urls_from_any(*objs: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for obj in objs:
        _collect_urls_from_text(_object_to_search_text(obj), out, seen)
    return out


def _extract_reply_info(event) -> dict | None:
    info: dict[str, Any] = {}

    reply_obj = getattr(event, "reply", None)
    if reply_obj is not None:
        try:
            info["source"] = "event.reply"
            info["message_id"] = getattr(reply_obj, "message_id", None) or getattr(reply_obj, "id", None)
            info["real_id"] = getattr(reply_obj, "real_id", None)
            info["time"] = getattr(reply_obj, "time", None)
            sender = getattr(reply_obj, "sender", None)
            if sender is not None:
                info["sender"] = {
                    "user_id": getattr(sender, "user_id", None) or getattr(sender, "id", None),
                    "nickname": getattr(sender, "nickname", None) or getattr(sender, "username", None),
                    "card": getattr(sender, "card", None),
                    "role": getattr(sender, "role", None),
                }
            message = getattr(reply_obj, "message", None)
            if message is not None:
                info["message"] = _message_to_simple(message)
                info["plain_text"] = _extract_plain_from_message(message)
            content = getattr(reply_obj, "content", None)
            if content is not None:
                info["content"] = content
            attachments = getattr(reply_obj, "attachments", None)
            if attachments is not None:
                info["attachments"] = attachments
            return info
        except Exception:
            pass

    try:
        original_message = getattr(event, "original_message", None)
        if original_message:
            for seg in original_message:
                if getattr(seg, "type", None) == "reply":
                    info["source"] = "original_message.reply_segment"
                    info["message_id"] = getattr(seg, "data", {}).get("id")
                    return info
    except Exception:
        pass

    try:
        message_reference = getattr(event, "message_reference", None)
        if message_reference is not None:
            info["source"] = "message_reference"
            info["message_id"] = getattr(message_reference, "message_id", None) or getattr(
                message_reference, "id", None
            )
            return info
    except Exception:
        pass

    try:
        message_scene = getattr(event, "message_scene", None)
        if message_scene:
            ext_list = getattr(message_scene, "ext", None)
            if ext_list is None and isinstance(message_scene, dict):
                ext_list = message_scene.get("ext")
            if isinstance(ext_list, list):
                for item in ext_list:
                    if isinstance(item, dict) and item.get("key") == "ref_msg_idx":
                        info["source"] = "message_scene.ext.ref_msg_idx"
                        info["ref_msg_idx"] = item.get("value")
                        return info
                    if isinstance(item, str) and "ref_msg_idx=" in item:
                        info["source"] = "message_scene.ext.ref_msg_idx"
                        info["ref_msg_idx"] = item.split("ref_msg_idx=", 1)[-1]
                        return info
    except Exception:
        pass

    return info or None


def _event_to_dict(event) -> dict:
    data = None
    for getter in (
        lambda: model_dump(event),
        lambda: event.dict() if hasattr(event, "dict") else None,
    ):
        try:
            data = getter()
            if data is not None:
                break
        except Exception:
            data = None

    if data is None:
        try:
            data = {
                k: v
                for k, v in getattr(event, "__dict__", {}).items()
                if not str(k).startswith("_")
            }
        except Exception:
            data = {"raw": _safe_str(event)}

    try:
        if hasattr(event, "message"):
            data["message"] = _message_to_simple(getattr(event, "message", None))
    except Exception:
        pass
    try:
        if hasattr(event, "original_message"):
            data["original_message"] = _message_to_simple(getattr(event, "original_message", None))
    except Exception:
        pass
    try:
        reply_info = _extract_reply_info(event)
        if reply_info:
            data["__parsed_reply__"] = reply_info
    except Exception:
        pass
    return data


def _sanitize_md(text: str, *, escape_urls: bool = True) -> str:
    if not isinstance(text, str):
        text = _safe_str(text)
    text = text.replace("\n", "\r")
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", text)
    if escape_urls:
        text = re.sub(r"(?i)\b(https?|mqqapi)://", lambda m: f"{m.group(1)}:\\/\\/", text)
    return text.replace("```", "'''")


def _pretty_json(data: Any) -> str:
    try:
        return _unescape_slashes(json.dumps(data, ensure_ascii=False, indent=2, default=str))
    except Exception:
        return _unescape_slashes(_safe_str(data))


def _build_event_info_blocks(event) -> tuple[str, str]:
    lines = ["【消息基本信息】"]

    for getter, label in (
        (lambda: event.get_type(), "事件类型"),
        (lambda: event.get_event_name(), "事件名称"),
        (lambda: event.get_user_id(), "用户ID"),
        (lambda: event.get_session_id(), "会话ID"),
        (lambda: event.is_tome(), "to_me"),
    ):
        try:
            lines.append(f"{label}：{getter()}")
        except Exception:
            pass

    for attr, label in (
        ("group_id", "群ID"),
        ("group_openid", "群OpenID"),
        ("channel_id", "频道ID"),
        ("guild_id", "Guild ID"),
        ("message_id", "消息ID"),
        ("id", "平台消息ID"),
        ("event_id", "事件ID"),
        ("self_id", "Bot ID"),
    ):
        value = getattr(event, attr, None)
        if value is not None:
            lines.append(f"{label}：{value}")

    sender = getattr(event, "sender", None)
    author = getattr(event, "author", None)
    if sender is not None:
        lines.append(f"发送者ID：{getattr(sender, 'user_id', None)}")
        lines.append(f"发送者昵称：{getattr(sender, 'nickname', None)}")
        lines.append(f"发送者群名片：{getattr(sender, 'card', None)}")
        lines.append(f"发送者角色：{getattr(sender, 'role', None)}")
    elif author is not None:
        author_id = (
            getattr(author, "id", None)
            or getattr(author, "user_openid", None)
            or getattr(author, "member_openid", None)
        )
        lines.append(f"发送者ID：{author_id}")
        lines.append(f"发送者昵称：{getattr(author, 'username', None)}")

    try:
        msg_obj = event.get_message()
        plain_text = _extract_plain_from_message(msg_obj)
        lines.append(f"纯文本：{plain_text if plain_text else '[空]'}")
        lines.append(f"消息对象：{_safe_str(msg_obj)}")
    except Exception:
        for attr, label in (
            ("raw_message", "raw_message"),
            ("content", "content"),
            ("message", "message"),
        ):
            value = getattr(event, attr, None)
            if value is not None:
                lines.append(f"{label}：{_safe_str(value)}")
                break
        else:
            lines.append("消息内容：<无>")

    reply_info = _extract_reply_info(event)
    if reply_info:
        lines.append(f"引用信息：{_pretty_json(reply_info)}")

    # 摘要：直接对整 event 抽链
    urls = _extract_urls_from_any(event)
    if urls:
        lines.append(f"链接数：{len(urls)}")
        for i, u in enumerate(urls[:5], 1):
            lines.append(f"链接{i}：{u}")

    basic_text = "\n".join(lines)
    raw_json = _truncate(_pretty_json(_event_to_dict(event)))
    return basic_text, raw_json


async def _send_blocks(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    title: str,
    body: str,
    *,
    code_lang: str = "text",
    escape_body_urls: bool = False,
) -> None:
    cfg = XiuConfig()
    safe_title = _sanitize_md(title, escape_urls=True)
    # 代码框默认不转义 URL
    safe_body = _sanitize_md(body, escape_urls=escape_body_urls)

    if cfg.markdown_status:
        if cfg.markdown_id:
            try:
                await send_msg_handler(bot, event, "event", bot.self_id, [safe_body], title=safe_title)
                return
            except Exception as e:
                logger.warning(f"{title} 模板MD失败，降级原生: {e}")
        try:
            md = (
                f"**{safe_title}**\r"
                f"```{code_lang}\r"
                f"{safe_body}\r"
                f"```"
            )
            await delivery_service.reply(bot, event, MessageSegment.markdown(bot, md))
            return
        except Exception as e:
            logger.warning(f"{title} 原生MD失败，降级纯文本: {e}")

    plain = f"{title}\n\n{body}"
    try:
        await delivery_service.reply(bot, event, plain)
    except Exception:
        await handle_send(bot, event, plain)


@parse_event_cmd.handle(parameterless=[Cooldown(cd_time=0.5)])
async def parse_event_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """超管：查看当前消息 / 引用消息的事件信息。"""
    bot, _ = await assign_bot(bot=bot, event=event)
    try:
        basic_text, raw_json = _build_event_info_blocks(event)
        cfg = XiuConfig()
        if cfg.markdown_status and cfg.markdown_id:
            await _send_blocks(bot, event, basic_text, raw_json, code_lang="json", escape_body_urls=False)
            return
        if cfg.markdown_status:
            safe_basic = _sanitize_md(basic_text, escape_urls=True)
            safe_raw = _sanitize_md(raw_json, escape_urls=False)
            md = (
                f"**消息基本信息**\r```text\r{safe_basic}\r```\r"
                f"**原始数据 (Event JSON)**\r```json\r{safe_raw}\r```"
            )
            try:
                await delivery_service.reply(bot, event, MessageSegment.markdown(bot, md))
                return
            except Exception as e:
                logger.warning(f"消息信息原生MD失败，降级纯文本: {e}")
        await handle_send(bot, event, f"{basic_text}\n\n【原始信息】\n{raw_json}")
    except Exception as e:
        logger.error(f"解析event并发送失败: {e}")
        await handle_send(bot, event, f"解析event失败：{e}")


@fetch_link_cmd.handle(parameterless=[Cooldown(cd_time=0.5)])
async def fetch_link_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """超管：引用一条消息，从触发 event 文本化结果中提取链接。"""
    bot, _ = await assign_bot(bot=bot, event=event)
    try:
        reply_info = _extract_reply_info(event)
        has_reply = bool(reply_info) or getattr(event, "reply", None) is not None
        if not has_reply:
            # 无引用时也允许从当前 event 抽（兼容某些平台把引用内容嵌在当前消息里）
            urls = _extract_urls_from_any(event)
            if not urls:
                await _send_blocks(bot, event, "获取失败", "请先引用一条消息后再发送【取链接】")
                return
        else:
            # 只看触发 event：整对象文本化 + 正则，不读 message.db，不赌字段名
            urls = _extract_urls_from_any(event)

        if not urls:
            await _send_blocks(bot, event, "获取失败", "未在引用消息中找到可用链接")
            return

        body = "\n".join(urls)
        await _send_blocks(bot, event, "获取成功", body, code_lang="text", escape_body_urls=False)
    except Exception as e:
        logger.error(f"取链接失败: {e}")
        await _send_blocks(bot, event, "获取失败", str(e))
