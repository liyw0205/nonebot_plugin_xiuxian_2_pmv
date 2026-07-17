"""超管调试：消息信息 / 取链接。"""

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

_URL_RE = re.compile(r"https?://[^\s\"'<>\\]]+", re.I)
_MD_URL_RE = re.compile(r"\[[^\]]*\]\((https?://[^)\s]+)\)", re.I)


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
    return text.replace("\\/", "/")


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


def _walk_collect_urls(obj: Any, out: list[str], seen: set[str]) -> None:
    if obj is None:
        return
    if isinstance(obj, str):
        text = _unescape_slashes(obj)
        for m in _MD_URL_RE.finditer(text):
            u = unquote(m.group(1).strip().rstrip(").,;，。；"))
            if u and u not in seen:
                seen.add(u)
                out.append(u)
        for m in _URL_RE.finditer(text):
            u = unquote(m.group(0).strip().rstrip(").,;，。；"))
            if u and u not in seen:
                seen.add(u)
                out.append(u)
        return
    if isinstance(obj, dict):
        # 常见媒体字段优先
        for key in (
            "url",
            "file_url",
            "fileUrl",
            "image_url",
            "imageUrl",
            "src",
            "href",
            "content",
            "file",
            "path",
            "proxy_url",
            "thumbnail",
            "thumb",
        ):
            if key in obj:
                _walk_collect_urls(obj.get(key), out, seen)
        for v in obj.values():
            _walk_collect_urls(v, out, seen)
        return
    if isinstance(obj, (list, tuple, set)):
        for item in obj:
            _walk_collect_urls(item, out, seen)
        return
    # segment-like
    try:
        data = getattr(obj, "data", None)
        if data is not None:
            _walk_collect_urls(data, out, seen)
        typ = getattr(obj, "type", None)
        if typ is not None:
            _walk_collect_urls(getattr(obj, "url", None), out, seen)
    except Exception:
        pass
    try:
        if hasattr(obj, "__dict__"):
            _walk_collect_urls(
                {k: v for k, v in obj.__dict__.items() if not str(k).startswith("_")},
                out,
                seen,
            )
    except Exception:
        pass


def _extract_urls_from_any(*objs: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for obj in objs:
        _walk_collect_urls(obj, out, seen)
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

    # QQ 群：message_scene.ext.ref_msg_idx
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


def _pretty_json(data: Any) -> str:
    try:
        return _unescape_slashes(json.dumps(data, ensure_ascii=False, indent=2, default=str))
    except Exception:
        return _unescape_slashes(_safe_str(data))


def _sanitize_md(text: str) -> str:
    if not isinstance(text, str):
        text = _safe_str(text)
    text = text.replace("\n", "\r")
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", text)
    text = re.sub(r"(?i)\b(https?|mqqapi)://", lambda m: f"{m.group(1)}:\\/\\/", text)
    return text.replace("```", "'''")


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

    # 当前消息链接摘要
    urls = _extract_urls_from_any(
        getattr(event, "message", None),
        getattr(event, "attachments", None),
        getattr(event, "content", None),
        reply_info,
    )
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
) -> None:
    cfg = XiuConfig()
    safe_title = _sanitize_md(title)
    safe_body = _sanitize_md(body)

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
            await _send_blocks(bot, event, basic_text, raw_json, code_lang="json")
            return
        if cfg.markdown_status:
            safe_basic = _sanitize_md(basic_text)
            safe_raw = _sanitize_md(raw_json)
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


def _reply_link_sources(event) -> list[Any]:
    """取链接只看被引用消息。"""
    sources: list[Any] = []
    reply = getattr(event, "reply", None)
    if reply is not None:
        sources.extend(
            [
                reply,
                getattr(reply, "message", None),
                getattr(reply, "attachments", None),
                getattr(reply, "content", None),
                getattr(reply, "raw_message", None),
            ]
        )
    # 有时引用内容嵌在 event 的 attachments/message 以外字段
    info = _extract_reply_info(event)
    if info:
        sources.append(info)
    return sources


@fetch_link_cmd.handle(parameterless=[Cooldown(cd_time=0.5)])
async def fetch_link_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """超管：引用一条消息，提取其中图片/附件链接。"""
    bot, _ = await assign_bot(bot=bot, event=event)
    try:
        reply_info = _extract_reply_info(event)
        if not reply_info and getattr(event, "reply", None) is None:
            await _send_blocks(bot, event, "获取失败", "请先引用一条消息后再发送【取链接】")
            return

        urls = _extract_urls_from_any(*_reply_link_sources(event))
        # 再兜底：整 event 里找，但优先 reply 相关
        if not urls:
            urls = _extract_urls_from_any(_event_to_dict(event).get("__parsed_reply__"))

        if not urls:
            await _send_blocks(bot, event, "获取失败", "未在引用消息中找到可用链接")
            return

        body = "\n".join(urls)
        await _send_blocks(bot, event, "获取成功", body, code_lang="text")
    except Exception as e:
        logger.error(f"取链接失败: {e}")
        await _send_blocks(bot, event, "获取失败", str(e))
