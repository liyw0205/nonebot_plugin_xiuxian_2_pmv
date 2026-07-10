try:
    import ujson as json
except ImportError:
    import json
import re
from typing import Tuple

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


@parse_event_cmd.handle(parameterless=[Cooldown(cd_time=0.5)])
async def parse_event_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
):
    """
    超管：解析当前 event 并按配置发送。

    规则：
    1. markdown_status=True 且 markdown_id 有值 -> 模板 Markdown（清洗后）
    2. 其他情况 -> 强制纯文本发送（避免原生 Markdown URL 风控/代码块截断）
    """
    bot, _ = await assign_bot(bot=bot, event=event)

    try:
        basic_text, raw_json = _build_event_info_blocks(event)
        await _send_event_info_by_config(bot, event, basic_text, raw_json)
    except Exception as e:
        logger.error(f"解析event并发送失败: {e}")
        await handle_send(bot, event, f"解析event失败：{e}")


def _safe_str(obj) -> str:
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


def _segment_to_simple(seg):
    try:
        seg_type = getattr(seg, "type", None)
        seg_data = getattr(seg, "data", None)
        return {
            "type": seg_type,
            "data": seg_data if seg_data is not None else _safe_str(seg),
        }
    except Exception:
        return _safe_str(seg)


def _message_to_simple(msg):
    if msg is None:
        return None
    if isinstance(msg, str):
        return msg
    try:
        return [_segment_to_simple(seg) for seg in msg]
    except Exception:
        return _safe_str(msg)


def _extract_plain_from_message(msg) -> str:
    if msg is None:
        return ""
    try:
        if hasattr(msg, "extract_plain_text"):
            return msg.extract_plain_text()
    except Exception:
        pass
    try:
        return str(msg)
    except Exception:
        return ""


def _extract_reply_info(event) -> dict | None:
    reply_info = {}

    reply_obj = getattr(event, "reply", None)
    if reply_obj is not None:
        try:
            reply_info["source"] = "event.reply"
            reply_info["message_id"] = getattr(reply_obj, "message_id", None)
            reply_info["real_id"] = getattr(reply_obj, "real_id", None)
            reply_info["time"] = getattr(reply_obj, "time", None)

            sender = getattr(reply_obj, "sender", None)
            if sender is not None:
                reply_info["sender"] = {
                    "user_id": getattr(sender, "user_id", None),
                    "nickname": getattr(sender, "nickname", None),
                    "card": getattr(sender, "card", None),
                    "role": getattr(sender, "role", None),
                }

            message = getattr(reply_obj, "message", None)
            if message is not None:
                reply_info["message"] = _message_to_simple(message)
                reply_info["plain_text"] = _extract_plain_from_message(message)

            return reply_info
        except Exception:
            pass

    try:
        original_message = getattr(event, "original_message", None)
        if original_message:
            for seg in original_message:
                if getattr(seg, "type", None) == "reply":
                    reply_info["source"] = "original_message.reply_segment"
                    reply_info["message_id"] = getattr(seg, "data", {}).get("id")
                    return reply_info
    except Exception:
        pass

    try:
        message_reference = getattr(event, "message_reference", None)
        if message_reference is not None:
            reply_info["source"] = "message_reference"
            reply_info["message_id"] = getattr(message_reference, "message_id", None)
            reply_info["ignore_get_message_error"] = getattr(
                message_reference, "ignore_get_message_error", None
            )
            return reply_info
    except Exception:
        pass

    try:
        message_scene = getattr(event, "message_scene", None)
        if message_scene:
            ext_list = getattr(message_scene, "ext", None)
            if ext_list is None and isinstance(message_scene, dict):
                ext_list = message_scene.get("ext")

            ref_msg_idx = None
            if isinstance(ext_list, list):
                for item in ext_list:
                    if isinstance(item, dict) and item.get("key") == "ref_msg_idx":
                        ref_msg_idx = item.get("value")
                        break

            if ref_msg_idx:
                reply_info["source"] = "message_scene.ext.ref_msg_idx"
                reply_info["ref_msg_idx"] = ref_msg_idx
                return reply_info
    except Exception:
        pass

    try:
        msg_elements = getattr(event, "msg_elements", None)
        if isinstance(msg_elements, list):
            for elem in msg_elements:
                if not isinstance(elem, dict):
                    continue
                for key in ("ref_msg_id", "ref_message_id", "message_id", "msg_id", "reply_id"):
                    if key in elem and elem.get(key):
                        reply_info["source"] = "msg_elements"
                        reply_info["message_id"] = elem.get(key)
                        reply_info["raw_element"] = elem
                        return reply_info
    except Exception:
        pass

    return reply_info or None


def _event_to_dict(event):
    data = None
    try:
        data = model_dump(event)
    except Exception:
        pass

    if data is None:
        try:
            if hasattr(event, "dict"):
                data = event.dict()
        except Exception:
            pass

    if data is None:
        try:
            if hasattr(event, "__dict__"):
                data = {}
                for k, v in event.__dict__.items():
                    if k.startswith("_"):
                        continue
                    data[k] = v
        except Exception:
            pass

    if data is None:
        return {"raw": _safe_str(event)}

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


def _pretty_event_json(data) -> str:
    try:
        text = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        return _unescape_slashes(text)
    except Exception:
        return _unescape_slashes(_safe_str(data))


def _truncate_for_send(text: str, limit: int = 10000) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n\n......\n（内容过长，已截断，原长度：{len(text)}）"


def _sanitize_markdown_unsafe_text(text: str) -> str:
    """
    清理可能触发 QQ 原生 Markdown 风控或渲染异常的内容。
    """
    if not isinstance(text, str):
        text = _safe_str(text)

    text = text.replace("\n", "\r")
    text = strip_md_links(text)
    text = text.replace("```", "'''")

    return text


def strip_md_links(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)

    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", text)
    text = re.sub(
        r"(?i)\b(https?|mqqapi)://",
        lambda m: f"{m.group(1)}:\\/\\/",
        text,
    )

    return text


def _build_event_info_blocks(event) -> Tuple[str, str]:
    """
    返回：
    - basic_text: 基本信息文本（纯文本）
    - raw_json: 原始 event json 文本（已美化）
    """
    lines = ["【消息基本信息】"]

    try:
        lines.append(f"事件类型：{event.get_type()}")
    except Exception:
        lines.append(f"事件类型：{getattr(event, 'post_type', getattr(event, '__type__', '未知'))}")

    try:
        lines.append(f"事件名称：{event.get_event_name()}")
    except Exception:
        pass

    try:
        lines.append(f"用户ID：{event.get_user_id()}")
    except Exception:
        uid = getattr(event, "user_id", None)
        if uid is not None:
            lines.append(f"用户ID：{uid}")

    try:
        lines.append(f"会话ID：{event.get_session_id()}")
    except Exception:
        pass

    for attr, label in [
        ("group_id", "群ID"),
        ("group_openid", "群OpenID"),
        ("channel_id", "频道ID"),
        ("guild_id", "Guild ID"),
        ("message_id", "消息ID"),
        ("id", "平台消息ID"),
        ("event_id", "事件ID"),
        ("self_id", "Bot ID"),
    ]:
        value = getattr(event, attr, None)
        if value is not None:
            lines.append(f"{label}：{value}")

    try:
        lines.append(f"to_me：{event.is_tome()}")
    except Exception:
        to_me = getattr(event, "to_me", None)
        if to_me is not None:
            lines.append(f"to_me：{to_me}")

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
        raw_message = getattr(event, "raw_message", None)
        content = getattr(event, "content", None)
        message = getattr(event, "message", None)
        if raw_message is not None:
            lines.append(f"raw_message：{raw_message}")
        elif content is not None:
            lines.append(f"content：{content}")
        elif message is not None:
            lines.append(f"message：{_safe_str(message)}")
        else:
            lines.append("消息内容：<无>")

    reply_info = _extract_reply_info(event)
    if reply_info:
        lines.append(f"引用信息：{_unescape_slashes(json.dumps(reply_info, ensure_ascii=False, default=str))}")

    basic_text = "\n".join(lines)

    event_dict = _event_to_dict(event)
    raw_json = _pretty_event_json(event_dict)
    raw_json = _truncate_for_send(raw_json)

    return basic_text, raw_json


async def _send_event_info_by_config(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    basic_text: str,
    raw_json: str,
):
    cfg = XiuConfig()

    safe_basic = _sanitize_markdown_unsafe_text(basic_text)
    safe_raw = _sanitize_markdown_unsafe_text(raw_json)

    if cfg.markdown_status:
        if cfg.markdown_id:
            try:
                content = [safe_raw]
                await send_msg_handler(bot, event, "event", bot.self_id, content, title=safe_basic)
                return
            except Exception as e:
                logger.warning(f"消息信息模板Markdown发送失败，降级原生: {e}")

        try:
            plain = (
                f"**消息基本信息**\r"
                f"```text\r"
                f"{safe_basic}\r"
                f"```\r"
                f"**原始数据 (Event JSON)**\r"
                f"```json\r"
                f"{safe_raw}\r"
                f"```"
            )
            await delivery_service.reply(bot, event, MessageSegment.markdown(bot, plain))
            return
        except Exception as e:
            logger.warning(f"消息信息原生Markdown发送失败，降级纯文本: {e}")

    plain = f"{basic_text}\n\n【原始信息】\n{raw_json}"
    try:
        await delivery_service.reply(bot, event, plain)
    except Exception:
        await handle_send(bot, event, plain)
