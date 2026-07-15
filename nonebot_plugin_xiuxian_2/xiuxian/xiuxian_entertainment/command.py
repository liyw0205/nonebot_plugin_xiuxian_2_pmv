import json
import re
import asyncio
import time
import random
from pathlib import Path
from typing import Any, Tuple
from urllib.parse import quote

from nonebot.log import logger
from nonebot.params import CommandArg, RegexGroup

from ..on_compat import on_command, on_regex
from nonebot.permission import SUPERUSER

from ..adapter_compat import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    is_channel_event,
    Message,
)

from ..xiuxian_utils.utils import (
    handle_send,
    handle_send_md,
    handle_pic_send as _handle_pic_send,
    handle_pic_msg_send as _handle_pic_msg_send,
    generate_command,
    send_help_message,
    escape_markdown_text,
)

from ..xiuxian_config import XiuConfig
from ..messaging import MediaInput, delivery_service
from ..xiuxian_utils.http_proxy import http_client
from ..xiuxian_utils.lay_out import Cooldown

from .media_parser.config import get_fun_media_parser_config
from .media_parser.service import extract_links, run_parse_and_build_messages, dedupe_media_urls_preserve_order
from .io_runtime import (
    AUDIO_SEND_TIMEOUT,
    IMAGE_SEND_TIMEOUT,
    VIDEO_SEND_TIMEOUT,
    run_blocking_io,
    run_media_send,
)


async def send_entertainment_media(bot: Bot, event, media, *, media_type: str):
    """发送娱乐媒体。

    - 普通 URL / 本地路径 / bytes：走 MediaInput + reply_media
    - 已构造好的 MessageSegment / Attachment：直接 reply，禁止再塞进 MediaInput
      （否则 QQ Adapter 的 Attachment 会触发「不支持的媒体输入」）
    """
    timeouts = {
        "图片": IMAGE_SEND_TIMEOUT,
        "音频": AUDIO_SEND_TIMEOUT,
        "视频": VIDEO_SEND_TIMEOUT,
    }
    media_names = {"图片": "image", "音频": "audio", "视频": "video"}

    # 已是消息段 / 适配器 Attachment：直接发
    if not isinstance(media, (str, bytes, bytearray, memoryview, Path)) and not hasattr(
        media, "read"
    ):
        await run_media_send(
            lambda: delivery_service.reply(
                bot,
                event,
                media,
                include_reference=False,
            ),
            timeout=timeouts[media_type],
            media_type=media_type,
        )
        return

    await run_media_send(
        lambda: delivery_service.reply_media(
            bot,
            event,
            MediaInput(media, media_names[media_type]),
        ),
        timeout=timeouts[media_type],
        media_type=media_type,
    )


async def handle_pic_send(bot: Bot, event, imgpath=None):
    await run_media_send(
        lambda: _handle_pic_send(bot, event, imgpath),
        timeout=IMAGE_SEND_TIMEOUT,
        media_type="图片",
    )


async def handle_pic_msg_send(bot: Bot, event, imgpath=None, text: str | None = None):
    await run_media_send(
        lambda: _handle_pic_msg_send(bot, event, imgpath, text),
        timeout=IMAGE_SEND_TIMEOUT,
        media_type="图片",
    )

# ---------- 流媒体链接解析（娱乐）----------

FUN_MEDIA_PARSE_CMDS: tuple[str, ...] = (
    "链接解析",
    "视频解析",
    "解析视频",
    "解析链接",
    "流媒体解析",
)

# 内嵌短链/长链：整段消息任意位置出现即可（前后可有文案，勿用 ^ $ 绑死整条）
# 例：菲比https://v.kuaishou.com/Kc9DxGU3 菲比啾比！……
_FUN_MEDIA_URL_PATH = r"[^\s\u200b\u00a0<>\"'，。！？、；：（）【】《》]+"
_FUN_MEDIA_SHARE_HOSTS = (
    r"v\.douyin\.com|"
    r"www\.iesdouyin\.com|"
    r"b23\.tv|"
    r"bili2233\.cn|"
    r"www\.bilibili\.com|"
    r"m\.bilibili\.com|"
    r"xhslink\.com|"
    r"www\.xiaohongshu\.com|"
    r"v\.kuaishou\.com|"
    r"www\.kuaishou\.com|"
    r"weibo\.com|"
    r"weibo\.cn|"
    r"t\.cn|"
    r"www\.toutiao\.com|"
    r"www\.xiaoheihe\.cn|"
    r"x\.com|"
    r"twitter\.com|"
    r"www\.instagram\.com|"
    r"www\.goofish\.com"
)

FUN_MEDIA_SHARE_URL_RE = re.compile(
    rf"https?://(?:{_FUN_MEDIA_SHARE_HOSTS})/{_FUN_MEDIA_URL_PATH}",
    re.I,
)

# on_regex 用：表示「消息中含有」分享域名的 http 链接（非整句只能是链接）
FUN_MEDIA_EMBEDDED_SHARE_MATCH_RE = re.compile(
    rf".*(https?://(?:{_FUN_MEDIA_SHARE_HOSTS})/{_FUN_MEDIA_URL_PATH}).*",
    re.I | re.S,
)

# 可选解析指令 + 任意 http(s) 链接（指令后整段可再跟其它字，链接用 search 取）
FUN_MEDIA_CMD_WITH_URL_RE = re.compile(
    r"(?:链接解析|视频解析|解析视频|解析链接|流媒体解析)\s+"
    rf"(https?://(?:{_FUN_MEDIA_SHARE_HOSTS})/{_FUN_MEDIA_URL_PATH}|https?://\S+)",
    re.I,
)

FUN_MEDIA_ANY_HTTP_RE = re.compile(
    rf"https?://\S+",
    re.I,
)

_FUN_MEDIA_URL_TRAIL_TRIM = re.compile(
    r"[\s\u200b\u00a0<>\"'，。！？、；：（）【】《》]+$",
)


def fun_media_trim_url(url: str) -> str:
    u = (url or "").strip()
    return _FUN_MEDIA_URL_TRAIL_TRIM.sub("", u)


def fun_media_plain_for_parse(event: GroupMessageEvent | PrivateMessageEvent) -> str:
    """优先整条纯文本，便于从中间抽出短链。"""
    text = fun_media_message_plain(event)
    if text:
        return text
    try:
        return event.get_message().extract_plain_text() or ""
    except Exception:
        return ""


def fun_media_message_has_embedded_share_url(text: str) -> bool:
    """文案中间含分享短链即可，不要求消息只有链接。"""
    if not text:
        return False
    return FUN_MEDIA_SHARE_URL_RE.search(text) is not None


def strip_fun_media_parse_command_prefix(text: str) -> str:
    plain = (text or "").strip()
    for prefix in FUN_MEDIA_PARSE_CMDS:
        if plain.startswith(prefix):
            return plain[len(prefix) :].strip()
    return plain


def fun_media_message_plain(event: GroupMessageEvent | PrivateMessageEvent) -> str:
    return event.get_plaintext() or ""


def fun_media_quick_has_share_url(text: str) -> bool:
    if not text:
        return False
    if fun_media_message_has_embedded_share_url(text):
        return True
    return FUN_MEDIA_ANY_HTTP_RE.search(text) is not None


async def fun_media_has_supported_link(text: str) -> bool:
    if not fun_media_quick_has_share_url(text):
        return False
    try:
        links = await extract_links(text)
    except Exception:
        return False
    return len(links) > 0


# 同一条群消息只解析发送一次（防止多个 on_regex 或重复投递）
_fun_media_parsed_message_ids: dict[str, float] = {}
_FUN_MEDIA_PARSE_DEDUP_SEC = 90.0


def _fun_media_event_dedupe_key(event: GroupMessageEvent | PrivateMessageEvent) -> str:
    mid = getattr(event, "message_id", None)
    if mid is not None:
        return f"mid:{mid}"
    return f"uid:{event.get_user_id()}:ts:{getattr(event, 'time', 0)}"


def fun_media_should_skip_duplicate_event(
    event: GroupMessageEvent | PrivateMessageEvent,
) -> bool:
    key = _fun_media_event_dedupe_key(event)
    now = time.time()
    expired = [k for k, t in _fun_media_parsed_message_ids.items() if now - t > _FUN_MEDIA_PARSE_DEDUP_SEC]
    for k in expired:
        _fun_media_parsed_message_ids.pop(k, None)
    if key in _fun_media_parsed_message_ids:
        return True
    _fun_media_parsed_message_ids[key] = now
    return False


async def fun_media_send_parse_result(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    source_text: str,
) -> None:
    if fun_media_should_skip_duplicate_event(event):
        logger.debug(f"娱乐媒体解析：跳过重复消息 {_fun_media_event_dedupe_key(event)}")
        return
    texts, images, videos, cards = await run_parse_and_build_messages(source_text)
    images = dedupe_media_urls_preserve_order(images)
    videos = dedupe_media_urls_preserve_order(videos)
    body = "\n\n".join(texts)
    has_video = bool(videos)
    has_card = bool(cards)

    def _is_passive_limit_error(exc: BaseException) -> bool:
        text = str(exc)
        return (
            "40034128" in text
            or "被动回复时间或者次数超过限制" in text
            or "被动回复" in text and "超过" in text
        )

    async def _safe_send_media(media, *, media_type: str, label: str) -> bool:
        try:
            await send_entertainment_media(bot, event, media, media_type=media_type)
            return True
        except Exception as e:
            if _is_passive_limit_error(e):
                logger.warning(f"{label}因 QQ 被动回复限制失败: {e}")
                raise
            logger.warning(f"{label}失败: {e}")
            return False

    # QQ 官方群：同一 msg_id 被动回复次数/窗口有限。
    # 视频场景优先「视频 +（可选）卡片」，避免 文案+卡片+视频 三连打满 40034128。
    # 卡片文件名 card_<hash>.png 是内容哈希，不是写死的用户/群 ID。
    try:
        if has_video:
            sent_video = False
            last_err: Exception | None = None
            for vid in videos[:2]:
                try:
                    ok = await _safe_send_media(
                        MessageSegment.video(bot, vid),
                        media_type="视频",
                        label=f"发送解析视频 {vid[:80]}",
                    )
                    if ok:
                        sent_video = True
                        break
                except Exception as e:
                    last_err = e
                    if _is_passive_limit_error(e):
                        break
                    continue

            # 视频已出则卡片尽量再发；被动超限就停
            if sent_video and has_card:
                try:
                    await _safe_send_media(cards[0], media_type="图片", label=f"发送解析卡片 {cards[0]}")
                except Exception as e:
                    if not _is_passive_limit_error(e):
                        pass
            elif not sent_video:
                # 视频全失败：退回文案 + 卡片/封面（仍控制条数）
                if body:
                    try:
                        await handle_send(
                            bot,
                            event,
                            body,
                            md_type="娱乐",
                            k1="娱乐帮助",
                            v1="娱乐帮助",
                            k2="链接解析",
                            v2="链接解析",
                        )
                    except Exception as e:
                        logger.warning(f"发送解析文案失败: {e}")
                        if _is_passive_limit_error(e):
                            return
                if has_card:
                    try:
                        await _safe_send_media(
                            cards[0], media_type="图片", label=f"发送解析卡片 {cards[0]}"
                        )
                    except Exception as e:
                        if _is_passive_limit_error(e):
                            return
                for img in images[:1]:
                    try:
                        await _safe_send_media(
                            img, media_type="图片", label=f"发送解析封面 {img[:80]}"
                        )
                    except Exception as e:
                        if _is_passive_limit_error(e):
                            return
                        break
                tip = "视频发送失败"
                if last_err:
                    tip = f"视频发送失败：{last_err}"
                try:
                    await handle_send(
                        bot,
                        event,
                        f"【媒体解析】{tip}\n可尝试打开上方原始链接观看。",
                        md_type="娱乐",
                        k1="链接解析",
                        v1="链接解析",
                        k2="娱乐帮助",
                        v2="娱乐帮助",
                    )
                except Exception:
                    pass
            return

        # 无视频：文案（无卡片时） + 卡片 + 少量图
        if body and not has_card:
            try:
                await handle_send(
                    bot,
                    event,
                    body,
                    md_type="娱乐",
                    k1="娱乐帮助",
                    v1="娱乐帮助",
                    k2="链接解析",
                    v2="链接解析",
                )
            except Exception as e:
                logger.warning(f"发送解析文案失败: {e}")
                if _is_passive_limit_error(e):
                    return
        elif body and has_card:
            # 有卡片时正文已在卡上，只发极短提示避免占被动次数
            try:
                await handle_send(
                    bot,
                    event,
                    "【媒体解析】见下方卡片",
                    md_type="娱乐",
                    k1="娱乐帮助",
                    v1="娱乐帮助",
                    k2="链接解析",
                    v2="链接解析",
                )
            except Exception as e:
                logger.warning(f"发送解析短提示失败: {e}")
                if _is_passive_limit_error(e):
                    # 仍尝试发卡
                    pass

        if has_card:
            try:
                await _safe_send_media(cards[0], media_type="图片", label=f"发送解析卡片 {cards[0]}")
            except Exception as e:
                if _is_passive_limit_error(e):
                    return
        for img in images[:3]:
            try:
                await _safe_send_media(img, media_type="图片", label=f"发送解析图片 {img[:80]}")
            except Exception as e:
                if _is_passive_limit_error(e):
                    return
                continue
    except Exception as e:
        if _is_passive_limit_error(e):
            logger.warning(f"媒体解析因 QQ 被动回复限制提前结束: {e}")
            return
        raise


def _get_json_api_sync(api_url: str, params: dict | None = None, timeout: int = 15) -> dict:
    """
    通用 JSON 接口请求
    - 优先 resp.json()
    - 失败时兼容 text -> json.loads
    - 失败抛异常给上层处理
    """
    return http_client.get_json(api_url, params=params, timeout=timeout)


async def get_json_api(api_url: str, params: dict | None = None, timeout: int = 15) -> dict:
    return await run_blocking_io(
        _get_json_api_sync, api_url, params, timeout, timeout=timeout + 5
    )


def _get_text_api_sync(api_url: str, params: dict | None = None, timeout: int = 15) -> str:
    """
    通用文本接口请求
    """
    resp = http_client.request("GET", api_url, params=params, timeout=timeout)
    return resp.text.strip()


async def get_text_api(api_url: str, params: dict | None = None, timeout: int = 15) -> str:
    return await run_blocking_io(
        _get_text_api_sync, api_url, params, timeout, timeout=timeout + 5
    )


_API_SUCCESS_CODES = {"0", "1", "200", "ok", "success", "true"}
_API_TEXT_KEYS = (
    "text",
    "content",
    "data",
    "result",
    "answer",
    "output",
    "duanzi",
    "sentence",
    "hitokoto",
)
_API_MESSAGE_KEYS = ("msg", "message", "error", "tips", "detail", "reason")


def normalize_api_text(value: Any) -> str:
    """把接口常见的文本返回规整成可直接发送的内容。"""
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return (
        text.replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\r", "\n")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    ).strip()


def _api_text_from_value(value: Any, keys: tuple[str, ...], depth: int = 0) -> str:
    if value is None or depth > 3:
        return ""

    if isinstance(value, (str, int, float, bool)):
        return normalize_api_text(value)

    if isinstance(value, list):
        parts = [
            part
            for item in value
            if (part := _api_text_from_value(item, keys, depth + 1))
        ]
        return "\n".join(parts).strip()

    if isinstance(value, dict):
        for key in keys:
            if key in value:
                text = _api_text_from_value(value.get(key), keys, depth + 1)
                if text:
                    return text
        return ""

    return normalize_api_text(value)


def extract_api_text(result: Any, *fields: str) -> str:
    """从 API 返回中按字段优先级提取正文，不把 msg/message 当正文兜底。"""
    keys = tuple(dict.fromkeys((*fields, *_API_TEXT_KEYS)))
    return _api_text_from_value(result, keys)


def extract_api_message(result: Any, default: str = "接口异常") -> str:
    if isinstance(result, dict):
        for key in _API_MESSAGE_KEYS:
            msg = normalize_api_text(result.get(key))
            if msg:
                return msg
    return default


def api_code_success(result: Any) -> bool:
    if not isinstance(result, dict):
        return False

    for key in ("success", "ok"):
        if key in result:
            value = result.get(key)
            if isinstance(value, bool):
                return value
            if value is not None:
                return str(value).strip().lower() in _API_SUCCESS_CODES

    for key in ("code", "status", "status_code"):
        if key in result:
            value = result.get(key)
            if value is None:
                continue
            return str(value).strip().lower() in _API_SUCCESS_CODES

    return True


def _get_media_url_api_sync(api_url: str, params: dict | None = None, timeout: int = 20) -> str:
    """
    通用媒体接口请求
    - 如果返回 JSON，则尝试从常见字段里找 URL
    - 如果不是 JSON，则使用 resp.url
    """
    resp = http_client.request(
        "GET", api_url, params=params, timeout=timeout, allow_redirects=True
    )

    content_type = resp.headers.get("Content-Type", "")
    if "application/json" in content_type:
        try:
            result = resp.json()
        except Exception:
            result = json.loads(resp.text)

        if isinstance(result, dict):
            media_url = (
                result.get("url")
                or result.get("image")
                or result.get("image_url")
                or result.get("data")
            )
            if media_url:
                return str(media_url)

        raise ValueError("接口未返回媒体地址")

    return str(resp.url)


async def get_media_url_api(api_url: str, params: dict | None = None, timeout: int = 20) -> str:
    return await run_blocking_io(
        _get_media_url_api_sync, api_url, params, timeout, timeout=timeout + 5
    )


async def handle_audio_send(bot: Bot, event, audio_url: str):
    """
    发送音频消息，失败时抛出异常给上层处理
    """
    if not audio_url:
        return
    await send_entertainment_media(bot, event, audio_url, media_type="音频")


async def send_entertainment_image_result(
    bot: Bot,
    event,
    image_url: str,
    text_msg: str = "",
    *,
    title: str = "娱乐图片",
    buttons: list[tuple[str, str]] | None = None,
):
    """发送娱乐图片结果，Markdown 文案和图片分开发送，避免 QQ 图片语法误解析。"""
    config = XiuConfig()
    body_text = str(text_msg or "").strip()
    title_text = str(title or "娱乐图片").strip()
    markdown_body = "" if body_text == title_text else body_text
    plain_text = body_text or f"【{title_text}】"
    buttons = buttons or []

    if config.markdown_status:
        md_lines = [f"**{escape_markdown_text(title_text)}**"]
        if markdown_body:
            md_lines.append("")
            for line in markdown_body.splitlines():
                line = line.strip()
                if line:
                    md_lines.append(f"> {escape_markdown_text(line)}")
        await handle_send(
            bot,
            event,
            "\n".join(md_lines),
            native_markdown=True,
            fallback_msg=plain_text or f"【{title}】",
            keyboard_rows=[buttons] if buttons else None,
            at_msg=False,
        )
        await send_entertainment_media(
            bot, event, image_url, media_type="图片"
        )
        return

    await handle_pic_msg_send(bot, event, image_url, body_text or title_text or None)
