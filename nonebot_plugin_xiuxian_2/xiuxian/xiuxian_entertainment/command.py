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
from ..xiuxian_utils.lay_out import Cooldown

# 娱乐子模块历史写法 parameterless=[Data(...)]，与 Cooldown 同义
Data = Cooldown

from .media_parser.config import get_fun_media_parser_config
from .media_parser.native import MEDIA_MAX_BYTES
from .io_runtime import (
    AUDIO_SEND_TIMEOUT,
    IMAGE_SEND_TIMEOUT,
    VIDEO_SEND_TIMEOUT,
    run_blocking_io,
    run_media_send,
)
from .room_store import entertainment_application

entertainment_application.media_parser.bind_blocking_runner(run_blocking_io)


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
    return entertainment_application.media_parser.has_supported_link(text)


def _guess_image_size_from_url(url: str) -> tuple[int, int] | None:
    """从 CDN URL 路径猜宽高（如抖音 :1106:932:）；猜不到返回 None。"""
    s = str(url or "")
    m = re.search(r":(\d{2,5}):(\d{2,5}):", s)
    if m:
        try:
            w, h = int(m.group(1)), int(m.group(2))
            if 16 <= w <= 10000 and 16 <= h <= 10000:
                return w, h
        except Exception:
            pass
    m = re.search(r"[?&](?:w|width)=(\d{2,5}).*?[?&](?:h|height)=(\d{2,5})", s, re.I)
    if m:
        try:
            w, h = int(m.group(1)), int(m.group(2))
            if 16 <= w <= 10000 and 16 <= h <= 10000:
                return w, h
        except Exception:
            pass
    # bilibili / 部分 CDN: ..._1080x1440.jpg 或 @1080w_1440h
    m = re.search(r"(?:_|@|/)(\d{2,5})[xX×](\d{2,5})(?:\.|_|\?|$)", s)
    if m:
        try:
            w, h = int(m.group(1)), int(m.group(2))
            if 16 <= w <= 10000 and 16 <= h <= 10000:
                return w, h
        except Exception:
            pass
    m = re.search(r"(\d{2,5})w[_-](\d{2,5})h", s, re.I)
    if m:
        try:
            w, h = int(m.group(1)), int(m.group(2))
            if 16 <= w <= 10000 and 16 <= h <= 10000:
                return w, h
        except Exception:
            pass
    return None


def _image_size_from_bytes(data: bytes) -> tuple[int, int] | None:
    """从图片头解析宽高（PNG/GIF/JPEG/WEBP）；失败返回 None。"""
    if not data or len(data) < 24:
        return None
    try:
        import struct

        # PNG
        if data[:8] == b"\x89PNG\r\n\x1a\n" and len(data) >= 24:
            w, h = struct.unpack(">II", data[16:24])
            if w > 0 and h > 0:
                return int(w), int(h)
        # GIF
        if data[:6] in (b"GIF87a", b"GIF89a") and len(data) >= 10:
            w, h = struct.unpack("<HH", data[6:10])
            if w > 0 and h > 0:
                return int(w), int(h)
        # JPEG
        if data[:2] == b"\xff\xd8":
            i = 2
            n = len(data)
            while i + 9 < n:
                if data[i] != 0xFF:
                    i += 1
                    continue
                marker = data[i + 1]
                i += 2
                if marker in (0xD8, 0xD9) or marker == 0x01 or 0xD0 <= marker <= 0xD7:
                    continue
                if i + 2 > n:
                    break
                seglen = struct.unpack(">H", data[i : i + 2])[0]
                if seglen < 2:
                    break
                if marker in (0xC0, 0xC1, 0xC2) and i + 7 <= n:
                    h, w = struct.unpack(">HH", data[i + 3 : i + 7])
                    if w > 0 and h > 0:
                        return int(w), int(h)
                i += seglen
        # WEBP
        if data[:4] == b"RIFF" and len(data) >= 30 and data[8:12] == b"WEBP":
            chunk = data[12:16]
            if chunk == b"VP8X" and len(data) >= 30:
                w = 1 + int.from_bytes(data[24:27], "little")
                h = 1 + int.from_bytes(data[27:30], "little")
                if w > 0 and h > 0:
                    return w, h
            if (
                chunk == b"VP8 "
                and len(data) >= 30
                and data[23] == 0x9D
                and data[24:27] == b"\x01\x2a"
            ):
                w, h = struct.unpack("<HH", data[26:30])
                w &= 0x3FFF
                h &= 0x3FFF
                if w > 0 and h > 0:
                    return w, h
            if chunk == b"VP8L" and len(data) >= 25 and data[20] == 0x2F:
                b0, b1, b2, b3 = data[21:25]
                w = 1 + (((b1 & 0x3F) << 8) | b0)
                h = 1 + (((b3 & 0xF) << 10) | (b2 << 2) | ((b1 & 0xC0) >> 6))
                if w > 0 and h > 0:
                    return w, h
    except Exception:
        pass
    # PIL 兜底（项目已依赖）
    try:
        from io import BytesIO
        from PIL import Image

        with Image.open(BytesIO(data)) as im:
            w, h = im.size
            if w > 0 and h > 0:
                return int(w), int(h)
    except Exception:
        pass
    return None


def _normalize_md_display_size(
    width: int,
    height: int,
    *,
    max_w: int = 1080,
    max_h: int = 1920,
) -> tuple[int, int]:
    """按真实比例缩放展示尺寸，避免固定 1080x1080 方图难看。"""
    try:
        w = int(width)
        h = int(height)
    except Exception:
        return 720, 720
    if w < 16 or h < 16:
        return 720, 720
    # 过大则等比缩小到聊天友好尺寸
    scale = min(float(max_w) / w, float(max_h) / h, 1.0)
    nw = max(1, int(round(w * scale)))
    nh = max(1, int(round(h * scale)))
    return nw, nh


def _probe_image_size(url: str) -> tuple[int, int]:
    """识别图片真实宽高：URL 线索 → 下载探测 → 默认比例。"""
    guessed = _guess_image_size_from_url(url)
    if guessed:
        return _normalize_md_display_size(*guessed)

    s = str(url or "").strip()
    if not s.lower().startswith(("http://", "https://")):
        return 720, 960

    try:
        from urllib.parse import urlparse

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
                "AppleWebKit/605.1.15 Mobile/15E148"
            ),
            "Accept": "image/avif,image/webp,image/*,*/*;q=0.8",
        }
        host = (urlparse(s).hostname or "").lower()
        if host:
            headers["Referer"] = f"https://{host}/"
        # 部分国内 CDN 需要平台 Referer
        if "douyin" in host or "byteimg" in host or "snssdk" in host:
            headers["Referer"] = "https://www.douyin.com/"
        elif "yximgs" in host or "kuaishou" in host or "kwimgs" in host:
            headers["Referer"] = "https://www.kuaishou.com/"
        elif "hdslb" in host or "bilibili" in host:
            headers["Referer"] = "https://www.bilibili.com/"

        resp = entertainment_application.media_parser.provider.request(
            "GET",
            s,
            timeout=5,
            max_bytes=512 * 1024,
            headers=headers,
            check_status=False,
            use_config_proxy=False,
        )
        try:
            if int(getattr(resp, "status_code", 0) or 0) >= 400:
                return 720, 960
            data = bytearray()
            # 头信息通常够用；过大则截断
            for chunk in resp.iter_content(32 * 1024):
                if not chunk:
                    continue
                data.extend(chunk)
                if len(data) >= 512 * 1024:
                    break
                if len(data) >= 64 * 1024:
                    sized = _image_size_from_bytes(bytes(data))
                    if sized:
                        return _normalize_md_display_size(*sized)
            sized = _image_size_from_bytes(bytes(data))
            if sized:
                return _normalize_md_display_size(*sized)
        finally:
            resp.close()
    except Exception as e:
        logger.debug(f"图片尺寸探测失败 {s[:80]}: {e}")
    # 竖图默认比方图更自然
    return 720, 960


async def fun_media_send_parse_result(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    source_text: str,
) -> None:
    """Compatibility entry point; all parsing and delivery belong to the feature owner."""
    await entertainment_application.media_parser_messages.send_parse_result(
        bot, event, source_text
    )

def _get_json_api_sync(
    api_url: str,
    params: dict | None = None,
    timeout: int = 15,
    max_bytes: int | None = None,
) -> dict:
    """Fetch parsed JSON through the feature-owned bounded provider."""
    return entertainment_application.external_json(
        api_url, params=params, timeout=timeout, max_bytes=max_bytes
    )


async def get_json_api(
    api_url: str,
    params: dict | None = None,
    timeout: int = 15,
    max_bytes: int | None = None,
) -> dict:
    return await run_blocking_io(
        _get_json_api_sync, api_url, params, timeout, max_bytes, timeout=timeout + 5
    )


def _get_text_api_sync(api_url: str, params: dict | None = None, timeout: int = 15) -> str:
    """
    通用文本接口请求
    """
    return entertainment_application.external_text(
        api_url, params=params, timeout=timeout
    )


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
    return entertainment_application.external_media_url(
        api_url, params=params, timeout=timeout
    )


async def get_media_url_api(api_url: str, params: dict | None = None, timeout: int = 20) -> str:
    return await run_blocking_io(
        _get_media_url_api_sync, api_url, params, timeout, timeout=timeout + 5
    )


async def handle_audio_send(bot: Bot, event, audio_url: str):
    """
    发送音频消息，失败时抛出异常给上层处理。

    网易 outer 链下载后体积常 3~8MB；QQ 群上传偶发 50015014「系统繁忙」，做短重试。
    """
    if not audio_url:
        return
    last_err: BaseException | None = None
    for attempt in range(3):
        try:
            await send_entertainment_media(bot, event, audio_url, media_type="音频")
            return
        except Exception as e:
            last_err = e
            msg = str(e)
            busy = (
                "50015014" in msg
                or "系统繁忙" in msg
                or "ActionFailed: 500" in msg
            )
            if not busy or attempt >= 2:
                raise
            logger.warning(f"音频发送繁忙，重试 {attempt + 1}/3：{e}")
            await asyncio.sleep(0.8 * (attempt + 1))
    if last_err is not None:
        raise last_err


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
