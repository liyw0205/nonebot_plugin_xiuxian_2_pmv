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
    """发送解析结果。

    策略（兼顾 QQ 被动回复次数）：
    1. 卡片 + 解析文案：图文接口一次发出（信息不会丢）
    2. 有视频：再发 1 条高清视频
       - B 站 CDN 需 Referer，QQ 直链常 850026，改为本地下载后 file_video
    3. 图集且开启 markdown：用原生 MD 图片链一次发出，避免多图被动超限
    """
    if fun_media_should_skip_duplicate_event(event):
        logger.debug(f"娱乐媒体解析：跳过重复消息 {_fun_media_event_dedupe_key(event)}")
        return

    texts, images, videos, cards = await run_parse_and_build_messages(source_text)
    images = dedupe_media_urls_preserve_order(images)
    videos = dedupe_media_urls_preserve_order(videos)
    body = "\n\n".join(t for t in texts if t).strip()
    has_video = bool(videos)
    has_card = bool(cards)
    md_on = bool(getattr(XiuConfig(), "markdown_status", False))

    def _is_passive_limit_error(exc: BaseException) -> bool:
        text = str(exc)
        return (
            "40034128" in text
            or "被动回复时间或者次数超过限制" in text
            or ("被动回复" in text and "超过" in text)
        )

    def _needs_local_video_download(url: str) -> bool:
        """B 站等 CDN 对 QQ 无 Referer 的直链拉取会 850026。"""
        low = (url or "").lower()
        return any(
            x in low
            for x in (
                "bilivideo.com",
                "hdslb.com",
                "bilibili.com/bfs",
                "upgcxcode",
            )
        )

    def _download_video_local(url: str, referer: str = "https://www.bilibili.com") -> Path:
        """带 Referer 下载到缓存，返回本地路径。"""
        import hashlib

        from nonebot_plugin_xiuxian_2.paths import get_paths

        cache = get_paths().data / "media_parser_cache" / "videos"
        cache.mkdir(parents=True, exist_ok=True)
        name = hashlib.sha1(url.encode("utf-8")).hexdigest()[:20] + ".mp4"
        out = cache / name
        if out.is_file() and out.stat().st_size > 1024:
            return out

        resp = http_client.request(
            "GET",
            url,
            timeout=90,
            stream=True,
            check_status=False,
            use_config_proxy=False,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Referer": referer,
                "Origin": "https://www.bilibili.com",
                "Accept": "*/*",
            },
        )
        code = int(getattr(resp, "status_code", 0) or 0)
        if code >= 400:
            raise RuntimeError(f"视频下载 HTTP {code}")
        max_bytes = 25 * 1024 * 1024
        size = 0
        tmp = out.with_suffix(".tmp")
        with tmp.open("wb") as f:
            for chunk in resp.iter_content(256 * 1024):
                if not chunk:
                    continue
                size += len(chunk)
                if size > max_bytes:
                    try:
                        tmp.unlink(missing_ok=True)
                    except Exception:
                        pass
                    raise RuntimeError(
                        f"视频超过 {max_bytes // (1024 * 1024)}MB，放弃本地下载"
                    )
                f.write(chunk)
        if size < 1024:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            raise RuntimeError("视频下载内容过小")
        tmp.replace(out)
        return out

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

    async def _send_video_url(vid: str) -> bool:
        """发送视频：B 站等先本地下载，其它仍 URL 直发。"""
        if _needs_local_video_download(vid):
            try:
                local = await run_blocking_io(
                    _download_video_local,
                    vid,
                    "https://www.bilibili.com",
                    timeout=120,
                )
                return await _safe_send_media(
                    Path(local),
                    media_type="视频",
                    label=f"发送本地下载视频 {Path(local).name}",
                )
            except Exception as e:
                logger.warning(f"B站视频本地下载失败，尝试直链: {e}")

        return await _safe_send_media(
            MessageSegment.video(bot, vid),
            media_type="视频",
            label=f"发送解析视频 {vid[:80]}",
        )

    async def _send_card_with_info() -> bool:
        """卡片 + 解析信息 用图文接口同发（1 条被动回复）。"""
        if has_card and body:
            try:
                card_path = Path(cards[0])
                await handle_pic_msg_send(bot, event, card_path, body)
                return True
            except Exception as e:
                logger.warning(f"卡片图文发送失败，降级: {e}")
                if _is_passive_limit_error(e):
                    raise
        if has_card:
            return await _safe_send_media(
                cards[0], media_type="图片", label=f"发送解析卡片 {cards[0]}"
            )
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
                return True
            except Exception as e:
                logger.warning(f"发送解析文案失败: {e}")
                if _is_passive_limit_error(e):
                    raise
        return False

    def _build_gallery_md(urls: list[str], caption: str = "") -> str:
        lines: list[str] = []
        if caption:
            lines.append(caption.replace("\r", "\n"))
            lines.append("")
        for i, u in enumerate(urls[:9], 1):
            lines.append(f"![图{i}]({u})")
        return "\n".join(lines)

    try:
        await _send_card_with_info()

        if has_video:
            sent_video = False
            last_err: Exception | None = None
            for vid in videos[:2]:
                try:
                    ok = await _send_video_url(vid)
                    if ok:
                        sent_video = True
                        break
                except Exception as e:
                    last_err = e
                    if _is_passive_limit_error(e):
                        return
                    continue
            if not sent_video and last_err:
                try:
                    await handle_send(
                        bot,
                        event,
                        f"【媒体解析】视频发送失败：{last_err}\n"
                        f"可尝试打开原始链接观看。",
                        md_type="娱乐",
                        k1="链接解析",
                        v1="链接解析",
                        k2="娱乐帮助",
                        v2="娱乐帮助",
                    )
                except Exception:
                    pass
            return

        gallery = [
            u
            for u in images
            if "emotion" not in u.lower()
            and "emoji" not in u.lower()
            and "uhead" not in u.lower()
            and "/bg" not in u.lower()
        ]
        if not gallery:
            return

        if md_on and len(gallery) >= 1:
            md_body = _build_gallery_md(gallery, caption="" if has_card else body)
            try:
                await handle_send(
                    bot,
                    event,
                    md_body,
                    native_markdown=True,
                    fallback_msg=body or "【媒体解析】图集",
                    k1="娱乐帮助",
                    v1="娱乐帮助",
                    k2="链接解析",
                    v2="链接解析",
                )
                return
            except Exception as e:
                logger.warning(f"图集 Markdown 发送失败，降级逐张: {e}")
                if _is_passive_limit_error(e):
                    return

        for img in gallery[:9]:
            try:
                await _safe_send_media(
                    img, media_type="图片", label=f"发送解析图片 {img[:80]}"
                )
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
