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
from .media_parser.native import MEDIA_MAX_BYTES, sort_media_urls_by_quality
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


def _guess_image_size_from_url(url: str) -> tuple[int, int]:
    """从 CDN URL 路径猜宽高（如抖音 :1106:932:），猜不到用 1080x1080。"""
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
    return 1080, 1080


def _format_qq_md_image(url: str, width: int = 1080, height: int = 1080) -> str:
    """QQ 原生 Markdown 图片语法：直接使用已有 http(s) 直链。

    示例：``![img #1080px #1004px](https://...)``
    不上传、不换图床；解析结果本身已有 CDN 链接。
    """
    u = str(url or "").strip().replace(" ", "%20").replace("(", "%28").replace(")", "%29")
    w = max(1, int(width or 1080))
    h = max(1, int(height or 1080))
    return f"![img #{w}px #{h}px]({u})"


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
    3. 图集且开启 markdown：直接用已有 CDN 直链拼
       ``![img #Wpx #Hpx](url)`` 一次发多图（不上传图床）
       卡片仅把首图当封面；图集发送仍含第一张，不得 gallery[1:]
    """
    if fun_media_should_skip_duplicate_event(event):
        logger.debug(f"娱乐媒体解析：跳过重复消息 {_fun_media_event_dedupe_key(event)}")
        return

    texts, images, videos, cards = await run_parse_and_build_messages(source_text)
    # 图片按「资源对象」去重后再按画质排序，避免同图多 CDN/多清晰度重复发
    try:
        from .media_parser.native import dedupe_media_urls_by_object
        images = dedupe_media_urls_by_object(images, kind="image")
    except Exception:
        images = dedupe_media_urls_preserve_order(images)
    # 保持解析顺序为主：卡片首图与图集第一张一致；仅对象去重，不再按画质重排打乱
    # （画质优选已在各平台解析时做过）
    images = list(images)
    videos = sort_media_urls_by_quality(
        dedupe_media_urls_preserve_order(videos), kind="video"
    )
    body = "\n\n".join(t for t in texts if t).strip()
    has_video = bool(videos)
    has_card = bool(cards)
    md_on = bool(getattr(XiuConfig(), "markdown_status", False))
    max_bytes = int(MEDIA_MAX_BYTES)

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

    def _media_headers(url: str) -> dict[str, str]:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
        }
        low = (url or "").lower()
        if any(x in low for x in ("bilivideo", "hdslb", "bilibili")):
            headers["Referer"] = "https://www.bilibili.com"
            headers["Origin"] = "https://www.bilibili.com"
        elif "weibo" in low or "sinaimg" in low:
            headers["Referer"] = "https://weibo.com/"
        elif "xhscdn" in low or "xiaohongshu" in low:
            headers["Referer"] = "https://www.xiaohongshu.com/"
        elif "douyin" in low or "byte" in low:
            headers["Referer"] = "https://www.douyin.com/"
        elif "kuaishou" in low or "kwimgs" in low or "yximgs" in low:
            headers["Referer"] = "https://www.kuaishou.com/"
        return headers

    def _probe_media_size(url: str) -> int | None:
        """探测媒体大小（字节）。未知返回 None。"""
        headers = _media_headers(url)
        # 1) HEAD
        try:
            resp = http_client.request(
                "HEAD",
                url,
                timeout=12,
                check_status=False,
                use_config_proxy=False,
                headers=headers,
            )
            cl = resp.headers.get("Content-Length") or resp.headers.get("content-length")
            if cl and str(cl).isdigit():
                return int(cl)
        except Exception:
            pass
        # 2) Range 0-0
        try:
            h2 = dict(headers)
            h2["Range"] = "bytes=0-0"
            resp = http_client.request(
                "GET",
                url,
                timeout=15,
                check_status=False,
                use_config_proxy=False,
                headers=h2,
                stream=True,
            )
            cr = resp.headers.get("Content-Range") or resp.headers.get("content-range") or ""
            # bytes 0-0/12345
            if "/" in cr:
                total = cr.rsplit("/", 1)[-1]
                if total.isdigit():
                    return int(total)
            cl = resp.headers.get("Content-Length") or resp.headers.get("content-length")
            if cl and str(cl).isdigit() and int(getattr(resp, "status_code", 0) or 0) != 206:
                return int(cl)
        except Exception:
            pass
        return None

    def _download_video_local(
        url: str,
        referer: str = "https://www.bilibili.com",
        max_bytes: int = MEDIA_MAX_BYTES,
    ) -> Path:
        """带 Referer 下载到缓存，返回本地路径。超过 max_bytes 抛错供降档。"""
        import hashlib

        # 插件部署在 src.plugins 命名空间下，相对导入才能同时兼容源码与部署路径。
        try:
            from ...paths import get_paths
        except Exception:
            from pathlib import Path as _P

            def get_paths():  # type: ignore
                class _Pth:
                    data = _P("data/xiuxian")

                return _Pth()

        # 放 cache 下，自动备份跳过
        try:
            from .media_parser.config import media_parser_cache_dir
            cache = media_parser_cache_dir() / "videos"
        except Exception:
            base = getattr(get_paths(), "cache", None) or (get_paths().data / "cache")
            cache = Path(base) / "media_parser" / "videos"
        cache.mkdir(parents=True, exist_ok=True)
        name = hashlib.sha1(url.encode("utf-8")).hexdigest()[:20] + ".mp4"
        out = cache / name
        # 兼容旧目录命中（只读）
        if not out.is_file():
            legacy = get_paths().data / "media_parser_cache" / "videos" / name
            if legacy.is_file() and 1024 < legacy.stat().st_size <= max_bytes:
                return legacy
        if out.is_file() and 1024 < out.stat().st_size <= max_bytes:
            return out
        # 缓存过大则删掉重下/换档
        if out.is_file() and out.stat().st_size > max_bytes:
            try:
                out.unlink(missing_ok=True)
            except Exception:
                pass

        headers = _media_headers(url)
        if referer:
            headers["Referer"] = referer
        resp = http_client.request(
            "GET",
            url,
            timeout=90,
            stream=True,
            check_status=False,
            use_config_proxy=False,
            headers=headers,
        )
        code = int(getattr(resp, "status_code", 0) or 0)
        if code >= 400:
            raise RuntimeError(f"视频下载 HTTP {code}")
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
                    raise RuntimeError(f"视频超过 {max_bytes // (1024 * 1024)}MB")
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

    async def _pick_and_send_video(candidates: list[str]) -> bool:
        """最高清优先；>20MB 自动降档。"""
        last_err: Exception | None = None
        for vid in candidates[:5]:
            try:
                size = await run_blocking_io(_probe_media_size, vid, timeout=20)
            except Exception:
                size = None
            if isinstance(size, int) and size > max_bytes:
                logger.info(
                    f"跳过超限视频 {size / (1024 * 1024):.1f}MB > "
                    f"{max_bytes // (1024 * 1024)}MB: {vid[:80]}"
                )
                continue

            # B站等：本地下载（同样受 20MB 限制）
            if _needs_local_video_download(vid):
                try:
                    local = await run_blocking_io(
                        _download_video_local,
                        vid,
                        "https://www.bilibili.com",
                        max_bytes,
                        timeout=120,
                    )
                    ok = await _safe_send_media(
                        Path(local),
                        media_type="视频",
                        label=f"发送本地下载视频 {Path(local).name}",
                    )
                    if ok:
                        return True
                except Exception as e:
                    last_err = e
                    msg = str(e)
                    if "超过" in msg and "MB" in msg:
                        logger.info(f"本地下载超限降档: {e}")
                        continue
                    logger.warning(f"B站视频本地下载失败，尝试下一档/直链: {e}")

            # URL 直发（快手/微博等）
            try:
                ok = await _safe_send_media(
                    MessageSegment.video(bot, vid),
                    media_type="视频",
                    label=f"发送解析视频 {vid[:80]}",
                )
                if ok:
                    return True
            except Exception as e:
                last_err = e
                if _is_passive_limit_error(e):
                    raise
                continue
        if last_err:
            raise last_err
        return False

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
        """QQ 原生 MD 图集：直接用已有 CDN 直链 + ![img #Wpx #Hpx](url)。不上传图床。"""
        lines: list[str] = []
        if caption:
            lines.append(caption.replace("\r", "\n"))
            lines.append("")
        # 最多 18 张，含第一张；与卡片封面可重复出现，避免“后面没发/首图被去掉”
        for u in urls[:18]:
            url = str(u or "").strip()
            if not url.lower().startswith(("http://", "https://")):
                continue
            w, h = _guess_image_size_from_url(url)
            lines.append(_format_qq_md_image(url, w, h))
        return "\n".join(lines)

    def _gallery_http_urls(urls: list[str]) -> list[str]:
        out: list[str] = []
        for u in urls:
            s = str(u or "").strip()
            if s.lower().startswith(("http://", "https://")):
                out.append(s)
        return out

    try:
        await _send_card_with_info()

        if has_video:
            last_err: Exception | None = None
            try:
                sent = await _pick_and_send_video(videos)
            except Exception as e:
                sent = False
                last_err = e
                if _is_passive_limit_error(e):
                    return
            if not sent:
                try:
                    tip = (
                        f"【媒体解析】视频发送失败"
                        + (f"：{last_err}" if last_err else "（可能均超过20MB或链路失败）")
                        + "\n可尝试打开原始链接观看。"
                    )
                    await handle_send(
                        bot,
                        event,
                        tip,
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

        # 卡片只占用首图作封面；图集列表必须保留第一张，禁止 gallery[1:]
        # MD 必须真发出去（保留 []）；失败则逐张普通发图，禁止剥括号纯文本
        gallery_http = _gallery_http_urls(gallery)[:18]
        md_sent = False
        if md_on and gallery_http:
            md_body = _build_gallery_md(
                gallery_http, caption="" if has_card else body
            )
            if "![img" in md_body and "](http" in md_body:
                try:
                    ok = await handle_send(
                        bot,
                        event,
                        md_body,
                        native_markdown=True,
                        # 失败不要剥 [] 当成功；交给下面逐张发图
                        allow_plain_fallback=False,
                        fallback_msg=md_body,
                        k1="娱乐帮助",
                        v1="娱乐帮助",
                        k2="链接解析",
                        v2="链接解析",
                    )
                    md_sent = bool(ok)
                except Exception as e:
                    logger.warning(f"图集 Markdown 发送失败，改逐张发图: {e}")
                    if _is_passive_limit_error(e):
                        return
                    md_sent = False

        if md_sent:
            return

        # 无 MD / MD 失败：逐张普通发图（含第一张，不因卡片而跳过）
        for img in gallery[:18]:
            try:
                await _safe_send_media(
                    img, media_type="图片", label=f"发送解析图片 {str(img)[:80]}"
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
