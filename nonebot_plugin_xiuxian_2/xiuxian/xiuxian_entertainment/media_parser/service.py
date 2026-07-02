"""封装上游 ParserManager：解析文本并格式化为 QQ 可发送内容。"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
from nonebot.log import logger

from .config import get_fun_media_parser_config
from .install_core import ensure_vendor_core, vendor_core_ready

_manager = None
_init_lock = asyncio.Lock()
_init_error: str | None = None


async def _get_manager():
    global _manager, _init_error
    if _manager is not None:
        return _manager
    async with _init_lock:
        if _manager is not None:
            return _manager
        try:
            ensure_vendor_core()
            from core.config_manager import ConfigManager
            from core.parser.manager import ParserManager

            raw = get_fun_media_parser_config().as_upstream_dict()
            cm = ConfigManager(raw)
            parsers = cm.create_parsers()
            _manager = ParserManager(parsers)
            _init_error = None
            logger.info(f"娱乐媒体解析：已加载 {len(parsers)} 个平台解析器")
        except Exception as e:
            _init_error = str(e)
            logger.warning(f"娱乐媒体解析初始化失败: {e}")
            raise
    return _manager


def last_init_error() -> str | None:
    return _init_error


async def extract_links(text: str) -> list[tuple[str, str]]:
    """返回 [(url, parser_name), ...]"""
    mgr = await _get_manager()
    pairs = mgr.extract_all_links(text)
    return [(url, p.name) for url, p in pairs]


async def parse_text(text: str) -> list[dict[str, Any]]:
    mgr = await _get_manager()
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        return await mgr.parse_text(text, session)


def _format_meta_line(meta: dict[str, Any]) -> str:
    platform = meta.get("platform") or meta.get("parser_name") or "未知"
    title = (meta.get("title") or "").strip()
    author = (meta.get("author") or "").strip()
    desc = (meta.get("desc") or "").strip()
    url = meta.get("url") or meta.get("source_url") or ""
    lines: list[str] = [f"【媒体解析】{platform}"]
    err = meta.get("error")
    if err:
        lines.append("状态：解析失败")
        lines.append(f"原因：{err}")
        if url:
            lines.append(f"原始链接：{url}")
        return "\n".join(lines)
    if title:
        lines.append(f"标题：{title}")
    if author:
        lines.append(f"作者：{author}")
    if desc and desc != title:
        d = desc if len(desc) <= 400 else desc[:400] + "…"
        lines.append(f"简介：{d}")
    if url:
        lines.append(f"原始链接：{url}")
    if len(lines) == 1:
        lines.append("提示：无标题信息")
    return "\n".join(lines)


def collect_media_urls(meta: dict[str, Any]) -> tuple[list[str], list[str]]:
    """扁平化 image_urls / video_urls（上游多为嵌套列表），单条 meta 内去重。"""
    images: list[str] = []
    videos: list[str] = []
    seen_i: set[str] = set()
    seen_v: set[str] = set()

    def _norm(u: str) -> str:
        return (u or "").strip().rstrip("/")

    def _flat(field: str, out: list[str], seen: set[str]) -> None:
        raw = meta.get(field) or []
        for item in raw:
            if isinstance(item, str) and item.startswith("http"):
                key = _norm(item)
                if key and key not in seen:
                    seen.add(key)
                    out.append(item.strip())
            elif isinstance(item, list):
                for u in item:
                    if isinstance(u, str) and u.startswith("http"):
                        key = _norm(u)
                        if key and key not in seen:
                            seen.add(key)
                            out.append(u.strip())

    _flat("image_urls", images, seen_i)
    _flat("video_urls", videos, seen_v)
    return images, videos


def dedupe_media_urls_preserve_order(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        key = (u or "").strip().rstrip("/")
        if not key.startswith("http") or key in seen:
            continue
        seen.add(key)
        out.append(u.strip())
    return out


async def run_parse_and_build_messages(
    text: str,
) -> tuple[list[str], list[str], list[str]]:
    """
    解析文本，返回 (text_chunks, image_urls, video_urls)。
    """
    if not text or not text.strip():
        return (["【媒体解析】\n请在消息中附带可解析的链接。"], [], [])

    try:
        metas = await parse_text(text)
    except Exception as e:
        hint = ""
        if not vendor_core_ready():
            hint = "（首次使用需联网下载解析核心，或查看日志）"
        return ([f"【媒体解析】\n状态：不可用\n原因：{e}{hint}"], [], [])

    if not metas:
        return (["【媒体解析】\n未识别到支持的流媒体链接。"], [], [])

    texts: list[str] = []
    all_images: list[str] = []
    all_videos: list[str] = []
    seen_meta_keys: set[str] = set()
    for meta in metas:
        if not isinstance(meta, dict):
            continue
        meta_key = (
            (meta.get("url") or meta.get("source_url") or meta.get("link") or "")
            .strip()
            .rstrip("/")
        )
        if meta_key and meta_key in seen_meta_keys:
            continue
        if meta_key:
            seen_meta_keys.add(meta_key)
        texts.append(_format_meta_line(meta))
        imgs, vids = collect_media_urls(meta)
        all_images.extend(imgs)
        all_videos.extend(vids)

    all_images = dedupe_media_urls_preserve_order(all_images)[:12]
    all_videos = dedupe_media_urls_preserve_order(all_videos)[:3]

    return (texts, all_images, all_videos)
