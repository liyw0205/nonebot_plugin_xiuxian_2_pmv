"""封装本插件原生 Parser：解析文本并格式化为 QQ 可发送内容。

不再从 GitHub 下载 astrbot_plugin_media_parser core。
可选 PIL 媒体卡片（参考 astrbot_plugin_parser 布局）。
"""
from __future__ import annotations

from typing import Any

from nonebot.log import logger

from ..io_runtime import run_blocking_io
from .card import render_media_card
from .config import get_fun_media_parser_config
from .io_runtime_safe import run_native_parse
from .native import _use_proxy_for, extract_supported_links, parse_text_native


async def extract_links(text: str) -> list[tuple[str, str]]:
    """返回 [(url, parser_name), ...]；纯本地正则，无需网络。"""
    return extract_supported_links(text or "")


async def parse_text(text: str) -> list[dict[str, Any]]:
    # 平台请求走线程池，避免阻塞事件循环
    return await run_native_parse(text or "")


def last_init_error() -> str | None:
    return None


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
    if not meta.get("video_urls") and not meta.get("image_urls"):
        lines.append("提示：未提取到可发送媒体，可尝试打开原始链接")
    if len(lines) == 1:
        lines.append("提示：无标题信息")
    return "\n".join(lines)


def collect_media_urls(meta: dict[str, Any]) -> tuple[list[str], list[str]]:
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


def _render_card_sync(meta: dict[str, Any]) -> str | None:
    imgs, vids = collect_media_urls(meta)
    cover = imgs[0] if imgs else None
    plat = str(meta.get("platform") or "")
    use_proxy = _use_proxy_for(plat)
    path = render_media_card(
        meta,
        cover_url=cover,
        has_video=bool(vids),
        use_proxy=use_proxy,
    )
    return str(path) if path else None


async def run_parse_and_build_messages(
    text: str,
) -> tuple[list[str], list[str], list[str], list[str]]:
    """解析文本，返回 (text_chunks, image_urls, video_urls, card_paths)。"""
    if not text or not text.strip():
        return (["【媒体解析】\n请在消息中附带可解析的链接。"], [], [], [])

    _ = get_fun_media_parser_config()

    try:
        metas = await parse_text(text)
    except Exception as e:
        logger.warning(f"娱乐媒体解析失败: {e}")
        return ([f"【媒体解析】\n状态：不可用\n原因：{e}"], [], [], [])

    if not metas:
        return (["【媒体解析】\n未识别到支持的流媒体链接。"], [], [], [])

    texts: list[str] = []
    all_images: list[str] = []
    all_videos: list[str] = []
    card_paths: list[str] = []
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
        if not meta.get("error") and (imgs or vids or meta.get("title")):
            try:
                path = await run_blocking_io(_render_card_sync, meta, timeout=45)
                if path:
                    card_paths.append(path)
            except Exception as e:
                logger.debug(f"媒体卡片渲染失败: {e}")

    all_images = dedupe_media_urls_preserve_order(all_images)[:12]
    # 视频优先挑较短/更可发的：kwaicdn 主链在前（native 已排序），这里只截前 3
    all_videos = dedupe_media_urls_preserve_order(all_videos)[:3]
    # 过滤明显非内容图（表情包等）
    all_images = [
        u
        for u in all_images
        if "emotion" not in u.lower() and "emoji" not in u.lower()
    ][:12]
    return (texts, all_images, all_videos, card_paths)
