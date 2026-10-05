from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Any, Awaitable, Callable

from .media_parser_provider import EntertainmentMediaParserProvider

MEDIA_PARSE_DEDUPE_SECONDS = 90.0
MEDIA_PARSE_DEDUPE_MAX_EVENTS = 2048
MEDIA_PARSE_MAX_TEXT_CHARS = 8192
MEDIA_PARSE_MAX_LINKS = 3


def format_media_meta_line(meta: dict[str, Any]) -> str:
    from ...xiuxian.xiuxian_utils.status_card import media_fail_hint

    platform = meta.get("platform") or meta.get("parser_name") or "未知"
    title = (meta.get("title") or "").strip()
    author = (meta.get("author") or "").strip()
    desc = (meta.get("desc") or "").strip()
    source = (meta.get("source_url") or "").strip()
    resolved = (meta.get("url") or "").strip()
    display_url = source or resolved
    lines = [f"【媒体解析】{platform}"]
    error = meta.get("error")
    if error:
        lines.append(media_fail_hint(str(error), platform=str(platform)))
        if display_url:
            lines.append(f"原始链接：{display_url}")
        return "\n".join(lines)
    if title:
        lines.append(f"标题：{title}")
    if author:
        lines.append(f"作者：{author}")
    if desc and desc != title:
        lines.append(f"简介：{desc if len(desc) <= 400 else desc[:400] + '…'}")
    if display_url:
        lines.append(f"原始链接：{display_url}")
    if not meta.get("video_urls") and not meta.get("image_urls") and not meta.get("audio_urls"):
        lines.append(media_fail_hint("未提取到可发送媒体", platform=str(platform)))
        lines.append("可尝试打开原始链接")
    if len(lines) == 1:
        lines.append("提示：无标题信息")
    return "\n".join(lines)


def collect_media_urls(meta: dict[str, Any]) -> tuple[list[str], list[str]]:
    images: list[str] = []
    videos: list[str] = []
    seen_images: set[str] = set()
    seen_videos: set[str] = set()

    def add(field: str, result: list[str], seen: set[str]) -> None:
        for item in meta.get(field) or []:
            values = item if isinstance(item, list) else [item]
            for value in values:
                if not isinstance(value, str) or not value.startswith("http"):
                    continue
                normalized = value.strip().rstrip("/")
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    result.append(value.strip())

    add("image_urls", images, seen_images)
    add("video_urls", videos, seen_videos)
    return images, videos


def dedupe_media_urls_preserve_order(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for url in urls:
        value = (url or "").strip()
        key = value.rstrip("/")
        if not key.startswith("http") or key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


class EntertainmentMediaParserApplication:
    """Owns link parsing, result aggregation and parser-specific transient state."""

    def __init__(
        self,
        provider: EntertainmentMediaParserProvider | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        blocking_runner: Callable[..., Awaitable[Any]] | None = None,
    ) -> None:
        self.provider = provider or EntertainmentMediaParserProvider()
        self._clock = clock
        self._blocking_runner = blocking_runner
        self._lock = threading.Lock()
        self._recent_events: OrderedDict[str, float] = OrderedDict()

    def bind_blocking_runner(self, runner: Callable[..., Awaitable[Any]]) -> None:
        self._blocking_runner = runner

    def _extract_supported_links(self, text: str) -> list[tuple[str, str]]:
        from ...xiuxian.xiuxian_entertainment.media_parser.native import (
            extract_supported_links,
        )

        return extract_supported_links(text or "")

    def extract_links(self, text: str) -> list[tuple[str, str]]:
        if not text or len(text) > MEDIA_PARSE_MAX_TEXT_CHARS:
            return []
        return self._extract_supported_links(text)[:MEDIA_PARSE_MAX_LINKS]

    def has_supported_link(self, text: str) -> bool:
        return bool(self.extract_links(text))

    def should_skip_duplicate(self, key: str) -> bool:
        now = self._clock()
        with self._lock:
            while self._recent_events:
                _, seen_at = next(iter(self._recent_events.items()))
                if now - seen_at <= MEDIA_PARSE_DEDUPE_SECONDS:
                    break
                self._recent_events.popitem(last=False)
            if key in self._recent_events:
                return True
            self._recent_events[key] = now
            while len(self._recent_events) > MEDIA_PARSE_DEDUPE_MAX_EVENTS:
                self._recent_events.popitem(last=False)
            return False

    def _parse_sync(self, text: str) -> list[dict[str, Any]]:
        from ...xiuxian.xiuxian_entertainment.media_parser.native import (
            parse_text_native,
        )

        with self.provider.operation():
            return parse_text_native(
                text,
                http_provider=self.provider,
                max_links=MEDIA_PARSE_MAX_LINKS,
            )

    async def parse_metas(self, text: str) -> list[dict[str, Any]]:
        if len(text) > MEDIA_PARSE_MAX_TEXT_CHARS:
            raise ValueError("媒体解析消息内容超过长度上限")
        if self._blocking_runner is None:
            raise RuntimeError("媒体解析 I/O runner 尚未绑定")
        return await self._blocking_runner(self._parse_sync, text, timeout=35)

    def _render_card_sync(self, meta: dict[str, Any]) -> str | None:
        from ...xiuxian.xiuxian_entertainment.media_parser.card import (
            render_media_card,
        )
        from ...xiuxian.xiuxian_entertainment.media_parser.native import _use_proxy_for

        images, videos = collect_media_urls(meta)
        platform = str(meta.get("platform") or "")
        with self.provider.operation():
            path = render_media_card(
                meta,
                cover_url=images[0] if images else None,
                has_video=bool(videos),
                use_proxy=_use_proxy_for(platform),
                http_provider=self.provider,
            )
        return str(path) if path else None

    def _prepare_media_urls(
        self, images: list[str], videos: list[str]
    ) -> tuple[list[str], list[str]]:
        from ...xiuxian.xiuxian_entertainment.media_parser.native import (
            dedupe_media_urls_by_object,
            sort_media_urls_by_quality,
        )

        videos = sort_media_urls_by_quality(
            dedupe_media_urls_preserve_order(videos), kind="video"
        )[:5]
        images = [
            url
            for url in dedupe_media_urls_preserve_order(images)
            if "emotion" not in url.lower() and "emoji" not in url.lower()
        ]
        images = dedupe_media_urls_by_object(images, kind="image")
        return sort_media_urls_by_quality(images, kind="image")[:18], videos

    def probe_media_size(self, url: str) -> int | None:
        from .media_parser_outputs import probe_media_size

        return probe_media_size(self.provider, url)

    def download_video_local(
        self,
        url: str,
        referer: str = "https://www.bilibili.com",
        max_bytes: int = 20 * 1024 * 1024,
    ):
        from .media_parser_outputs import download_video_local

        return download_video_local(self.provider, url, referer, max_bytes)

    async def parse_and_build_messages(
        self, text: str
    ) -> tuple[list[str], list[str], list[str], list[str]]:
        if not text or not text.strip():
            return (["【媒体解析】\n请在消息中附带可解析的链接。"], [], [], [])
        if len(text) > MEDIA_PARSE_MAX_TEXT_CHARS:
            return (["【媒体解析】\n消息内容过长，请只发送需要解析的链接。"], [], [], [])

        try:
            metas = await self.parse_metas(text)
        except Exception as exc:
            return ([f"【媒体解析】\n状态：不可用\n原因：{exc}"], [], [], [])
        if not metas:
            return (["【媒体解析】\n未识别到支持的流媒体链接。"], [], [], [])

        texts: list[str] = []
        all_images: list[str] = []
        all_videos: list[str] = []
        card_path: str | None = None
        seen_meta_keys: set[str] = set()
        for meta in metas[:MEDIA_PARSE_MAX_LINKS]:
            if not isinstance(meta, dict):
                continue
            meta_key = str(
                meta.get("url") or meta.get("source_url") or meta.get("link") or ""
            ).strip().rstrip("/")
            if meta_key and meta_key in seen_meta_keys:
                continue
            if meta_key:
                seen_meta_keys.add(meta_key)
            texts.append(format_media_meta_line(meta))
            images, videos = collect_media_urls(meta)
            all_images.extend(images)
            all_videos.extend(videos)
            if (
                card_path is None
                and not meta.get("error")
                and (images or videos or meta.get("title"))
            ):
                if self._blocking_runner is None:
                    card_path = None
                else:
                    try:
                        card_path = await self._blocking_runner(
                            self._render_card_sync, meta, timeout=20
                        )
                    except Exception:
                        card_path = None

        images, videos = self._prepare_media_urls(all_images, all_videos)
        return texts, images, videos, ([card_path] if card_path else [])

    def cleanup_cache(self, **options: Any) -> dict[str, Any]:
        from .media_parser_cache import cleanup_media_parser_cache

        return cleanup_media_parser_cache(**options)


__all__ = [
    "EntertainmentMediaParserApplication",
    "MEDIA_PARSE_DEDUPE_MAX_EVENTS",
    "MEDIA_PARSE_DEDUPE_SECONDS",
    "MEDIA_PARSE_MAX_LINKS",
    "MEDIA_PARSE_MAX_TEXT_CHARS",
    "collect_media_urls",
    "dedupe_media_urls_preserve_order",
    "format_media_meta_line",
]
