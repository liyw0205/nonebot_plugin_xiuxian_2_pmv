from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .media_parser_application import EntertainmentMediaParserApplication

logger = logging.getLogger(__name__)

MEDIA_PARSE_COMMANDS = (
    "链接解析",
    "视频解析",
    "解析视频",
    "解析链接",
    "流媒体解析",
)
_URL_PATH = r"[^\s\u200b\u00a0<>\"'，。！？、；：（）【】《》]+"
_SHARE_HOSTS = (
    r"v\.douyin\.com|www\.iesdouyin\.com|b23\.tv|bili2233\.cn|"
    r"www\.bilibili\.com|m\.bilibili\.com|xhslink\.com|"
    r"www\.xiaohongshu\.com|v\.kuaishou\.com|www\.kuaishou\.com|"
    r"weibo\.com|weibo\.cn|t\.cn|www\.toutiao\.com|"
    r"www\.xiaoheihe\.cn|x\.com|twitter\.com|www\.instagram\.com|"
    r"www\.goofish.com"
)
EMBEDDED_SHARE_URL_RE = re.compile(
    rf"https?://(?:{_SHARE_HOSTS})/{_URL_PATH}", re.I
)
EMBEDDED_SHARE_MATCH_RE = re.compile(
    rf".*(https?://(?:{_SHARE_HOSTS})/{_URL_PATH}).*", re.I | re.S
)
COMMAND_WITH_URL_RE = re.compile(
    r"(?:链接解析|视频解析|解析视频|解析链接|流媒体解析)\s+"
    rf"(https?://(?:{_SHARE_HOSTS})/{_URL_PATH}|https?://\S+)",
    re.I,
)
ANY_HTTP_RE = re.compile(r"https?://\S+", re.I)


def plain_text_for_parse(event: Any) -> str:
    try:
        text = event.get_plaintext() or ""
        if text:
            return text
    except Exception:
        pass
    try:
        return event.get_message().extract_plain_text() or ""
    except Exception:
        return ""


@dataclass
class MediaParserDelivery:
    send_media: Callable[..., Any]
    send_text: Callable[..., Any]
    send_card_with_text: Callable[..., Any]
    send_markdown: Callable[..., Any]
    probe_media_size: Callable[..., Any]
    download_video: Callable[..., Any]
    probe_image_size: Callable[..., Any]
    video_segment: Callable[..., Any]
    markdown_enabled: Callable[[], bool]
    max_media_bytes: Callable[[], int]

    @classmethod
    def from_runtime(cls, parser: EntertainmentMediaParserApplication):
        # Resolve adapters after plugin registration to avoid an import cycle.
        from ...xiuxian.xiuxian_entertainment import command as runtime

        async def probe_media_size(url: str):
            return await runtime.run_blocking_io(
                parser.probe_media_size, url, timeout=20
            )

        async def download_video(url: str, max_bytes: int):
            return await runtime.run_blocking_io(
                parser.download_video_local,
                url,
                "https://www.bilibili.com",
                max_bytes,
                timeout=35,
            )

        async def probe_image_size(url: str):
            return await runtime.run_blocking_io(
                runtime._probe_image_size, url, timeout=15
            )

        return cls(
            send_media=runtime.send_entertainment_media,
            send_text=runtime.handle_send,
            send_card_with_text=runtime.handle_pic_msg_send,
            send_markdown=runtime.handle_send,
            probe_media_size=probe_media_size,
            download_video=download_video,
            probe_image_size=probe_image_size,
            video_segment=runtime.MessageSegment.video,
            markdown_enabled=lambda: bool(
                getattr(runtime.XiuConfig(), "markdown_status", False)
            ),
            max_media_bytes=lambda: int(runtime.MEDIA_MAX_BYTES),
        )


class EntertainmentMediaParserMessageApplication:
    """Owns media-message qualification, event dedupe and delivery orchestration."""

    def __init__(
        self,
        parser: EntertainmentMediaParserApplication,
        *,
        delivery: MediaParserDelivery | None = None,
        config_provider: Callable[[], Any] | None = None,
    ) -> None:
        self.parser = parser
        self._delivery = delivery
        self._config_provider = config_provider

    @property
    def delivery(self) -> MediaParserDelivery:
        if self._delivery is None:
            self._delivery = MediaParserDelivery.from_runtime(self.parser)
        return self._delivery

    def _config(self):
        if self._config_provider is not None:
            return self._config_provider()
        from ...xiuxian.xiuxian_entertainment.media_parser.config import (
            get_fun_media_parser_config,
        )

        return get_fun_media_parser_config()

    def has_embedded_share_url(self, text: str) -> bool:
        return bool(text and EMBEDDED_SHARE_URL_RE.search(text))

    def _starts_with_parse_command(self, text: str) -> bool:
        stripped = (text or "").lstrip()
        return any(stripped.startswith(command) for command in MEDIA_PARSE_COMMANDS)

    def should_parse_embedded_share(self, text: str) -> bool:
        if not self._config().should_parse_message(text):
            return False
        if self._starts_with_parse_command(text) or "原始链接：" in text:
            return False
        return self.has_embedded_share_url(text) and self.parser.has_supported_link(text)

    def should_parse_any_http(self, text: str) -> bool:
        if not self._config().auto_parse:
            return False
        if self.has_embedded_share_url(text) or self._starts_with_parse_command(text):
            return False
        if "原始链接：" in text or not ANY_HTTP_RE.search(text):
            return False
        return self.parser.has_supported_link(text)

    async def handle_command_url(self, bot: Any, event: Any) -> None:
        text = plain_text_for_parse(event)
        if text:
            await self.send_parse_result(bot, event, text)

    async def handle_embedded_share(self, bot: Any, event: Any) -> None:
        text = plain_text_for_parse(event)
        if self.should_parse_embedded_share(text):
            await self.send_parse_result(bot, event, text)

    async def handle_any_http(self, bot: Any, event: Any) -> None:
        text = plain_text_for_parse(event)
        if self.should_parse_any_http(text):
            await self.send_parse_result(bot, event, text)

    @staticmethod
    def _event_key(event: Any) -> str:
        message_id = getattr(event, "message_id", None)
        if message_id is not None:
            return f"mid:{message_id}"
        try:
            user_id = event.get_user_id()
        except Exception:
            user_id = "unknown"
        return f"uid:{user_id}:ts:{getattr(event, 'time', 0)}"

    async def send_parse_result(self, bot: Any, event: Any, source_text: str) -> None:
        if self.parser.should_skip_duplicate(self._event_key(event)):
            return

        texts, images, videos, cards = await self.parser.parse_and_build_messages(
            source_text
        )
        body = "\n\n".join(text for text in texts if text).strip()
        delivery = self.delivery
        max_bytes = delivery.max_media_bytes()

        def is_passive_limit(exc: BaseException) -> bool:
            message = str(exc)
            return (
                "40034128" in message
                or "被动回复时间或者次数超过限制" in message
                or ("被动回复" in message and "超过" in message)
            )

        async def safe_send_media(media: Any, media_type: str) -> bool:
            try:
                await delivery.send_media(
                    bot, event, media, media_type=media_type
                )
                return True
            except Exception as exc:
                if is_passive_limit(exc):
                    raise
                logger.warning("发送解析%s失败: %s", media_type, exc)
                return False

        async def send_card_and_text() -> None:
            if cards and body:
                try:
                    await delivery.send_card_with_text(bot, event, Path(cards[0]), body)
                    return
                except Exception as exc:
                    if is_passive_limit(exc):
                        raise
            if cards:
                await safe_send_media(cards[0], "图片")
            elif body:
                try:
                    await delivery.send_text(
                        bot,
                        event,
                        body,
                        md_type="娱乐",
                        k1="娱乐帮助",
                        v1="娱乐帮助",
                        k2="链接解析",
                        v2="链接解析",
                    )
                except Exception as exc:
                    if is_passive_limit(exc):
                        raise
                    logger.warning("发送解析文案失败: %s", exc)

        try:
            await send_card_and_text()
            if videos:
                last_error: Exception | None = None
                sent = False

                async def attempt_video(media: Any) -> bool:
                    nonlocal last_error
                    try:
                        await delivery.send_media(
                            bot, event, media, media_type="视频"
                        )
                        return True
                    except Exception as exc:
                        last_error = exc
                        if is_passive_limit(exc):
                            raise
                        logger.warning("发送解析视频失败: %s", exc)
                        return False

                for video in videos[:5]:
                    try:
                        size = await delivery.probe_media_size(video)
                    except Exception:
                        size = None
                    if isinstance(size, int) and size > max_bytes:
                        continue
                    if any(
                        token in video.lower()
                        for token in (
                            "bilivideo.com",
                            "hdslb.com",
                            "bilibili.com/bfs",
                            "upgcxcode",
                        )
                    ):
                        try:
                            local = await delivery.download_video(video, max_bytes)
                            if await attempt_video(Path(local)):
                                sent = True
                                break
                        except Exception as exc:
                            last_error = exc
                            if is_passive_limit(exc):
                                return
                            if "超过" in str(exc) and "MB" in str(exc):
                                continue
                    try:
                        if await attempt_video(delivery.video_segment(bot, video)):
                            sent = True
                            break
                    except Exception as exc:
                        if is_passive_limit(exc):
                            return
                if not sent:
                    tip = (
                        "【媒体解析】视频发送失败"
                        + (
                            f"：{last_error}"
                            if last_error
                            else "（可能均超过20MB或链路失败）"
                        )
                        + "\n可尝试打开原始链接观看。"
                    )
                    try:
                        await delivery.send_text(
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
                image
                for image in images
                if not any(
                    token in str(image or "").lower()
                    for token in ("emotion", "emoji", "uhead", "/bg")
                )
            ][:18]
            if not gallery:
                return

            gallery_http = [
                str(image or "").strip()
                for image in gallery
                if str(image or "").strip().lower().startswith(
                    ("http://", "https://")
                )
            ]
            if delivery.markdown_enabled() and gallery_http:
                async def image_line(url: str) -> str:
                    try:
                        width, height = await delivery.probe_image_size(url)
                    except Exception:
                        width, height = 720, 960
                    normalized = (
                        url.replace(" ", "%20")
                        .replace("(", "%28")
                        .replace(")", "%29")
                    )
                    return (
                        f"![img #{max(1, int(width))}px "
                        f"#{max(1, int(height))}px]({normalized})"
                    )

                semaphore = asyncio.Semaphore(4)

                async def bounded_image_line(url: str) -> str:
                    async with semaphore:
                        return await image_line(url)

                lines = await asyncio.gather(
                    *(bounded_image_line(url) for url in gallery_http)
                )
                caption = body.replace("\r", "\n")
                md_body = "\n".join(
                    ([caption, ""] if caption and not cards else []) + lines
                )
                try:
                    ok = await delivery.send_markdown(
                        bot,
                        event,
                        md_body,
                        native_markdown=True,
                        allow_plain_fallback=False,
                        fallback_msg=md_body,
                        k1="娱乐帮助",
                        v1="娱乐帮助",
                        k2="链接解析",
                        v2="链接解析",
                    )
                    if ok:
                        return
                except Exception as exc:
                    if is_passive_limit(exc):
                        return
            for image in gallery:
                try:
                    await safe_send_media(image, "图片")
                except Exception as exc:
                    if is_passive_limit(exc):
                        return
        except Exception as exc:
            if is_passive_limit(exc):
                return
            raise


__all__ = [
    "ANY_HTTP_RE",
    "COMMAND_WITH_URL_RE",
    "EMBEDDED_SHARE_MATCH_RE",
    "EntertainmentMediaParserMessageApplication",
    "MEDIA_PARSE_COMMANDS",
    "MediaParserDelivery",
    "plain_text_for_parse",
]
