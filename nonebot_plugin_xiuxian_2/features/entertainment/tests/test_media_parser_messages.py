from __future__ import annotations

import asyncio
import unittest
from contextlib import contextmanager
from pathlib import Path

from ..media_parser_application import EntertainmentMediaParserApplication
from ..media_parser_messages import (
    EntertainmentMediaParserMessageApplication,
    MediaParserDelivery,
)


class _Config:
    auto_parse = True

    def should_parse_message(self, text: str) -> bool:
        return bool(text)


class _Parser:
    def __init__(self, result=None):
        self.result = result or (["parsed"], [], [], [])
        self.parsed = []
        self.deduped = []

    def has_supported_link(self, text: str) -> bool:
        return any(
            host in text
            for host in ("supported.invalid", "b23.tv", "v.kuaishou.com")
        )

    def should_skip_duplicate(self, key: str) -> bool:
        self.deduped.append(key)
        return key in self.deduped[:-1]

    async def parse_and_build_messages(self, text: str):
        self.parsed.append(text)
        return self.result


class _Event:
    def __init__(self, text: str, message_id: int = 1):
        self.text = text
        self.message_id = message_id
        self.time = 123

    def get_plaintext(self) -> str:
        return self.text

    def get_user_id(self) -> str:
        return "user-1"


class _FakeBot:
    pass


class _FakeProvider:
    @contextmanager
    def operation(self):
        yield self


async def _inline_runner(func, *args, timeout):
    return func(*args)


class _Delivery:
    def __init__(self):
        self.calls = []
        self.media_errors = set()
        self.media_error_message = "send failed"
        self.card_error = None
        self.markdown_result = True
        self.media_size = 1024
        self.media_sizes = {}
        self.download_error = None

    async def send_media(self, bot, event, media, *, media_type):
        self.calls.append(("media", media, media_type))
        if media_type in self.media_errors:
            raise RuntimeError(self.media_error_message)

    async def send_text(self, bot, event, text, **kwargs):
        self.calls.append(("text", text))
        return True

    async def send_card_with_text(self, bot, event, path, text):
        self.calls.append(("card", str(path), text))
        if self.card_error:
            raise RuntimeError(self.card_error)

    async def send_markdown(self, bot, event, text, **kwargs):
        self.calls.append(("markdown", text))
        return self.markdown_result

    async def probe_media_size(self, url):
        self.calls.append(("size", url))
        return self.media_sizes.get(url, self.media_size)

    async def download_video(self, url, max_bytes):
        self.calls.append(("download", url, max_bytes))
        if self.download_error:
            raise RuntimeError(self.download_error)
        return "/tmp/video.mp4"

    async def probe_image_size(self, url):
        self.calls.append(("image-size", url))
        return (640, 480)

    @staticmethod
    def video_segment(bot, url):
        return f"video:{url}"

    @staticmethod
    def markdown_enabled():
        return False

    @staticmethod
    def max_media_bytes():
        return 1024 * 1024

    def build(self):
        return MediaParserDelivery(
            send_media=self.send_media,
            send_text=self.send_text,
            send_card_with_text=self.send_card_with_text,
            send_markdown=self.send_markdown,
            probe_media_size=self.probe_media_size,
            download_video=self.download_video,
            probe_image_size=self.probe_image_size,
            video_segment=self.video_segment,
            markdown_enabled=self.markdown_enabled,
            max_media_bytes=self.max_media_bytes,
        )


class MediaParserMessageApplicationTests(unittest.TestCase):
    def test_qualification_preserves_embedded_and_fallback_filters(self):
        config = _Config()
        parser = _Parser()
        app = EntertainmentMediaParserMessageApplication(
            parser, config_provider=lambda: config
        )

        self.assertTrue(
            app.should_parse_embedded_share(
                "分享 https://v.kuaishou.com/abc 说明"
            )
        )
        self.assertFalse(app.should_parse_embedded_share("链接解析 https://b23.tv/abc"))
        self.assertFalse(app.should_parse_embedded_share("原始链接：https://b23.tv/abc"))
        self.assertTrue(app.should_parse_any_http("访问 https://supported.invalid/a"))
        self.assertFalse(app.should_parse_any_http("分享 https://b23.tv/abc"))
        self.assertFalse(app.should_parse_any_http("https://example.invalid/a"))

        config.auto_parse = False
        self.assertFalse(app.should_parse_any_http("https://supported.invalid/a"))

    def test_duplicate_event_is_suppressed_before_parse_or_send(self):
        parser = _Parser()
        delivery = _Delivery()
        app = EntertainmentMediaParserMessageApplication(
            parser,
            delivery=delivery.build(),
            config_provider=lambda: _Config(),
        )
        event = _Event("https://supported.invalid/a")

        asyncio.run(app.send_parse_result(object(), event, event.text))
        asyncio.run(app.send_parse_result(object(), event, event.text))

        self.assertEqual(parser.parsed, [event.text])
        self.assertEqual([call[0] for call in delivery.calls], ["text"])

    def test_send_orchestration_uses_injected_delivery_without_network(self):
        parser = _Parser(
            (["video info"], [], ["https://video.invalid/a.mp4"], [])
        )
        delivery = _Delivery()
        app = EntertainmentMediaParserMessageApplication(
            parser, delivery=delivery.build()
        )

        asyncio.run(
            app.send_parse_result(
                object(), _Event("https://supported.invalid/a"), "input"
            )
        )

        self.assertEqual(
            [call[0] for call in delivery.calls], ["text", "size", "media"]
        )
        self.assertEqual(delivery.calls[-1][1], "video:https://video.invalid/a.mp4")

    def test_existing_parser_application_runs_with_fake_provider_and_bot(self):
        parser = EntertainmentMediaParserApplication(
            _FakeProvider(), blocking_runner=_inline_runner
        )
        async def parse_result(text):
            return (["fixture parser result"], [], [], [])

        parser.parse_and_build_messages = parse_result
        delivery = _Delivery()
        app = EntertainmentMediaParserMessageApplication(
            parser, delivery=delivery.build()
        )

        asyncio.run(
            app.handle_command_url(
                _FakeBot(),
                _Event("链接解析 https://supported.invalid/item"),
            )
        )

        self.assertEqual(len(parser._recent_events), 1)
        self.assertEqual([call[0] for call in delivery.calls], ["text"])
        self.assertIn("fixture parser result", delivery.calls[0][1])

    def test_image_gallery_keeps_first_image_and_builds_markdown_when_enabled(self):
        parser = _Parser(
            (["image info"], ["https://image.invalid/1.jpg", "https://image.invalid/2.jpg"], [], [])
        )
        delivery = _Delivery()
        base = delivery.build()
        base.markdown_enabled = lambda: True
        app = EntertainmentMediaParserMessageApplication(parser, delivery=base)

        asyncio.run(
            app.send_parse_result(
                object(), _Event("https://supported.invalid/a"), "input"
            )
        )

        markdown = next(call[1] for call in delivery.calls if call[0] == "markdown")
        self.assertIn("https://image.invalid/1.jpg", markdown)
        self.assertIn("https://image.invalid/2.jpg", markdown)

    def test_gallery_markdown_failure_falls_back_to_each_original_image(self):
        first = "https://image.invalid/1.jpg"
        second = "https://image.invalid/2.jpg"
        parser = _Parser(([], [first, second], [], []))
        delivery = _Delivery()
        base = delivery.build()
        base.markdown_enabled = lambda: True
        delivery.markdown_result = False
        app = EntertainmentMediaParserMessageApplication(parser, delivery=base)

        asyncio.run(
            app.send_parse_result(object(), _Event("input"), "input")
        )

        sent_images = [call[1] for call in delivery.calls if call[0] == "media"]
        self.assertEqual(sent_images, [first, second])

    def test_gallery_fallback_preserves_local_path_images(self):
        local_image = Path("/tmp/gallery.jpg")
        parser = _Parser(([], [local_image], [], []))
        delivery = _Delivery()
        app = EntertainmentMediaParserMessageApplication(
            parser, delivery=delivery.build()
        )

        asyncio.run(
            app.send_parse_result(object(), _Event("input"), "input")
        )

        media = [call for call in delivery.calls if call[0] == "media"]
        self.assertEqual(media, [("media", local_image, "图片")])

    def test_bilibili_video_prefers_local_download_before_url_segment(self):
        url = "https://cdn.bilivideo.com/video.mp4"
        parser = _Parser(([], [], [url], []))
        delivery = _Delivery()
        app = EntertainmentMediaParserMessageApplication(
            parser, delivery=delivery.build()
        )

        asyncio.run(
            app.send_parse_result(object(), _Event("input"), "input")
        )

        self.assertIn(("download", url, 1024 * 1024), delivery.calls)
        media = [call for call in delivery.calls if call[0] == "media"]
        self.assertEqual(len(media), 1)
        self.assertEqual(media[0][1], Path("/tmp/video.mp4"))

    def test_video_failure_sends_fallback_tip(self):
        url = "https://video.invalid/video.mp4"
        parser = _Parser(([], [], [url], []))
        delivery = _Delivery()
        delivery.media_errors.add("视频")
        app = EntertainmentMediaParserMessageApplication(
            parser, delivery=delivery.build()
        )

        asyncio.run(
            app.send_parse_result(object(), _Event("input"), "input")
        )

        self.assertTrue(
            any(
                call[0] == "text" and "视频发送失败" in call[1]
                for call in delivery.calls
            )
        )

    def test_video_size_limit_skips_highest_candidate_and_sends_next(self):
        oversized = "https://cdn.bilivideo.com/large.mp4"
        fallback = "https://video.invalid/smaller.mp4"
        parser = _Parser(([], [], [oversized, fallback], []))
        delivery = _Delivery()
        delivery.media_sizes[oversized] = 2 * 1024 * 1024
        app = EntertainmentMediaParserMessageApplication(
            parser, delivery=delivery.build()
        )

        asyncio.run(
            app.send_parse_result(object(), _Event("input"), "input")
        )

        self.assertNotIn(("download", oversized, 1024 * 1024), delivery.calls)
        self.assertEqual(
            [call[1] for call in delivery.calls if call[0] == "size"],
            [oversized, fallback],
        )
        self.assertEqual(
            [call[1] for call in delivery.calls if call[0] == "media"],
            ["video:https://video.invalid/smaller.mp4"],
        )

    def test_bilibili_download_failure_falls_back_to_direct_video(self):
        url = "https://cdn.bilivideo.com/video.mp4"
        parser = _Parser(([], [], [url], []))
        delivery = _Delivery()
        delivery.download_error = "download failed"
        app = EntertainmentMediaParserMessageApplication(
            parser, delivery=delivery.build()
        )

        asyncio.run(
            app.send_parse_result(object(), _Event("input"), "input")
        )

        self.assertIn(("download", url, 1024 * 1024), delivery.calls)
        self.assertIn(("media", f"video:{url}", "视频"), delivery.calls)

    def test_passive_reply_limit_stops_video_fallback_attempts(self):
        url = "https://video.invalid/video.mp4"
        parser = _Parser(([], [], [url], []))
        delivery = _Delivery()
        delivery.media_errors.add("视频")
        delivery.media_error_message = "ActionFailed: 40034128 passive limit"
        app = EntertainmentMediaParserMessageApplication(
            parser, delivery=delivery.build()
        )

        asyncio.run(
            app.send_parse_result(object(), _Event("input"), "input")
        )

        self.assertEqual(len([call for call in delivery.calls if call[0] == "media"]), 1)
        self.assertFalse(any(call[0] == "text" for call in delivery.calls))

    def test_card_and_text_failure_falls_back_to_card_image(self):
        parser = _Parser((["video info"], [], [], ["/tmp/card.png"]))
        delivery = _Delivery()
        delivery.card_error = "image+text unavailable"
        app = EntertainmentMediaParserMessageApplication(
            parser, delivery=delivery.build()
        )

        asyncio.run(
            app.send_parse_result(object(), _Event("input"), "input")
        )

        self.assertIn(
            ("media", "/tmp/card.png", "图片"), delivery.calls
        )


if __name__ == "__main__":
    unittest.main()
