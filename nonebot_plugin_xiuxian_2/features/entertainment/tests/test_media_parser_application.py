from __future__ import annotations

import asyncio
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from ..media_parser_application import (
    MEDIA_PARSE_DEDUPE_MAX_EVENTS,
    EntertainmentMediaParserApplication,
)


class _Provider:
    @contextmanager
    def operation(self):
        yield self


async def _inline_runner(func, *args, timeout):
    return func(*args)


class MediaParserApplicationTests(unittest.TestCase):
    def test_parsing_aggregates_bounded_links_and_only_renders_sent_card(self):
        app = EntertainmentMediaParserApplication(
            _Provider(), blocking_runner=_inline_runner  # type: ignore[arg-type]
        )
        metas = [
            {
                "platform": "bilibili",
                "url": f"https://www.bilibili.com/video/BV{index}",
                "source_url": f"https://b23.tv/{index}",
                "title": f"title {index}",
                "image_urls": [f"https://img.invalid/{index}.jpg"],
                "video_urls": [f"https://video.invalid/{index}.mp4"],
            }
            for index in range(4)
        ]
        with patch.object(app, "_parse_sync", return_value=metas) as parse, patch(
            "nonebot_plugin_xiuxian_2.features.entertainment.media_parser_application.format_media_meta_line",
            side_effect=lambda meta: meta["title"],
        ), patch.object(
            app, "_render_card_sync", side_effect=lambda meta: f"{meta['title']}.png"
        ) as render, patch.object(
            app,
            "_prepare_media_urls",
            side_effect=lambda images, videos: (images[:18], videos[:5]),
        ):
            texts, images, videos, cards = asyncio.run(
                app.parse_and_build_messages("https://b23.tv/1")
            )

        self.assertEqual(len(texts), 3)
        self.assertEqual(len(images), 3)
        self.assertEqual(len(videos), 3)
        self.assertEqual(cards, ["title 0.png"])
        parse.assert_called_once()
        render.assert_called_once()

    def test_event_dedupe_expires_and_caps_state(self):
        now = [100.0]
        app = EntertainmentMediaParserApplication(
            _Provider(), clock=lambda: now[0]  # type: ignore[arg-type]
        )

        self.assertFalse(app.should_skip_duplicate("mid:1"))
        self.assertTrue(app.should_skip_duplicate("mid:1"))
        now[0] += 91
        self.assertFalse(app.should_skip_duplicate("mid:1"))

        for index in range(MEDIA_PARSE_DEDUPE_MAX_EVENTS + 10):
            app.should_skip_duplicate(f"mid:{index}")
        self.assertLessEqual(len(app._recent_events), MEDIA_PARSE_DEDUPE_MAX_EVENTS)

    def test_link_detection_is_local_and_uses_the_supported_platform_map(self):
        app = EntertainmentMediaParserApplication(_Provider())  # type: ignore[arg-type]

        app._extract_supported_links = lambda text: [("https://b23.tv/abc", "bilibili")] if "b23.tv" in text else []
        self.assertTrue(app.has_supported_link("说明 https://b23.tv/abc 其它文字"))
        self.assertFalse(app.has_supported_link("https://example.invalid/page"))
        self.assertFalse(app.has_supported_link("x" * 8193 + " https://b23.tv/abc"))


if __name__ == "__main__":
    unittest.main()
