from __future__ import annotations

import io
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.messaging import (
    MediaInput,
    MediaResolver,
    ResolvedMedia,
)


class MediaResolverTests(unittest.IsolatedAsyncioTestCase):
    async def test_bytes_path_and_stream_share_content_cache(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.png"
            source.write_bytes(b"image-data")
            resolver = MediaResolver(root / "cache", max_bytes=100)

            first = await resolver.resolve(MediaInput(b"image-data", "image", "a.png"))
            second = await resolver.resolve(MediaInput(source, "image"))
            third = await resolver.resolve(MediaInput(io.BytesIO(b"image-data"), "image"))

            self.assertFalse(first.from_cache)
            self.assertTrue(second.from_cache)
            self.assertTrue(third.from_cache)
            self.assertEqual(first.cache_key, second.cache_key)

    async def test_media_size_limit_is_enforced_for_all_local_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            resolver = MediaResolver(Path(directory) / "cache", max_bytes=3)
            with self.assertRaises(ValueError):
                await resolver.resolve(MediaInput(b"1234", "file"))
            path = Path(directory) / "large.bin"
            path.write_bytes(b"1234")
            with self.assertRaises(ValueError):
                await resolver.resolve(MediaInput(path, "file"))

    async def test_private_url_is_rejected_before_adapter_segment_build(self) -> None:
        resolver = MediaResolver(allow_private_urls=False)
        with patch("socket.getaddrinfo", return_value=[(0, 0, 0, "", ("127.0.0.1", 80))]):
            with self.assertRaisesRegex(ValueError, "非公网地址"):
                await resolver.build_segment(
                    SimpleNamespace(),
                    MediaInput("http://example.invalid/image.png", "image"),
                )

    async def test_adapter_media_builder_is_reused(self) -> None:
        resolver = MediaResolver(allow_private_urls=True)
        bot = object()
        resolved = ResolvedMedia(
            media_type="image",
            content=b"image-data",
            filename="image.png",
            content_type="image/png",
            cache_key="key",
        )
        with (
            patch.object(resolver, "resolve", new=AsyncMock(return_value=resolved)),
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.media.MessageSegment.image",
                return_value="segment",
            ) as image,
        ):
            result = await resolver.build_segment(
                bot,
                MediaInput("https://example.invalid/image.png", "image"),
            )
        self.assertEqual(result, "segment")
        image.assert_called_once_with(bot, b"image-data")

    async def test_transient_download_failure_is_retried(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            resolver = MediaResolver(
                Path(directory) / "cache",
                allow_private_urls=True,
                download_retries=1,
            )
            with patch.object(
                resolver,
                "_download_once",
                new=AsyncMock(
                    side_effect=[
                        httpx.ConnectError("temporary"),
                        (b"data", "media.bin", "application/octet-stream"),
                    ]
                ),
            ) as download:
                result = await resolver.resolve(
                    MediaInput("https://example.invalid/media.bin", "file")
                )
            self.assertEqual(result.content, b"data")
            self.assertEqual(download.await_count, 2)

    async def test_expired_cache_cleanup_removes_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            resolver = MediaResolver(Path(directory) / "cache", cache_ttl=1)
            resolved = await resolver.resolve(MediaInput(b"old", "file"))
            cache_file = resolver._cache_path(resolved.cache_key)
            old = time.time() - 5
            os.utime(cache_file, (old, old))
            self.assertEqual(await resolver.cleanup(), 1)
            self.assertFalse(cache_file.exists())


if __name__ == "__main__":
    unittest.main()
