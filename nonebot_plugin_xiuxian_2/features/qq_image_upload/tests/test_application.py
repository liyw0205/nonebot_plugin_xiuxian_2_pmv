from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock

from ..application import QqImageUploadApplication


class QqImageUploadApplicationTests(unittest.TestCase):
    def test_selects_first_qq_bot_and_returns_none_when_unavailable(self) -> None:
        class Adapter:
            def __init__(self, name: str) -> None:
                self.name = name

            def get_name(self) -> str:
                return self.name

        first_qq = type("Bot", (), {"adapter": Adapter("QQ")})()
        bots = {
            "other": type("Bot", (), {"adapter": Adapter("Other")})(),
            "qq-1": first_qq,
            "qq-2": type("Bot", (), {"adapter": Adapter("QQ")})(),
        }
        application = QqImageUploadApplication(AsyncMock())

        self.assertIs(application.select_qq_bot(bots), first_qq)
        self.assertIsNone(application.select_qq_bot({"other": bots["other"]}))

    def test_upload_delegates_bytes_channel_and_md5_mode(self) -> None:
        upload = AsyncMock(return_value="https://image.example.test/hash")
        application = QqImageUploadApplication(upload)
        bot = object()

        result = asyncio.run(
            application.upload_image(
                bot=bot,
                channel_id=123,
                image=b"image-bytes",
            )
        )

        self.assertEqual(result, "https://image.example.test/hash")
        upload.assert_awaited_once_with(
            bot=bot,
            channel_id="123",
            image=b"image-bytes",
            mode="md5",
        )

    def test_adapter_exception_propagates_to_route_boundary(self) -> None:
        upload = AsyncMock(side_effect=RuntimeError("adapter unavailable"))
        application = QqImageUploadApplication(upload)

        with self.assertRaisesRegex(RuntimeError, "adapter unavailable"):
            asyncio.run(
                application.upload_image(
                    bot=object(),
                    channel_id="channel-1",
                    image=b"image-bytes",
                )
            )


if __name__ == "__main__":
    unittest.main()
