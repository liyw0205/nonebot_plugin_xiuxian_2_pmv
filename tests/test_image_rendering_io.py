from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


class ImageRenderingIoTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_msg_pic_offloads_base64_rendering(self) -> None:
        image = MagicMock()
        image.sync_draw_to.return_value = "base64://result"

        with (
            patch.object(utils, "Txt2Img", return_value=image),
            patch.object(utils.XiuConfig, "__new__", return_value=MagicMock(img_send_type="base64")),
            patch.object(asyncio, "to_thread", new=AsyncMock(return_value="base64://result")) as to_thread,
        ):
            result = await utils.get_msg_pic("长文本", "boss", False)

        self.assertEqual(result, "base64://result")
        to_thread.assert_awaited_once_with(image.sync_draw_to, "长文本", "boss", False)

    async def test_get_msg_pic_keeps_io_renderer_path(self) -> None:
        image = MagicMock()
        image.io_draw_to = AsyncMock(return_value="buffer")

        with (
            patch.object(utils, "Txt2Img", return_value=image),
            patch.object(utils.XiuConfig, "__new__", return_value=MagicMock(img_send_type="io")),
        ):
            result = await utils.get_msg_pic("长文本")

        self.assertEqual(result, "buffer")
        image.io_draw_to.assert_awaited_once_with("长文本", "", True)


if __name__ == "__main__":
    unittest.main()
