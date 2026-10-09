from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .repository import UploadImageAndResolve
from .schemas import QQ_ADAPTER_NAME, UPLOAD_FILE_MODE


class QqImageUploadApplication:
    def __init__(self, upload_image_and_get_url: UploadImageAndResolve) -> None:
        self._upload_image_and_get_url = upload_image_and_get_url

    @staticmethod
    def select_qq_bot(bots: Mapping[Any, Any]) -> Any | None:
        for bot in bots.values():
            if bot.adapter.get_name() == QQ_ADAPTER_NAME:
                return bot
        return None

    async def upload_image(
        self,
        *,
        bot: Any,
        channel_id: str,
        image: bytes,
    ) -> str | None:
        return await self._upload_image_and_get_url(
            bot=bot,
            channel_id=str(channel_id),
            image=image,
            mode=UPLOAD_FILE_MODE,
        )


__all__ = ["QqImageUploadApplication"]
