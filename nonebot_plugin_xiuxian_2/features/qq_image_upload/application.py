from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any


class QqImageUploadApplication:
    def __init__(
        self,
        upload_image_and_get_url: Callable[..., Awaitable[str | None]],
    ) -> None:
        self._upload_image_and_get_url = upload_image_and_get_url

    @staticmethod
    def select_qq_bot(bots: Mapping[Any, Any]) -> Any | None:
        for bot in bots.values():
            if bot.adapter.get_name() == "QQ":
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
            mode="md5",
        )


__all__ = ["QqImageUploadApplication"]
