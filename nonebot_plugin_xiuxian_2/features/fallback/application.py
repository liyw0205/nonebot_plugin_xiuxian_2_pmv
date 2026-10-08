from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from nonebot.log import logger

from ...xiuxian.adapter_compat import GroupMessageEvent, PrivateMessageEvent
from ...xiuxian.xiuxian_config import XiuConfig
from ...xiuxian.xiuxian_utils.http_proxy import http_client
from ...xiuxian.xiuxian_utils.utils import handle_pic_msg_send, handle_send


def get_random_acg_pic_url(timeout: int = 5) -> str | None:
    """Fetch a random image URL for the configured fallback reply."""
    api_url = "https://v2.xxapi.cn/api/randomAcgPic"
    params = {"type": "pc", "return": "json"}

    try:
        data = http_client.get_json(api_url, params=params, timeout=timeout)
        if str(data.get("code")) != "200":
            logger.warning(
                f"默认回复图片接口请求失败: code={data.get('code')} msg={data.get('msg')}"
            )
            return None

        image_url = data.get("data")
        if image_url and isinstance(image_url, str):
            return image_url.strip()

        logger.warning("默认回复图片接口未返回有效图片地址")
        return None
    except Exception as exc:
        logger.warning(f"获取默认回复随机图片失败: {exc}")
        return None


async def get_random_acg_pic_url_async(timeout: int = 3) -> str | None:
    return await asyncio.to_thread(get_random_acg_pic_url, timeout)


class EmptyFallbackApplication:
    def __init__(
        self,
        *,
        config_provider: Callable[[], Any] = XiuConfig,
        full_message_group_provider: Callable[[str], bool] | None = None,
        event_kind_provider: Callable[[Any], str | None] | None = None,
        image_url_provider: Callable[..., Awaitable[str | None]] = get_random_acg_pic_url_async,
        image_sender: Callable[..., Awaitable[Any]] = handle_pic_msg_send,
        text_sender: Callable[..., Awaitable[Any]] = handle_send,
        log: Any = logger,
    ) -> None:
        self._config_provider = config_provider
        self._full_message_group_provider = (
            full_message_group_provider or self._is_full_message_group
        )
        self._event_kind_provider = event_kind_provider or self._event_kind
        self._image_url_provider = image_url_provider
        self._image_sender = image_sender
        self._text_sender = text_sender
        self._log = log

    @staticmethod
    def _event_kind(event: Any) -> str | None:
        if isinstance(event, PrivateMessageEvent):
            return "private"
        if isinstance(event, GroupMessageEvent):
            return "group"
        return None

    @staticmethod
    def _is_full_message_group(group_id: str) -> bool:
        from ...xiuxian.xiuxian_config import JsonConfig

        return JsonConfig().is_full_message_group(group_id)

    def should_respond(self, event: Any, text: str = "") -> bool:
        del text
        config = self._config_provider()
        if not config.empty_fallback or not config.empty_msg:
            return False

        event_kind = self._event_kind_provider(event)
        if event_kind is None:
            return False
        if event_kind == "private":
            return True

        event_name = " ".join(
            str(value)
            for value in (
                getattr(event, "__type__", None),
                getattr(event, "type", None),
            )
            if value is not None
        )
        try:
            event_name = f"{event_name} {event.get_event_name()}"
        except Exception:
            pass
        event_name = event_name.upper()

        to_me = bool(getattr(event, "to_me", False))
        if "GROUP_MESSAGE_CREATE" in event_name:
            return to_me

        try:
            group_id = str(getattr(event, "group_id", "") or "").strip()
            if group_id and self._full_message_group_provider(group_id):
                return to_me
        except Exception:
            pass

        return True

    async def respond(self, bot: Any, event: Any) -> None:
        config = self._config_provider()
        text_msg = config.empty_msg
        image_url = None
        if config.empty_fallback_image:
            image_url = await self._image_url_provider(timeout=3)

        if image_url:
            try:
                await self._image_sender(bot, event, image_url, text_msg)
            except Exception as exc:
                self._log.warning(f"默认回复图文发送失败，准备降级纯文字: {exc}")

        try:
            await self._text_sender(bot, event, text_msg)
        except Exception as exc:
            self._log.warning(f"默认回复纯文字发送失败: {exc}")


empty_fallback_application = EmptyFallbackApplication()


__all__ = [
    "EmptyFallbackApplication",
    "empty_fallback_application",
    "get_random_acg_pic_url",
    "get_random_acg_pic_url_async",
]
