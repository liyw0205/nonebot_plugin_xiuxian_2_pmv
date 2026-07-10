from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any


def _adapter_name(bot: Any) -> str:
    try:
        return str(bot.adapter.get_name() or "")
    except (AttributeError, TypeError):
        return ""


def _adapter_matches(actual: str, requested: str) -> bool:
    actual_lower = actual.strip().lower()
    requested_lower = requested.strip().lower()
    if not requested_lower:
        return True
    if actual_lower == requested_lower:
        return True
    if requested_lower in {"ob11", "onebot", "onebot v11"}:
        return "onebot" in actual_lower or "v11" in actual_lower
    return requested_lower == "qq" and actual_lower == "qq"


class BotSelector:
    """按 AppID、Adapter 和显式优先级稳定选择在线 Bot。"""

    def __init__(
        self,
        bot_provider: Callable[[], Mapping[str, Any]] | None = None,
        *,
        preferred_ids: tuple[str, ...] = (),
    ) -> None:
        self._bot_provider = bot_provider or self._get_nonebot_bots
        self._preferred_ids = tuple(str(value) for value in preferred_ids if value)

    @staticmethod
    def _get_nonebot_bots() -> Mapping[str, Any]:
        from nonebot import get_bots

        return get_bots()

    def select(
        self,
        *,
        app_id: str | None = None,
        adapter: str | None = None,
    ) -> Any | None:
        bots = self._bot_provider()
        if app_id:
            direct = bots.get(str(app_id))
            if direct is not None and _adapter_matches(_adapter_name(direct), adapter or ""):
                return direct

        candidates = [
            (str(key), bot)
            for key, bot in bots.items()
            if _adapter_matches(_adapter_name(bot), adapter or "")
        ]
        if not candidates:
            return None
        priority = {bot_id: index for index, bot_id in enumerate(self._preferred_ids)}
        candidates.sort(key=lambda item: (priority.get(item[0], len(priority)), item[0]))
        return candidates[0][1]


bot_selector = BotSelector()


__all__ = ["BotSelector", "bot_selector"]
