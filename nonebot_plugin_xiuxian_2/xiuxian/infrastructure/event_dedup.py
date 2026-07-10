from __future__ import annotations

from typing import Any

from ..qq_compat import from_nonebot_event, is_qq_event
from .ttl_store import TTLStore
from .metrics import RuntimeMetrics, runtime_metrics


class QQEventDeduplicator:
    def __init__(
        self,
        *,
        ttl: float = 300,
        max_size: int = 5000,
        metrics: RuntimeMetrics | None = None,
    ) -> None:
        self._seen: TTLStore[str, bool] = TTLStore(ttl=ttl, max_size=max_size)
        self._metrics = metrics or runtime_metrics

    @staticmethod
    def build_key(bot: Any, event: Any) -> str | None:
        if not is_qq_event(event):
            return None
        context = from_nonebot_event(event)
        stable_id = context.event_id or context.message_id
        if not stable_id:
            return None
        bot_id = str(getattr(bot, "self_id", ""))
        return f"{bot_id}:{context.event_type}:{stable_id}"

    async def is_duplicate(self, bot: Any, event: Any) -> bool:
        key = self.build_key(bot, event)
        if key is None:
            self._metrics.increment("dedup.skipped_no_stable_id")
            return False
        self._metrics.increment("dedup.checked")
        duplicate = not await self._seen.add_if_absent(key, True)
        if duplicate:
            self._metrics.increment("dedup.hit")
        return duplicate


__all__ = ["QQEventDeduplicator"]
