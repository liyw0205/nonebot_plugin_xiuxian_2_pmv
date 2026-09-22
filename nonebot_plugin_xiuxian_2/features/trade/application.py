from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import LegacyTradeFeatureRepository, TradeFeatureRepository



class TradeApplication(LegacyApplication):
    def __init__(self, game_database: str | Path, trade_database: str | Path, *, repository: TradeFeatureRepository | None = None) -> None:
        super().__init__(game_database, repository=repository or LegacyTradeFeatureRepository(game_database, trade_database), feature="trade")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"trade.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def deposit(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("deposit", operation_id=operation_id, user_id=user_id, **kwargs)
    def withdraw(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("withdraw", operation_id=operation_id, user_id=user_id, **kwargs)
    def enqueue(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("enqueue", operation_id=operation_id, user_id=user_id, **kwargs)
    def dequeue(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("dequeue", operation_id=operation_id, user_id=user_id, **kwargs)
    def session_start(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("session_start", operation_id=operation_id, user_id=user_id, **kwargs)
    def session_finish(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("session_finish", operation_id=operation_id, user_id=user_id, **kwargs)
    def purchase(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self.repository is not None:
            return self._action("purchase", operation_id=operation_id, user_id=user_id, **kwargs)
        max_goods_num = int(kwargs.pop("max_goods_num", 1) or 1)
        listing_id = kwargs.pop("listing_id")
        quantity = kwargs.pop("quantity")
        from ...xiuxian.xiuxian_trade.repository import TradeRepository

        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action="trade.purchase",
            payload={"user_id": user_id, "listing_id": listing_id, "quantity": quantity, **kwargs},
            call=lambda: TradeRepository(self.game_database, max_goods_num=max_goods_num).purchase_xianshi_item(
                operation_id, user_id, listing_id, quantity,
                stamina_operation_id=kwargs.get("stamina_operation_id"),
                stamina_cost=int(kwargs.get("stamina_cost", 0) or 0),
            ),
        )


__all__ = ["TradeApplication"]
