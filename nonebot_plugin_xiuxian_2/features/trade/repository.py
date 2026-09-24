from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class TradeFeatureRepository(Protocol):
    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...


class LegacyTradeFeatureRepository:
    def __init__(self, game_database: str | Path, trade_database: str | Path) -> None:
        self.game_database, self.trade_database = str(game_database), str(trade_database)

    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_trade.transaction_service import (
            AuctionSessionService, GuishiStoneService,
        )
        if action in {"enqueue", "dequeue"}:
            from ..auction.queue_application import AuctionQueueApplication

            max_goods_num = int(kwargs.pop("max_goods_num", 1))
            application = AuctionQueueApplication(
                self.game_database,
                self.trade_database,
                max_goods_num,
            )
            return getattr(application, action)(
                operation_id, user_id, **kwargs
            )
        if action in {"deposit", "withdraw"}:
            return getattr(GuishiStoneService(self.game_database, self.trade_database), action)(operation_id, user_id, **kwargs)
        if action in {"session_start", "session_finish"}:
            method = "start" if action == "session_start" else "finish"
            return getattr(AuctionSessionService(self.game_database, self.trade_database, int(kwargs.pop("max_goods_num", 1))), method)(operation_id, **kwargs)
        from ...xiuxian.xiuxian_trade.repository import TradeRepository
        max_goods_num = int(kwargs.pop("max_goods_num", 1) or 1)
        return TradeRepository(self.game_database, max_goods_num=max_goods_num).purchase_xianshi_item(
            operation_id,
            str(user_id),
            kwargs.pop("listing_id"),
            kwargs.pop("quantity"),
            **kwargs,
        )


__all__ = ["LegacyTradeFeatureRepository", "TradeFeatureRepository"]
