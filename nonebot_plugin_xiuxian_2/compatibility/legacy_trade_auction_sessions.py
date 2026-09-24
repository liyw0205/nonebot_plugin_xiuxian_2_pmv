from __future__ import annotations

from pathlib import Path
from typing import Any


class LegacyTradeAuctionSessionAdapter:
    """Rollback-only bridge for callers that explicitly inject the legacy trade repository."""

    def __init__(self, game_database: str | Path, trade_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)

    def invoke(self, action: str, operation_id: str, **kwargs: Any):
        from ..xiuxian.xiuxian_trade.transaction_service import AuctionSessionService

        method = {"session_start": "start", "session_finish": "finish"}.get(action)
        if method is None:
            raise ValueError(f"unsupported legacy auction session action: {action}")
        max_goods_num = int(kwargs.pop("max_goods_num", 1) or 1)
        service = AuctionSessionService(
            self.game_database, self.trade_database, max_goods_num=max_goods_num
        )
        return getattr(service, method)(operation_id, **kwargs)


__all__ = ["LegacyTradeAuctionSessionAdapter"]
