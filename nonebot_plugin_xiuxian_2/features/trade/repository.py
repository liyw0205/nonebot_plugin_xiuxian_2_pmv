from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class TradeFeatureRepository(Protocol):
    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...


class LegacyTradeFeatureRepository:
    def __init__(self, game_database: str | Path, trade_database: str | Path) -> None:
        self.game_database, self.trade_database = str(game_database), str(trade_database)

    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any:
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
            if action == "deposit":
                from .guishi_deposit_repository import GuishiDepositSqlRepository

                amount = kwargs.pop("amount")
                if kwargs:
                    raise TypeError(
                        f"unexpected deposit arguments: {', '.join(sorted(kwargs))}"
                    )
                return GuishiDepositSqlRepository(
                    self.game_database, self.trade_database
                ).deposit(
                    operation_id=operation_id,
                    user_id=user_id,
                    amount=amount,
                )
            from .guishi_withdraw_repository import GuishiWithdrawSqlRepository

            amount = kwargs.pop("amount")
            withdrawal_open = kwargs.pop("withdrawal_open", True)
            if kwargs:
                raise TypeError(
                    f"unexpected withdraw arguments: {', '.join(sorted(kwargs))}"
                )
            return GuishiWithdrawSqlRepository(
                self.game_database, self.trade_database
            ).withdraw(
                operation_id=operation_id,
                user_id=user_id,
                amount=amount,
                withdrawal_open=withdrawal_open,
            )
        if action in {"session_start", "session_finish"}:
            from ...compatibility.legacy_trade_auction_sessions import (
                LegacyTradeAuctionSessionAdapter,
            )

            return LegacyTradeAuctionSessionAdapter(
                self.game_database, self.trade_database
            ).invoke(action, operation_id, **kwargs)
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
