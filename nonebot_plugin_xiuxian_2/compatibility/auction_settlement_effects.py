from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Mapping

from ..features.auction.settlement_statistics import AuctionSettlementStatisticsRepository
from .auction_bid_effects import log_auction_event_once


class LegacyAuctionSettlementEffects:
    """Replay-safe projections into the existing auction logs and game events."""

    def __init__(
        self,
        player_database: str | Path,
        *,
        statistics: AuctionSettlementStatisticsRepository | None = None,
        log_writer: Callable[..., Any] = log_auction_event_once,
        game_event_writer: Callable[..., Mapping[str, Any]] | None = None,
    ) -> None:
        self.statistics = statistics or AuctionSettlementStatisticsRepository(player_database)
        self.log_writer = log_writer
        self.game_event_writer = game_event_writer

    def on_settlement(
        self,
        *,
        event_id: str,
        event_key: str,
        operation_id: str,
        settlement: Mapping[str, Any],
        occurred_at: str,
        replayed: bool,
    ) -> None:
        from ..xiuxian.xiuxian_utils.utils import number_to

        auction_id = str(settlement["auction_id"])
        item_id = int(settlement["item_id"])
        item_name = str(settlement["item_name"])
        seller_id = str(settlement.get("seller_id", "0"))
        final_price = settlement.get("final_price")
        if event_key == "winner":
            user_id = str(settlement["winner_id"])
            price = int(final_price)
            increments = {
                "拍卖成交次数": 1,
                "拍卖消费灵石": price,
                "交易购买": 1,
                "拍卖成交": 1,
            }
            message = (
                f"[拍卖成交] 拍得{item_name}，成交价{number_to(price)}灵石，拍卖ID:{auction_id}"
            )
            game_event_key = "trade_buy"
            stone_delta = -price
            item_delta = [{"id": item_id, "name": item_name, "amount": 1}]
            detail = {
                "auction_id": auction_id,
                "seller_id": seller_id,
                "final_price": price,
            }
        elif event_key == "seller":
            user_id = seller_id
            price = int(final_price)
            fee = int(settlement.get("fee", 0))
            earnings = int(settlement.get("seller_earnings", 0))
            increments = {
                "拍卖售出次数": 1,
                "拍卖收入灵石": earnings,
                "拍卖手续费消耗": fee,
                "交易出售": 1,
                "拍卖成交": 1,
            }
            message = (
                f"[拍卖成交] 售出{item_name}，成交价{number_to(price)}灵石，"
                f"手续费{number_to(fee)}灵石，收入{number_to(earnings)}灵石"
            )
            game_event_key = "trade_sell"
            stone_delta = earnings
            item_delta = [{"id": item_id, "name": item_name, "amount": -1}]
            detail = {
                "auction_id": auction_id,
                "winner_id": str(settlement["winner_id"]),
                "final_price": price,
                "fee": fee,
            }
        elif event_key == "seller_miss":
            user_id = seller_id
            increments = {"拍卖流拍次数": 1}
            message = f"[拍卖流拍] {item_name}流拍，已退回背包，拍卖ID:{auction_id}"
            game_event_key = ""
            stone_delta = 0
            item_delta = []
            detail = {"auction_id": auction_id, "status": str(settlement.get("status", "流拍"))}
        else:
            raise ValueError(f"unsupported auction settlement effect: {event_key}")

        if user_id == "0":
            return
        self.statistics.record(
            event_id=event_id,
            user_id=user_id,
            increments=increments,
            occurred_at=occurred_at,
        )
        legacy_utils = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
        invalidate = getattr(legacy_utils, "invalidate_player_data_cache", None)
        if callable(invalidate):
            invalidate("statistics", increments)
        self.log_writer(
            user_id=user_id,
            message=message,
            event_id=event_id,
            occurred_at=occurred_at,
        )
        if not game_event_key:
            return
        writer = self.game_event_writer
        if writer is None:
            from ..xiuxian.xiuxian_utils.game_events import safe_record_game_event

            writer = safe_record_game_event
        result = writer(
            user_id,
            game_event_key,
            1,
            {
                "source": "auction",
                "action": "auction_buy" if game_event_key == "trade_buy" else "auction_sell",
                "stone_delta": stone_delta,
                "item_delta": item_delta,
                "detail": detail,
                "trace_id": f"trade:auction:{auction_id}",
                "event_id": f"{event_id}:game-event",
                "occurred_at": occurred_at,
                "skip_statistics": True,
                "require_effects": True,
            },
        )
        if int((result or {}).get("economy_log_id", 0) or 0) <= 0:
            raise RuntimeError(f"auction game event did not persist economy log: {event_id}")
        if len((result or {}).get("season_rank", ())) != 3:
            raise RuntimeError(f"auction game event did not persist season scores: {event_id}")


__all__ = ["LegacyAuctionSettlementEffects"]
