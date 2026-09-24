"""Idempotent compatibility projections for auction bid outbox events."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from ..features.auction.bid_statistics import AuctionBidStatisticsRepository
from ..infrastructure.database import DatabaseUnitOfWork
from ..infrastructure.filesystem import atomic_write


def log_auction_event_once(
    *, user_id: str, message: str, event_id: str, occurred_at: str,
    players_dir: str | Path | None = None,
) -> bool:
    """Write the legacy JSON log with an event marker so outbox replay is safe."""
    from ..paths import get_paths

    if str(user_id) == "0":
        return False
    paths = get_paths()
    root = Path(players_dir) if players_dir is not None else paths.players
    legacy_utils = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
    impersonating_users = getattr(legacy_utils, "_impersonating_users", {})
    target_user = impersonating_users.get(str(user_id), str(user_id))
    lock_database = root.parent / "player.db"
    _clean_expired_logs(root)
    try:
        when = datetime.fromisoformat(str(occurred_at))
    except ValueError as exc:
        raise ValueError("auction bid event has an invalid occurred_at") from exc
    logs_dir = root / target_user / "logs"
    log_file = logs_dir / f"{when.strftime('%y%m%d')}.log"
    record = {
        "timestamp": when.strftime("%Y-%m-%d %H:%M:%S"),
        "message": str(message),
        "event_id": str(event_id),
    }
    with DatabaseUnitOfWork(lock_database, immediate=True):
        logs_dir.mkdir(parents=True, exist_ok=True)
        logs = json.loads(log_file.read_text(encoding="utf-8")) if log_file.exists() else []
        if any(str(item.get("event_id", "")) == str(event_id) for item in logs):
            return False
        logs.insert(0, record)
        atomic_write(log_file, json.dumps(logs, ensure_ascii=False, indent=4).encode("utf-8"))
    return True


def _clean_expired_logs(players_dir: Path, *, keep_days: int = 10) -> None:
    now = datetime.now()
    for logs_dir in players_dir.rglob("logs"):
        for path in logs_dir.glob("*.log"):
            try:
                if len(path.stem) == 6 and (now - datetime.strptime(path.stem, "%y%m%d")).days > keep_days:
                    path.unlink()
            except (OSError, ValueError):
                continue


class LegacyAuctionBidEffects:
    """Project durable bid events into the existing player stats and log formats."""

    def __init__(
        self,
        player_database: str | Path,
        *,
        log_writer: Callable[..., Any] = log_auction_event_once,
        statistics: AuctionBidStatisticsRepository | None = None,
    ) -> None:
        self.statistics = statistics or AuctionBidStatisticsRepository(player_database)
        self.log_writer = log_writer

    def on_bid(
        self,
        *,
        operation_id: str,
        auction_id: str,
        bidder_id: str,
        item_name: str,
        bid_price: int,
        replayed: bool,
        occurred_at: str,
    ) -> str | None:
        if str(bidder_id) == "0":
            return None
        self.statistics.record_bid(
            operation_id=operation_id,
            user_id=bidder_id,
            bid_price=bid_price,
            occurred_at=occurred_at,
        )
        legacy_utils = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
        invalidate = getattr(legacy_utils, "invalidate_player_data_cache", None)
        if callable(invalidate):
            invalidate("statistics", ("拍卖出价次数", "拍卖出价灵石"))
        self.log_writer(
            user_id=str(bidder_id),
            message=f"[拍卖竞拍] 竞拍{item_name}，出价{int(bid_price)}灵石，拍卖ID:{auction_id}",
            event_id=f"auction.bid:{operation_id}",
            occurred_at=occurred_at,
        )
        return None

    def reconcile_event(self, record: Mapping[str, Any]) -> None:
        payload = record.get("payload") or {}
        self.on_bid(
            operation_id=str(payload["operation_id"]),
            auction_id=str(payload["auction_id"]),
            bidder_id=str(payload["bidder_id"]),
            item_name=str(payload.get("item_name", "")),
            bid_price=int(payload["bid_price"]),
            replayed=True,
            occurred_at=str(payload["occurred_at"]),
        )


log_auction_bid_once = log_auction_event_once


__all__ = ["LegacyAuctionBidEffects", "log_auction_event_once", "log_auction_bid_once"]
