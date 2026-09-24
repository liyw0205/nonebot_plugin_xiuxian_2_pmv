from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AuctionSessionStartResult:
    """Stable result shape kept for explicit legacy session callers."""

    status: str
    operation_id: str
    session_id: str = ""
    start_time: float = 0.0
    end_time: float = 0.0
    items_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"started", "duplicate"}


@dataclass(frozen=True)
class AuctionSessionFinishResult:
    """Stable result shape kept for explicit legacy session callers."""

    status: str
    operation_id: str
    session_id: str = ""
    results: tuple[dict[str, Any], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


class AuctionSessionService:
    """Compatibility facade backed by the auction feature repositories.

    This preserves the historical start/finish method signatures for callers
    that explicitly inject the rollback repository.  The transaction logic is
    owned by the feature repositories, so this adapter no longer reaches into
    the legacy transaction service.
    """

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        max_goods_num: int | None = None,
        lock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)
        self.max_goods_num = max(int(max_goods_num or 1), 1)

    def _start_repository(self):
        from ..features.auction.session_start_repository import (
            AuctionSessionStartSqlRepository,
        )

        return AuctionSessionStartSqlRepository(
            self.game_database, self.trade_database
        )

    def _settlement_repository(self):
        from ..features.auction.settlement_repository import AuctionSettlementSqlRepository

        return AuctionSettlementSqlRepository(
            self.game_database, max_goods_num=self.max_goods_num
        )

    def get_active_session(self) -> dict[str, Any] | None:
        return self._start_repository().get_active_session()

    def get_start_operation(self, operation_id: str) -> AuctionSessionStartResult | None:
        result = self._start_repository().get_start_operation(operation_id)
        if result is None:
            return None
        return AuctionSessionStartResult(
            result.status,
            result.operation_id,
            result.session_id,
            result.start_time,
            result.end_time,
            result.items_count,
        )

    def start(
        self,
        operation_id: str,
        session_id: str,
        *,
        start_time: float,
        end_time: float,
        system_items: list[dict[str, Any]],
    ) -> AuctionSessionStartResult:
        result = self._start_repository().start(
            operation_id,
            session_id,
            start_time=start_time,
            end_time=end_time,
            system_items=system_items,
        )
        return AuctionSessionStartResult(
            result.status,
            result.operation_id,
            result.session_id,
            result.start_time,
            result.end_time,
            result.items_count,
        )

    def finish(
        self,
        operation_id: str,
        session_id: str,
        *,
        end_time: float,
        fee_rate: float,
        item_types: dict[int, str],
    ) -> AuctionSessionFinishResult:
        operation_id, session_id = str(operation_id).strip(), str(session_id).strip()
        active = self.get_active_session()
        if active is not None and str(active["session_id"]) != session_id:
            return AuctionSessionFinishResult("not_active", operation_id, session_id)
        result = self._settlement_repository().settle_active(
            operation_id,
            end_time=end_time,
            fee_rate=fee_rate,
            item_types=item_types,
        )
        if result.status == "empty":
            return AuctionSessionFinishResult("not_active", operation_id, session_id)
        if result.status == "duplicate" and result.session_id and result.session_id != session_id:
            return AuctionSessionFinishResult("state_changed", operation_id, session_id)
        return AuctionSessionFinishResult(
            result.status,
            result.operation_id,
            result.session_id or session_id,
            tuple(dict(item) for item in result.results),
        )


class LegacyTradeAuctionSessionAdapter:
    """Rollback-only bridge for callers that explicitly inject the legacy trade repository."""

    def __init__(self, game_database: str | Path, trade_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)

    def invoke(self, action: str, operation_id: str, **kwargs: Any):
        method = {"session_start": "start", "session_finish": "finish"}.get(action)
        if method is None:
            raise ValueError(f"unsupported legacy auction session action: {action}")
        max_goods_num = int(kwargs.pop("max_goods_num", 1) or 1)
        service = AuctionSessionService(
            self.game_database, self.trade_database, max_goods_num=max_goods_num
        )
        return getattr(service, method)(operation_id, **kwargs)


__all__ = [
    "AuctionSessionFinishResult",
    "AuctionSessionService",
    "AuctionSessionStartResult",
    "LegacyTradeAuctionSessionAdapter",
]
