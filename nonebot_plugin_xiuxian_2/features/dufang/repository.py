from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .._service_port import ServicePort
from .bet_repository import DufangBetSqlRepository
from .payout_repository import DufangPayoutSqlRepository
from .player_stats_repository import (
    DufangPlayerStatsResult,
    DufangPlayerStatsSnapshot,
    DufangPlayerStatsSqlRepository,
)
from .share_repository import DufangShareSqlRepository


class DufangRepository(ServicePort):
    def __init__(self, database: str | Path, player_database: str | Path | None = None) -> None:
        super().__init__("dufang", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang")
        self.database = str(database)
        self.player_database = None if player_database is None else str(player_database)
        self.share = (
            None
            if self.player_database is None
            else DufangShareSqlRepository(self.database, self.player_database)
        )
        self.player_stats = (
            None
            if self.player_database is None
            else DufangPlayerStatsSqlRepository(self.database, self.player_database)
        )

    def execute(self, operation_id: str, user_id: str, action: str, payload: dict[str, Any]) -> Any:
        action = str(action).casefold()
        if action == "share_settle" and self.share is not None:
            return self.share.settle(
                operation_id=operation_id,
                source_id=user_id,
                event_type=payload["event_type"],
                event_title=payload["title"],
                event_description=payload["desc"],
                effect_amount=payload["effect_amount"],
                bonus_percent=payload["cost_bonus_percent"],
                recipients=payload["recipients"],
                occurred_at=payload["settled_at"],
            )
        if action == "share_resume" and self.share is not None:
            return self.share.resume(
                operation_id=operation_id,
                source_id=user_id,
                occurred_at=str(payload.get("settled_at", "")),
            )
        if action == "bet" and self.player_database is not None:
            return DufangBetSqlRepository(self.database).place(
                operation_id,
                user_id,
                payload["cost"],
                payload["placed_at"],
                payload["resolution"],
            )
        if str(action).casefold() == "payout" and self.player_database is not None:
            return DufangPayoutSqlRepository(self.database).settle(
                operation_id,
                payload["bet_id"],
                user_id,
                payload["settled_at"],
            )
        return super().execute(operation_id, user_id, action, payload)

    def payout_result(self, operation_id: str) -> Any:
        return DufangPayoutSqlRepository(self.database).get_result(operation_id)

    def bet_resolution(self, operation_id: str) -> Any:
        return DufangBetSqlRepository(self.database).get_resolution(operation_id)

    def player_projection_ready(self) -> bool:
        return self.player_stats is not None and self.player_stats.schema_ready()

    def bet_schema_ready(self) -> bool:
        return DufangBetSqlRepository(self.database).schema_ready()

    def player_total_cost(self, user_id: str) -> int:
        projected = 0 if self.player_stats is None else self.player_stats.total_cost(user_id)
        durable = DufangBetSqlRepository(self.database).total_cost(user_id)
        return max(projected, durable)

    def player_stats_snapshot(self, user_id: str) -> DufangPlayerStatsSnapshot:
        if self.player_stats is None:
            return DufangPlayerStatsSnapshot("schema_missing")
        return self.player_stats.snapshot(user_id)

    def reconcile_player_outbox(
        self, *, limit: int = 25, priority_event_id: str = ""
    ) -> DufangPlayerStatsResult:
        if self.player_stats is None:
            return DufangPlayerStatsResult("schema_missing")
        return self.player_stats.reconcile(limit=limit, priority_event_id=priority_event_id)

    def pending_bets(self, *, limit: int = 25) -> list[Mapping[str, str]]:
        return DufangBetSqlRepository(self.database).pending_resolutions(limit=limit)

    def pending_bet_count(self) -> int:
        return DufangBetSqlRepository(self.database).pending_count()

    def share_exists(self, operation_id: str) -> bool:
        return self.share is not None and self.share.exists(operation_id)


__all__ = ["DufangRepository"]
