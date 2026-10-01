from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .._service_port import ServicePort
from .bet_repository import DufangBetSqlRepository
from .payout_repository import DufangPayoutSqlRepository
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
            return DufangBetSqlRepository(self.database, self.player_database).place(
                operation_id,
                user_id,
                payload["cost"],
                payload["placed_at"],
            )
        if str(action).casefold() == "payout" and self.player_database is not None:
            return DufangPayoutSqlRepository(self.database, self.player_database).settle(
                operation_id,
                payload["bet_id"],
                user_id,
                payload["outcome"],
                payload["gain"],
                payload["requested_loss"],
                payload["settled_at"],
            )
        return super().execute(operation_id, user_id, action, payload)

    def payout_result(self, operation_id: str) -> Any:
        if self.player_database is None:
            return None
        return DufangPayoutSqlRepository(self.database, self.player_database).get_result(operation_id)

    def share_exists(self, operation_id: str) -> bool:
        return self.share is not None and self.share.exists(operation_id)


__all__ = ["DufangRepository"]
