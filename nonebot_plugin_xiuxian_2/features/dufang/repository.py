from __future__ import annotations

from pathlib import Path
from typing import Any

from .._service_port import ServicePort


class DufangRepository(ServicePort):
    def __init__(self, database: str | Path, player_database: str | Path | None = None) -> None:
        super().__init__("dufang", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang")
        self.database = str(database)
        self.player_database = None if player_database is None else str(player_database)

    def execute(self, operation_id: str, user_id: str, action: str, payload: dict[str, Any]) -> Any:
        if str(action).casefold() == "bet" and self.player_database is not None:
            from ...xiuxian.xiuxian_dufang.transaction_service import DufangBetService

            return DufangBetService(self.database, self.player_database).place(
                operation_id,
                user_id,
                payload["cost"],
                payload["placed_at"],
            )
        return super().execute(operation_id, user_id, action, payload)


__all__ = ["DufangRepository"]
