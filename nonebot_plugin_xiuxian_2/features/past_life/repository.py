from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort



class PastLifeRepository(ServicePort):
    def __init__(self, database: str | Path, player_database: str | Path | None = None) -> None:
        self.player_database = str(player_database or database)
        super().__init__("past_life", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life", handlers={"reset_one": self._reset_one})
        self.database = str(database)

    def _reset_one(self, *, operation_id: str, user_id: str, clear_history: bool):
        from ...xiuxian.xiuxian_past_life.transaction_service import PastLifeResetService

        return PastLifeResetService(self.database, self.player_database).reset_one(
            operation_id, user_id, bool(clear_history)
        )


__all__ = ["PastLifeRepository"]
