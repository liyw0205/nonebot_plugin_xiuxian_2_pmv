from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


class TiantiTrainingRepository(Protocol):
    def train(self, operation_id: str, user_id: str, requested_stone: int) -> Any: ...

    def apply_bath(
        self,
        operation_id: str,
        user_id: str,
        consume_plan: Sequence[Mapping[str, Any]],
        effect: float,
        slot_name: str,
        started_at: datetime,
        duration_minutes: int,
        *,
        sect_fairyland_level: int = 0,
    ) -> Any: ...

    def breakthrough(self, operation_id: str, user_id: str, *, cultivation_rank: int, roll_success: bool) -> Any: ...

    def open_qiaoxue(self, operation_id: str, user_id: str, roll: int) -> Any: ...


class LegacyTiantiTrainingRepository:
    """Lazy adapter for the already transactional Tianti services."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_tianti.transaction_service import (
            MedicineBathService,
            QiaoxueService,
            StoneTrainingService,
            TiantiBreakthroughService,
        )

        return (
            StoneTrainingService(self.game_database, self.player_database),
            MedicineBathService(self.game_database, self.player_database),
            TiantiBreakthroughService(self.player_database),
            QiaoxueService(self.player_database),
        )

    def train(self, operation_id: str, user_id: str, requested_stone: int) -> Any:
        return self._services()[0].train(operation_id, user_id, requested_stone)

    def apply_bath(
        self,
        operation_id: str,
        user_id: str,
        consume_plan: Sequence[Mapping[str, Any]],
        effect: float,
        slot_name: str,
        started_at: datetime,
        duration_minutes: int,
        *,
        sect_fairyland_level: int = 0,
    ) -> Any:
        return self._services()[1].apply(
            operation_id,
            user_id,
            consume_plan,
            effect,
            slot_name,
            started_at,
            duration_minutes,
            sect_fairyland_level=sect_fairyland_level,
        )

    def breakthrough(self, operation_id: str, user_id: str, *, cultivation_rank: int, roll_success: bool) -> Any:
        return self._services()[2].attempt(
            operation_id,
            user_id,
            cultivation_rank=cultivation_rank,
            roll_success=roll_success,
        )

    def open_qiaoxue(self, operation_id: str, user_id: str, roll: int) -> Any:
        return self._services()[3].open(operation_id, user_id, roll)


__all__ = ["LegacyTiantiTrainingRepository", "TiantiTrainingRepository"]
