from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .attack_repository import (
    DemonAttackSettlementRepository,
    DemonAttackSettlementSqlRepository,
)
from .domain import DemonAttackSettlementResult


class DemonAttackApplication:
    """Feature boundary for replayable demon attack settlement."""

    def __init__(
        self,
        player_database: str | Path,
        *,
        repository: DemonAttackSettlementRepository | None = None,
    ) -> None:
        self.repository = repository or DemonAttackSettlementSqlRepository(player_database)

    def get_result(self, operation_id: str) -> DemonAttackSettlementResult | None:
        return self.repository.get_result(operation_id)

    def settle(
        self,
        *,
        operation_id: str,
        user_id: str,
        event_key: str,
        user_name: str,
        realm: str,
        total_damage: int,
        expected_event: Mapping[str, Any],
        expected_boss: Mapping[str, Any],
        expected_participants: Mapping[str, Any],
        attack_limit: int,
        real_hp_multiplier: float,
        max_damage_ratio: float,
        max_pursuit_ratio: float,
    ) -> DemonAttackSettlementResult:
        if not str(operation_id).strip() or not str(user_id).strip():
            raise ValueError("operation_id and user_id are required")
        if not isinstance(expected_event, Mapping) or not isinstance(expected_boss, Mapping):
            raise ValueError("expected event and boss snapshots must be objects")
        if not isinstance(expected_participants, Mapping):
            raise ValueError("expected participants snapshot must be an object")
        return self.repository.settle(
            operation_id=str(operation_id),
            event_key=str(event_key),
            user_id=str(user_id),
            user_name=str(user_name),
            realm=str(realm),
            total_damage=int(total_damage),
            expected_event=expected_event,
            expected_boss=expected_boss,
            expected_participants=expected_participants,
            attack_limit=int(attack_limit),
            real_hp_multiplier=float(real_hp_multiplier),
            max_damage_ratio=float(max_damage_ratio),
            max_pursuit_ratio=float(max_pursuit_ratio),
        )


__all__ = ["DemonAttackApplication"]
