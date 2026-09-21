from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ...features._legacy_application import LegacyApplication
from ...core.result import OperationOutcome
from ...xiuxian.xiuxian_world_events.transaction_service import DemonAttackSettlementService


class DemonAttackApplication(LegacyApplication):
    def __init__(self, player_database: str | Path) -> None:
        super().__init__(player_database, feature="world_events.demon_attack")
        self.player_database = str(player_database)
        self._service_instance: DemonAttackSettlementService | None = None

    def _service(self) -> DemonAttackSettlementService:
        if self._service_instance is None:
            self._service_instance = DemonAttackSettlementService(self.player_database)
        return self._service_instance

    def settle(
        self,
        *,
        operation_id: str,
        user_id: str,
        event_key: str,
        user_name: str,
        realm: str,
        total_damage: int,
        expected_event: dict,
        expected_boss: dict,
        expected_participants: dict,
        attack_limit: int,
        real_hp_multiplier: float,
        max_damage_ratio: float,
        max_pursuit_ratio: float,
    ) -> Any:
        payload = {
            "event_key": event_key,
            "user_id": user_id,
            "realm": realm,
            "total_damage": total_damage,
            "expected_event": expected_event,
            "expected_boss": expected_boss,
            "expected_participants": expected_participants,
            "attack_limit": attack_limit,
            "real_hp_multiplier": real_hp_multiplier,
            "max_damage_ratio": max_damage_ratio,
            "max_pursuit_ratio": max_pursuit_ratio,
        }
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action="world_events.demon_attack.settle",
            payload=payload,
            call=lambda: self._service().settle(
                operation_id,
                event_key,
                user_id,
                user_name,
                realm,
                total_damage,
                expected_event,
                expected_boss,
                expected_participants,
                attack_limit=attack_limit,
                real_hp_multiplier=real_hp_multiplier,
                max_damage_ratio=max_damage_ratio,
                max_pursuit_ratio=max_pursuit_ratio,
            ),
        )
