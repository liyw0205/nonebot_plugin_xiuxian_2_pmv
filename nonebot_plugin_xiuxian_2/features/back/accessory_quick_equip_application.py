from __future__ import annotations

from pathlib import Path
from typing import Any

from .accessory_quick_equip_domain import (
    AccessoryQuickEquipChange,
    plan_quick_equip,
)
from .accessory_quick_equip_repository import AccessoryQuickEquipSqlRepository


class AccessoryQuickEquipApplication:
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: AccessoryQuickEquipSqlRepository | None = None,
    ) -> None:
        self.repository = repository or AccessoryQuickEquipSqlRepository(
            game_database, player_database
        )

    def replay(self, operation_id: str) -> AccessoryQuickEquipChange | None:
        return self.repository.replay(operation_id)

    def equip(
        self,
        operation_id: str,
        user_id: str,
        preset_idx: int,
        expected_equipped: dict[str, Any],
        expected_bag: list[Any],
        expected_preset: dict[str, Any],
    ) -> AccessoryQuickEquipChange:
        plan = plan_quick_equip(
            preset_idx, expected_equipped, expected_bag, expected_preset
        )
        if plan.status != "applied":
            return AccessoryQuickEquipChange(plan.status, "quick_equip_preset", str(user_id))
        return self.repository.equip(
            operation_id,
            user_id,
            plan,
        )


__all__ = ["AccessoryQuickEquipApplication", "AccessoryQuickEquipChange"]
