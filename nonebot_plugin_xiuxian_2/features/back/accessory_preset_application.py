from __future__ import annotations

from pathlib import Path
from typing import Any

from .accessory_preset_domain import AccessoryPresetChange, plan_save_preset
from .accessory_preset_repository import AccessoryPresetSqlRepository


class AccessoryPresetApplication:
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: AccessoryPresetSqlRepository | None = None,
    ) -> None:
        self.repository = repository or AccessoryPresetSqlRepository(
            game_database, player_database
        )

    def replay(self, operation_id: str) -> AccessoryPresetChange | None:
        return self.repository.replay(operation_id)

    def save(
        self,
        operation_id: str,
        user_id: str,
        preset_idx: int,
        expected_equipped: dict[str, Any],
        expected_preset: dict[str, Any],
    ) -> AccessoryPresetChange:
        plan = plan_save_preset(preset_idx, expected_equipped, expected_preset)
        if plan.status != "applied":
            return AccessoryPresetChange(plan.status, "save_preset", str(user_id))
        return self.repository.save(
            operation_id,
            user_id,
            plan.preset_idx,
            plan.expected_equipped,
            plan.expected_preset,
            plan.preset,
        )


__all__ = ["AccessoryPresetApplication", "AccessoryPresetChange"]
