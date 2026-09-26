from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.random_source import SystemRandom
from .accessory_upgrade_domain import (
    AccessoryUpgradeChange,
    plan_upgrade,
)
from .accessory_upgrade_repository import AccessoryUpgradeSqlRepository


class AccessoryUpgradeApplication:
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: AccessoryUpgradeSqlRepository | None = None,
        random_source: Any | None = None,
    ) -> None:
        self.repository = repository or AccessoryUpgradeSqlRepository(
            game_database, player_database
        )
        self.random_source = random_source or SystemRandom()

    def replay(self, operation_id: str) -> AccessoryUpgradeChange | None:
        return self.repository.replay(operation_id)

    def upgrade(
        self,
        operation_id: str,
        user_id: str,
        part: str,
        expected_equipped: dict[str, Any],
        expected_bag: list[dict[str, Any]],
        material_uids: tuple[str, ...],
    ) -> AccessoryUpgradeChange:
        plan = plan_upgrade(
            expected_equipped,
            expected_bag,
            part,
            material_uids,
            self.random_source,
        )
        if plan.status != "applied" or plan.accessory is None:
            return AccessoryUpgradeChange(plan.status, "upgrade", str(user_id))
        return self.repository.upgrade(
            operation_id,
            user_id,
            part,
            expected_equipped,
            expected_bag,
            material_uids,
            plan.accessory,
        )


__all__ = ["AccessoryUpgradeApplication", "AccessoryUpgradeChange"]
