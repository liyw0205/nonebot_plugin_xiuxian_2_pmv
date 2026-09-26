from __future__ import annotations

from pathlib import Path
from typing import Any

from .accessory_affix_domain import AccessoryAffixChange, plan_affix_locks
from .accessory_affix_repository import AccessoryAffixSqlRepository


class AccessoryAffixApplication:
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: AccessoryAffixSqlRepository | None = None,
    ) -> None:
        self.repository = repository or AccessoryAffixSqlRepository(
            game_database, player_database
        )

    def replay(self, operation_id: str, action: str) -> AccessoryAffixChange | None:
        return self.repository.replay(operation_id, action)

    def set_locks(
        self,
        operation_id: str,
        action: str,
        user_id: str,
        uid: str,
        expected_accessory: dict[str, Any],
        locked_indexes: tuple[int, ...],
    ) -> AccessoryAffixChange:
        plan = plan_affix_locks(expected_accessory, action, locked_indexes)
        if plan.status != "applied" or plan.accessory is None:
            return AccessoryAffixChange(plan.status, action, str(user_id))
        return self.repository.set_locks(
            operation_id,
            action,
            user_id,
            uid,
            expected_accessory,
            plan.accessory,
        )


__all__ = ["AccessoryAffixApplication", "AccessoryAffixChange"]
