from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.random_source import SystemRandom
from .accessory_wash_domain import AccessoryWashChange, plan_wash
from .accessory_wash_repository import AccessoryWashSqlRepository


class AccessoryWashApplication:
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: AccessoryWashSqlRepository | None = None,
        random_source: Any | None = None,
    ) -> None:
        self.repository = repository or AccessoryWashSqlRepository(game_database, player_database)
        self.random_source = random_source or SystemRandom()

    def replay(self, operation_id: str) -> AccessoryWashChange | None:
        return self.repository.replay(operation_id)

    def wash(
        self,
        operation_id: str,
        user_id: str,
        uid: str,
        expected_accessory: dict[str, Any],
        expected_stones: int,
        stone_id: int,
        stone_cost: int,
    ) -> AccessoryWashChange:
        plan = plan_wash(expected_accessory, self.random_source)
        if plan.status != "applied" or plan.accessory is None:
            return AccessoryWashChange(plan.status, "wash", str(user_id))
        return self.repository.wash(
            operation_id,
            user_id,
            uid,
            expected_accessory,
            expected_stones,
            stone_id,
            stone_cost,
            plan.accessory,
        )


__all__ = ["AccessoryWashApplication"]
