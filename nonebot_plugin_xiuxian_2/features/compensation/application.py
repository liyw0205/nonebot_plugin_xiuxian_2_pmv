from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import CompensationRepository


class CompensationApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: CompensationRepository | None = None) -> None:
        super().__init__(database, feature="compensation", repository=repository or CompensationRepository(database))

    def claim_reward(
        self,
        *,
        operation_id,
        reward_type,
        record_id,
        user_id,
        reward_items,
        max_goods_num,
        expected_definition_version=None,
    ):
        return self.repository.claim_reward(
            operation_id,
            reward_type,
            record_id,
            user_id,
            reward_items,
            max_goods_num,
            expected_definition_version,
        )


__all__ = ["CompensationApplication"]
