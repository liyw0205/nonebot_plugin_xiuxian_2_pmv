from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import CompensationRepository


class CompensationApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: CompensationRepository | None = None) -> None:
        super().__init__(database, feature="compensation", repository=repository or CompensationRepository(database))

    def claim_reward(self, *, operation_id, reward_type, record_id, user_id, reward_items, max_goods_num, usage_limit=0, legacy_used_count=0, expected_definition_version=None):
        return self.repository.claim_reward(operation_id, reward_type, record_id, user_id, reward_items, max_goods_num, usage_limit, legacy_used_count, expected_definition_version)

    def has_claimed(self, reward_type, record_id, user_id):
        return self.repository.has_claimed(reward_type, record_id, user_id)

    def get_used_count(self, reward_type, record_id):
        return self.repository.get_used_count(reward_type, record_id)


__all__ = ["CompensationApplication"]
