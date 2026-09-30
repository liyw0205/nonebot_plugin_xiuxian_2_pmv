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

    def get_used_count(self, reward_type, record_id, legacy_used_count=0):
        return self.repository.get_used_count(
            reward_type, record_id, legacy_used_count
        )

    def invitation_claimed_thresholds(self, user_id):
        return self.repository.invitation_claimed_thresholds(user_id)

    def invitation_count(self, inviter_id, legacy_records=None):
        return self.repository.invitation_count(inviter_id, legacy_records)

    def invitation_inviter_id(self, user_id, legacy_records=None):
        return self.repository.invitation_inviter_id(user_id, legacy_records)

    def invitation_has_code(self, user_id, legacy_records=None):
        return self.repository.invitation_has_code(user_id, legacy_records)

    def invitation_bind(self, inviter_id, invited_id, legacy_records=None):
        return self.repository.invitation_bind(inviter_id, invited_id, legacy_records)

    def invitation_rewards(self, legacy_rewards=None):
        return self.repository.invitation_rewards(legacy_rewards)

    def invitation_set_reward(self, threshold, reward_items, legacy_rewards=None):
        return self.repository.invitation_set_reward(threshold, reward_items, legacy_rewards)

    def invitation_get_result(self, operation_id):
        return self.repository.invitation_get_result(operation_id)

    def invitation_claim(
        self,
        *,
        operation_id,
        user_id,
        invited_user_ids,
        rewards_by_threshold,
        requested_thresholds,
        legacy_claimed_thresholds,
        max_goods_num,
    ):
        return self.repository.invitation_claim(
            operation_id,
            user_id,
            invited_user_ids,
            rewards_by_threshold,
            requested_thresholds,
            legacy_claimed_thresholds,
            max_goods_num,
        )


__all__ = ["CompensationApplication"]
