from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import CompensationRepository


# Claim-state lookups are reads over the claim ledger, not claim mutations.
READ_ONLY_METHODS = ("compensation_claimed_data", "invitation_claimed_thresholds")


class CompensationApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: CompensationRepository | None = None) -> None:
        super().__init__(database, feature="compensation", repository=repository or CompensationRepository(database))

    def compensation_definitions(self):
        return self.repository.list_compensation_definitions()

    def compensation_definition(self, record_id):
        return self.repository.get_compensation_definition(record_id)

    def compensation_claimed_data(self):
        return self.repository.compensation_claimed_data()

    def compensation_catalog_version(self):
        return self.repository.compensation_catalog_version()

    def reward_center_records(self, reward_type):
        return self.repository.reward_center_records(reward_type)

    def replay_compensation_definition_upsert(self, operation_id, request_identity):
        return self.repository.replay_compensation_definition_upsert(
            operation_id, request_identity
        )

    def upsert_compensation_definition(
        self, operation_id, request_identity, record_id, record, expected_version=None
    ):
        return self.repository.upsert_compensation_definition(
            operation_id,
            request_identity,
            record_id,
            record,
            expected_version,
        )

    def delete_compensation_definition(
        self, operation_id, record_id, expected_version=None
    ):
        return self.repository.delete_compensation_definition(
            operation_id, record_id, expected_version
        )

    def clear_compensation_definitions(self, operation_id, expected_catalog_version):
        return self.repository.clear_compensation_definitions(
            operation_id, expected_catalog_version
        )

    def claim_reward(self, *, operation_id, reward_type, record_id, user_id, reward_items, max_goods_num, usage_limit=0, legacy_used_count=0, expected_definition_version=None):
        return self.repository.claim_reward(operation_id, reward_type, record_id, user_id, reward_items, max_goods_num, usage_limit, legacy_used_count, expected_definition_version)

    def delete_reward_claims(self, *, operation_id, reward_type, record_id=None):
        return self.repository.delete_reward_claims(
            operation_id, reward_type, record_id
        )

    def has_claimed(self, reward_type, record_id, user_id):
        return self.repository.has_claimed(reward_type, record_id, user_id)

    def list_claims(self, reward_type):
        return self.repository.list_claims(reward_type)

    def get_claim_count(self, reward_type, record_id):
        return self.repository.get_claim_count(reward_type, record_id)

    def get_used_count(self, reward_type, record_id, legacy_used_count=0):
        return self.repository.get_used_count(
            reward_type, record_id, legacy_used_count
        )

    def reward_definitions(self, reward_type):
        return self.repository.list_reward_definitions(reward_type)

    def reward_definition(self, reward_type, record_id):
        return self.repository.get_reward_definition(reward_type, record_id)

    def replay_reward_definition_upsert(self, operation_id, reward_type, request_identity):
        return self.repository.replay_reward_definition_upsert(
            operation_id, reward_type, request_identity
        )

    def upsert_reward_definition(
        self, operation_id, reward_type, record_id, request_identity, record,
        expected_version=None,
    ):
        return self.repository.upsert_reward_definition(
            operation_id,
            reward_type,
            record_id,
            request_identity,
            record,
            expected_version,
        )

    def delete_reward_definition(self, operation_id, reward_type, record_id):
        return self.repository.delete_reward_definition(
            operation_id, reward_type, record_id
        )

    def clear_reward_definitions(self, operation_id, reward_type):
        return self.repository.clear_reward_definitions(operation_id, reward_type)

    def invitation_claimed_thresholds(self, user_id):
        return self.repository.invitation_claimed_thresholds(user_id)

    def invitation_count(self, inviter_id):
        return self.repository.invitation_count(inviter_id)

    def invitation_inviter_id(self, user_id):
        return self.repository.invitation_inviter_id(user_id)

    def invitation_has_code(self, user_id):
        return self.repository.invitation_has_code(user_id)

    def invitation_bind(self, inviter_id, invited_id):
        return self.repository.invitation_bind(inviter_id, invited_id)

    def invitation_rewards(self):
        return self.repository.invitation_rewards()

    def invitation_set_reward(self, threshold, reward_items):
        return self.repository.invitation_set_reward(threshold, reward_items)

    def invitation_get_result(self, operation_id):
        return self.repository.invitation_get_result(operation_id)

    def invitation_claim(
        self,
        *,
        operation_id,
        user_id,
        rewards_by_threshold,
        requested_thresholds,
        max_goods_num,
    ):
        return self.repository.invitation_claim(
            operation_id,
            user_id,
            rewards_by_threshold,
            requested_thresholds,
            max_goods_num,
        )


__all__ = ["CompensationApplication"]
