from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort
from .invitation_repository import InvitationRewardClaimSqlRepository
from .reward_claim_repository import CompensationRewardClaimSqlRepository
from .reward_definition_repository import CompensationRewardDefinitionSqlRepository


class CompensationRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("compensation", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_compensation")
        self.database = str(database)
        self.reward_definitions = CompensationRewardDefinitionSqlRepository(database)

    def claim_reward(
        self,
        operation_id,
        reward_type,
        record_id,
        user_id,
        reward_items,
        max_goods_num,
        usage_limit=0,
        legacy_used_count=0,
        expected_definition_version=None,
    ):
        return CompensationRewardClaimSqlRepository(self.database, max_goods_num).claim(
            operation_id,
            reward_type,
            record_id,
            user_id,
            reward_items,
            usage_limit=usage_limit,
            legacy_used_count=legacy_used_count,
            expected_definition_version=expected_definition_version,
        )

    def delete_reward_claims(self, operation_id, reward_type, record_id=None):
        return CompensationRewardClaimSqlRepository(self.database, 0).delete_claims(
            operation_id, reward_type, record_id
        )

    def has_claimed(self, reward_type, record_id, user_id) -> bool:
        return CompensationRewardClaimSqlRepository(self.database, 0).has_claimed(
            reward_type, record_id, user_id
        )

    def list_claims(self, reward_type):
        return CompensationRewardClaimSqlRepository(self.database, 0).list_claims(
            reward_type
        )

    def get_claim_count(self, reward_type, record_id):
        return CompensationRewardClaimSqlRepository(self.database, 0).get_claim_count(
            reward_type, record_id
        )

    def get_used_count(self, reward_type, record_id, legacy_used_count=0) -> int:
        return CompensationRewardClaimSqlRepository(self.database, 0).get_used_count(
            reward_type, record_id, legacy_used_count
        )

    def list_reward_definitions(self, reward_type):
        return self.reward_definitions.list_definitions(reward_type)

    def get_reward_definition(self, reward_type, record_id):
        return self.reward_definitions.get_definition(reward_type, record_id)

    def replay_reward_definition_upsert(self, operation_id, reward_type, request_identity):
        return self.reward_definitions.replay_upsert(
            operation_id, reward_type, request_identity
        )

    def upsert_reward_definition(
        self, operation_id, reward_type, record_id, request_identity, record,
        expected_version=None,
    ):
        return self.reward_definitions.upsert(
            operation_id,
            reward_type,
            record_id,
            request_identity,
            record,
            expected_version,
        )

    def delete_reward_definition(self, operation_id, reward_type, record_id):
        return self.reward_definitions.delete(operation_id, reward_type, record_id)

    def clear_reward_definitions(self, operation_id, reward_type):
        return self.reward_definitions.clear(operation_id, reward_type)

    def invitation_claimed_thresholds(self, user_id):
        return InvitationRewardClaimSqlRepository(self.database).claimed_thresholds(user_id)

    def invitation_count(self, inviter_id, legacy_records=None):
        return InvitationRewardClaimSqlRepository(self.database).invitation_count(inviter_id, legacy_records)

    def invitation_inviter_id(self, user_id, legacy_records=None):
        return InvitationRewardClaimSqlRepository(self.database).inviter_id(user_id, legacy_records)

    def invitation_has_code(self, user_id, legacy_records=None):
        return InvitationRewardClaimSqlRepository(self.database).has_invitation_code(user_id, legacy_records)

    def invitation_bind(self, inviter_id, invited_id, legacy_records=None):
        return InvitationRewardClaimSqlRepository(self.database).bind(inviter_id, invited_id, legacy_records)

    def invitation_rewards(self, legacy_rewards=None):
        return InvitationRewardClaimSqlRepository(self.database).reward_definitions(legacy_rewards)

    def invitation_set_reward(self, threshold, reward_items, legacy_rewards=None):
        return InvitationRewardClaimSqlRepository(self.database).set_reward_definition(threshold, reward_items, legacy_rewards)

    def invitation_get_result(self, operation_id):
        return InvitationRewardClaimSqlRepository(self.database).get_result(operation_id)

    def invitation_claim(
        self,
        operation_id,
        user_id,
        invited_user_ids,
        rewards_by_threshold,
        requested_thresholds,
        legacy_claimed_thresholds,
        max_goods_num,
    ):
        return InvitationRewardClaimSqlRepository(self.database).claim(
            operation_id,
            user_id,
            invited_user_ids,
            rewards_by_threshold,
            requested_thresholds,
            legacy_claimed_thresholds,
            max_goods_num,
        )


__all__ = ["CompensationRepository"]
