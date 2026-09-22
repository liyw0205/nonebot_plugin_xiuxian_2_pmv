from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort
from .reward_claim_repository import CompensationRewardClaimSqlRepository


class CompensationRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("compensation", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_compensation")
        self.database = str(database)

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

    def has_claimed(self, reward_type, record_id, user_id) -> bool:
        return CompensationRewardClaimSqlRepository(self.database, 0).has_claimed(
            reward_type, record_id, user_id
        )

    def get_used_count(self, reward_type, record_id) -> int:
        return CompensationRewardClaimSqlRepository(self.database, 0).get_used_count(
            reward_type, record_id
        )


__all__ = ["CompensationRepository"]
