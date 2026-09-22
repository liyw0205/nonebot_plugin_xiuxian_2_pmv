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
        expected_definition_version=None,
    ):
        return CompensationRewardClaimSqlRepository(self.database, max_goods_num).claim(
            operation_id,
            reward_type,
            record_id,
            user_id,
            reward_items,
            expected_definition_version=expected_definition_version,
        )


__all__ = ["CompensationRepository"]
