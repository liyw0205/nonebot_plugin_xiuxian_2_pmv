from __future__ import annotations

import json
from pathlib import Path

from .._service_port import ServicePort
from ...infrastructure.database import DatabaseUnitOfWork
from .definition_repository import CompensationDefinitionSqlRepository
from .invitation_repository import InvitationRewardClaimSqlRepository
from .reward_claim_repository import CompensationRewardClaimSqlRepository
from .reward_definition_repository import CompensationRewardDefinitionSqlRepository


class RewardCenterSqlRepository:
    """Read reward definitions and their claim counters in one snapshot."""

    _DEFINITION_TABLES = {
        "补偿": "compensation_definitions",
        "礼包": "compensation_reward_definitions",
        "兑换码": "compensation_reward_definitions",
    }

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    def list_records(self, reward_type: str) -> list[dict]:
        reward_type = str(reward_type).strip()
        table = self._DEFINITION_TABLES.get(reward_type)
        if table is None:
            raise ValueError("发放类型无效")
        if not self.database.is_file():
            raise RuntimeError("compensation reward definition database is missing")

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if reward_type == "补偿":
                ready = CompensationDefinitionSqlRepository._schema_ready(uow)
                missing_message = "compensation definition schema is missing"
                predicate = ""
                params = (reward_type, reward_type)
            else:
                ready = CompensationRewardDefinitionSqlRepository._schema_ready(uow)
                missing_message = "compensation reward definition schema is missing"
                predicate = "WHERE definitions.reward_type=?"
                params = (reward_type, reward_type, reward_type)
            if not ready:
                raise RuntimeError(missing_message)

            rows = uow.query_all(
                f"SELECT definitions.record_id,definitions.version,definitions.record_json,"
                "COALESCE(counters.baseline_count,0) AS baseline_count,"
                "COALESCE(claims.claimed_count,0) AS claimed_count "
                f"FROM {table} AS definitions "
                "LEFT JOIN (SELECT record_id,MAX(baseline_count) AS baseline_count "
                "FROM reward_claim_counters WHERE reward_type=? "
                "GROUP BY record_id) AS counters "
                "ON counters.record_id=definitions.record_id "
                "LEFT JOIN (SELECT record_id,COUNT(*) AS claimed_count "
                "FROM reward_claims WHERE reward_type=? "
                "GROUP BY record_id) AS claims "
                "ON claims.record_id=definitions.record_id "
                f"{predicate} ORDER BY definitions.record_id",
                params,
            )
            result = []
            for row in rows:
                record = json.loads(str(row["record_json"]))
                record["_definition_version"] = int(row["version"])
                legacy_used = max(int(record.get("used_count") or 0), 0)
                claimed_count = int(row["claimed_count"])
                result.append(
                    {
                        "id": str(row["record_id"]),
                        "record": record,
                        "used_count": max(int(row["baseline_count"]), legacy_used)
                        + claimed_count,
                        "claimed_count": claimed_count,
                    }
                )
            return result


class CompensationRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("compensation", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_compensation")
        self.database = str(database)
        self.compensation_definitions = CompensationDefinitionSqlRepository(database)
        self.reward_definitions = CompensationRewardDefinitionSqlRepository(database)
        self.reward_center = RewardCenterSqlRepository(database)

    def reward_center_records(self, reward_type):
        return self.reward_center.list_records(reward_type)

    def list_compensation_definitions(self):
        return self.compensation_definitions.list_definitions()

    def get_compensation_definition(self, record_id):
        return self.compensation_definitions.get_definition(record_id)

    def compensation_claimed_data(self):
        return self.compensation_definitions.claimed_data()

    def compensation_catalog_version(self):
        return self.compensation_definitions.catalog_version()

    def replay_compensation_definition_upsert(self, operation_id, request_identity):
        return self.compensation_definitions.replay_upsert(
            operation_id, request_identity
        )

    def upsert_compensation_definition(
        self, operation_id, request_identity, record_id, record, expected_version=None
    ):
        return self.compensation_definitions.upsert(
            operation_id,
            request_identity,
            record_id,
            record,
            expected_version,
        )

    def delete_compensation_definition(
        self, operation_id, record_id, expected_version=None
    ):
        return self.compensation_definitions.delete(
            operation_id, record_id, expected_version
        )

    def clear_compensation_definitions(self, operation_id, expected_catalog_version):
        return self.compensation_definitions.clear(
            operation_id, expected_catalog_version
        )

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

    def invitation_count(self, inviter_id):
        return InvitationRewardClaimSqlRepository(self.database).invitation_count(inviter_id)

    def invitation_inviter_id(self, user_id):
        return InvitationRewardClaimSqlRepository(self.database).inviter_id(user_id)

    def invitation_has_code(self, user_id):
        return InvitationRewardClaimSqlRepository(self.database).has_invitation_code(user_id)

    def invitation_bind(self, inviter_id, invited_id):
        return InvitationRewardClaimSqlRepository(self.database).bind(inviter_id, invited_id)

    def invitation_rewards(self):
        return InvitationRewardClaimSqlRepository(self.database).reward_definitions()

    def invitation_set_reward(self, threshold, reward_items):
        return InvitationRewardClaimSqlRepository(self.database).set_reward_definition(threshold, reward_items)

    def invitation_get_result(self, operation_id):
        return InvitationRewardClaimSqlRepository(self.database).get_result(operation_id)

    def invitation_claim(
        self,
        operation_id,
        user_id,
        rewards_by_threshold,
        requested_thresholds,
        max_goods_num,
    ):
        return InvitationRewardClaimSqlRepository(self.database).claim(
            operation_id,
            user_id,
            rewards_by_threshold,
            requested_thresholds,
            max_goods_num,
        )


__all__ = ["CompensationRepository"]
