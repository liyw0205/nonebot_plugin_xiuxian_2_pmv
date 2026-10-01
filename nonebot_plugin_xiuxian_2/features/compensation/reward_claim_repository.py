from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.database import OperationLedger
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class CompensationRewardClaimResult:
    status: str
    reward_type: str = ""
    record_id: str = ""
    user_id: str = ""
    used_count: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "claimed"


@dataclass(frozen=True)
class CompensationRewardClaimsDeleteResult:
    status: str
    reward_type: str = ""
    record_id: str | None = None
    deleted_claims: int = 0
    deleted_counters: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "replayed"}


class CompensationRewardClaimSqlRepository:
    def __init__(
        self,
        database: str | Path,
        max_goods_num: int,
        *,
        ledger: OperationLedger | None = None,
    ) -> None:
        self.database = str(database)
        self.max_goods_num = int(max_goods_num)
        self.ledger = ledger or OperationLedger()

    @staticmethod
    def _inventory_type(goods_type: str) -> str:
        if goods_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
            return "技能"
        if goods_type in {"法器", "防具"}:
            return "装备"
        return goods_type

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('reward_claims','reward_claim_counters')"
            )
        }
        return tables == {"reward_claims", "reward_claim_counters"}

    def get_used_count(
        self, reward_type: str, record_id: str, legacy_used_count: int = 0
    ) -> int:
        legacy_used_count = max(int(legacy_used_count or 0), 0)
        if not Path(self.database).is_file():
            return legacy_used_count
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return legacy_used_count
            counter = uow.query_one(
                "SELECT baseline_count FROM reward_claim_counters "
                "WHERE reward_type=? AND record_id=?",
                (str(reward_type), str(record_id)),
            )
            claimed = int(
                uow.execute(
                    "SELECT COUNT(*) FROM reward_claims WHERE reward_type=? AND record_id=?",
                    (str(reward_type), str(record_id)),
                ).fetchone()[0]
            )
            baseline = 0 if counter is None else int(counter["baseline_count"])
            return max(baseline, legacy_used_count) + claimed

    def has_claimed(self, reward_type: str, record_id: str, user_id: str) -> bool:
        if not Path(self.database).is_file():
            return False
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return False
            return (
                uow.query_one(
                    "SELECT 1 AS found FROM reward_claims "
                    "WHERE reward_type=? AND record_id=? AND user_id=?",
                    (str(reward_type), str(record_id), str(user_id)),
                )
                is not None
            )

    def delete_claims(
        self,
        operation_id: str,
        reward_type: str,
        record_id: str | None = None,
    ) -> CompensationRewardClaimsDeleteResult:
        operation_id = str(operation_id).strip()
        reward_type = str(reward_type).strip()
        record_id = None if record_id is None else str(record_id)
        if not operation_id or not reward_type:
            raise ValueError("operation_id and reward_type are required")
        if not Path(self.database).is_file():
            return CompensationRewardClaimsDeleteResult(
                "schema_missing", reward_type, record_id
            )

        action = "compensation.delete_reward_claims"
        payload = {"reward_type": reward_type, "record_id": record_id}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            tables = {
                str(row["name"])
                for row in uow.query_all(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name IN ('operation_ledger','operation_audit')"
                )
            }
            if not self._schema_ready(uow) or tables != {
                "operation_ledger",
                "operation_audit",
            }:
                return CompensationRewardClaimsDeleteResult(
                    "schema_missing", reward_type, record_id
                )

            try:
                existing = self.ledger.begin(uow, operation_id, action, payload)
            except OperationConflictError:
                return CompensationRewardClaimsDeleteResult(
                    "operation_conflict", reward_type, record_id
                )
            if existing is not None:
                previous = existing.outcome()
                if previous is None:
                    return CompensationRewardClaimsDeleteResult(
                        "in_progress", reward_type, record_id
                    )
                saved = dict(previous.data or {})
                return CompensationRewardClaimsDeleteResult(
                    "replayed" if previous.ok else previous.status,
                    reward_type,
                    record_id,
                    int(saved.get("deleted_claims", 0)),
                    int(saved.get("deleted_counters", 0)),
                )

            predicate = "reward_type=?"
            params: tuple[Any, ...] = (reward_type,)
            if record_id is not None:
                predicate += " AND record_id=?"
                params = (reward_type, record_id)
            deleted_claims = int(
                uow.execute(
                    f"SELECT COUNT(*) FROM reward_claims WHERE {predicate}", params
                ).fetchone()[0]
            )
            deleted_counters = int(
                uow.execute(
                    f"SELECT COUNT(*) FROM reward_claim_counters WHERE {predicate}",
                    params,
                ).fetchone()[0]
            )
            uow.execute(f"DELETE FROM reward_claims WHERE {predicate}", params)
            uow.execute(
                f"DELETE FROM reward_claim_counters WHERE {predicate}", params
            )
            result = CompensationRewardClaimsDeleteResult(
                "applied",
                reward_type,
                record_id,
                deleted_claims,
                deleted_counters,
            )
            self.ledger.finish(
                uow,
                OperationOutcome.applied(
                    operation_id,
                    action,
                    data={
                        "reward_type": reward_type,
                        "record_id": record_id,
                        "deleted_claims": deleted_claims,
                        "deleted_counters": deleted_counters,
                    },
                    audit_category="compensation",
                    after={
                        "deleted_claims": deleted_claims,
                        "deleted_counters": deleted_counters,
                    },
                ),
            )
            return result

    def claim(
        self,
        operation_id: str,
        reward_type: str,
        record_id: str,
        user_id: str,
        reward_items: Iterable[dict[str, Any]],
        *,
        usage_limit: int = 0,
        legacy_used_count: int = 0,
        expected_definition_version: int | None = None,
    ) -> CompensationRewardClaimResult:
        operation_id = str(operation_id)
        reward_type = str(reward_type)
        record_id = str(record_id)
        user_id = str(user_id)
        usage_limit = max(int(usage_limit or 0), 0)
        legacy_used_count = max(int(legacy_used_count or 0), 0)
        expected_version = (
            None
            if expected_definition_version in (None, "")
            else int(expected_definition_version)
        )

        if not Path(self.database).is_file():
            return CompensationRewardClaimResult(
                "schema_missing", reward_type, record_id, user_id
            )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return CompensationRewardClaimResult(
                    "schema_missing", reward_type, record_id, user_id
                )
            if expected_version is not None:
                definition = uow.query_one(
                    "SELECT version FROM compensation_definitions WHERE record_id=?",
                    (record_id,),
                )
                if definition is None:
                    return CompensationRewardClaimResult(
                        "record_missing", reward_type, record_id, user_id
                    )
                if int(definition["version"]) != expected_version:
                    return CompensationRewardClaimResult(
                        "definition_changed", reward_type, record_id, user_id
                    )

            if uow.query_one(
                "SELECT 1 AS found FROM user_xiuxian WHERE user_id=?", (user_id,)
            ) is None:
                return CompensationRewardClaimResult(
                    "user_missing", reward_type, record_id, user_id
                )

            if uow.query_one(
                "SELECT 1 AS found FROM reward_claims "
                "WHERE reward_type=? AND record_id=? AND user_id=?",
                (reward_type, record_id, user_id),
            ) is not None:
                used_count = self._used_count_in_uow(uow, reward_type, record_id)
                return CompensationRewardClaimResult(
                    "duplicate", reward_type, record_id, user_id, used_count
                )

            if usage_limit:
                uow.execute(
                    "INSERT INTO reward_claim_counters(reward_type,record_id,baseline_count) "
                    "VALUES(?,?,?) ON CONFLICT(reward_type,record_id) DO NOTHING",
                    (reward_type, record_id, legacy_used_count),
                )
                used_count = self._used_count_in_uow(uow, reward_type, record_id)
                if used_count >= usage_limit:
                    return CompensationRewardClaimResult(
                        "exhausted", reward_type, record_id, user_id, used_count
                    )

            now = datetime.now().isoformat(sep=" ", timespec="seconds")
            for item in reward_items:
                quantity = max(int(item["quantity"]), 0)
                if quantity <= 0:
                    continue
                if item["type"] == "stone":
                    updated = uow.execute(
                        "UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? "
                        "WHERE user_id=?",
                        (quantity, user_id),
                    )
                    if updated.rowcount != 1:
                        raise RuntimeError("reward user disappeared")
                    continue

                goods_id = int(item["id"])
                goods_type = self._inventory_type(str(item["type"]))
                quantity = min(quantity, self.max_goods_num)
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                    "create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                    "update_time=excluded.update_time,"
                    "goods_num=MIN(COALESCE(back.goods_num,0)+excluded.goods_num,?),"
                    "bind_num=MIN(COALESCE(back.bind_num,0)+excluded.goods_num,"
                    "MIN(COALESCE(back.goods_num,0)+excluded.goods_num,?))",
                    (
                        user_id,
                        goods_id,
                        str(item["name"]),
                        goods_type,
                        quantity,
                        now,
                        now,
                        quantity,
                        self.max_goods_num,
                        self.max_goods_num,
                    ),
                )

            uow.execute(
                "INSERT INTO reward_claims(reward_type,record_id,user_id,created_at) "
                "VALUES(?,?,?,CURRENT_TIMESTAMP)",
                (reward_type, record_id, user_id),
            )
            used_count = self._used_count_in_uow(uow, reward_type, record_id)
            return CompensationRewardClaimResult(
                "claimed", reward_type, record_id, user_id, used_count
            )

    @staticmethod
    def _used_count_in_uow(
        uow: DatabaseUnitOfWork, reward_type: str, record_id: str
    ) -> int:
        counter = uow.query_one(
            "SELECT baseline_count FROM reward_claim_counters "
            "WHERE reward_type=? AND record_id=?",
            (reward_type, record_id),
        )
        claimed = int(
            uow.execute(
                "SELECT COUNT(*) FROM reward_claims WHERE reward_type=? AND record_id=?",
                (reward_type, record_id),
            ).fetchone()[0]
        )
        return (0 if counter is None else int(counter["baseline_count"])) + claimed


__all__ = ["CompensationRewardClaimSqlRepository", "CompensationRewardClaimResult"]
