from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

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


class CompensationRewardClaimSqlRepository:
    def __init__(self, database: str | Path, max_goods_num: int) -> None:
        self.database = str(database)
        self.max_goods_num = int(max_goods_num)

    @staticmethod
    def _inventory_type(goods_type: str) -> str:
        if goods_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
            return "技能"
        if goods_type in {"法器", "防具"}:
            return "装备"
        return goods_type

    @staticmethod
    def _ensure_schema(uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            "CREATE TABLE IF NOT EXISTS reward_claims("
            "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,user_id TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(reward_type,record_id,user_id))"
        )
        uow.execute(
            "CREATE TABLE IF NOT EXISTS reward_claim_counters("
            "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,"
            "baseline_count INTEGER NOT NULL DEFAULT 0,"
            "PRIMARY KEY(reward_type,record_id))"
        )

    def get_used_count(self, reward_type: str, record_id: str) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            self._ensure_schema(uow)
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
            return (0 if counter is None else int(counter["baseline_count"])) + claimed

    def has_claimed(self, reward_type: str, record_id: str, user_id: str) -> bool:
        with DatabaseUnitOfWork(self.database) as uow:
            self._ensure_schema(uow)
            return (
                uow.query_one(
                    "SELECT 1 AS found FROM reward_claims "
                    "WHERE reward_type=? AND record_id=? AND user_id=?",
                    (str(reward_type), str(record_id), str(user_id)),
                )
                is not None
            )

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

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._ensure_schema(uow)
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
