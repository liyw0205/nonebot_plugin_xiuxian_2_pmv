from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class InvitationRewardClaimResult:
    status: str
    thresholds: tuple[int, ...] = ()
    invitation_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class InvitationRewardClaimSqlRepository:
    """Own invitation reward receipts while preserving legacy JSON inputs."""

    _TABLES = frozenset(
        {
            "invitation_reward_invites",
            "invitation_reward_claims",
            "invitation_reward_operations",
        }
    )

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('invitation_reward_invites',"
                "'invitation_reward_claims','invitation_reward_operations')"
            )
        }
        return tables == cls._TABLES

    @staticmethod
    def _inventory_type(item_type: str) -> str:
        if item_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
            return "技能"
        if item_type in {"法器", "防具"}:
            return "装备"
        return item_type

    @classmethod
    def _normalize_rewards(
        cls, rewards_by_threshold: Mapping[Any, Iterable[Mapping[str, Any]]]
    ) -> dict[int, tuple[tuple[str, Any, str, str, int], ...]]:
        normalized: dict[int, tuple[tuple[str, Any, str, str, int], ...]] = {}
        for raw_threshold, rewards in rewards_by_threshold.items():
            threshold = int(raw_threshold)
            if threshold <= 0:
                raise ValueError("invitation threshold must be positive")
            rows = []
            for reward in rewards:
                quantity = int(reward["quantity"])
                if quantity <= 0:
                    raise ValueError("reward quantity must be positive")
                if str(reward["type"]) == "stone":
                    rows.append(("stone", "stone", "灵石", "stone", quantity))
                    continue
                rows.append(
                    (
                        "item",
                        int(reward["id"]),
                        str(reward["name"]),
                        cls._inventory_type(str(reward["type"])),
                        quantity,
                    )
                )
            normalized[threshold] = tuple(rows)
        return normalized

    def claimed_thresholds(self, user_id: str) -> set[int]:
        if not self.database.is_file():
            return set()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return set()
            rows = uow.query_all(
                "SELECT threshold FROM invitation_reward_claims WHERE user_id=?",
                (str(user_id),),
            )
            return {int(row["threshold"]) for row in rows}

    def get_result(self, operation_id: str) -> InvitationRewardClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id or not self.database.is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            previous = uow.query_one(
                "SELECT payload,thresholds_json,invitation_count "
                "FROM invitation_reward_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is None:
                return None
            return InvitationRewardClaimResult(
                "duplicate",
                tuple(int(value) for value in json.loads(str(previous["thresholds_json"]))),
                int(previous["invitation_count"]),
            )

    def claim(
        self,
        operation_id: str,
        user_id: str,
        invited_user_ids: Iterable[Any],
        rewards_by_threshold: Mapping[Any, Iterable[Mapping[str, Any]]],
        requested_thresholds: Iterable[Any],
        legacy_claimed_thresholds: Iterable[Any],
        max_goods_num: int,
    ) -> InvitationRewardClaimResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        max_goods_num = int(max_goods_num)
        if not operation_id or max_goods_num < 0:
            raise ValueError("valid invitation reward claim is required")

        invited_ids = tuple(
            sorted(
                {
                    str(invited_id).strip()
                    for invited_id in invited_user_ids
                    if str(invited_id).strip() and str(invited_id).strip() != user_id
                }
            )
        )
        rewards = self._normalize_rewards(rewards_by_threshold)
        requested = tuple(sorted({int(value) for value in requested_thresholds}))
        legacy_claimed = tuple(
            sorted({int(value) for value in legacy_claimed_thresholds if int(value) > 0})
        )
        payload = json.dumps([user_id, requested], ensure_ascii=True, separators=(",", ":"))

        if not self.database.is_file():
            return InvitationRewardClaimResult("schema_missing")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return InvitationRewardClaimResult("schema_missing")

            for invited_id in invited_ids:
                uow.execute(
                    "INSERT INTO invitation_reward_invites(inviter_id,invited_id,source) "
                    "VALUES(?,?,?) ON CONFLICT(inviter_id,invited_id) DO NOTHING",
                    (user_id, invited_id, "legacy_json"),
                )
            for threshold in legacy_claimed:
                uow.execute(
                    "INSERT INTO invitation_reward_claims(user_id,threshold,source) "
                    "VALUES(?,?,?) ON CONFLICT(user_id,threshold) DO NOTHING",
                    (user_id, threshold, "legacy_json"),
                )

            previous = uow.query_one(
                "SELECT payload,thresholds_json,invitation_count "
                "FROM invitation_reward_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return InvitationRewardClaimResult("operation_conflict")
                return InvitationRewardClaimResult(
                    "duplicate",
                    tuple(int(value) for value in json.loads(str(previous["thresholds_json"]))),
                    int(previous["invitation_count"]),
                )

            user = uow.query_one(
                "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if user is None:
                return InvitationRewardClaimResult("user_missing")

            invitation_count = int(
                uow.execute(
                    "SELECT COUNT(*) FROM invitation_reward_invites WHERE inviter_id=?",
                    (user_id,),
                ).fetchone()[0]
            )
            claimed = {
                int(row["threshold"])
                for row in uow.query_all(
                    "SELECT threshold FROM invitation_reward_claims WHERE user_id=?",
                    (user_id,),
                )
            }
            eligible = tuple(
                threshold
                for threshold in requested
                if threshold in rewards
                and threshold <= invitation_count
                and threshold not in claimed
            )
            if not eligible:
                return InvitationRewardClaimResult(
                    "no_available", invitation_count=invitation_count
                )

            stone = 0
            items: dict[int, list[Any]] = {}
            for threshold in eligible:
                for kind, item_id, name, item_type, quantity in rewards[threshold]:
                    if kind == "stone":
                        stone += quantity
                        continue
                    metadata = [name, item_type]
                    if item_id in items and items[item_id][:2] != metadata:
                        raise ValueError("conflicting reward metadata")
                    items.setdefault(item_id, metadata + [0])[2] += quantity

            for item_id, (_, _, quantity) in items.items():
                current = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                    "WHERE user_id=? AND goods_id=?",
                    (user_id, item_id),
                )
                if (int(current["goods_num"]) if current else 0) + quantity > max_goods_num:
                    return InvitationRewardClaimResult(
                        "inventory_full", invitation_count=invitation_count
                    )

            now = datetime.now()
            if stone:
                changed = uow.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+? "
                    "WHERE user_id=?",
                    (stone, user_id),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("invitation reward user disappeared")
            for item_id, (name, item_type, quantity) in items.items():
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                    "create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                    "goods_num=back.goods_num+excluded.goods_num,"
                    "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                    "update_time=excluded.update_time",
                    (user_id, item_id, name, item_type, quantity, now, now, quantity),
                )
            for threshold in eligible:
                uow.execute(
                    "INSERT INTO invitation_reward_claims(user_id,threshold,source) "
                    "VALUES(?,?,?)",
                    (user_id, threshold, "transaction"),
                )
            uow.execute(
                "INSERT INTO invitation_reward_operations(operation_id,payload,"
                "thresholds_json,invitation_count) VALUES(?,?,?,?)",
                (
                    operation_id,
                    payload,
                    json.dumps(eligible, separators=(",", ":")),
                    invitation_count,
                ),
            )
            return InvitationRewardClaimResult("applied", eligible, invitation_count)


__all__ = ["InvitationRewardClaimResult", "InvitationRewardClaimSqlRepository"]
