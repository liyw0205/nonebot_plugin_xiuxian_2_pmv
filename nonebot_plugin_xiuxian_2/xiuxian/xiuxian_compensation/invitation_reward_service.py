from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class InvitationRewardClaimResult:
    status: str
    thresholds: tuple[int, ...] = ()
    invitation_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class InvitationRewardClaimService:
    """Claim one or more invitation milestones in one game-db transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_tables(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS invitation_reward_invites ("
            "inviter_id TEXT NOT NULL,invited_id TEXT NOT NULL,source TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(inviter_id,invited_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS invitation_reward_claims ("
            "user_id TEXT NOT NULL,threshold INTEGER NOT NULL,source TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(user_id,threshold))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS invitation_reward_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "thresholds_json TEXT NOT NULL,invitation_count INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _inventory_type(item_type: str) -> str:
        if item_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
            return "技能"
        if item_type in {"法器", "防具"}:
            return "装备"
        return item_type

    @classmethod
    def _normalize_rewards(cls, rewards_by_threshold):
        normalized = {}
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

    def claimed_thresholds(self, user_id) -> set[int]:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_tables(conn)
            rows = conn.execute(
                "SELECT threshold FROM invitation_reward_claims WHERE user_id=%s",
                (str(user_id),),
            ).fetchall()
            conn.commit()
            return {int(row[0]) for row in rows}

    def get_result(self, operation_id: str) -> InvitationRewardClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_tables(conn)
            previous = conn.execute(
                "SELECT payload,thresholds_json,invitation_count "
                "FROM invitation_reward_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            conn.commit()
            if previous is None:
                return None
            return InvitationRewardClaimResult(
                "duplicate",
                tuple(int(value) for value in json.loads(str(previous[1]))),
                int(previous[2]),
            )

    def claim(
        self,
        operation_id,
        user_id,
        invited_user_ids,
        rewards_by_threshold,
        requested_thresholds,
        legacy_claimed_thresholds,
        max_goods_num,
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
        payload = json.dumps(
            [user_id, requested],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_tables(conn)
                for invited_id in invited_ids:
                    conn.execute(
                        "INSERT INTO invitation_reward_invites(inviter_id,invited_id,source) "
                        "VALUES (%s,%s,'legacy_json') "
                        "ON CONFLICT(inviter_id,invited_id) DO NOTHING",
                        (user_id, invited_id),
                    )
                for threshold in legacy_claimed:
                    conn.execute(
                        "INSERT INTO invitation_reward_claims(user_id,threshold,source) "
                        "VALUES (%s,%s,'legacy_json') ON CONFLICT(user_id,threshold) DO NOTHING",
                        (user_id, threshold),
                    )

                previous = conn.execute(
                    "SELECT payload,thresholds_json,invitation_count "
                    "FROM invitation_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.commit()
                    if str(previous[0]) != payload:
                        return InvitationRewardClaimResult("operation_conflict")
                    return InvitationRewardClaimResult(
                        "duplicate",
                        tuple(int(value) for value in json.loads(str(previous[1]))),
                        int(previous[2]),
                    )

                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.commit()
                    return InvitationRewardClaimResult("user_missing")

                invitation_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM invitation_reward_invites WHERE inviter_id=%s",
                        (user_id,),
                    ).fetchone()[0]
                )
                claimed = {
                    int(row[0])
                    for row in conn.execute(
                        "SELECT threshold FROM invitation_reward_claims WHERE user_id=%s",
                        (user_id,),
                    ).fetchall()
                }
                eligible = tuple(
                    threshold
                    for threshold in requested
                    if threshold in rewards
                    and threshold <= invitation_count
                    and threshold not in claimed
                )
                if not eligible:
                    conn.commit()
                    return InvitationRewardClaimResult(
                        "no_available", invitation_count=invitation_count
                    )

                stone = 0
                items = {}
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
                    current = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(current[0]) if current else 0) + quantity > max_goods_num:
                        conn.rollback()
                        return InvitationRewardClaimResult(
                            "inventory_full", invitation_count=invitation_count
                        )

                now = datetime.now()
                if stone:
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET stone=COALESCE(stone,0)+%s "
                        "WHERE user_id=%s",
                        (stone, user_id),
                    )
                    if changed.rowcount != 1:
                        raise db_backend.IntegrityError("invitation reward user disappeared")
                for item_id, (name, item_type, quantity) in items.items():
                    conn.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                        "goods_num=back.goods_num+excluded.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                        "update_time=excluded.update_time",
                        (user_id, item_id, name, item_type, quantity, now, now, quantity),
                    )
                for threshold in eligible:
                    conn.execute(
                        "INSERT INTO invitation_reward_claims(user_id,threshold,source) "
                        "VALUES (%s,%s,'transaction')",
                        (user_id, threshold),
                    )
                conn.execute(
                    "INSERT INTO invitation_reward_operations(operation_id,payload,"
                    "thresholds_json,invitation_count) VALUES (%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        json.dumps(eligible, separators=(",", ":")),
                        invitation_count,
                    ),
                )
                conn.commit()
                return InvitationRewardClaimResult("applied", eligible, invitation_count)
            except Exception:
                conn.rollback()
                raise


__all__ = ["InvitationRewardClaimResult", "InvitationRewardClaimService"]
