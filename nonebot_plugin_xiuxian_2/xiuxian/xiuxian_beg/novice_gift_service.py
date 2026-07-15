from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class NoviceGiftClaimResult:
    status: str
    stone: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class NoviceGiftClaimService:
    """Claim every novice-gift asset and its eligibility flag atomically."""

    def __init__(
        self,
        database: str | Path,
        lock: RLock | None = None,
        failure_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()
        self._failure_hook = failure_hook

    @staticmethod
    def _parse_datetime(value) -> datetime:
        if isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        if not text:
            raise ValueError("create_time is required")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            for pattern in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(text, pattern)
                except ValueError:
                    continue
        raise ValueError("invalid create_time")

    @staticmethod
    def _canonical_create_time(value) -> str:
        return NoviceGiftClaimService._parse_datetime(value).isoformat(sep=" ")

    @staticmethod
    def _payload(user_id) -> str:
        # Request identity only — reward rolls/create_time snapshots are outcomes or concurrency checks.
        return json.dumps([str(user_id)], ensure_ascii=True, separators=(",", ":"))

    def _checkpoint(self, name: str) -> None:
        if self._failure_hook is not None:
            self._failure_hook(name)

    def get_result(self, operation_id: str) -> NoviceGiftClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS novice_gift_claim_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER "
                "NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,stone FROM novice_gift_claim_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return NoviceGiftClaimResult("duplicate", int(previous[1]))

    def claim(
        self,
        operation_id,
        user_id,
        expected_create_time,
        claimed_at,
        max_age_days,
        stone,
        rewards,
        max_goods_num,
    ) -> NoviceGiftClaimResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_create_time = self._canonical_create_time(expected_create_time)
        claimed_at = self._parse_datetime(claimed_at)
        max_age_days, stone, max_goods_num = map(
            int, (max_age_days, stone, max_goods_num)
        )

        totals: dict[int, list] = {}
        for reward in rewards:
            item_id = int(reward["id"])
            amount = int(reward["amount"])
            if amount <= 0:
                continue
            if item_id not in totals:
                totals[item_id] = [
                    str(reward["name"]), str(reward["type"]), 0
                ]
            elif totals[item_id][:2] != [str(reward["name"]), str(reward["type"])]:
                raise ValueError("conflicting reward metadata")
            totals[item_id][2] += amount
        reward_rows = tuple(
            (item_id, values[0], values[1], values[2])
            for item_id, values in sorted(totals.items())
        )
        if (
            not operation_id
            or max_age_days < 0
            or stone < 0
            or max_goods_num < 0
        ):
            raise ValueError("valid novice gift claim is required")

        payload = self._payload(user_id)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS novice_gift_claim_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER "
                    "NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,stone FROM novice_gift_claim_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    status = "duplicate" if str(previous[0]) == payload else "operation_conflict"
                    return NoviceGiftClaimResult(status, int(previous[1]))

                user = conn.execute(
                    "SELECT create_time,COALESCE(is_novice,0) FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return NoviceGiftClaimResult("user_missing")
                actual_create_time = self._canonical_create_time(user[0])
                if actual_create_time != expected_create_time:
                    conn.rollback()
                    return NoviceGiftClaimResult("state_changed")
                if int(user[1]) != 0:
                    conn.rollback()
                    return NoviceGiftClaimResult("already_claimed")
                if claimed_at > self._parse_datetime(user[0]) + timedelta(days=max_age_days):
                    conn.rollback()
                    return NoviceGiftClaimResult("expired")

                for item_id, _, _, amount in reward_rows:
                    current = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(current[0]) if current else 0) + amount > max_goods_num:
                        conn.rollback()
                        return NoviceGiftClaimResult("inventory_full")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET stone=COALESCE(stone,0)+%s,is_novice=1 "
                    "WHERE user_id=%s AND COALESCE(is_novice,0)=0 AND create_time=%s",
                    (stone, user_id, user[0]),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return NoviceGiftClaimResult("state_changed")
                self._checkpoint("after_user_update")

                now = datetime.now()
                for item_id, name, item_type, amount in reward_rows:
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,"
                        "goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,"
                        "goods_num=back.goods_num+EXCLUDED.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,"
                        "update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                self._checkpoint("after_rewards")
                conn.execute(
                    "INSERT INTO novice_gift_claim_operations "
                    "(operation_id,payload,stone) VALUES (%s,%s,%s)",
                    (operation_id, payload, stone),
                )
                conn.commit()
                return NoviceGiftClaimResult("applied", stone)
            except Exception:
                conn.rollback()
                raise


__all__ = ["NoviceGiftClaimResult", "NoviceGiftClaimService"]
