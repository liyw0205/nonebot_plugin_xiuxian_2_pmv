from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Iterable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class RewardClaim:
    status: str
    reward_type: str
    record_id: str
    user_id: str

    @property
    def applied(self) -> bool:
        return self.status == "claimed"


class RewardClaimService:
    def __init__(
        self,
        database: str | Path,
        *,
        max_goods_num: int,
        lock: RLock | None = None,
    ) -> None:
        self._database = Path(database)
        self._max_goods_num = int(max_goods_num)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_claims(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reward_claims (
                reward_type TEXT NOT NULL,
                record_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (reward_type, record_id, user_id)
            )
            """
        )

    @staticmethod
    def _inventory_type(goods_type: str) -> str:
        if goods_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
            return "技能"
        if goods_type in {"法器", "防具"}:
            return "装备"
        return goods_type

    def has_claimed(self, reward_type, record_id, user_id) -> bool:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_claims(conn)
            row = conn.execute(
                "SELECT 1 FROM reward_claims "
                "WHERE reward_type=%s AND record_id=%s AND user_id=%s",
                (str(reward_type), str(record_id), str(user_id)),
            ).fetchone()
            conn.commit()
            return row is not None

    def claim(
        self,
        reward_type,
        record_id,
        user_id,
        reward_items: Iterable[dict[str, Any]],
    ) -> RewardClaim:
        reward_type = str(reward_type)
        record_id = str(record_id)
        user_id = str(user_id)
        normalized_items = list(reward_items)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_claims(conn)
                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return RewardClaim("user_missing", reward_type, record_id, user_id)
                previous = conn.execute(
                    "SELECT 1 FROM reward_claims "
                    "WHERE reward_type=%s AND record_id=%s AND user_id=%s",
                    (reward_type, record_id, user_id),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return RewardClaim("duplicate", reward_type, record_id, user_id)

                now = datetime.now().isoformat(sep=" ", timespec="seconds")
                for item in normalized_items:
                    quantity = max(int(item["quantity"]), 0)
                    if quantity <= 0:
                        continue
                    if item["type"] == "stone":
                        updated = conn.execute(
                            "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                            (quantity, user_id),
                        )
                        if updated.rowcount != 1:
                            raise db_backend.IntegrityError("reward user disappeared")
                        continue

                    goods_id = int(item["id"])
                    goods_type = self._inventory_type(str(item["type"]))
                    quantity = min(quantity, self._max_goods_num)
                    conn.execute(
                        """
                        INSERT INTO back (
                            user_id, goods_id, goods_name, goods_type, goods_num,
                            create_time, update_time, bind_num
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, goods_id) DO UPDATE SET
                            goods_name=EXCLUDED.goods_name,
                            goods_type=EXCLUDED.goods_type,
                            update_time=EXCLUDED.update_time,
                            goods_num=LEAST(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s),
                            bind_num=LEAST(
                                COALESCE(back.bind_num, 0)+EXCLUDED.goods_num,
                                LEAST(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s),
                                %s
                            )
                        """,
                        (
                            user_id,
                            goods_id,
                            str(item["name"]),
                            goods_type,
                            quantity,
                            now,
                            now,
                            quantity,
                            self._max_goods_num,
                            self._max_goods_num,
                            self._max_goods_num,
                        ),
                    )

                conn.execute(
                    "INSERT INTO reward_claims (reward_type, record_id, user_id) "
                    "VALUES (%s, %s, %s)",
                    (reward_type, record_id, user_id),
                )
                conn.commit()
                return RewardClaim("claimed", reward_type, record_id, user_id)
            except Exception:
                conn.rollback()
                raise

    def delete_claims(self, reward_type, record_id: str | None = None) -> None:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_claims(conn)
            if record_id is None:
                conn.execute(
                    "DELETE FROM reward_claims WHERE reward_type=%s",
                    (str(reward_type),),
                )
            else:
                conn.execute(
                    "DELETE FROM reward_claims WHERE reward_type=%s AND record_id=%s",
                    (str(reward_type), str(record_id)),
                )
            conn.commit()


__all__ = ["RewardClaim", "RewardClaimService"]
