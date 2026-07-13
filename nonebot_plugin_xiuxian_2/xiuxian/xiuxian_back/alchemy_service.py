from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AlchemyResult:
    status: str
    user_id: str
    reward_stone: int
    consumed: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AlchemyService:
    """Atomically consume an alchemy batch and grant its stone reward."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alchemy_operations (
                operation_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                user_id TEXT NOT NULL,
                reward_stone INTEGER NOT NULL,
                consumed TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(self, operation_id, user_id, reward_stone, consume_items) -> AlchemyResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        reward_stone = int(reward_stone)
        normalized: dict[int, int] = {}
        for goods_id, quantity in consume_items:
            goods_id = int(goods_id)
            quantity = int(quantity)
            if quantity <= 0:
                raise ValueError("alchemy quantity must be positive")
            normalized[goods_id] = normalized.get(goods_id, 0) + quantity
        consumed = tuple(sorted(normalized.items()))
        if reward_stone <= 0 or not consumed:
            raise ValueError("alchemy reward and consumed items must be positive")
        payload = json.dumps(
            [user_id, reward_stone, consumed], separators=(",", ":"), ensure_ascii=True
        )

        def result(status: str) -> AlchemyResult:
            return AlchemyResult(status, user_id, reward_stone, consumed)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT payload FROM alchemy_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate" if str(previous[0]) == payload else "conflict")

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")

                back_columns = set(conn.column_names("back"))
                for goods_id, quantity in consumed:
                    row = conn.execute(
                        "SELECT goods_num, state FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, goods_id),
                    ).fetchone()
                    if row is None or int(row[0] or 0) - int(row[1] or 0) < quantity:
                        conn.rollback()
                        return result("item_insufficient")

                for goods_id, quantity in consumed:
                    updates = ["goods_num=goods_num-%s"]
                    params: list[object] = [quantity]
                    if "bind_num" in back_columns:
                        updates.append("bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s)")
                        params.append(quantity)
                    if "update_time" in back_columns:
                        updates.append("update_time=CURRENT_TIMESTAMP")
                    if "action_time" in back_columns:
                        updates.append("action_time=CURRENT_TIMESTAMP")
                    updated = conn.execute(
                        f"UPDATE back SET {', '.join(updates)} "
                        "WHERE user_id=%s AND goods_id=%s "
                        "AND COALESCE(goods_num, 0)-COALESCE(state, 0)>=%s",
                        (*params, user_id, goods_id, quantity),
                    )
                    if updated.rowcount != 1:
                        conn.rollback()
                        return result("item_insufficient")

                granted = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                    (reward_stone, user_id),
                )
                if granted.rowcount != 1:
                    conn.rollback()
                    return result("user_missing")
                conn.execute(
                    "INSERT INTO alchemy_operations "
                    "(operation_id,payload,user_id,reward_stone,consumed) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, payload, user_id, reward_stone, json.dumps(consumed)),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["AlchemyResult", "AlchemyService"]
