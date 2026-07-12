from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class IllusionChoiceResult:
    status: str
    choice_count: int
    stone: int
    exp: int
    item_id: int


class IllusionChoiceService:
    """Record a daily choice, update statistics, and grant its reward atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def period_key(now: datetime | None = None) -> str:
        now = now or datetime.now()
        boundary = now.replace(hour=8, minute=0, second=0, microsecond=0)
        day = now.date() if now >= boundary else (now.date() - __import__("datetime").timedelta(days=1))
        return day.isoformat()

    def choose(
        self,
        operation_id,
        user_id,
        period_key,
        question_index,
        choice_index,
        selected_option,
        stone,
        exp,
        item,
        max_goods_num,
    ) -> IllusionChoiceResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        period_key = str(period_key)
        question_index = int(question_index)
        choice_index = int(choice_index)
        stone = int(stone)
        exp = int(exp)
        max_goods_num = int(max_goods_num)
        item_data = None if item is None else (
            int(item["id"]), str(item["name"]), str(item["type"]), int(item.get("amount", 1))
        )
        if not operation_id or not period_key or min(question_index, choice_index, stone, exp, max_goods_num) < 0:
            raise ValueError("valid operation, period, choice and reward are required")
        payload = json.dumps(
            [user_id, period_key, question_index, choice_index, selected_option, stone, exp, item_data, max_goods_num],
            ensure_ascii=True,
        )

        def result(status: str, count=0) -> IllusionChoiceResult:
            applied = status in {"applied", "duplicate"}
            return IllusionChoiceResult(status, int(count), stone if applied else 0, exp if applied else 0, item_data[0] if applied and item_data else 0)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS illusion_choices (user_id TEXT NOT NULL, period_key TEXT NOT NULL, "
                    "question_index INTEGER NOT NULL, choice_index INTEGER NOT NULL, selected_option TEXT NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id, period_key))"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS illusion_choice_stats (period_key TEXT NOT NULL, question_index INTEGER NOT NULL, "
                    "choice_index INTEGER NOT NULL, choice_count INTEGER NOT NULL, PRIMARY KEY(period_key, question_index, choice_index))"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS illusion_choice_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
                    "choice_count INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, choice_count FROM illusion_choice_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[1]) if str(previous[0]) == payload else result("state_changed")
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                if conn.execute(
                    "SELECT 1 FROM illusion_choices WHERE user_id=%s AND period_key=%s", (user_id, period_key)
                ).fetchone() is not None:
                    conn.rollback()
                    return result("already_chosen")
                if item_data:
                    inventory = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_data[0])
                    ).fetchone()
                    if (int(inventory[0]) if inventory else 0) + item_data[3] > max_goods_num:
                        conn.rollback()
                        return result("inventory_full")

                conn.execute(
                    "INSERT INTO illusion_choices (user_id, period_key, question_index, choice_index, selected_option) "
                    "VALUES (%s,%s,%s,%s,%s)", (user_id, period_key, question_index, choice_index, str(selected_option))
                )
                conn.execute(
                    "INSERT INTO illusion_choice_stats VALUES (%s,%s,%s,1) ON CONFLICT(period_key,question_index,choice_index) "
                    "DO UPDATE SET choice_count=illusion_choice_stats.choice_count+1",
                    (period_key, question_index, choice_index),
                )
                count = int(conn.execute(
                    "SELECT choice_count FROM illusion_choice_stats WHERE period_key=%s AND question_index=%s AND choice_index=%s",
                    (period_key, question_index, choice_index),
                ).fetchone()[0])
                conn.execute("UPDATE user_xiuxian SET stone=stone+%s, exp=exp+%s WHERE user_id=%s", (stone, exp, user_id))
                if item_data:
                    now = datetime.now()
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, "
                        "update_time=EXCLUDED.update_time",
                        (user_id, *item_data[:4], now, now, item_data[3]),
                    )
                conn.execute(
                    "INSERT INTO illusion_choice_operations VALUES (%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload, count)
                )
                conn.commit()
                return result("applied", count)
            except Exception:
                conn.rollback()
                raise

    def get_choice(self, user_id, period_key):
        with closing(db_backend.connect(self._database)) as conn:
            if not conn.table_exists("illusion_choices"):
                return None
            row = conn.execute(
                "SELECT question_index, choice_index, selected_option FROM illusion_choices WHERE user_id=%s AND period_key=%s",
                (str(user_id), str(period_key)),
            ).fetchone()
            return None if row is None else {"question_index": int(row[0]), "choice_index": int(row[1]), "selected_option": str(row[2])}


__all__ = ["IllusionChoiceResult", "IllusionChoiceService"]
