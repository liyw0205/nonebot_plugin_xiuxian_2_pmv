from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class IllusionChoiceResult:
    status: str
    choice_count: int = 0
    stone: int = 0
    exp: int = 0
    item_id: int = 0
    item_name: str = ""
    item_type: str = ""
    selected_option: str = ""
    question_index: int = 0
    choice_index: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class IllusionChoiceService:
    """Record a daily choice, update statistics, and grant its reward atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def period_key(now: datetime | None = None) -> str:
        now = now or datetime.now()
        boundary = now.replace(hour=8, minute=0, second=0, microsecond=0)
        day = now.date() if now >= boundary else (now.date() - timedelta(days=1))
        return day.isoformat()

    @staticmethod
    def _payload(user_id, period_key, question_index, choice_index) -> str:
        # Request identity only — random rewards are outcomes stored in result_json.
        return json.dumps(
            [str(user_id), str(period_key), int(question_index), int(choice_index)],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    def _ensure_ops(self, conn) -> None:
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
            "choice_count INTEGER NOT NULL, result_json TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(illusion_choice_operations)").fetchall()}
        if "result_json" not in cols:
            try:
                conn.execute("ALTER TABLE illusion_choice_operations ADD COLUMN result_json TEXT")
            except Exception:
                pass

    def _from_row(self, status: str, count: int, result_json: str | None) -> IllusionChoiceResult:
        data = {}
        if result_json:
            try:
                data = json.loads(result_json)
            except Exception:
                data = {}
        return IllusionChoiceResult(
            status,
            int(count),
            int(data.get("stone") or 0),
            int(data.get("exp") or 0),
            int(data.get("item_id") or 0),
            str(data.get("item_name") or ""),
            str(data.get("item_type") or ""),
            str(data.get("selected_option") or ""),
            int(data.get("question_index") or 0),
            int(data.get("choice_index") or 0),
        )

    def get_result(self, operation_id: str) -> IllusionChoiceResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_ops(conn)
            previous = conn.execute(
                "SELECT payload, choice_count, result_json FROM illusion_choice_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return self._from_row("duplicate", int(previous[1]), previous[2])

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
        selected_option = str(selected_option)
        item_data = None if item is None else (
            int(item["id"]), str(item["name"]), str(item["type"]), int(item.get("amount", 1))
        )
        if not operation_id or not period_key or min(question_index, choice_index, stone, exp, max_goods_num) < 0:
            raise ValueError("valid operation, period, choice and reward are required")
        payload = self._payload(user_id, period_key, question_index, choice_index)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_ops(conn)
                previous = conn.execute(
                    "SELECT payload, choice_count, result_json FROM illusion_choice_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return IllusionChoiceResult("state_changed")
                    return self._from_row("duplicate", int(previous[1]), previous[2])

                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return IllusionChoiceResult("user_missing")
                if conn.execute(
                    "SELECT 1 FROM illusion_choices WHERE user_id=%s AND period_key=%s", (user_id, period_key)
                ).fetchone() is not None:
                    conn.rollback()
                    return IllusionChoiceResult("already_chosen")
                if item_data:
                    inventory = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_data[0]),
                    ).fetchone()
                    if (int(inventory[0]) if inventory else 0) + item_data[3] > max_goods_num:
                        conn.rollback()
                        return IllusionChoiceResult("inventory_full")

                conn.execute(
                    "INSERT INTO illusion_choices (user_id, period_key, question_index, choice_index, selected_option) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (user_id, period_key, question_index, choice_index, selected_option),
                )
                conn.execute(
                    "INSERT INTO illusion_choice_stats VALUES (%s,%s,%s,1) ON CONFLICT(period_key,question_index,choice_index) "
                    "DO UPDATE SET choice_count=illusion_choice_stats.choice_count+1",
                    (period_key, question_index, choice_index),
                )
                count = int(
                    conn.execute(
                        "SELECT choice_count FROM illusion_choice_stats WHERE period_key=%s AND question_index=%s AND choice_index=%s",
                        (period_key, question_index, choice_index),
                    ).fetchone()[0]
                )
                conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s, exp=exp+%s WHERE user_id=%s",
                    (stone, exp, user_id),
                )
                if item_data:
                    now = datetime.now()
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, "
                        "update_time=EXCLUDED.update_time",
                        (user_id, *item_data[:4], now, now, item_data[3]),
                    )
                result_json = json.dumps(
                    {
                        "stone": stone,
                        "exp": exp,
                        "item_id": item_data[0] if item_data else 0,
                        "item_name": item_data[1] if item_data else "",
                        "item_type": item_data[2] if item_data else "",
                        "selected_option": selected_option,
                        "question_index": question_index,
                        "choice_index": choice_index,
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO illusion_choice_operations (operation_id,payload,choice_count,result_json) "
                    "VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, count, result_json),
                )
                conn.commit()
                return IllusionChoiceResult(
                    "applied",
                    count,
                    stone,
                    exp,
                    item_data[0] if item_data else 0,
                    item_data[1] if item_data else "",
                    item_data[2] if item_data else "",
                    selected_option,
                    question_index,
                    choice_index,
                )
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
            return (
                None
                if row is None
                else {
                    "question_index": int(row[0]),
                    "choice_index": int(row[1]),
                    "selected_option": str(row[2]),
                }
            )


__all__ = ["IllusionChoiceResult", "IllusionChoiceService"]
