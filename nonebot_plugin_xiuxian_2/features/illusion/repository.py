from __future__ import annotations

import json
import random
from typing import Any, Callable, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import IllusionChoiceResult


class IllusionRepository:
    """SQLite repository for the daily illusion choice projection."""

    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        # The user projection is the feature owner for the daily question and
        # selected answer.  Keeping the current period here removes the
        # request-path JSON read/modify/write cycle from the legacy adapter.
        uow.execute(
            "CREATE TABLE IF NOT EXISTS illusion_user_state ("
            "user_id TEXT PRIMARY KEY, period_key TEXT NOT NULL, question_index INTEGER NOT NULL, "
            "today_choice TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        uow.execute(
            "CREATE TABLE IF NOT EXISTS illusion_legacy_imports (import_key TEXT PRIMARY KEY)"
        )
        uow.execute(
            "CREATE TABLE IF NOT EXISTS illusion_choices ("
            "user_id TEXT NOT NULL, period_key TEXT NOT NULL, question_index INTEGER NOT NULL, "
            "choice_index INTEGER NOT NULL, selected_option TEXT NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id, period_key))"
        )
        uow.execute(
            "CREATE TABLE IF NOT EXISTS illusion_choice_stats ("
            "period_key TEXT NOT NULL, question_index INTEGER NOT NULL, choice_index INTEGER NOT NULL, "
            "choice_count INTEGER NOT NULL, PRIMARY KEY(period_key, question_index, choice_index))"
        )
        uow.execute(
            "CREATE TABLE IF NOT EXISTS illusion_choice_operations ("
            "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, choice_count INTEGER NOT NULL, "
            "result_json TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        columns = {
            str(row[1])
            for row in uow.execute("PRAGMA table_info(illusion_choice_operations)").fetchall()
        }
        if "result_json" not in columns:
            uow.execute("ALTER TABLE illusion_choice_operations ADD COLUMN result_json TEXT")

    def get_or_create_state(
        self,
        uow: DatabaseUnitOfWork,
        *,
        user_id: str,
        period: str,
        question_count: int,
        legacy_loader: Callable[[], Mapping[str, Any] | None] | None = None,
    ) -> dict[str, Any]:
        """Return the current daily question state, assigning it once."""
        user_id = str(user_id)
        period = str(period)
        question_count = int(question_count)
        if not user_id or not period or question_count <= 0:
            raise ValueError("user_id, period and question_count are required")
        existing = self.get_state(uow, user_id, period)
        if existing is not None:
            return existing
        row = uow.execute(
            "SELECT period_key, question_index, today_choice FROM illusion_user_state WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        legacy_reset = uow.execute(
            "SELECT 1 FROM illusion_legacy_imports WHERE import_key = 'user-state-cleared'"
        ).fetchone()
        legacy = (
            dict(legacy_loader() or {})
            if row is None and legacy_reset is None and legacy_loader
            else {}
        )
        legacy_index = legacy.get("question_index")
        question_index = (
            int(legacy_index)
            if isinstance(legacy_index, int) and 0 <= legacy_index < question_count
            else random.randrange(question_count)
        )
        today_choice = legacy.get("today_choice")
        if today_choice is not None:
            today_choice = str(today_choice)
        now = self._timestamp(uow)
        uow.execute(
            "INSERT INTO illusion_user_state(user_id, period_key, question_index, today_choice, updated_at) "
            "VALUES (?, ?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET period_key=excluded.period_key, "
            "question_index=excluded.question_index, today_choice=excluded.today_choice, updated_at=excluded.updated_at",
            (user_id, period, question_index, today_choice, now),
        )
        return {"period": period, "question_index": question_index, "today_choice": today_choice}

    def get_state(self, uow: DatabaseUnitOfWork, user_id: str, period: str) -> dict[str, Any] | None:
        row = uow.execute(
            "SELECT period_key, question_index, today_choice FROM illusion_user_state WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
        if row is None or str(row[0]) != str(period):
            return None
        return {
            "period": str(row[0]),
            "question_index": int(row[1]),
            "today_choice": str(row[2]) if row[2] is not None else None,
        }

    def get_question_stats(
        self,
        uow: DatabaseUnitOfWork,
        *,
        period: str,
        question_index: int,
        option_count: int,
        legacy_loader: Callable[[], list[list[int]] | None] | None = None,
    ) -> list[int]:
        if legacy_loader is not None and not self.has_legacy_import(uow, "stats"):
            legacy_stats = legacy_loader()
            if legacy_stats:
                for legacy_question_index, legacy_counts in enumerate(legacy_stats):
                    for legacy_choice_index, legacy_count in enumerate(legacy_counts):
                        if int(legacy_count) > 0:
                            uow.execute(
                                "INSERT OR IGNORE INTO illusion_choice_stats(period_key, question_index, choice_index, choice_count) "
                                "VALUES (?, ?, ?, ?) ON CONFLICT(period_key, question_index, choice_index) "
                                "DO UPDATE SET choice_count = illusion_choice_stats.choice_count + excluded.choice_count",
                                (str(period), legacy_question_index, legacy_choice_index, int(legacy_count)),
                            )
            uow.execute("INSERT OR IGNORE INTO illusion_legacy_imports(import_key) VALUES ('stats')")
        rows = uow.execute(
            "SELECT choice_index, SUM(choice_count) FROM illusion_choice_stats "
            "WHERE period_key <= ? AND question_index = ? GROUP BY choice_index",
            (str(period), int(question_index)),
        ).fetchall()
        counts = [0] * max(0, int(option_count))
        for row in rows:
            index = int(row[0])
            if 0 <= index < len(counts):
                counts[index] = int(row[1])
        return counts

    @staticmethod
    def has_legacy_import(uow: DatabaseUnitOfWork, import_key: str) -> bool:
        return uow.execute(
            "SELECT 1 FROM illusion_legacy_imports WHERE import_key = ?",
            (str(import_key),),
        ).fetchone() is not None

    def clear(
        self,
        uow: DatabaseUnitOfWork,
        *,
        reset_stats: bool,
    ) -> dict[str, int]:
        """Clear feature-owned projections while retaining operation history."""
        choices = int(uow.execute("SELECT COUNT(*) FROM illusion_choices").fetchone()[0])
        operations = int(uow.execute("SELECT COUNT(*) FROM illusion_choice_operations").fetchone()[0])
        uow.execute("DELETE FROM illusion_choices")
        uow.execute("DELETE FROM illusion_user_state")
        if reset_stats:
            uow.execute("DELETE FROM illusion_choice_stats")
            uow.execute("INSERT OR IGNORE INTO illusion_legacy_imports(import_key) VALUES ('stats')")
        uow.execute(
            "INSERT OR IGNORE INTO illusion_legacy_imports(import_key) VALUES ('user-state-cleared')"
        )
        # Keep historical operation receipts.  The ledger makes replay safe
        # after a reset, and deleting audit history would weaken recovery.
        return {"choices": choices, "operations": operations, "stats_reset": int(reset_stats)}

    @staticmethod
    def _payload(user_id: str, period: str, question_index: int, choice_index: int) -> str:
        return json.dumps(
            [str(user_id), str(period), int(question_index), int(choice_index)],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _from_row(status: str, count: int, result_json: str | None) -> IllusionChoiceResult:
        data: Mapping[str, Any] = {}
        if result_json:
            try:
                loaded = json.loads(result_json)
                if isinstance(loaded, Mapping):
                    data = loaded
            except (TypeError, ValueError):
                data = {}
        return IllusionChoiceResult(
            status=status,
            choice_count=int(count),
            stone=int(data.get("stone") or 0),
            exp=int(data.get("exp") or 0),
            item_id=int(data.get("item_id") or 0),
            item_name=str(data.get("item_name") or ""),
            item_type=str(data.get("item_type") or ""),
            selected_option=str(data.get("selected_option") or ""),
            question_index=int(data.get("question_index") or 0),
            choice_index=int(data.get("choice_index") or 0),
        )

    def get_result(self, uow: DatabaseUnitOfWork, operation_id: str) -> IllusionChoiceResult | None:
        row = uow.execute(
            "SELECT choice_count, result_json FROM illusion_choice_operations WHERE operation_id = ?",
            (str(operation_id).strip(),),
        ).fetchone()
        if row is None:
            return None
        return self._from_row("duplicate", int(row[0]), row[1])

    def get_choice(self, uow: DatabaseUnitOfWork, user_id: str, period: str) -> dict[str, Any] | None:
        row = uow.execute(
            "SELECT question_index, choice_index, selected_option FROM illusion_choices "
            "WHERE user_id = ? AND period_key = ?",
            (str(user_id), str(period)),
        ).fetchone()
        if row is None:
            return None
        return {
            "question_index": int(row[0]),
            "choice_index": int(row[1]),
            "selected_option": str(row[2]),
        }

    def choose(
        self,
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        user_id: str,
        period: str,
        question_index: int,
        choice_index: int,
        selected_option: str,
        stone: int,
        exp: int,
        item: Mapping[str, Any] | None,
        max_goods_num: int,
    ) -> IllusionChoiceResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        period = str(period)
        question_index = int(question_index)
        choice_index = int(choice_index)
        stone = int(stone)
        exp = int(exp)
        max_goods_num = int(max_goods_num)
        selected_option = str(selected_option)
        item_data = None
        if item is not None:
            item_data = (
                int(item["id"]),
                str(item["name"]),
                str(item["type"]),
                int(item.get("amount", 1)),
            )
        if not operation_id or not period or min(question_index, choice_index, stone, exp, max_goods_num) < 0:
            raise ValueError("valid operation, period, choice and reward are required")
        payload = self._payload(user_id, period, question_index, choice_index)
        previous = uow.execute(
            "SELECT payload, choice_count, result_json FROM illusion_choice_operations WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        if previous is not None:
            if str(previous[0]) != payload:
                return IllusionChoiceResult("state_changed")
            return self._from_row("duplicate", int(previous[1]), previous[2])
        if uow.execute("SELECT 1 FROM user_xiuxian WHERE user_id = ?", (user_id,)).fetchone() is None:
            return IllusionChoiceResult("user_missing")
        if uow.execute(
            "SELECT 1 FROM illusion_choices WHERE user_id = ? AND period_key = ?",
            (user_id, period),
        ).fetchone() is not None:
            return IllusionChoiceResult("already_chosen")
        state = uow.execute(
            "SELECT period_key, question_index, today_choice FROM illusion_user_state WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if state is None:
            now = self._timestamp(uow)
            uow.execute(
                "INSERT INTO illusion_user_state(user_id, period_key, question_index, today_choice, updated_at) "
                "VALUES (?, ?, ?, NULL, ?)",
                (user_id, period, question_index, now),
            )
        elif str(state[0]) != period or int(state[1]) != question_index:
            return IllusionChoiceResult("state_changed")
        elif state[2] is not None:
            return IllusionChoiceResult("already_chosen")
        if item_data:
            row = uow.execute(
                "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id = ? AND goods_id = ?",
                (user_id, item_data[0]),
            ).fetchone()
            if (int(row[0]) if row else 0) + item_data[3] > max_goods_num:
                return IllusionChoiceResult("inventory_full")

        uow.execute(
            "INSERT INTO illusion_choices(user_id, period_key, question_index, choice_index, selected_option) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, period, question_index, choice_index, selected_option),
        )
        uow.execute(
            "UPDATE illusion_user_state SET today_choice = ?, updated_at = ? WHERE user_id = ?",
            (selected_option, self._timestamp(uow), user_id),
        )
        uow.execute(
            "INSERT INTO illusion_choice_stats(period_key, question_index, choice_index, choice_count) "
            "VALUES (?, ?, ?, 1) ON CONFLICT(period_key, question_index, choice_index) DO UPDATE SET "
            "choice_count = illusion_choice_stats.choice_count + 1",
            (period, question_index, choice_index),
        )
        count_row = uow.execute(
            "SELECT choice_count FROM illusion_choice_stats WHERE period_key = ? AND question_index = ? "
            "AND choice_index = ?",
            (period, question_index, choice_index),
        ).fetchone()
        count = int(count_row[0])
        uow.execute(
            "UPDATE user_xiuxian SET stone = CAST(COALESCE(stone, 0) AS REAL) + ?, "
            "exp = CAST(COALESCE(exp, 0) AS REAL) + ? WHERE user_id = ?",
            (stone, exp, user_id),
        )
        if item_data:
            now = self._timestamp(uow)
            uow.execute(
                "INSERT INTO back(user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(user_id, goods_id) DO UPDATE SET "
                "goods_num = back.goods_num + excluded.goods_num, bind_num = COALESCE(back.bind_num, 0) + excluded.goods_num, "
                "update_time = excluded.update_time",
                (user_id, *item_data[:4], now, now, item_data[3]),
            )
        result = {
            "stone": stone,
            "exp": exp,
            "item_id": item_data[0] if item_data else 0,
            "item_name": item_data[1] if item_data else "",
            "item_type": item_data[2] if item_data else "",
            "selected_option": selected_option,
            "question_index": question_index,
            "choice_index": choice_index,
        }
        uow.execute(
            "INSERT INTO illusion_choice_operations(operation_id, payload, choice_count, result_json) VALUES (?, ?, ?, ?)",
            (operation_id, payload, count, json.dumps(result, ensure_ascii=True, separators=(",", ":"))),
        )
        return IllusionChoiceResult("applied", count, **result)

    @staticmethod
    def _timestamp(uow: DatabaseUnitOfWork) -> str:
        row = uow.execute("SELECT CURRENT_TIMESTAMP").fetchone()
        return str(row[0])


__all__ = ["IllusionRepository"]
