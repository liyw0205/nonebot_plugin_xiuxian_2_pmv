from __future__ import annotations

import random
from datetime import datetime
from typing import Any

from ..xiuxian_utils.json_store import safe_json_dumps as _json_dumps
from ..xiuxian_utils.json_store import safe_json_loads
from ..xiuxian_utils.periods import get_daily_key
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _json_loads(text: str | None) -> dict[str, Any]:
    return safe_json_loads(text, {}, dict)


class SectTaskStateManager:
    table_name = "sect_task_state"

    def __init__(self):
        self.sql_message = XiuxianDateManage()
        self.ensure_table()

    def ensure_table(self) -> None:
        with self.sql_message.lock:
            cur = self.sql_message.conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sect_task_state (
                    user_id TEXT NOT NULL,
                    sect_id INTEGER NOT NULL,
                    task_key TEXT NOT NULL,
                    task_data TEXT NOT NULL,
                    period TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'accepted',
                    progress INTEGER NOT NULL DEFAULT 0,
                    target INTEGER NOT NULL DEFAULT 1,
                    accepted_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    PRIMARY KEY (user_id, period)
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_sect_task_state_sect_period "
                "ON sect_task_state(sect_id, period, status)"
            )
            self.sql_message._commit_write()

    @staticmethod
    def _period() -> str:
        return get_daily_key()

    @staticmethod
    def _row_to_task(row) -> dict[str, Any] | None:
        if not row:
            return None
        task_data = _json_loads(row["task_data"])
        return {
            "任务名称": row["task_key"],
            "任务内容": task_data,
            "sect_id": row["sect_id"],
            "period": row["period"],
            "status": row["status"],
            "progress": row["progress"],
            "target": row["target"],
            "accepted_at": row["accepted_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
        }

    def get_active_task(self, user_id: str | int) -> dict[str, Any] | None:
        self.ensure_table()
        period = self._period()
        row = self.sql_message._read_query(
            """
            SELECT *
            FROM sect_task_state
            WHERE user_id = %s
              AND period = %s
              AND status = 'accepted'
            LIMIT 1
            """,
            (str(user_id), period),
            one=True,
            dict_row=True,
        )
        return self._row_to_task(row)

    def accept_task(
        self,
        user_id: str | int,
        sect_id: str | int,
        task_config: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        self.ensure_table()
        task_key = random.choice(list(task_config))
        task_data = dict(task_config[task_key])
        period = self._period()
        now = _now_text()

        with self.sql_message.lock:
            cur = self.sql_message.conn.cursor()
            cur.execute(
                """
                INSERT INTO sect_task_state (
                    user_id, sect_id, task_key, task_data, period,
                    status, progress, target, accepted_at, updated_at, completed_at
                )
                VALUES (%s, %s, %s, %s, %s, 'accepted', 0, 1, %s, %s, NULL)
                ON CONFLICT(user_id, period) DO UPDATE SET
                    sect_id = EXCLUDED.sect_id,
                    task_key = EXCLUDED.task_key,
                    task_data = EXCLUDED.task_data,
                    status = 'accepted',
                    progress = 0,
                    target = 1,
                    accepted_at = EXCLUDED.accepted_at,
                    updated_at = EXCLUDED.updated_at,
                    completed_at = NULL
                """,
                (
                    str(user_id),
                    int(sect_id),
                    task_key,
                    _json_dumps(task_data),
                    period,
                    now,
                    now,
                ),
            )
            self.sql_message._commit_write()

        return {
            "任务名称": task_key,
            "任务内容": task_data,
            "sect_id": int(sect_id),
            "period": period,
            "status": "accepted",
            "progress": 0,
            "target": 1,
            "accepted_at": now,
            "updated_at": now,
            "completed_at": None,
        }

    def complete_task(self, user_id: str | int) -> None:
        self.ensure_table()
        period = self._period()
        now = _now_text()
        with self.sql_message.lock:
            cur = self.sql_message.conn.cursor()
            cur.execute(
                """
                UPDATE sect_task_state
                SET status = 'completed',
                    progress = target,
                    updated_at = %s,
                    completed_at = %s
                WHERE user_id = %s
                  AND period = %s
                  AND status = 'accepted'
                """,
                (now, now, str(user_id), period),
            )
            self.sql_message._commit_write()

    def clear_task(self, user_id: str | int) -> None:
        self.ensure_table()
        period = self._period()
        with self.sql_message.lock:
            cur = self.sql_message.conn.cursor()
            cur.execute(
                "DELETE FROM sect_task_state WHERE user_id = %s AND period = %s",
                (str(user_id), period),
            )
            self.sql_message._commit_write()


sect_task_state_manager = SectTaskStateManager()
