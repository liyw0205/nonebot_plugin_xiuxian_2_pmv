from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..xiuxian_utils.json_store import safe_json_dumps as _json_dumps
from ..xiuxian_utils.json_store import safe_json_loads
from ..xiuxian_utils.periods import get_weekly_key
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage


@dataclass(frozen=True)
class SectWeeklyGoalDefinition:
    key: str
    name: str
    desc: str
    target: int
    event_keys: tuple[str, ...]
    rewards: dict[str, Any]


SECT_WEEKLY_GOALS: tuple[SectWeeklyGoalDefinition, ...] = (
    SectWeeklyGoalDefinition(
        key="sect_diligence",
        name="同门勤务",
        desc="本周全宗完成宗门任务 15 次",
        target=15,
        event_keys=("sect_task_complete",),
        rewards={"sect_materials": 20_000_000, "sect_contribution": 500_000},
    ),
    SectWeeklyGoalDefinition(
        key="sect_supply",
        name="广纳资粮",
        desc="本周累计宗门捐献 500 万灵石",
        target=5_000_000,
        event_keys=("sect_donate",),
        rewards={"sect_materials": 30_000_000, "sect_contribution": 800_000},
    ),
    SectWeeklyGoalDefinition(
        key="sect_battle",
        name="合力伏魔",
        desc="本周参与世界 BOSS 或世界事件 20 次",
        target=20,
        event_keys=("boss_attack", "world_event_attack"),
        rewards={"sect_scale": 1_000_000, "sect_contribution": 300_000},
    ),
    SectWeeklyGoalDefinition(
        key="sect_elixir",
        name="丹火不熄",
        desc="本周炼丹或洞府收获累计 20 次",
        target=20,
        event_keys=("mix_elixir_complete", "dongfu_harvest"),
        rewards={"items": [{"id": 20015, "amount": 2}], "sect_contribution": 300_000},
    ),
    SectWeeklyGoalDefinition(
        key="sect_dungeon",
        name="组队试炼",
        desc="本周副本通关 10 次",
        target=10,
        event_keys=("dungeon_clear",),
        rewards={"items": [{"id": 18172, "amount": 1}], "sect_contribution": 500_000},
    ),
)

SECT_WEEKLY_GOALS_BY_KEY = {goal.key: goal for goal in SECT_WEEKLY_GOALS}
SECT_WEEKLY_EVENT_MAP: dict[str, tuple[SectWeeklyGoalDefinition, ...]] = {}
for _goal in SECT_WEEKLY_GOALS:
    for _event_key in _goal.event_keys:
        SECT_WEEKLY_EVENT_MAP.setdefault(_event_key, tuple())
        SECT_WEEKLY_EVENT_MAP[_event_key] = (*SECT_WEEKLY_EVENT_MAP[_event_key], _goal)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _json_loads(text: str | None, default: Any):
    return safe_json_loads(text, default, type(default))


class SectWeeklyGoalManager:
    table_name = "sect_weekly_goal"

    def __init__(self):
        self.sql_message = XiuxianDateManage()
        self.ensure_table()

    @staticmethod
    def current_week_key() -> str:
        return get_weekly_key()

    def ensure_table(self) -> None:
        with self.sql_message.lock:
            cur = self.sql_message.conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sect_weekly_goal (
                    sect_id INTEGER NOT NULL,
                    week_key TEXT NOT NULL,
                    goal_key TEXT NOT NULL,
                    progress INTEGER NOT NULL DEFAULT 0,
                    target INTEGER NOT NULL,
                    participants TEXT NOT NULL DEFAULT '{}',
                    claimed_users TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (sect_id, week_key, goal_key)
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_sect_weekly_goal_week "
                "ON sect_weekly_goal(week_key, goal_key, progress)"
            )
            self.sql_message._commit_write()

    def ensure_goals(self, sect_id: int | str, week_key: str | None = None) -> None:
        self.ensure_table()
        week_key = week_key or self.current_week_key()
        now = _now_text()
        with self.sql_message.lock:
            cur = self.sql_message.conn.cursor()
            for goal in SECT_WEEKLY_GOALS:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO sect_weekly_goal (
                        sect_id, week_key, goal_key, progress, target,
                        participants, claimed_users, updated_at
                    )
                    VALUES (%s, %s, %s, 0, %s, '{}', '[]', %s)
                    """,
                    (int(sect_id), week_key, goal.key, goal.target, now),
                )
            self.sql_message._commit_write()

    def _get_goal_row(self, sect_id: int | str, goal_key: str, week_key: str | None = None):
        self.ensure_goals(sect_id, week_key)
        week_key = week_key or self.current_week_key()
        return self.sql_message._read_query(
            """
            SELECT *
            FROM sect_weekly_goal
            WHERE sect_id = %s AND week_key = %s AND goal_key = %s
            """,
            (int(sect_id), week_key, goal_key),
            one=True,
            dict_row=True,
        )

    def list_goals(self, sect_id: int | str, week_key: str | None = None) -> list[dict[str, Any]]:
        self.ensure_goals(sect_id, week_key)
        week_key = week_key or self.current_week_key()
        rows = self.sql_message._read_query(
            """
            SELECT *
            FROM sect_weekly_goal
            WHERE sect_id = %s AND week_key = %s
            ORDER BY goal_key
            """,
            (int(sect_id), week_key),
            dict_row=True,
        )
        row_map = {row["goal_key"]: row for row in rows or []}
        result = []
        for goal in SECT_WEEKLY_GOALS:
            row = row_map.get(goal.key) or {}
            progress = int(row.get("progress", 0) or 0)
            claimed_users = _json_loads(row.get("claimed_users"), [])
            result.append(
                {
                    "key": goal.key,
                    "name": goal.name,
                    "desc": goal.desc,
                    "target": goal.target,
                    "progress": min(progress, goal.target),
                    "raw_progress": progress,
                    "rewards": goal.rewards,
                    "claimed_users": [str(item) for item in claimed_users],
                    "completed": progress >= goal.target,
                }
            )
        return result

    def resolve_goal_key(self, key_or_name: str) -> str | None:
        text = str(key_or_name or "").strip()
        if not text:
            return None
        if text in SECT_WEEKLY_GOALS_BY_KEY:
            return text
        for goal in SECT_WEEKLY_GOALS:
            if text == goal.name or text in goal.name:
                return goal.key
        return None

    def record_event(
        self,
        user_id: str | int,
        event_key: str,
        amount: int = 1,
        meta: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        goals = SECT_WEEKLY_EVENT_MAP.get(str(event_key), ())
        if not goals:
            return []

        meta = meta or {}
        sect_id = meta.get("sect_id")
        if not sect_id:
            user_info = self.sql_message.get_user_info_with_id(str(user_id)) or {}
            sect_id = user_info.get("sect_id")
        if not sect_id:
            return []

        amount = max(0, int(amount or 0))
        if amount <= 0:
            return []

        self.ensure_goals(sect_id)
        week_key = self.current_week_key()
        now = _now_text()
        updated = []
        with self.sql_message.lock:
            cur = self.sql_message.conn.cursor()
            for goal in goals:
                row = self._get_goal_row(sect_id, goal.key, week_key)
                old_progress = int(row.get("progress", 0) or 0)
                participants = _json_loads(row.get("participants"), {})
                participants[str(user_id)] = int(participants.get(str(user_id), 0) or 0) + amount
                new_progress = min(old_progress + amount, goal.target)
                cur.execute(
                    """
                    UPDATE sect_weekly_goal
                    SET progress = %s,
                        participants = %s,
                        updated_at = %s
                    WHERE sect_id = %s AND week_key = %s AND goal_key = %s
                    """,
                    (
                        new_progress,
                        _json_dumps(participants),
                        now,
                        int(sect_id),
                        week_key,
                        goal.key,
                    ),
                )
                updated.append(
                    {
                        "goal_key": goal.key,
                        "name": goal.name,
                        "old_progress": old_progress,
                        "progress": new_progress,
                        "target": goal.target,
                        "completed": old_progress < goal.target <= new_progress,
                    }
                )
            self.sql_message._commit_write()
        return updated

    def mark_claimed(self, sect_id: int | str, user_id: str | int, goal_key: str) -> bool:
        row = self._get_goal_row(sect_id, goal_key)
        if not row:
            return False
        goal = SECT_WEEKLY_GOALS_BY_KEY.get(goal_key)
        if not goal or int(row.get("progress", 0) or 0) < goal.target:
            return False
        claimed_users = [str(item) for item in _json_loads(row.get("claimed_users"), [])]
        user_id = str(user_id)
        if user_id in claimed_users:
            return False
        claimed_users.append(user_id)
        with self.sql_message.lock:
            cur = self.sql_message.conn.cursor()
            cur.execute(
                """
                UPDATE sect_weekly_goal
                SET claimed_users = %s,
                    updated_at = %s
                WHERE sect_id = %s AND week_key = %s AND goal_key = %s
                """,
                (
                    _json_dumps(claimed_users),
                    _now_text(),
                    int(sect_id),
                    self.current_week_key(),
                    goal_key,
                ),
            )
            self.sql_message._commit_write()
        return True

    def weekly_rank(self, limit: int = 10, week_key: str | None = None) -> list[dict[str, Any]]:
        self.ensure_table()
        week_key = week_key or self.current_week_key()
        limit = max(1, min(int(limit or 10), 50))
        rows = self.sql_message._read_query(
            """
            SELECT
                g.sect_id,
                COALESCE(s.sect_name, g.sect_id) AS sect_name,
                SUM(g.progress) AS total_progress
            FROM sect_weekly_goal AS g
            LEFT JOIN sects AS s ON s.sect_id = g.sect_id
            WHERE g.week_key = %s
            GROUP BY g.sect_id, s.sect_name
            ORDER BY total_progress DESC
            LIMIT %s
            """,
            (week_key, limit),
            dict_row=True,
        )
        return rows or []


sect_weekly_goal_manager = SectWeeklyGoalManager()


def record_sect_weekly_event(
    user_id: str | int,
    event_key: str,
    amount: int = 1,
    meta: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return sect_weekly_goal_manager.record_event(user_id, event_key, amount, meta)
