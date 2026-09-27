from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..xiuxian_utils.json_store import safe_json_loads
from ..xiuxian_utils.periods import get_weekly_key


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


def _sect_application():
    from . import sect_application

    return sect_application


class SectWeeklyGoalManager:
    table_name = "sect_weekly_goal"

    def __init__(self):
        self.sql_message = None

    @staticmethod
    def current_week_key() -> str:
        return get_weekly_key()

    def ensure_table(self) -> None:
        _sect_application().assert_weekly_progress_schema()

    @staticmethod
    def _goal_rows() -> tuple[dict[str, Any], ...]:
        return tuple({"key": goal.key, "target": goal.target} for goal in SECT_WEEKLY_GOALS)

    def ensure_goals(self, sect_id: int | str, week_key: str | None = None) -> None:
        week_key = week_key or self.current_week_key()
        _sect_application().ensure_weekly_goals(
            sect_id, week_key, self._goal_rows(), _now_text()
        )

    def list_goals(self, sect_id: int | str, week_key: str | None = None) -> list[dict[str, Any]]:
        week_key = week_key or self.current_week_key()
        rows = _sect_application().list_weekly_goal_rows(
            sect_id, week_key, self._goal_rows(), _now_text()
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
            user_info = _sect_application().get_user_profile(str(user_id)) or {}
            sect_id = user_info.get("sect_id")
        if not sect_id:
            return []

        amount = max(0, int(amount or 0))
        if amount <= 0:
            return []

        week_key = self.current_week_key()
        rows = _sect_application().record_weekly_progress(
            sect_id,
            week_key,
            user_id,
            amount,
            ({"key": goal.key, "target": goal.target} for goal in goals),
            _now_text(),
        )
        goal_by_key = {goal.key: goal for goal in goals}
        return [{**row, "name": goal_by_key[row["goal_key"]].name} for row in rows]

    def weekly_rank(self, limit: int = 10, week_key: str | None = None) -> list[dict[str, Any]]:
        week_key = week_key or self.current_week_key()
        limit = max(1, min(int(limit or 10), 50))
        return _sect_application().weekly_rank(limit, week_key)


sect_weekly_goal_manager = SectWeeklyGoalManager()


def record_sect_weekly_event(
    user_id: str | int,
    event_key: str,
    amount: int = 1,
    meta: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return sect_weekly_goal_manager.record_event(user_id, event_key, amount, meta)
