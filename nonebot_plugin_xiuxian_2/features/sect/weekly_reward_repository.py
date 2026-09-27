from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Iterable, Mapping, Protocol

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import AttachedDatabaseUnitOfWork


@dataclass(frozen=True)
class SectWeeklyRewardClaimResult:
    status: str
    rewards: tuple[tuple[str, str], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class SectWeeklyRewardRepository(Protocol):
    def claim(
        self,
        operation_id: str,
        user_id: str,
        sect_id: int,
        week_key: str,
        goals: Iterable[Mapping[str, Any]],
        max_goods_num: int,
    ) -> SectWeeklyRewardClaimResult: ...


class SectWeeklyRewardSqlRepository:
    """Cross-database, idempotent claim of completed sect weekly goals."""

    _GAME_TABLES = {"user_xiuxian", "sects", "back", "sect_weekly_goal", "sect_weekly_reward_operations"}
    _PLAYER_TABLES = {"boss_limit"}
    _USER_COLUMNS = {"user_id", "sect_id", "stone", "exp", "sect_contribution"}
    _SECT_COLUMNS = {"sect_id", "sect_scale", "sect_materials"}
    _BACK_COLUMNS = {"user_id", "goods_id", "goods_name", "goods_type", "goods_num", "create_time", "update_time", "bind_num"}
    _GOAL_COLUMNS = {"sect_id", "week_key", "goal_key", "progress", "target", "claimed_users", "updated_at"}
    _OPERATION_COLUMNS = {"operation_id", "payload", "result_json", "created_at"}

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Any | None = None,
        lock: RLock | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()
        self.lock = lock or RLock()

    def _now(self) -> str:
        value = self.clock.now()
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone()
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)

    @classmethod
    def _tables(cls, uow: AttachedDatabaseUnitOfWork, schema: str = "main") -> set[str]:
        prefix = "" if schema == "main" else f"{schema}."
        return {
            str(row["name"])
            for row in uow.query_all(f"SELECT name FROM {prefix}sqlite_master WHERE type='table'")
        }

    @classmethod
    def _columns(cls, uow: AttachedDatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        pragma = f"PRAGMA {schema}.table_info({table})" if schema != "main" else f"PRAGMA table_info({table})"
        return {str(row["name"]) for row in uow.query_all(pragma)}

    @classmethod
    def _schema_ready(cls, uow: AttachedDatabaseUnitOfWork) -> bool:
        if not cls._GAME_TABLES.issubset(cls._tables(uow)):
            return False
        if not cls._PLAYER_TABLES.issubset(cls._tables(uow, "player_data")):
            return False
        if not cls._USER_COLUMNS.issubset(cls._columns(uow, "user_xiuxian")):
            return False
        if not cls._SECT_COLUMNS.issubset(cls._columns(uow, "sects")):
            return False
        if not cls._BACK_COLUMNS.issubset(cls._columns(uow, "back")):
            return False
        if not cls._GOAL_COLUMNS.issubset(cls._columns(uow, "sect_weekly_goal")):
            return False
        if not cls._OPERATION_COLUMNS.issubset(cls._columns(uow, "sect_weekly_reward_operations")):
            return False
        return "integral" in cls._columns(uow, "boss_limit", "player_data")

    @staticmethod
    def _normalize_goals(goals: Iterable[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in goals:
            goal = dict(raw)
            key = str(goal["key"]).strip()
            if not key or key in seen:
                raise ValueError(f"duplicate or empty weekly goal: {key}")
            seen.add(key)
            reward = dict(goal.get("rewards") or {})
            items: list[dict[str, Any]] = []
            for raw_item in reward.get("items", ()) or ():
                item = dict(raw_item)
                amount = int(item.get("amount", item.get("num", 1)) or 0)
                if amount <= 0:
                    continue
                item_id = int(item.get("id") or item.get("goods_id"))
                if item_id <= 0:
                    raise ValueError("weekly reward item id is required")
                items.append(
                    {
                        "id": item_id,
                        "name": str(item.get("name") or ""),
                        "type": str(item.get("type") or item.get("item_type") or "道具"),
                        "amount": amount,
                        "bind_flag": int(item.get("bind_flag", item.get("bind", 1)) or 0),
                    }
                )
            normalized.append(
                {
                    "key": key,
                    "name": str(goal.get("name") or key),
                    "target": int(goal["target"]),
                    "rewards": {
                        "items": sorted(items, key=lambda item: item["id"]),
                        **{
                            field: max(0, int(reward.get(field, 0) or 0))
                            for field in ("stone", "exp", "sect_contribution", "sect_scale", "sect_materials", "boss_integral")
                        },
                    },
                }
            )
        return tuple(sorted(normalized, key=lambda goal: goal["key"]))

    @staticmethod
    def _format_reward(reward: Mapping[str, Any]) -> str:
        parts = [f"{item['name']}x{item['amount']}" for item in reward["items"]]
        labels = (
            ("stone", "灵石"),
            ("exp", "修为"),
            ("sect_contribution", "宗门贡献"),
            ("sect_scale", "宗门建设度"),
            ("sect_materials", "宗门资材"),
            ("boss_integral", "BOSS积分"),
        )
        parts.extend(f"{label}{reward[key]}" for key, label in labels if reward[key] > 0)
        return "、".join(parts) if parts else "无"

    def _grant_items(
        self,
        uow: AttachedDatabaseUnitOfWork,
        user_id: str,
        items: Iterable[Mapping[str, Any]],
        max_goods_num: int,
    ) -> bool:
        totals: dict[int, dict[str, Any]] = {}
        for raw_item in items:
            item = dict(raw_item)
            item_id = int(item["id"])
            current = totals.get(item_id)
            if current is None:
                totals[item_id] = item
                continue
            if current["name"] != item["name"] or current["type"] != item["type"]:
                raise ValueError(f"conflicting metadata for item {item_id}")
            current["amount"] += int(item["amount"])
            current["bind_flag"] = min(int(current["bind_flag"]), int(item["bind_flag"]))

        now = self._now()
        for item_id, item in totals.items():
            row = uow.query_one(
                "SELECT goods_num,COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            current_num = int(row["goods_num"] or 0) if row else 0
            if current_num + int(item["amount"]) > max_goods_num:
                return False
            bind_delta = int(item["amount"]) if int(item["bind_flag"]) == 1 else 0
            if row:
                uow.execute(
                    "UPDATE back SET goods_name=?,goods_type=?,goods_num=COALESCE(goods_num,0)+?,"
                    "bind_num=COALESCE(bind_num,0)+?,update_time=? WHERE user_id=? AND goods_id=?",
                    (item["name"], item["type"], item["amount"], bind_delta, now, user_id, item_id),
                )
            else:
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (user_id, item_id, item["name"], item["type"], item["amount"], now, now, bind_delta),
                )
        return True

    def claim(
        self,
        operation_id: str,
        user_id: str | int,
        sect_id: str | int,
        week_key: str,
        goals: Iterable[Mapping[str, Any]],
        max_goods_num: int,
    ) -> SectWeeklyRewardClaimResult:
        operation_id = str(operation_id).strip()
        user_id, week_key = str(user_id).strip(), str(week_key).strip()
        sect_id, max_goods_num = int(sect_id), int(max_goods_num)
        normalized = self._normalize_goals(goals)
        if not operation_id or not user_id or sect_id <= 0 or not week_key or not normalized or max_goods_num < 0:
            raise ValueError("valid operation, user, sect, week and inventory limit are required")
        payload = self._json([user_id, sect_id, week_key, normalized, max_goods_num])

        with self.lock, AttachedDatabaseUnitOfWork(
            self.game_database,
            attachments={"player_data": self.player_database},
            immediate=True,
        ) as uow:
            if not self._schema_ready(uow):
                return SectWeeklyRewardClaimResult("schema_missing")

            previous = uow.query_one(
                "SELECT payload,result_json FROM sect_weekly_reward_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return SectWeeklyRewardClaimResult("operation_conflict")
                result = json.loads(str(previous["result_json"]))
                return SectWeeklyRewardClaimResult("duplicate", tuple(tuple(row) for row in result))

            user = uow.query_one("SELECT sect_id FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return SectWeeklyRewardClaimResult("user_missing")
            if user["sect_id"] is None or int(user["sect_id"]) != sect_id:
                return SectWeeklyRewardClaimResult("sect_changed")
            if uow.query_one("SELECT 1 AS present FROM sects WHERE sect_id=?", (sect_id,)) is None:
                return SectWeeklyRewardClaimResult("sect_missing")

            claimed_by_goal: dict[str, list[str]] = {}
            for goal in normalized:
                row = uow.query_one(
                    "SELECT progress,target,claimed_users FROM sect_weekly_goal "
                    "WHERE sect_id=? AND week_key=? AND goal_key=?",
                    (sect_id, week_key, goal["key"]),
                )
                if row is None or int(row["progress"] or 0) < int(goal["target"]) or int(row["target"]) != int(goal["target"]):
                    return SectWeeklyRewardClaimResult("not_completed")
                try:
                    claimed = [str(value) for value in json.loads(str(row["claimed_users"] or "[]"))]
                except (TypeError, ValueError, json.JSONDecodeError):
                    return SectWeeklyRewardClaimResult("state_changed")
                if user_id in claimed:
                    return SectWeeklyRewardClaimResult("already_claimed")
                claimed_by_goal[goal["key"]] = claimed

            if not self._grant_items(uow, user_id, (item for goal in normalized for item in goal["rewards"]["items"]), max_goods_num):
                return SectWeeklyRewardClaimResult("inventory_full")

            totals = {
                field: sum(int(goal["rewards"][field]) for goal in normalized)
                for field in ("stone", "exp", "sect_contribution", "sect_scale", "sect_materials", "boss_integral")
            }
            uow.execute(
                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(? AS REAL),"
                "exp=CAST(COALESCE(exp,0) AS REAL)+CAST(? AS REAL),"
                "sect_contribution=CAST(COALESCE(sect_contribution,0) AS REAL)+CAST(? AS REAL) WHERE user_id=?",
                (totals["stone"], totals["exp"], totals["sect_contribution"], user_id),
            )
            uow.execute(
                "UPDATE sects SET sect_scale=CAST(COALESCE(sect_scale,0) AS REAL)+CAST(? AS REAL),"
                "sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)+CAST(? AS REAL) WHERE sect_id=?",
                (totals["sect_scale"], totals["sect_materials"], sect_id),
            )
            uow.execute(
                "INSERT INTO player_data.boss_limit(user_id,integral) VALUES(?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET integral=COALESCE(integral,0)+excluded.integral",
                (user_id, totals["boss_integral"]),
            )

            now = self._now()
            for goal in normalized:
                claimed = claimed_by_goal[goal["key"]]
                claimed.append(user_id)
                uow.execute(
                    "UPDATE sect_weekly_goal SET claimed_users=?,updated_at=? "
                    "WHERE sect_id=? AND week_key=? AND goal_key=?",
                    (self._json(claimed), now, sect_id, week_key, goal["key"]),
                )
            result = tuple((goal["name"], self._format_reward(goal["rewards"])) for goal in normalized)
            uow.execute(
                "INSERT INTO sect_weekly_reward_operations(operation_id,payload,result_json,created_at) VALUES(?,?,?,?)",
                (operation_id, payload, self._json(result), now),
            )
            return SectWeeklyRewardClaimResult("applied", result)


__all__ = [
    "SectWeeklyRewardClaimResult",
    "SectWeeklyRewardRepository",
    "SectWeeklyRewardSqlRepository",
]
