from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Iterable, Mapping, Protocol

from ...core.errors import DomainError
from ...core.numeric import bind_sqlite_param, require_exact_integer
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import AttachedDatabaseUnitOfWork


@dataclass(frozen=True)
class SectWeeklyRewardClaimResult:
    status: str
    rewards: tuple[tuple[str, str], ...] = ()
    reason: str = ""
    subject: str = ""

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


class _WeeklyWriteRejected(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.subject = subject


def _capture_weekly_write_rejection(method):
    def wrapped(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except _WeeklyWriteRejected as exc:
            return SectWeeklyRewardClaimResult(
                "write_rejected", reason=exc.reason, subject=exc.subject
            )

    return wrapped


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

    @staticmethod
    def _reject_write(step: str, subject: str | int) -> None:
        reason = f"sect_weekly_{step}_write_rejected"
        raise _WeeklyWriteRejected(reason, str(subject))

    @staticmethod
    def _integer(value: Any, field: str, subject: str) -> int:
        return require_exact_integer(0 if value is None else value, field=field, subject=subject)

    @staticmethod
    def _write_saved(changed: Any, step: str, subject: str) -> None:
        if changed.rowcount != 1:
            SectWeeklyRewardSqlRepository._reject_write(step, subject)

    @classmethod
    def _same_integer(cls, actual: Any, expected: int, field: str, subject: str) -> bool:
        try:
            return cls._integer(actual, field, subject) == expected
        except (DomainError, TypeError, ValueError):
            return False

    @classmethod
    def _user_assets_match(cls, uow, user_id: str, expected) -> bool:
        row = uow.query_one(
            "SELECT stone,CAST(COALESCE(exp,0) AS REAL) AS exp,sect_contribution "
            "FROM user_xiuxian WHERE user_id=?",
            (user_id,),
        )
        return row is not None and (
            cls._same_integer(row["stone"], expected[0], "stone", user_id)
            and float(row["exp"]) == expected[1]
            and cls._same_integer(row["sect_contribution"], expected[2], "sect_contribution", user_id)
        )

    @classmethod
    def _sect_assets_match(cls, uow, sect_id: int, expected) -> bool:
        row = uow.query_one(
            "SELECT sect_scale,sect_materials FROM sects WHERE sect_id=?", (sect_id,)
        )
        return row is not None and (
            float(row["sect_scale"] or 0) == expected[0]
            and cls._same_integer(row["sect_materials"], expected[1], "sect_materials", str(sect_id))
        )

    @classmethod
    def _boss_integral_matches(cls, uow, user_id: str, expected: int) -> bool:
        row = uow.query_one(
            "SELECT integral FROM player_data.boss_limit WHERE user_id=?", (user_id,)
        )
        return row is not None and cls._same_integer(
            row["integral"], expected, "boss_integral", user_id
        )

    @staticmethod
    def _item_matches(uow, user_id: str, item_id: int, expected) -> bool:
        row = uow.query_one(
            "SELECT goods_name,goods_type,goods_num,COALESCE(bind_num,0) AS bind_num "
            "FROM back WHERE user_id=? AND goods_id=?",
            (user_id, item_id),
        )
        return row is not None and (
            str(row["goods_name"] or ""),
            str(row["goods_type"] or ""),
            int(row["goods_num"] or 0),
            int(row["bind_num"] or 0),
        ) == expected

    @staticmethod
    def _goal_claim_matches(uow, sect_id: int, week_key: str, goal_key: str, expected, now: str) -> bool:
        row = uow.query_one(
            "SELECT claimed_users,updated_at FROM sect_weekly_goal "
            "WHERE sect_id=? AND week_key=? AND goal_key=?",
            (sect_id, week_key, goal_key),
        )
        if row is None or str(row["updated_at"]) != now:
            return False
        try:
            return json.loads(str(row["claimed_users"] or "[]")) == expected
        except (TypeError, ValueError, json.JSONDecodeError):
            return False

    def _grant_items(
        self,
        uow: AttachedDatabaseUnitOfWork,
        user_id: str,
        items: Iterable[Mapping[str, Any]],
        max_goods_num: int,
    ) -> dict[int, tuple[str, str, int, int]] | None:
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
        expected: dict[int, tuple[str, str, int, int]] = {}
        for item_id, item in totals.items():
            row = uow.query_one(
                "SELECT goods_name,goods_type,goods_num,COALESCE(bind_num,0) AS bind_num "
                "FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            current_num = int(row["goods_num"] or 0) if row else 0
            if current_num + int(item["amount"]) > max_goods_num:
                return None
            bind_delta = int(item["amount"]) if int(item["bind_flag"]) == 1 else 0
            current_bind = int(row["bind_num"] or 0) if row else 0
            expected[item_id] = (
                str(item["name"]),
                str(item["type"]),
                current_num + int(item["amount"]),
                current_bind + bind_delta,
            )
            if row:
                changed = uow.execute(
                    "UPDATE back SET goods_name=?,goods_type=?,goods_num=COALESCE(goods_num,0)+?,"
                    "bind_num=COALESCE(bind_num,0)+?,update_time=? WHERE user_id=? AND goods_id=?",
                    (item["name"], item["type"], item["amount"], bind_delta, now, user_id, item_id),
                )
            else:
                changed = uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (user_id, item_id, item["name"], item["type"], item["amount"], now, now, bind_delta),
                )
            self._write_saved(changed, "item", f"{user_id}:{item_id}")
            saved = uow.query_one(
                "SELECT goods_name,goods_type,goods_num,COALESCE(bind_num,0) AS bind_num "
                "FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if saved is None or (
                str(saved["goods_name"] or ""),
                str(saved["goods_type"] or ""),
                int(saved["goods_num"] or 0),
                int(saved["bind_num"] or 0),
            ) != expected[item_id]:
                self._reject_write("item", f"{user_id}:{item_id}")
        return expected

    @_capture_weekly_write_rejection
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

            user = uow.query_one(
                "SELECT sect_id,stone,CAST(COALESCE(exp,0) AS REAL) AS exp,sect_contribution "
                "FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if user is None:
                return SectWeeklyRewardClaimResult("user_missing")
            if user["sect_id"] is None or int(user["sect_id"]) != sect_id:
                return SectWeeklyRewardClaimResult("sect_changed")
            sect = uow.query_one(
                "SELECT sect_scale,sect_materials FROM sects WHERE sect_id=?", (sect_id,)
            )
            if sect is None:
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

            expected_items = self._grant_items(
                uow,
                user_id,
                (item for goal in normalized for item in goal["rewards"]["items"]),
                max_goods_num,
            )
            if expected_items is None:
                return SectWeeklyRewardClaimResult("inventory_full")

            totals = {
                field: sum(int(goal["rewards"][field]) for goal in normalized)
                for field in ("stone", "exp", "sect_contribution", "sect_scale", "sect_materials", "boss_integral")
            }
            user_expected = (
                self._integer(user["stone"], "stone", user_id) + totals["stone"],
                float(user["exp"]) + float(totals["exp"]),
                self._integer(user["sect_contribution"], "sect_contribution", user_id)
                + totals["sect_contribution"],
            )
            sect_expected = (
                float(sect["sect_scale"] or 0) + float(totals["sect_scale"]),
                self._integer(sect["sect_materials"], "sect_materials", str(sect_id))
                + totals["sect_materials"],
            )
            boss_before = uow.query_one(
                "SELECT integral FROM player_data.boss_limit WHERE user_id=?", (user_id,)
            )
            boss_expected = self._integer(
                boss_before["integral"] if boss_before is not None else 0,
                "boss_integral",
                user_id,
            ) + totals["boss_integral"]
            changed = uow.execute(
                "UPDATE user_xiuxian SET stone=xiuxian_int_store(xiuxian_int_add(COALESCE(stone,0),?,'stone','actor')),"
                "exp=CAST(COALESCE(exp,0) AS REAL)+CAST(? AS REAL),"
                "sect_contribution=xiuxian_int_store(xiuxian_int_add(COALESCE(sect_contribution,0),?,'sect_contribution','actor')) WHERE user_id=?",
                (
                    bind_sqlite_param(totals["stone"], exact=True),
                    totals["exp"],
                    bind_sqlite_param(totals["sect_contribution"], exact=True),
                    user_id,
                ),
            )
            self._write_saved(changed, "user_assets", user_id)
            if not self._user_assets_match(uow, user_id, user_expected):
                self._reject_write("user_assets", user_id)

            changed = uow.execute(
                "UPDATE sects SET sect_scale=CAST(COALESCE(sect_scale,0) AS REAL)+CAST(? AS REAL),"
                "sect_materials=xiuxian_int_store(xiuxian_int_add(COALESCE(sect_materials,0),?,'sect_materials','actor')) WHERE sect_id=?",
                (totals["sect_scale"], bind_sqlite_param(totals["sect_materials"], exact=True), sect_id),
            )
            self._write_saved(changed, "sect_assets", sect_id)
            if not self._sect_assets_match(uow, sect_id, sect_expected):
                self._reject_write("sect_assets", sect_id)

            changed = uow.execute(
                "INSERT INTO player_data.boss_limit(user_id,integral) VALUES(?,xiuxian_int_store(?,'boss_integral','actor')) "
                "ON CONFLICT(user_id) DO UPDATE SET integral=xiuxian_int_store(xiuxian_int_add(COALESCE(integral,0),excluded.integral,'boss_integral','actor'))",
                (user_id, bind_sqlite_param(totals["boss_integral"], exact=True)),
            )
            self._write_saved(changed, "boss_integral", user_id)
            if not self._boss_integral_matches(uow, user_id, boss_expected):
                self._reject_write("boss_integral", user_id)

            now = self._now()
            expected_claims: dict[str, list[str]] = {}
            for goal in normalized:
                claimed = claimed_by_goal[goal["key"]]
                claimed.append(user_id)
                expected_claims[goal["key"]] = claimed
                changed = uow.execute(
                    "UPDATE sect_weekly_goal SET claimed_users=?,updated_at=? "
                    "WHERE sect_id=? AND week_key=? AND goal_key=?",
                    (self._json(claimed), now, sect_id, week_key, goal["key"]),
                )
                subject = f"{sect_id}:{week_key}:{goal['key']}"
                self._write_saved(changed, "claim", subject)
                if not self._goal_claim_matches(uow, sect_id, week_key, goal["key"], claimed, now):
                    self._reject_write("claim", subject)
            result = tuple((goal["name"], self._format_reward(goal["rewards"])) for goal in normalized)
            result_json = self._json(result)
            changed = uow.execute(
                "INSERT INTO sect_weekly_reward_operations(operation_id,payload,result_json,created_at) VALUES(?,?,?,?)",
                (operation_id, payload, result_json, now),
            )
            self._write_saved(changed, "receipt", operation_id)

            if not self._user_assets_match(uow, user_id, user_expected):
                self._reject_write("user_assets", user_id)
            if not self._sect_assets_match(uow, sect_id, sect_expected):
                self._reject_write("sect_assets", sect_id)
            if not self._boss_integral_matches(uow, user_id, boss_expected):
                self._reject_write("boss_integral", user_id)
            for item_id, expected_item in expected_items.items():
                if not self._item_matches(uow, user_id, item_id, expected_item):
                    self._reject_write("item", f"{user_id}:{item_id}")
            for goal in normalized:
                goal_key = goal["key"]
                if not self._goal_claim_matches(
                    uow, sect_id, week_key, goal_key, expected_claims[goal_key], now
                ):
                    self._reject_write("claim", f"{sect_id}:{week_key}:{goal_key}")
            receipt = uow.query_one(
                "SELECT payload,result_json,created_at FROM sect_weekly_reward_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if receipt is None or (
                str(receipt["payload"]),
                str(receipt["result_json"]),
                str(receipt["created_at"]),
            ) != (payload, result_json, now):
                self._reject_write("receipt", operation_id)
            return SectWeeklyRewardClaimResult("applied", result)


__all__ = [
    "SectWeeklyRewardClaimResult",
    "SectWeeklyRewardRepository",
    "SectWeeklyRewardSqlRepository",
]
