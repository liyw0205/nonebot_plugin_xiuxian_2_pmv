from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Iterable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectWeeklyRewardClaimResult:
    status: str
    rewards: tuple[tuple[str, str], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class SectWeeklyRewardClaimService:
    """Claim one or more sect weekly goals in one cross-database transaction."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sect_weekly_reward_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.boss_limit ("
            "user_id TEXT PRIMARY KEY,integral INTEGER DEFAULT 0)"
        )

    @staticmethod
    def _normalize_goals(goals: Iterable[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
        normalized = []
        goal_keys = set()
        for goal in goals:
            goal_key = str(goal["key"])
            if goal_key in goal_keys:
                raise ValueError(f"duplicate weekly goal: {goal_key}")
            goal_keys.add(goal_key)
            reward = dict(goal.get("rewards") or {})
            items = []
            for item in reward.get("items", ()) or ():
                amount = int(item.get("amount", item.get("num", 1)) or 0)
                if amount <= 0:
                    continue
                items.append(
                    {
                        "id": int(item.get("id") or item.get("goods_id")),
                        "name": str(item.get("name") or ""),
                        "type": str(item.get("type") or "道具"),
                        "amount": amount,
                        "bind_flag": int(item.get("bind_flag", item.get("bind", 1)) or 0),
                    }
                )
            normalized.append(
                {
                    "key": goal_key,
                    "name": str(goal.get("name") or goal["key"]),
                    "target": int(goal["target"]),
                    "rewards": {
                        "items": sorted(items, key=lambda row: row["id"]),
                        "stone": max(0, int(reward.get("stone", 0) or 0)),
                        "exp": max(0, int(reward.get("exp", 0) or 0)),
                        "sect_contribution": max(0, int(reward.get("sect_contribution", 0) or 0)),
                        "sect_scale": max(0, int(reward.get("sect_scale", 0) or 0)),
                        "sect_materials": max(0, int(reward.get("sect_materials", 0) or 0)),
                        "boss_integral": max(0, int(reward.get("boss_integral", 0) or 0)),
                    },
                }
            )
        return tuple(sorted(normalized, key=lambda row: row["key"]))

    @staticmethod
    def _format_reward(reward: dict[str, Any]) -> str:
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
    def _grant_items(conn, user_id: str, items: Iterable[dict[str, Any]], max_goods_num: int) -> bool:
        totals: dict[int, dict[str, Any]] = {}
        for item in items:
            item_id = int(item["id"])
            current = totals.get(item_id)
            if current is None:
                totals[item_id] = dict(item)
                continue
            if current["name"] != item["name"] or current["type"] != item["type"]:
                raise ValueError(f"conflicting metadata for item {item_id}")
            current["amount"] += int(item["amount"])
            current["bind_flag"] = min(int(current["bind_flag"]), int(item["bind_flag"]))

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for item_id, item in totals.items():
            row = conn.execute(
                "SELECT goods_num,COALESCE(bind_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                (user_id, item_id),
            ).fetchone()
            current_num = int(row[0] or 0) if row else 0
            if current_num + int(item["amount"]) > max_goods_num:
                return False
            bind_delta = int(item["amount"]) if int(item["bind_flag"]) == 1 else 0
            if row:
                conn.execute(
                    "UPDATE back SET goods_name=%s,goods_type=%s,goods_num=COALESCE(goods_num,0)+%s,"
                    "bind_num=COALESCE(bind_num,0)+%s,update_time=%s WHERE user_id=%s AND goods_id=%s",
                    (item["name"], item["type"], item["amount"], bind_delta, now, user_id, item_id),
                )
            else:
                conn.execute(
                    "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (user_id, item_id, item["name"], item["type"], item["amount"], now, now, bind_delta),
                )
        return True

    def claim(
        self,
        operation_id: str,
        user_id: str | int,
        sect_id: str | int,
        week_key: str,
        goals: Iterable[dict[str, Any]],
        max_goods_num: int,
    ) -> SectWeeklyRewardClaimResult:
        operation_id = str(operation_id).strip()
        user_id, sect_id, week_key = str(user_id), int(sect_id), str(week_key).strip()
        max_goods_num = int(max_goods_num)
        goals = self._normalize_goals(goals)
        if not operation_id or not week_key or not goals or max_goods_num < 0:
            raise ValueError("valid operation, week, goals and inventory limit are required")
        payload = self._json([user_id, sect_id, week_key, goals, max_goods_num])

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM sect_weekly_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    if str(previous[0]) != payload:
                        conn.rollback()
                        return SectWeeklyRewardClaimResult("operation_conflict")
                    result = json.loads(str(previous[1]))
                    conn.rollback()
                    return SectWeeklyRewardClaimResult("duplicate", tuple(tuple(row) for row in result))

                user = conn.execute(
                    "SELECT sect_id FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if not user:
                    conn.rollback()
                    return SectWeeklyRewardClaimResult("user_missing")
                if user[0] is None or int(user[0]) != sect_id:
                    conn.rollback()
                    return SectWeeklyRewardClaimResult("sect_changed")
                if not conn.execute("SELECT 1 FROM sects WHERE sect_id=%s", (sect_id,)).fetchone():
                    conn.rollback()
                    return SectWeeklyRewardClaimResult("sect_missing")

                rows = {}
                for goal in goals:
                    row = conn.execute(
                        "SELECT progress,target,claimed_users FROM sect_weekly_goal "
                        "WHERE sect_id=%s AND week_key=%s AND goal_key=%s",
                        (sect_id, week_key, goal["key"]),
                    ).fetchone()
                    if not row or int(row[0] or 0) < int(goal["target"]) or int(row[1]) != int(goal["target"]):
                        conn.rollback()
                        return SectWeeklyRewardClaimResult("not_completed")
                    claimed = [str(value) for value in json.loads(str(row[2] or "[]"))]
                    if user_id in claimed:
                        conn.rollback()
                        return SectWeeklyRewardClaimResult("already_claimed")
                    rows[goal["key"]] = claimed

                all_items = [item for goal in goals for item in goal["rewards"]["items"]]
                if not self._grant_items(conn, user_id, all_items, max_goods_num):
                    conn.rollback()
                    return SectWeeklyRewardClaimResult("inventory_full")

                totals = {
                    key: sum(goal["rewards"][key] for goal in goals)
                    for key in ("stone", "exp", "sect_contribution", "sect_scale", "sect_materials", "boss_integral")
                }
                conn.execute(
                    "UPDATE user_xiuxian SET stone=COALESCE(stone,0)+%s,exp=COALESCE(exp,0)+%s,"
                    "sect_contribution=COALESCE(sect_contribution,0)+%s WHERE user_id=%s",
                    (totals["stone"], totals["exp"], totals["sect_contribution"], user_id),
                )
                conn.execute(
                    "UPDATE sects SET sect_scale=COALESCE(sect_scale,0)+%s,"
                    "sect_materials=COALESCE(sect_materials,0)+%s WHERE sect_id=%s",
                    (totals["sect_scale"], totals["sect_materials"], sect_id),
                )
                conn.execute(
                    "INSERT INTO player_data.boss_limit(user_id,integral) VALUES(%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET integral=COALESCE(integral,0)+EXCLUDED.integral",
                    (user_id, totals["boss_integral"]),
                )

                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for goal in goals:
                    claimed = rows[goal["key"]]
                    claimed.append(user_id)
                    conn.execute(
                        "UPDATE sect_weekly_goal SET claimed_users=%s,updated_at=%s "
                        "WHERE sect_id=%s AND week_key=%s AND goal_key=%s",
                        (self._json(claimed), now, sect_id, week_key, goal["key"]),
                    )
                result = tuple((goal["name"], self._format_reward(goal["rewards"])) for goal in goals)
                conn.execute(
                    "INSERT INTO sect_weekly_reward_operations(operation_id,payload,result_json,created_at) "
                    "VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, self._json(result), now),
                )
                conn.commit()
                return SectWeeklyRewardClaimResult("applied", result)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["SectWeeklyRewardClaimResult", "SectWeeklyRewardClaimService"]
