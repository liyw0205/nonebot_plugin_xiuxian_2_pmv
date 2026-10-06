from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


_ACTIVITY_STATE_TABLES = frozenset(
    {
        "activity_user",
        "activity_sign_log",
        "activity_collect_inventory",
        "activity_collect_claim",
        "activity_collect_drop_log",
        "activity_collect_pity_state",
        "activity_point_balance",
        "activity_point_event_log",
        "activity_point_purchase",
        "activity_task_progress",
        "activity_task_claim_log",
        "activity_pass_balance",
        "activity_pass_event_log",
        "activity_pass_reward_claim",
        "activity_item_inventory",
        "activity_boss_state",
        "activity_boss_damage",
        "activity_boss_fight_log",
        "activity_boss_milestone",
        "activity_boss_milestone_claim",
        "activity_boss_rank_claim",
    }
)
_ACTIVITY_GAMEPLAY_RESET_TABLES = (
    "activity_collect_inventory",
    "activity_collect_claim",
    "activity_collect_drop_log",
    "activity_collect_pity_state",
    "activity_point_balance",
    "activity_point_event_log",
    "activity_point_purchase",
    "activity_item_inventory",
    "activity_boss_state",
    "activity_boss_damage",
    "activity_boss_fight_log",
    "activity_boss_milestone",
    "activity_boss_milestone_claim",
    "activity_boss_rank_claim",
)
_ACTIVITY_TASK_PASS_RESET_TABLES = (
    "activity_task_progress",
    "activity_task_claim_log",
    "activity_pass_balance",
    "activity_pass_event_log",
    "activity_pass_reward_claim",
)


class ActivityAdminDataSqlRepository:
    """Own the Activity admin overview and mutations over the migrated game DB."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _assert_schema(uow: DatabaseUnitOfWork) -> set[str]:
        tables = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        missing = sorted(_ACTIVITY_STATE_TABLES - tables)
        if missing:
            raise RuntimeError(f"activity_state.001 schema_missing: {', '.join(missing)}")
        if "activity_event_operations" not in tables:
            raise RuntimeError("activity_state.003 schema_missing: activity_event_operations")
        return tables

    @staticmethod
    def _count(uow: DatabaseUnitOfWork, sql: str, params: tuple[Any, ...] = ()) -> int:
        row = uow.query_one(sql, params)
        return max(0, int(next(iter(row.values()), 0) if row else 0))

    @staticmethod
    def _display_names(
        uow: DatabaseUnitOfWork,
        rows: list[dict[str, Any]],
        *,
        has_user_table: bool,
    ) -> None:
        user_ids = list(dict.fromkeys(str(row.get("user_id") or "") for row in rows))
        names: dict[str, str] = {}
        if has_user_table:
            for offset in range(0, len(user_ids), 300):
                batch = user_ids[offset : offset + 300]
                if not batch:
                    continue
                placeholders = ",".join("?" for _ in batch)
                names.update(
                    {
                        str(row["user_id"]): str(row.get("user_name") or "").strip()
                        for row in uow.query_all(
                            "SELECT user_id,user_name FROM user_xiuxian "
                            f"WHERE user_id IN ({placeholders})",
                            batch,
                        )
                    }
                )
        for row in rows:
            user_id = str(row.get("user_id") or "")
            name = names.get(user_id, "")
            if not name:
                name = f"修士·{user_id[-4:]}" if len(user_id) > 6 else user_id or "无名修士"
            row["user_name"] = name
            row["display_name"] = name

    def overview_snapshot(
        self,
        *,
        activities: list[dict[str, Any]],
        user_id: str,
        limit: int,
        today: str,
        activity_key: str,
        daily_task_key: str,
        weekly_task_key: str,
        tasks: list[dict[str, Any]],
        pass_config: dict[str, Any],
        elapsed_days: int,
    ) -> dict[str, Any]:
        row_limit = max(1, min(int(limit), 50))
        named_groups: list[list[dict[str, Any]]] = []
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            tables = self._assert_schema(uow)
            sign_summary = {
                "user_count": self._count(uow, "SELECT COUNT(*) AS count FROM activity_user"),
                "today_count": self._count(
                    uow,
                    "SELECT COUNT(*) AS count FROM activity_sign_log WHERE sign_date=?",
                    (today,),
                ),
                "log_count": self._count(uow, "SELECT COUNT(*) AS count FROM activity_sign_log"),
            }
            sign_rank = uow.query_all(
                "SELECT user_id,sign_days,total_sign_days,last_sign_date FROM activity_user "
                "ORDER BY sign_days DESC,total_sign_days DESC,last_sign_date ASC LIMIT ?",
                (row_limit,),
            )
            named_groups.append(sign_rank)

            activity_rows: list[dict[str, Any]] = []
            for activity in activities:
                key = str(activity["key"])
                activity_type = str(activity.get("type") or "")
                row = {
                    "key": key,
                    "name": activity.get("name", ""),
                    "type": activity_type,
                    "enabled": bool(activity.get("enabled")),
                    "state": activity.get("state", ""),
                }
                if activity_type == "event_points":
                    stats = uow.query_one(
                        "SELECT COUNT(*) AS user_count,COALESCE(SUM(points),0) AS current_points,"
                        "COALESCE(SUM(total_points),0) AS total_points FROM activity_point_balance "
                        "WHERE activity_key=?",
                        (key,),
                    ) or {}
                    row["counts"] = {
                        "user_count": int(stats.get("user_count") or 0),
                        "current_points": int(stats.get("current_points") or 0),
                        "total_points": int(stats.get("total_points") or 0),
                        "purchase_count": self._count(
                            uow,
                            "SELECT COALESCE(SUM(count),0) AS count FROM activity_point_purchase "
                            "WHERE activity_key=?",
                            (key,),
                        ),
                    }
                    row["top_users"] = uow.query_all(
                        "SELECT user_id,points,total_points,update_time FROM activity_point_balance "
                        "WHERE activity_key=? ORDER BY total_points DESC,points DESC,update_time ASC LIMIT ?",
                        (key, row_limit),
                    )
                    named_groups.append(row["top_users"])
                    if user_id:
                        balance = uow.query_one(
                            "SELECT points,total_points FROM activity_point_balance "
                            "WHERE activity_key=? AND user_id=?",
                            (key, user_id),
                        ) or {}
                        purchases = uow.query_all(
                            "SELECT item_key,count FROM activity_point_purchase "
                            "WHERE activity_key=? AND user_id=?",
                            (key, user_id),
                        )
                        row["user"] = {
                            "balance": {
                                "points": max(0, int(balance.get("points") or 0)),
                                "total_points": max(0, int(balance.get("total_points") or 0)),
                            },
                            "purchases": {
                                str(item["item_key"]): max(0, int(item["count"] or 0))
                                for item in purchases
                            },
                        }
                elif activity_type == "activity_boss":
                    hp = uow.query_one(
                        "SELECT hp_left,max_hp FROM activity_boss_state WHERE activity_key=?",
                        (key,),
                    ) or {}
                    damage = uow.query_one(
                        "SELECT COUNT(*) AS user_count,COALESCE(SUM(total_damage),0) AS total_damage "
                        "FROM activity_boss_damage WHERE activity_key=?",
                        (key,),
                    ) or {}
                    row["counts"] = {
                        "user_count": int(damage.get("user_count") or 0),
                        "total_damage": int(damage.get("total_damage") or 0),
                        "fight_count": self._count(
                            uow,
                            "SELECT COUNT(*) AS count FROM activity_boss_fight_log WHERE activity_key=?",
                            (key,),
                        ),
                        "item_count": self._count(
                            uow,
                            "SELECT COALESCE(SUM(count),0) AS count FROM activity_item_inventory "
                            "WHERE activity_key=?",
                            (key,),
                        ),
                        "milestone_count": self._count(
                            uow,
                            "SELECT COUNT(*) AS count FROM activity_boss_milestone WHERE activity_key=?",
                            (key,),
                        ),
                        "hp_left": int(hp.get("hp_left") or 0),
                        "max_hp": int(hp.get("max_hp") or 0),
                    }
                    row["top_users"] = uow.query_all(
                        "SELECT user_id,total_damage,update_time FROM activity_boss_damage "
                        "WHERE activity_key=? AND total_damage>0 "
                        "ORDER BY total_damage DESC,update_time ASC LIMIT ?",
                        (key, row_limit),
                    )
                    named_groups.append(row["top_users"])
                    if user_id:
                        damage_row = uow.query_one(
                            "SELECT total_damage,update_time FROM activity_boss_damage "
                            "WHERE activity_key=? AND user_id=?",
                            (key, user_id),
                        )
                        items = uow.query_all(
                            "SELECT item_id,count FROM activity_item_inventory "
                            "WHERE activity_key=? AND user_id=?",
                            (key, user_id),
                        )
                        row["user"] = {
                            "damage": damage_row or {"total_damage": 0, "update_time": ""},
                            "items": {
                                str(item["item_id"]): max(0, int(item["count"] or 0))
                                for item in items
                            },
                            "today_fight_count": self._count(
                                uow,
                                "SELECT COUNT(*) AS count FROM activity_boss_fight_log "
                                "WHERE activity_key=? AND user_id=? AND fight_date=?",
                                (key, user_id, today),
                            ),
                        }
                else:
                    inventory = uow.query_one(
                        "SELECT COUNT(DISTINCT user_id) AS user_count,COALESCE(SUM(count),0) "
                        "AS inventory_count FROM activity_collect_inventory WHERE activity_key=?",
                        (key,),
                    ) or {}
                    row["counts"] = {
                        "user_count": int(inventory.get("user_count") or 0),
                        "inventory_count": int(inventory.get("inventory_count") or 0),
                        "drop_count": self._count(
                            uow,
                            "SELECT COUNT(*) AS count FROM activity_collect_drop_log WHERE activity_key=?",
                            (key,),
                        ),
                        "claim_count": self._count(
                            uow,
                            "SELECT COALESCE(SUM(count),0) AS count FROM activity_collect_claim "
                            "WHERE activity_key=?",
                            (key,),
                        ),
                    }
                    row["top_users"] = uow.query_all(
                        "SELECT user_id,COUNT(*) AS drop_count,MAX(create_time) AS last_time "
                        "FROM activity_collect_drop_log WHERE activity_key=? GROUP BY user_id "
                        "ORDER BY drop_count DESC,last_time ASC LIMIT ?",
                        (key, row_limit),
                    )
                    named_groups.append(row["top_users"])
                    if user_id:
                        inventory_rows = uow.query_all(
                            "SELECT word_char,count FROM activity_collect_inventory "
                            "WHERE activity_key=? AND user_id=?",
                            (key, user_id),
                        )
                        claim_rows = uow.query_all(
                            "SELECT phrase,count FROM activity_collect_claim "
                            "WHERE activity_key=? AND user_id=?",
                            (key, user_id),
                        )
                        pity_rows = uow.query_all(
                            "SELECT event_key,miss_count FROM activity_collect_pity_state "
                            "WHERE activity_key=? AND user_id=?",
                            (key, user_id),
                        )
                        row["user"] = {
                            "inventory": {
                                str(item["word_char"]): max(0, int(item["count"] or 0))
                                for item in inventory_rows
                            },
                            "claims": {
                                str(item["phrase"]): max(0, int(item["count"] or 0))
                                for item in claim_rows
                            },
                            "pity": {
                                str(item["event_key"]): max(0, int(item["miss_count"] or 0))
                                for item in pity_rows
                            },
                        }
                activity_rows.append(row)

            task_stats = uow.query_one(
                "SELECT COUNT(DISTINCT user_id) AS user_count,"
                "COALESCE(SUM(CASE WHEN progress>=target THEN 1 ELSE 0 END),0) AS complete_count,"
                "COALESCE(SUM(claimed),0) AS claim_count FROM activity_task_progress "
                "WHERE activity_key=? AND scope_key IN (?,?)",
                (activity_key, daily_task_key, weekly_task_key),
            ) or {}
            task_user_rows = (
                uow.query_all(
                    "SELECT scope_type,scope_key,task_key,progress,target,claimed "
                    "FROM activity_task_progress WHERE activity_key=? AND user_id=?",
                    (activity_key, user_id),
                )
                if user_id
                else []
            )

            pass_result: dict[str, Any]
            if not pass_config.get("enabled"):
                pass_result = {"enabled": False, "activity_key": activity_key}
            else:
                pass_key = str(activity_key)
                pass_stats = uow.query_one(
                    "SELECT COUNT(*) AS user_count,COALESCE(SUM(total_exp),0) AS total_exp,"
                    "COALESCE(MAX(level),0) AS max_level FROM activity_pass_balance "
                    "WHERE activity_key=?",
                    (pass_key,),
                ) or {}
                pass_top_users = uow.query_all(
                    "SELECT user_id,total_exp,level,update_time FROM activity_pass_balance "
                    "WHERE activity_key=? ORDER BY level DESC,total_exp DESC,update_time ASC LIMIT ?",
                    (pass_key, row_limit),
                )
                named_groups.append(pass_top_users)
                pass_result = {
                    "enabled": True,
                    "activity_key": pass_key,
                    "name": pass_config.get("name"),
                    "exp_name": pass_config.get("exp_name"),
                    "level_exp": pass_config.get("level_exp"),
                    "max_level": pass_config.get("max_level"),
                    "catchup": {
                        "enabled": bool(pass_config.get("catchup_enabled")),
                        "start_day": pass_config.get("catchup_start_day"),
                        "level_gap": pass_config.get("catchup_level_gap"),
                        "multiplier": pass_config.get("catchup_multiplier"),
                    },
                    "user_count": int(pass_stats.get("user_count") or 0),
                    "total_exp": int(pass_stats.get("total_exp") or 0),
                    "highest_level": int(pass_stats.get("max_level") or 0),
                    "top_users": pass_top_users,
                }
                if user_id:
                    user_pass = uow.query_one(
                        "SELECT total_exp FROM activity_pass_balance "
                        "WHERE activity_key=? AND user_id=?",
                        (pass_key, user_id),
                    ) or {}
                    pass_result["user_state"] = {
                        "total_exp": max(0, int(user_pass.get("total_exp") or 0)),
                    }

            user_sign = None
            if user_id:
                user_sign = uow.query_one(
                    "SELECT * FROM activity_user WHERE user_id=?", (user_id,)
                )
                if user_sign:
                    user_sign["sign_days"] = int(user_sign.get("sign_days") or 0)
                    user_sign["total_sign_days"] = int(
                        user_sign.get("total_sign_days")
                        if user_sign.get("total_sign_days") is not None
                        else user_sign["sign_days"]
                    )
                else:
                    user_sign = {
                        "user_id": user_id,
                        "sign_days": 0,
                        "last_sign_date": "",
                        "total_sign_days": 0,
                    }

            self._display_names(
                uow,
                [row for group in named_groups for row in group],
                has_user_table="user_xiuxian" in tables,
            )

        return {
            "sign": sign_summary,
            "sign_rank": sign_rank,
            "activities": activity_rows,
            "tasks": {
                "activity_key": activity_key,
                "daily_task_count": sum(1 for task in tasks if task.get("scope_type") == "daily"),
                "weekly_task_count": sum(1 for task in tasks if task.get("scope_type") == "weekly"),
                "user_count": int(task_stats.get("user_count") or 0),
                "complete_count": int(task_stats.get("complete_count") or 0),
                "claim_count": int(task_stats.get("claim_count") or 0),
            },
            "task_user_rows": task_user_rows,
            "pass": pass_result,
            "pass_elapsed_days": max(0, int(elapsed_days)),
            "user_id": user_id,
            "user_sign": user_sign,
        }

    def reset(self, scope: str, activity_key: str, main_activity_key: str) -> int:
        target_scope = str(scope or "activity").strip() or "activity"
        key = str(activity_key or "").strip()
        valid_scopes = {"sign", "activity", "gameplay", "task", "tasks", "pass", "activity_pass", "all"}
        if target_scope not in valid_scopes:
            raise ValueError("清理范围无效")
        if target_scope == "activity" and not key:
            raise ValueError("请选择要清空的玩法活动")

        deleted = 0
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_schema(uow)
            if target_scope in {"sign", "all"}:
                for table in ("activity_user", "activity_sign_log"):
                    deleted += max(0, uow.execute(f"DELETE FROM {table}").rowcount)

            if target_scope in {"activity", "gameplay", "all"}:
                for table in _ACTIVITY_GAMEPLAY_RESET_TABLES:
                    if key:
                        cursor = uow.execute(
                            f"DELETE FROM {table} WHERE activity_key=?", (key,)
                        )
                    else:
                        cursor = uow.execute(f"DELETE FROM {table}")
                    deleted += max(0, cursor.rowcount)

            if target_scope in {"task", "tasks", "pass", "activity_pass", "all"}:
                task_activity_key = key or ("" if target_scope == "all" else main_activity_key)
                for table in _ACTIVITY_TASK_PASS_RESET_TABLES:
                    if task_activity_key:
                        cursor = uow.execute(
                            f"DELETE FROM {table} WHERE activity_key=?",
                            (task_activity_key,),
                        )
                    else:
                        cursor = uow.execute(f"DELETE FROM {table}")
                    deleted += max(0, cursor.rowcount)
        return deleted

    def adjust_points(self, activity_key: str, user_id: str, amount: int, timestamp: str) -> dict[str, int]:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT points,total_points FROM activity_point_balance "
                "WHERE activity_key=? AND user_id=?",
                (activity_key, user_id),
            ) or {}
            points = max(0, int(row.get("points") or 0) + amount)
            total_points = max(0, int(row.get("total_points") or 0)) + max(0, amount)
            uow.execute(
                "INSERT INTO activity_point_balance(activity_key,user_id,points,total_points,update_time) "
                "VALUES(?,?,?,?,?) ON CONFLICT(activity_key,user_id) DO UPDATE SET "
                "points=excluded.points,total_points=excluded.total_points,update_time=excluded.update_time",
                (activity_key, user_id, points, total_points, timestamp),
            )
        return {"points": points, "total_points": total_points}

    def adjust_collect_word(
        self, activity_key: str, user_id: str, word_char: str, amount: int, timestamp: str
    ) -> dict[str, Any]:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT count FROM activity_collect_inventory "
                "WHERE activity_key=? AND user_id=? AND word_char=?",
                (activity_key, user_id, word_char),
            ) or {}
            count = max(0, int(row.get("count") or 0) + amount)
            uow.execute(
                "INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count,update_time) "
                "VALUES(?,?,?,?,?) ON CONFLICT(activity_key,user_id,word_char) DO UPDATE SET "
                "count=excluded.count,update_time=excluded.update_time",
                (activity_key, user_id, word_char, count, timestamp),
            )
        return {"word_char": word_char, "count": count}

    def adjust_pass_exp(
        self,
        activity_key: str,
        user_id: str,
        amount: int,
        *,
        level_exp: int,
        max_level: int,
        timestamp: str,
    ) -> dict[str, int]:
        level_exp = max(1, int(level_exp))
        max_level = max(1, int(max_level))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT total_exp FROM activity_pass_balance "
                "WHERE activity_key=? AND user_id=?",
                (activity_key, user_id),
            ) or {}
            total_exp = max(0, int(row.get("total_exp") or 0) + amount)
            level = min(total_exp // level_exp, max_level)
            current_exp = level_exp if level >= max_level else max(0, total_exp - level * level_exp)
            uow.execute(
                "INSERT INTO activity_pass_balance(activity_key,user_id,exp,total_exp,level,update_time) "
                "VALUES(?,?,?,?,?,?) ON CONFLICT(activity_key,user_id) DO UPDATE SET "
                "exp=excluded.exp,total_exp=excluded.total_exp,level=excluded.level,update_time=excluded.update_time",
                (activity_key, user_id, current_exp, total_exp, level, timestamp),
            )
        return {
            "exp": current_exp,
            "total_exp": total_exp,
            "level": level,
            "level_exp": level_exp,
            "max_level": max_level,
        }


__all__ = ["ActivityAdminDataSqlRepository"]
