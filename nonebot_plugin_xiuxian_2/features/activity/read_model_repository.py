from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class ActivityReadModelSqlRepository:
    """Read Activity progress projections without creating schema or files."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _table_names(uow: DatabaseUnitOfWork) -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }

    @classmethod
    def _assert_tables(cls, uow: DatabaseUnitOfWork, required: set[str]) -> set[str]:
        existing = cls._table_names(uow)
        missing = sorted(required - existing)
        if missing:
            raise RuntimeError(
                f"activity_state.001 schema_missing: {', '.join(missing)}"
            )
        return existing

    def task_progress(
        self, activity_key: str, user_id: str
    ) -> dict[tuple[str, str, str], dict[str, Any]]:
        if not self.database.is_file():
            return {}
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_tables(uow, {"activity_task_progress"})
            rows = uow.query_all(
                "SELECT scope_type,scope_key,task_key,progress,target,claimed,claim_time "
                "FROM activity_task_progress WHERE activity_key=? AND user_id=?",
                (str(activity_key), str(user_id)),
            )
        return {
            (str(row["scope_type"]), str(row["scope_key"]), str(row["task_key"])): {
                "progress": max(0, int(row["progress"] or 0)),
                "target": max(1, int(row["target"] or 1)),
                "claimed": bool(int(row["claimed"] or 0)),
                "claim_time": str(row["claim_time"] or "").strip(),
            }
            for row in rows
        }

    def pass_summary(
        self,
        activity_key: str,
        user_id: str,
        *,
        level_exp: int,
        max_level: int,
    ) -> dict[str, Any]:
        if not self.database.is_file():
            return {
                "exp": 0,
                "total_exp": 0,
                "level": 0,
                "level_exp": max(1, int(level_exp)),
                "max_level": max(1, int(max_level)),
                "claimed_levels": set(),
                "highest_level": 0,
            }
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_tables(
                uow,
                {"activity_pass_balance", "activity_pass_reward_claim"},
            )
            balance_row = uow.query_one(
                "SELECT total_exp FROM activity_pass_balance "
                "WHERE activity_key=? AND user_id=?",
                (str(activity_key), str(user_id)),
            )
            claimed_rows = uow.query_all(
                "SELECT level FROM activity_pass_reward_claim "
                "WHERE activity_key=? AND user_id=?",
                (str(activity_key), str(user_id)),
            )
            highest_row = uow.query_one(
                "SELECT COALESCE(MAX(level),0) AS level "
                "FROM activity_pass_balance WHERE activity_key=?",
                (str(activity_key),),
            )

        level_exp = max(1, int(level_exp))
        max_level = max(1, int(max_level))
        total_exp = max(0, int((balance_row or {}).get("total_exp") or 0))
        level = min(total_exp // level_exp, max_level)
        current_exp = (
            level_exp if level >= max_level else max(0, total_exp - level * level_exp)
        )
        return {
            "exp": current_exp,
            "total_exp": total_exp,
            "level": level,
            "level_exp": level_exp,
            "max_level": max_level,
            "claimed_levels": {
                max(0, int(row["level"] or 0)) for row in claimed_rows
            },
            "highest_level": max(0, int((highest_row or {}).get("level") or 0)),
        }

    def sign_rank(self, limit: int = 10) -> list[dict[str, Any]]:
        if not self.database.is_file():
            return []
        limit = max(0, min(int(limit), 100))
        if limit == 0:
            return []
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_tables(uow, {"activity_user", "user_xiuxian"})
            rows = uow.query_all(
                "SELECT activity_user.user_id,activity_user.sign_days,"
                "activity_user.total_sign_days,activity_user.last_sign_date,"
                "user_xiuxian.user_name AS user_name FROM activity_user "
                "LEFT JOIN user_xiuxian ON user_xiuxian.user_id=activity_user.user_id "
                "ORDER BY activity_user.sign_days DESC,"
                "activity_user.total_sign_days DESC,activity_user.last_sign_date ASC "
                "LIMIT ?",
                (limit,),
            )
        result = []
        for row in rows:
            user_id = str(row.get("user_id") or "")
            name = str(row.get("user_name") or "").strip()
            if not name:
                name = f"修士·{user_id[-4:]}" if len(user_id) > 6 else user_id or "无名修士"
            result.append({**row, "display_name": name})
        return result

    def collect_state(
        self, user_id: str, activity_keys: list[str]
    ) -> dict[str, dict[tuple[str, str], int]]:
        keys = tuple(dict.fromkeys(str(key) for key in activity_keys if str(key)))
        empty = {"inventory": {}, "claims": {}, "pity": {}}
        if not self.database.is_file() or not keys:
            return empty

        placeholders = ",".join("?" for _ in keys)
        params = (*keys, str(user_id))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_tables(
                uow,
                {
                    "activity_collect_inventory",
                    "activity_collect_claim",
                    "activity_collect_pity_state",
                },
            )
            inventory_rows = uow.query_all(
                "SELECT activity_key,word_char,count FROM activity_collect_inventory "
                f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                params,
            )
            claim_rows = uow.query_all(
                "SELECT activity_key,phrase,count FROM activity_collect_claim "
                f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                params,
            )
            pity_rows = uow.query_all(
                "SELECT activity_key,event_key,miss_count FROM activity_collect_pity_state "
                f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                params,
            )
        return {
            "inventory": {
                (str(row["activity_key"]), str(row["word_char"])): max(
                    0, int(row["count"] or 0)
                )
                for row in inventory_rows
            },
            "claims": {
                (str(row["activity_key"]), str(row["phrase"])): max(
                    0, int(row["count"] or 0)
                )
                for row in claim_rows
            },
            "pity": {
                (str(row["activity_key"]), str(row["event_key"])): max(
                    0, int(row["miss_count"] or 0)
                )
                for row in pity_rows
            },
        }

    def point_balances(
        self, user_id: str, activity_keys: list[str]
    ) -> dict[str, dict[str, int]]:
        keys = tuple(dict.fromkeys(str(key) for key in activity_keys if str(key)))
        if not self.database.is_file() or not keys:
            return {}
        placeholders = ",".join("?" for _ in keys)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_tables(uow, {"activity_point_balance"})
            rows = uow.query_all(
                "SELECT activity_key,points,total_points FROM activity_point_balance "
                f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                (*keys, str(user_id)),
            )
        return {
            str(row["activity_key"]): {
                "points": max(0, int(row["points"] or 0)),
                "total_points": max(0, int(row["total_points"] or 0)),
            }
            for row in rows
        }

    def point_event_daily_totals(
        self, user_id: str, activity_keys: list[str], date: str
    ) -> dict[tuple[str, str], int] | None:
        """Today's earned points per ``(activity_key, event_key)``.

        Read-only and bounded (activity keys + user + date); returns ``None``
        when the optional event-log table is missing so the view skips the
        progress segment instead of advertising zeros. Mirrors the
        settlement-side daily-limit accumulation exactly.
        """
        keys = tuple(dict.fromkeys(str(key) for key in activity_keys if str(key)))
        if not self.database.is_file() or not keys:
            return {}
        placeholders = ",".join("?" for _ in keys)
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if "activity_point_event_log" not in self._table_names(uow):
                    return None
                rows = uow.query_all(
                    "SELECT activity_key,event_key,COALESCE(SUM(points),0) AS earned "
                    "FROM activity_point_event_log "
                    f"WHERE activity_key IN ({placeholders}) AND user_id=? AND record_date=? "
                    "GROUP BY activity_key,event_key",
                    (*keys, str(user_id), str(date)),
                )
        except sqlite3.Error:
            return None
        return {
            (str(row["activity_key"]), str(row["event_key"])): max(
                0, int(row["earned"] or 0)
            )
            for row in rows
        }

    def point_shop_state(
        self, user_id: str, activity_keys: list[str]
    ) -> dict[str, dict[tuple[str, str], int]]:
        keys = tuple(dict.fromkeys(str(key) for key in activity_keys if str(key)))
        empty = {"balances": {}, "purchases": {}, "stock": {}}
        if not self.database.is_file() or not keys:
            return empty
        placeholders = ",".join("?" for _ in keys)
        params = (*keys, str(user_id))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_tables(
                uow, {"activity_point_balance", "activity_point_purchase"}
            )
            balance_rows = uow.query_all(
                "SELECT activity_key,points FROM activity_point_balance "
                f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                params,
            )
            purchase_rows = uow.query_all(
                "SELECT activity_key,item_key,count FROM activity_point_purchase "
                f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                params,
            )
            stock_rows = uow.query_all(
                "SELECT activity_key,item_key,COALESCE(SUM(count),0) AS count "
                "FROM activity_point_purchase "
                f"WHERE activity_key IN ({placeholders}) GROUP BY activity_key,item_key",
                keys,
            )
        return {
            "balances": {
                str(row["activity_key"]): max(0, int(row["points"] or 0))
                for row in balance_rows
            },
            "purchases": {
                (str(row["activity_key"]), str(row["item_key"])): max(
                    0, int(row["count"] or 0)
                )
                for row in purchase_rows
            },
            "stock": {
                (str(row["activity_key"]), str(row["item_key"])): max(
                    0, int(row["count"] or 0)
                )
                for row in stock_rows
            },
        }

    def boss_snapshot(
        self,
        user_id: str,
        activity_keys: list[str],
        fight_date: str,
    ) -> dict[str, dict[str, Any]]:
        """Read boss state in one bounded, read-only database snapshot."""
        keys = tuple(dict.fromkeys(str(key) for key in activity_keys if str(key)))
        if not self.database.is_file() or not keys:
            return {}
        placeholders = ",".join("?" for _ in keys)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            tables = self._assert_tables(
                uow,
                {
                    "activity_boss_state",
                    "activity_boss_damage",
                    "activity_boss_fight_log",
                },
            )
            state_rows = uow.query_all(
                "SELECT activity_key,hp_left,max_hp FROM activity_boss_state "
                f"WHERE activity_key IN ({placeholders})",
                keys,
            )
            damage_rows = uow.query_all(
                "SELECT activity_key,total_damage FROM activity_boss_damage "
                f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                (*keys, str(user_id)),
            )
            fight_rows = uow.query_all(
                "SELECT activity_key,COUNT(*) AS fight_count FROM activity_boss_fight_log "
                f"WHERE activity_key IN ({placeholders}) AND user_id=? AND fight_date=? "
                "AND source IN ('coop','world_boss','item') GROUP BY activity_key",
                (*keys, str(user_id), str(fight_date)),
            )
            inventory_rows = []
            if "activity_item_inventory" in tables:
                inventory_rows = uow.query_all(
                    "SELECT activity_key,item_id,count FROM activity_item_inventory "
                    f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                    (*keys, str(user_id)),
                )
            milestone_rows = []
            if "activity_boss_milestone" in tables:
                milestone_rows = uow.query_all(
                    "SELECT activity_key,milestone_key FROM activity_boss_milestone "
                    f"WHERE activity_key IN ({placeholders})",
                    keys,
                )
            milestone_claim_rows = []
            if "activity_boss_milestone_claim" in tables:
                milestone_claim_rows = uow.query_all(
                    "SELECT activity_key,milestone_key FROM activity_boss_milestone_claim "
                    f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                    (*keys, str(user_id)),
                )
        result = {
            key: {
                "hp_left": max(0, int(row["hp_left"] or 0)),
                "stored_max_hp": max(1, int(row["max_hp"] or 1)),
                "damage": 0,
                "fight_count": 0,
                "inventory": {},
            }
            for row in state_rows
            for key in (str(row["activity_key"]),)
        }
        for row in damage_rows:
            result.setdefault(str(row["activity_key"]), {}).update(
                damage=max(0, int(row["total_damage"] or 0))
            )
        for row in fight_rows:
            result.setdefault(str(row["activity_key"]), {}).update(
                fight_count=max(0, int(row["fight_count"] or 0))
            )
        for row in inventory_rows:
            result.setdefault(str(row["activity_key"]), {}).setdefault("inventory", {})[
                str(row["item_id"])
            ] = max(0, int(row["count"] or 0))
        for row in milestone_rows:
            result.setdefault(str(row["activity_key"]), {}).setdefault(
                "milestones_unlocked", set()
            ).add(str(row["milestone_key"]))
        for row in milestone_claim_rows:
            result.setdefault(str(row["activity_key"]), {}).setdefault(
                "milestones_claimed", set()
            ).add(str(row["milestone_key"]))
        return result

    def boss_rank(self, activity_key: str, limit: int = 10) -> list[dict[str, Any]]:
        if not self.database.is_file():
            return []
        activity_key = str(activity_key).strip()
        limit = max(0, min(int(limit), 100))
        if not activity_key or limit == 0:
            return []
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            tables = self._assert_tables(uow, {"activity_boss_damage"})
            if "user_xiuxian" in tables:
                rows = uow.query_all(
                    "SELECT d.user_id,d.total_damage,u.user_name FROM activity_boss_damage d "
                    "LEFT JOIN user_xiuxian u ON u.user_id=d.user_id "
                    "WHERE d.activity_key=? AND d.total_damage>0 "
                    "ORDER BY d.total_damage DESC,d.user_id ASC LIMIT ?",
                    (activity_key, limit),
                )
            else:
                rows = uow.query_all(
                    "SELECT user_id,total_damage FROM activity_boss_damage "
                    "WHERE activity_key=? AND total_damage>0 "
                    "ORDER BY total_damage DESC,user_id ASC LIMIT ?",
                    (activity_key, limit),
                )
        return [
            {
                "user_id": str(row["user_id"] or ""),
                "total_damage": max(0, int(row["total_damage"] or 0)),
                "user_name": str(row.get("user_name") or "").strip(),
            }
            for row in rows
        ]

    def stage_claims(self, user_id: str, activity_key: str) -> set[str]:
        """Stage keys the player already claimed (empty when the table is absent)."""
        if not self.database.is_file():
            return set()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if "activity_stage_claim" not in self._table_names(uow):
                return set()
            rows = uow.query_all(
                "SELECT stage_key FROM activity_stage_claim "
                "WHERE activity_key=? AND user_id=?",
                (str(activity_key), str(user_id)),
            )
        return {str(row["stage_key"]) for row in rows}

    _BOARD_RANK_SQL = {
        "points": (
            "activity_point_balance",
            "SELECT {name}b.user_id,b.total_points AS value FROM activity_point_balance b {join}"
            "WHERE b.activity_key=? AND b.total_points>0 "
            "ORDER BY b.total_points DESC,b.points DESC,b.user_id ASC LIMIT ?",
        ),
        "collect": (
            "activity_collect_inventory",
            "SELECT {name}b.user_id,SUM(b.count) AS value FROM activity_collect_inventory b {join}"
            "WHERE b.activity_key=? GROUP BY b.user_id HAVING SUM(b.count)>0 "
            "ORDER BY value DESC,b.user_id ASC LIMIT ?",
        ),
        "pass": (
            "activity_pass_balance",
            "SELECT {name}b.user_id,b.level AS value FROM activity_pass_balance b {join}"
            "WHERE b.activity_key=? AND b.level>0 "
            "ORDER BY b.level DESC,b.total_exp DESC,b.user_id ASC LIMIT ?",
        ),
    }
    _BOARD_POSITION_SQL = {
        "points": (
            "activity_point_balance",
            "SELECT b.total_points AS value,b.points AS tie FROM activity_point_balance b "
            "WHERE b.activity_key=? AND b.user_id=?",
            "SELECT COUNT(*) AS better FROM activity_point_balance b WHERE b.activity_key=? AND b.total_points>0 "
            "AND (b.total_points>? OR (b.total_points=? AND b.points>?) "
            "OR (b.total_points=? AND b.points=? AND b.user_id<?))",
            "SELECT COUNT(*) AS total FROM activity_point_balance b WHERE b.activity_key=? AND b.total_points>0",
        ),
        "pass": (
            "activity_pass_balance",
            "SELECT b.level AS value,b.total_exp AS tie FROM activity_pass_balance b "
            "WHERE b.activity_key=? AND b.user_id=?",
            "SELECT COUNT(*) AS better FROM activity_pass_balance b WHERE b.activity_key=? AND b.level>0 "
            "AND (b.level>? OR (b.level=? AND b.total_exp>?) "
            "OR (b.level=? AND b.total_exp=? AND b.user_id<?))",
            "SELECT COUNT(*) AS total FROM activity_pass_balance b WHERE b.activity_key=? AND b.level>0",
        ),
        "collect": (
            "activity_collect_inventory",
            "SELECT COALESCE(SUM(b.count),0) AS value,0 AS tie FROM activity_collect_inventory b "
            "WHERE b.activity_key=? AND b.user_id=?",
            "SELECT COUNT(*) AS better FROM (SELECT b.user_id AS user_id,SUM(b.count) AS value "
            "FROM activity_collect_inventory b WHERE b.activity_key=? GROUP BY b.user_id "
            "HAVING SUM(b.count)>0) t WHERE t.value>? OR (t.value=? AND t.user_id<?)",
            "SELECT COUNT(*) AS total FROM (SELECT b.user_id FROM activity_collect_inventory b "
            "WHERE b.activity_key=? GROUP BY b.user_id HAVING SUM(b.count)>0) t",
        ),
    }

    def board_rank(
        self, source: str, activity_key: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Top-N rows for one configured rank board (empty when unavailable)."""
        source = str(source or "").strip()
        limit = max(0, min(int(limit), 100))
        if limit == 0:
            return []
        if source == "sign":
            return [
                {
                    "user_id": str(row.get("user_id") or ""),
                    "value": max(0, int(row.get("sign_days") or 0)),
                    "user_name": str(row.get("user_name") or ""),
                }
                for row in self.sign_rank(limit)
            ]
        if source == "boss":
            return [
                {
                    "user_id": str(row.get("user_id") or ""),
                    "value": max(0, int(row.get("total_damage") or 0)),
                    "user_name": str(row.get("user_name") or ""),
                }
                for row in self.boss_rank(str(activity_key), limit)
            ]
        spec = self._BOARD_RANK_SQL.get(source)
        key = str(activity_key).strip()
        if spec is None or not key or not self.database.is_file():
            return []
        table, sql = spec
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            tables = self._table_names(uow)
            if table not in tables:
                return []
            use_names = "user_xiuxian" in tables
            rows = uow.query_all(
                sql.format(
                    name="u.user_name," if use_names else "",
                    join="LEFT JOIN user_xiuxian u ON u.user_id=b.user_id " if use_names else "",
                ),
                (key, limit),
            )
        return [
            {
                "user_id": str(row["user_id"] or ""),
                "value": max(0, int(row["value"] or 0)),
                "user_name": str(row.get("user_name") or "").strip(),
            }
            for row in rows
        ]

    def board_position(
        self, source: str, activity_key: str, user_id: str
    ) -> tuple[int, int]:
        """Return the player's (1-based rank, value); (0, 0) when unranked."""
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return self.board_position_in_uow(uow, source, activity_key, user_id)

    def board_position_in_uow(
        self,
        uow: DatabaseUnitOfWork,
        source: str,
        activity_key: str,
        user_id: str,
    ) -> tuple[int, int]:
        """``board_position`` inside the caller's already-open read-only unit.

        ``overview_snapshot`` uses this so the whole personal overview stays
        ONE read-only unit of work instead of one per board.
        """
        source = str(source or "").strip()
        uid = str(user_id or "").strip()
        key = str(activity_key).strip()
        if not uid or not self.database.is_file():
            return 0, 0
        if source == "sign":
            if "activity_user" not in self._table_names(uow):
                return 0, 0
            mine = uow.query_one(
                "SELECT sign_days,total_sign_days,last_sign_date FROM activity_user "
                "WHERE user_id=?",
                (uid,),
            )
            if not mine:
                return 0, 0
            value = max(0, int(mine["sign_days"] or 0))
            tie = max(0, int(mine["total_sign_days"] or 0))
            if value <= 0:
                return 0, 0
            last = str(mine["last_sign_date"] or "")
            better = uow.query_one(
                "SELECT COUNT(*) AS better FROM activity_user WHERE sign_days>? "
                "OR (sign_days=? AND total_sign_days>?) "
                "OR (sign_days=? AND total_sign_days=? AND last_sign_date<?) "
                "OR (sign_days=? AND total_sign_days=? AND last_sign_date=? AND user_id<?)",
                (value, value, tie, value, tie, last, value, tie, last, uid),
            )
            return int((better or {}).get("better") or 0) + 1, value
        if source == "boss":
            if not key:
                return 0, 0
            if "activity_boss_damage" not in self._table_names(uow):
                return 0, 0
            mine = uow.query_one(
                "SELECT total_damage FROM activity_boss_damage "
                "WHERE activity_key=? AND user_id=?",
                (key, uid),
            )
            value = max(0, int((mine or {}).get("total_damage") or 0))
            if value <= 0:
                return 0, 0
            better = uow.query_one(
                "SELECT COUNT(*) AS better FROM activity_boss_damage "
                "WHERE activity_key=? AND (total_damage>? OR (total_damage=? AND user_id<?))",
                (key, value, value, uid),
            )
            return int((better or {}).get("better") or 0) + 1, value
        spec = self._BOARD_POSITION_SQL.get(source)
        if spec is None or not key:
            return 0, 0
        table, self_sql, better_sql, _total_sql = spec
        if table not in self._table_names(uow):
            return 0, 0
        mine = uow.query_one(self_sql, (key, uid))
        value = max(0, int((mine or {}).get("value") or 0))
        if value <= 0:
            return 0, 0
        tie = max(0, int((mine or {}).get("tie") or 0))
        if source == "collect":
            better = uow.query_one(better_sql, (key, value, value, uid))
        else:
            better = uow.query_one(
                better_sql, (key, value, value, tie, value, tie, uid)
            )
        return int((better or {}).get("better") or 0) + 1, value

    def board_total(self, source: str, activity_key: str) -> int:
        """Number of ranked participants on one board (0 when unavailable)."""
        source = str(source or "").strip()
        key = str(activity_key).strip()
        if not self.database.is_file():
            return 0
        if source == "sign":
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if "activity_user" not in self._table_names(uow):
                    return 0
                row = uow.query_one(
                    "SELECT COUNT(*) AS total FROM activity_user WHERE sign_days>0"
                )
            return max(0, int((row or {}).get("total") or 0))
        if source == "boss":
            if not key:
                return 0
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if "activity_boss_damage" not in self._table_names(uow):
                    return 0
                row = uow.query_one(
                    "SELECT COUNT(*) AS total FROM activity_boss_damage "
                    "WHERE activity_key=? AND total_damage>0",
                    (key,),
                )
            return max(0, int((row or {}).get("total") or 0))
        spec = self._BOARD_POSITION_SQL.get(source)
        if spec is None or not key:
            return 0
        table, _self_sql, _better_sql, total_sql = spec
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if table not in self._table_names(uow):
                return 0
            row = uow.query_one(total_sql, (key,))
        return max(0, int((row or {}).get("total") or 0))

    def overview_snapshot(
        self,
        user_id: str,
        activity_key: str,
        *,
        include_tasks: bool,
        include_pass: bool,
        collect_activity_keys: list[str],
        point_activity_keys: list[str] | tuple[str, ...] = (),
        stage_windows: Sequence[tuple[str, str | None, str | None]] = (),
        board_sources: Sequence[str] = (),
        boss_activity_keys: Sequence[str] = (),
        now: Any = None,
    ) -> dict[str, Any]:
        """Read all personal overview state within a single game_db snapshot.

        ``stage_windows``/``board_sources`` opt into the claim state the activity
        center needs to decide whether anything is claimable; both default to
        empty so existing callers keep their current snapshot shape. ``now``
        (datetime) selects the day for the optional point-event daily totals;
        it defaults to the current time.
        """
        empty: dict[str, Any] = {
            "sign": {},
            "tasks": {},
            "pass_total_exp": 0,
            "pass_claimed_levels": set(),
            "pass_highest_level": 0,
            "collect_pity": {},
            "point_balances": {},
            "stage_participated": set(),
            "stage_claimed": set(),
            "rank_claimed": set(),
            "boss_milestone_unlocked": set(),
            "boss_milestone_claimed": set(),
            "boss_rank_claimed": set(),
            "board_positions": {},
            "boss_board_positions": {},
            "boss_progress": {},
            "point_event_daily_totals": None,
        }
        if not self.database.is_file():
            return empty

        required = {"activity_user"}
        if include_tasks:
            required.add("activity_task_progress")
        if include_pass:
            required.update({"activity_pass_balance", "activity_pass_reward_claim"})
        collect_keys = tuple(dict.fromkeys(str(key) for key in collect_activity_keys if str(key)))
        if collect_keys:
            required.add("activity_collect_pity_state")
        point_keys = tuple(
            dict.fromkeys(str(key) for key in point_activity_keys if str(key))
        )
        if point_keys:
            required.add("activity_point_balance")

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_tables(uow, required)
            sign = uow.query_one(
                "SELECT sign_days,last_sign_date,total_sign_days FROM activity_user "
                "WHERE user_id=?",
                (str(user_id),),
            ) or {}
            task_rows = []
            if include_tasks:
                task_rows = uow.query_all(
                    "SELECT scope_type,scope_key,task_key,progress,target,claimed,claim_time "
                    "FROM activity_task_progress WHERE activity_key=? AND user_id=?",
                    (str(activity_key), str(user_id)),
                )
            pass_total_exp = 0
            claimed_levels: set[int] = set()
            highest_level = 0
            if include_pass:
                pass_row = uow.query_one(
                    "SELECT total_exp FROM activity_pass_balance "
                    "WHERE activity_key=? AND user_id=?",
                    (str(activity_key), str(user_id)),
                )
                pass_total_exp = max(0, int((pass_row or {}).get("total_exp") or 0))
                claimed_levels = {
                    max(0, int(row["level"] or 0))
                    for row in uow.query_all(
                        "SELECT level FROM activity_pass_reward_claim "
                        "WHERE activity_key=? AND user_id=?",
                        (str(activity_key), str(user_id)),
                    )
                }
                highest_row = uow.query_one(
                    "SELECT COALESCE(MAX(level),0) AS level FROM activity_pass_balance "
                    "WHERE activity_key=?",
                    (str(activity_key),),
                )
                highest_level = max(0, int((highest_row or {}).get("level") or 0))
            pity_rows = []
            if collect_keys:
                placeholders = ",".join("?" for _ in collect_keys)
                pity_rows = uow.query_all(
                    "SELECT activity_key,event_key,miss_count FROM activity_collect_pity_state "
                    f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                    (*collect_keys, str(user_id)),
                )
            point_rows = []
            if point_keys:
                placeholders = ", ".join("?" for _ in point_keys)
                point_rows = uow.query_all(
                    "SELECT activity_key,points FROM activity_point_balance "
                    f"WHERE activity_key IN ({placeholders}) AND user_id=?",
                    (*point_keys, str(user_id)),
                )
            tables = self._table_names(uow)
            stage_claimed: set[str] = set()
            if "activity_stage_claim" in tables:
                stage_claimed = {
                    str(row["stage_key"])
                    for row in uow.query_all(
                        "SELECT stage_key FROM activity_stage_claim "
                        "WHERE activity_key=? AND user_id=?",
                        (str(activity_key), str(user_id)),
                    )
                }
            rank_claimed: set[tuple[str, str]] = set()
            if "activity_rank_claim" in tables:
                rank_claimed = {
                    (str(row["board_key"]), str(row["tier_key"]))
                    for row in uow.query_all(
                        "SELECT board_key,tier_key FROM activity_rank_claim "
                        "WHERE activity_key=? AND user_id=?",
                        (str(activity_key), str(user_id)),
                    )
                }
            boss_milestone_unlocked: set[str] = set()
            if "activity_boss_milestone" in tables:
                boss_milestone_unlocked = {
                    str(row["milestone_key"])
                    for row in uow.query_all(
                        "SELECT milestone_key FROM activity_boss_milestone "
                        "WHERE activity_key=?",
                        (str(activity_key),),
                    )
                }
            boss_milestone_claimed: set[str] = set()
            if "activity_boss_milestone_claim" in tables:
                boss_milestone_claimed = {
                    str(row["milestone_key"])
                    for row in uow.query_all(
                        "SELECT milestone_key FROM activity_boss_milestone_claim "
                        "WHERE activity_key=? AND user_id=?",
                        (str(activity_key), str(user_id)),
                    )
                }
            boss_rank_claimed: set[str] = set()
            if "activity_boss_rank_claim" in tables:
                boss_rank_claimed = {
                    str(row["tier_key"])
                    for row in uow.query_all(
                        "SELECT tier_key FROM activity_boss_rank_claim "
                        "WHERE activity_key=? AND user_id=?",
                        (str(activity_key), str(user_id)),
                    )
                }
            boss_progress: dict[str, dict[str, Any]] = {}
            boss_keys = tuple(
                dict.fromkeys(str(key) for key in boss_activity_keys if str(key))
            )
            if boss_keys:
                boss_placeholders = ",".join("?" for _ in boss_keys)
                boss_progress = {
                    key: {
                        "hp_left": 0,
                        "max_hp": 0,
                        "damage": 0,
                        "milestones_unlocked": set(),
                        "milestones_claimed": set(),
                        "rank_claimed": set(),
                    }
                    for key in boss_keys
                }
                if "activity_boss_state" in tables:
                    for row in uow.query_all(
                        "SELECT activity_key,hp_left,max_hp FROM activity_boss_state "
                        f"WHERE activity_key IN ({boss_placeholders})",
                        boss_keys,
                    ):
                        boss_progress[str(row["activity_key"])].update(
                            hp_left=max(0, int(row["hp_left"] or 0)),
                            max_hp=max(1, int(row["max_hp"] or 1)),
                        )
                if "activity_boss_damage" in tables:
                    for row in uow.query_all(
                        "SELECT activity_key,total_damage FROM activity_boss_damage "
                        f"WHERE activity_key IN ({boss_placeholders}) AND user_id=?",
                        (*boss_keys, str(user_id)),
                    ):
                        boss_progress[str(row["activity_key"])]["damage"] = max(
                            0, int(row["total_damage"] or 0)
                        )
                if "activity_boss_milestone" in tables:
                    for row in uow.query_all(
                        "SELECT activity_key,milestone_key FROM activity_boss_milestone "
                        f"WHERE activity_key IN ({boss_placeholders})",
                        boss_keys,
                    ):
                        boss_progress[str(row["activity_key"])][
                            "milestones_unlocked"
                        ].add(str(row["milestone_key"]))
                if "activity_boss_milestone_claim" in tables:
                    for row in uow.query_all(
                        "SELECT activity_key,milestone_key FROM activity_boss_milestone_claim "
                        f"WHERE activity_key IN ({boss_placeholders}) AND user_id=?",
                        (*boss_keys, str(user_id)),
                    ):
                        boss_progress[str(row["activity_key"])][
                            "milestones_claimed"
                        ].add(str(row["milestone_key"]))
                if "activity_boss_rank_claim" in tables:
                    for row in uow.query_all(
                        "SELECT activity_key,tier_key FROM activity_boss_rank_claim "
                        f"WHERE activity_key IN ({boss_placeholders}) AND user_id=?",
                        (*boss_keys, str(user_id)),
                    ):
                        boss_progress[str(row["activity_key"])]["rank_claimed"].add(
                            str(row["tier_key"])
                        )

            stage_participated: set[str] = set()
            if stage_windows:
                from .stage_participation import ActivityStageParticipationRepository

                stage_participated = (
                    ActivityStageParticipationRepository(self.database)
                    .participated_stages_in_uow(
                        uow, str(user_id), str(activity_key), list(stage_windows)
                    )
                )
            board_positions: dict[str, int] = {}
            boss_board_keys = tuple(
                dict.fromkeys(str(key) for key in boss_activity_keys if str(key))
            )
            for source in board_sources:
                # The boss damage tables are keyed by the boss gameplay
                # activity's own key, not the main activity key; without this
                # the boss board position would always read 0 for players of a
                # differently-keyed boss activity.
                source_key = (
                    boss_board_keys[0]
                    if str(source) == "boss" and boss_board_keys
                    else str(activity_key)
                )
                position, _ = self.board_position_in_uow(
                    uow, str(source), source_key, str(user_id)
                )
                board_positions[str(source)] = max(0, int(position))
            # Per-boss-activity rank positions: with multiple boss activities
            # one shared "boss" position would match one boss's rank against
            # another boss's reward tiers.
            boss_board_positions: dict[str, int] = {}
            for boss_key in boss_board_keys:
                position, _ = self.board_position_in_uow(
                    uow, "boss", boss_key, str(user_id)
                )
                boss_board_positions[boss_key] = max(0, int(position))
            # Optional per-event daily totals for the points progress line;
            # the event log is optional so older databases get None (the
            # progress segment then renders without the 今日 part).
            point_event_daily_totals: dict[tuple[str, str], int] | None = None
            if point_keys and "activity_point_event_log" in tables:
                point_placeholders = ",".join("?" for _ in point_keys)
                day = (
                    now.strftime("%Y-%m-%d")
                    if now is not None
                    else datetime.now().strftime("%Y-%m-%d")
                )
                point_event_daily_totals = {}
                for row in uow.query_all(
                    "SELECT activity_key,event_key,COALESCE(SUM(points),0) AS earned "
                    "FROM activity_point_event_log "
                    f"WHERE activity_key IN ({point_placeholders}) AND user_id=? AND record_date=? "
                    "GROUP BY activity_key,event_key",
                    (*point_keys, str(user_id), day),
                ):
                    point_event_daily_totals[
                        (str(row["activity_key"]), str(row["event_key"]))
                    ] = max(0, int(row["earned"] or 0))

        tasks = {
            (str(row["scope_type"]), str(row["scope_key"]), str(row["task_key"])): {
                "progress": max(0, int(row["progress"] or 0)),
                "target": max(1, int(row["target"] or 1)),
                "claimed": bool(int(row["claimed"] or 0)),
                "claim_time": str(row["claim_time"] or "").strip(),
            }
            for row in task_rows
        }
        return {
            "sign": {
                "sign_days": max(0, int(sign.get("sign_days") or 0)),
                "last_sign_date": str(sign.get("last_sign_date") or "").strip(),
                "total_sign_days": max(0, int(sign.get("total_sign_days") or 0)),
            },
            "tasks": tasks,
            "pass_total_exp": pass_total_exp,
            "pass_claimed_levels": claimed_levels,
            "pass_highest_level": highest_level,
            "collect_pity": {
                (str(row["activity_key"]), str(row["event_key"])): max(
                    0, int(row["miss_count"] or 0)
                )
                for row in pity_rows
            },
            "point_balances": {
                str(row["activity_key"]): max(0, int(row["points"] or 0))
                for row in point_rows
            },
            "stage_participated": stage_participated,
            "stage_claimed": stage_claimed,
            "rank_claimed": rank_claimed,
            "boss_milestone_unlocked": boss_milestone_unlocked,
            "boss_milestone_claimed": boss_milestone_claimed,
            "boss_rank_claimed": boss_rank_claimed,
            "board_positions": board_positions,
            "boss_board_positions": boss_board_positions,
            "boss_progress": boss_progress,
            "point_event_daily_totals": point_event_daily_totals,
        }


__all__ = ["ActivityReadModelSqlRepository"]
