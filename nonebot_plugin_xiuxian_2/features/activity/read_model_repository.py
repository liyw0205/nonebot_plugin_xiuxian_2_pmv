from __future__ import annotations

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


__all__ = ["ActivityReadModelSqlRepository"]
