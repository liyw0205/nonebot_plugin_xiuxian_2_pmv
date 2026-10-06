from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class ActivityReadModelSqlRepository:
    """Read Activity progress projections without creating schema or files."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _assert_tables(uow: DatabaseUnitOfWork, required: set[str]) -> None:
        existing = {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        missing = sorted(required - existing)
        if missing:
            raise RuntimeError(
                f"activity_state.001 schema_missing: {', '.join(missing)}"
            )

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


__all__ = ["ActivityReadModelSqlRepository"]
