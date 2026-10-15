"""Stage reward claim persistence: one row per player per ended stage."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ...infrastructure.database import DatabaseUnitOfWork


class ActivityStageClaimRepository:
    """Record and read stage reward claims without creating schema."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _table_names(uow: DatabaseUnitOfWork) -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }

    def claimed_stages(self, user_id: str, activity_key: str) -> set[str]:
        """Stage keys already claimed by the player (empty when table absent)."""
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

    def record_claims(
        self,
        user_id: str,
        activity_key: str,
        rows: Iterable[tuple[str, str]],
        *,
        timestamp: str,
    ) -> int:
        """Insert claim rows idempotently; returns how many were newly inserted."""
        pending = [(str(stage_key), str(reward)) for stage_key, reward in rows]
        if not pending:
            return 0
        inserted = 0
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            for stage_key, reward in pending:
                cursor = uow.execute(
                    "INSERT OR IGNORE INTO activity_stage_claim "
                    "(activity_key,user_id,stage_key,reward,create_time) "
                    "VALUES(?,?,?,?,?)",
                    (str(activity_key), str(user_id), stage_key, reward, str(timestamp)),
                )
                inserted += max(0, int(cursor.rowcount or 0))
        return inserted


__all__ = ["ActivityStageClaimRepository"]
