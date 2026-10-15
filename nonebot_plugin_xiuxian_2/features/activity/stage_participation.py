"""Read-only stage participation evidence for one player.

A stage counts as participated when the player has at least one row in an
existing activity log table inside that stage's time window. No new logging is
introduced: the evidence is the activity's own history.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

# (table, scoped_by_activity_key) — activity_sign_log is global per player.
EVIDENCE_TABLES: tuple[tuple[str, bool], ...] = (
    ("activity_sign_log", False),
    ("activity_point_event_log", True),
    ("activity_pass_event_log", True),
    ("activity_collect_drop_log", True),
    ("activity_boss_fight_log", True),
)


class ActivityStageParticipationRepository:
    """Read participation evidence without creating schema or files."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _table_names(uow: DatabaseUnitOfWork) -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }

    @staticmethod
    def _has_evidence(
        uow: DatabaseUnitOfWork,
        existing: set[str],
        user_id: str,
        activity_key: str,
        start: str | None,
        end: str | None,
    ) -> bool:
        for table, scoped in EVIDENCE_TABLES:
            if table not in existing:
                continue
            clauses = ["user_id=?"]
            params: list[Any] = [user_id]
            if scoped:
                clauses.append("activity_key=?")
                params.append(activity_key)
            if start:
                clauses.append("create_time>=?")
                params.append(start)
            if end:
                clauses.append("create_time<=?")
                params.append(end)
            row = uow.query_one(
                f"SELECT 1 AS found FROM {table} WHERE {' AND '.join(clauses)} LIMIT 1",
                tuple(params),
            )
            if row:
                return True
        return False

    def participated_stages(
        self,
        user_id: str,
        activity_key: str,
        windows: list[tuple[str, str | None, str | None]],
    ) -> set[str]:
        """Stage keys the player took part in.

        ``windows`` items are ``(stage_key, start_text, end_text)`` with
        ``"%Y-%m-%d %H:%M:%S"`` bounds; ``None`` means unbounded. Missing log
        tables are skipped so older databases keep working.
        """
        if not self.database.is_file() or not windows:
            return set()
        found: set[str] = set()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            existing = self._table_names(uow)
            for stage_key, start, end in windows:
                if self._has_evidence(
                    uow, existing, str(user_id), str(activity_key), start, end
                ):
                    found.add(str(stage_key))
        return found


__all__ = ["EVIDENCE_TABLES", "ActivityStageParticipationRepository"]
