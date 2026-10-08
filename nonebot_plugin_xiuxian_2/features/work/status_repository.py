from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .refresh_repository import WorkRefreshResult, WorkRefreshSqlRepository


@dataclass(frozen=True)
class WorkStatusState:
    cooldown: dict[str, Any] | None = None
    offer: dict[str, Any] | None = None
    active_snapshot: dict[str, Any] | None = None
    has_sql_offer: bool = False


class WorkStatusSqlRepository:
    """Read work state without request-time schema creation."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)
        self.refresh_repository = WorkRefreshSqlRepository(database)

    @staticmethod
    def _snapshot(value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        parsed = json.loads(str(value))
        if not isinstance(parsed, dict):
            raise ValueError("work snapshot must be an object")
        return parsed

    def get_state(self, user_id: str) -> WorkStatusState:
        if not self.database.is_file():
            return WorkStatusState()

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            tables = {
                str(row["name"])
                for row in uow.query_all(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name IN ('user_cd','work_offer_snapshots','work_active_snapshots')"
                )
            }
            cooldown = None
            if "user_cd" in tables:
                cooldown = uow.query_one(
                    "SELECT * FROM user_cd WHERE user_id=?", (str(user_id),)
                )

            has_sql_offer = "work_offer_snapshots" in tables
            offer = None
            if has_sql_offer:
                row = uow.query_one(
                    "SELECT snapshot FROM work_offer_snapshots WHERE user_id=?",
                    (str(user_id),),
                )
                offer = self._snapshot(row["snapshot"]) if row is not None else None

            active_snapshot = None
            if "work_active_snapshots" in tables:
                row = uow.query_one(
                    "SELECT snapshot FROM work_active_snapshots WHERE user_id=?",
                    (str(user_id),),
                )
                active_snapshot = self._snapshot(row["snapshot"]) if row is not None else None

        return WorkStatusState(
            cooldown=dict(cooldown) if cooldown is not None else None,
            offer=offer,
            active_snapshot=active_snapshot,
            has_sql_offer=has_sql_offer and offer is not None,
        )

    def get_offer(self, user_id: str) -> dict[str, Any] | None:
        if not self.database.is_file():
            return None

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            table = uow.query_one(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='work_offer_snapshots'"
            )
            if table is None:
                return None
            row = uow.query_one(
                "SELECT snapshot FROM work_offer_snapshots WHERE user_id=?",
                (str(user_id),),
            )
        return self._snapshot(row["snapshot"]) if row is not None else None

    def mark_offer_expired(
        self, user_id: str, expected_offer: Mapping[str, Any], updated_at: str
    ) -> WorkRefreshResult:
        return self.refresh_repository.mark_offer_expired(
            user_id, expected_offer, updated_at
        )


__all__ = ["WorkStatusSqlRepository", "WorkStatusState"]
