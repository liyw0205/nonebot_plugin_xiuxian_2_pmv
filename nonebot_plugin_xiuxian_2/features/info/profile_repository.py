"""Read-only ownership for the shared player profile projection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

from ...core.errors import DomainError
from ...core.numeric import normalize_user_row
from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.database.uow import LIVE_PROJECTION_READ_OWNER


MAX_USER_SEARCH_QUERY_CHARS = 64
MAX_USER_SEARCH_RESULTS = 10


class PlayerProfileSqlRepository:
    """Read the legacy ``user_xiuxian`` projection without opening a writer."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def get_user_profile(
        self, user_id: int | str, *, raise_errors: bool = False
    ) -> dict[str, Any] | None:
        try:
            with DatabaseUnitOfWork(
                self.database, read_only=True,
                read_owner=LIVE_PROJECTION_READ_OWNER.when_unscoped(),
            ) as uow:
                row = uow.query_one(
                    "SELECT * FROM user_xiuxian WHERE user_id=? "
                    "ORDER BY rowid ASC LIMIT 1",
                    (str(user_id),),
                )
        except Exception as exc:
            if raise_errors:
                logger.opt(exception=exc).error(
                    "玩家档案读取失败 subject=player_profile"
                )
                raise DomainError(
                    "profile_read_failed",
                    "player profile could not be read",
                    {"reason": "profile_read_failed", "subject": "player_profile"},
                ) from exc
            # A missing database/schema is not a reason to create one during a
            # command.  Callers treat the closed projection as an unknown user.
            return None
        return normalize_user_row(row)

    def get_user_profile_by_name(self, user_name: str) -> dict[str, Any] | None:
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT * FROM user_xiuxian WHERE user_name=? "
                    "ORDER BY rowid ASC LIMIT 1",
                    (str(user_name),),
                )
        except Exception:
            return None
        return normalize_user_row(row)

    def get_user_display_profiles_by_ids(
        self, user_ids: list[str]
    ) -> dict[str, dict[str, Any]]:
        ids = list(dict.fromkeys(
            str(user_id) for user_id in user_ids
            if user_id is not None and str(user_id)
        ))
        if not ids:
            return {}

        profiles: dict[str, dict[str, Any]] = {}
        try:
            with DatabaseUnitOfWork(
                self.database, read_only=True,
                read_owner=LIVE_PROJECTION_READ_OWNER.when_unscoped(),
            ) as uow:
                for offset in range(0, len(ids), 400):
                    batch = ids[offset : offset + 400]
                    placeholders = ",".join("?" for _ in batch)
                    rows = uow.query_all(
                        "SELECT user_id,user_name,level FROM user_xiuxian "
                        f"WHERE user_id IN ({placeholders}) ORDER BY rowid ASC",
                        tuple(batch),
                    )
                    for row in rows:
                        profiles.setdefault(
                            str(row["user_id"]),
                            {"user_name": row["user_name"], "level": row["level"]},
                        )
        except Exception as exc:
            logger.opt(exception=exc).error("玩家展示档案读取失败 subject=player_profile")
            raise DomainError(
                "profile_read_failed",
                "player display profiles could not be read",
                {"reason": "profile_read_failed", "subject": "player_profile"},
            ) from exc
        return profiles

    @staticmethod
    def _escape_like(value: str) -> str:
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def search_users(self, query: str) -> list[dict[str, Any]]:
        query = str(query)
        if len(query) > MAX_USER_SEARCH_QUERY_CHARS:
            raise ValueError(f"query must be at most {MAX_USER_SEARCH_QUERY_CHARS} characters")
        pattern = f"%{self._escape_like(query)}%"
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                rows = uow.query_all(
                    "SELECT user_id, user_name FROM user_xiuxian "
                    "WHERE COALESCE(CAST(user_name AS TEXT), '') LIKE ? ESCAPE '\\' "
                    "ORDER BY rowid ASC LIMIT ?",
                    (pattern, MAX_USER_SEARCH_RESULTS),
                )
        except Exception:
            # Missing files/schemas must not trigger setup during a read.
            return []
        return [
            {"id": row.get("user_id"), "name": row.get("user_name")}
            for row in rows
        ]


__all__ = [
    "MAX_USER_SEARCH_QUERY_CHARS",
    "MAX_USER_SEARCH_RESULTS",
    "PlayerProfileSqlRepository",
]
