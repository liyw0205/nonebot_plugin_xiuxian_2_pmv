from __future__ import annotations

from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class DufangSharingPreferencesSqlRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = Path(player_database)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        columns = {
            str(row[1]).casefold()
            for row in uow.execute('PRAGMA table_info("dufang_sharing_preferences")').fetchall()
        }
        return {"user_id", "enabled_at"}.issubset(columns)

    def enabled(self, user_id: str) -> bool | None:
        user_id = str(user_id).strip()
        if not user_id or not self.player_database.is_file():
            return None
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT 1 AS enabled FROM dufang_sharing_preferences WHERE user_id=?",
                (user_id,),
            )
        return row is not None

    def enabled_users(self, excluding_user_id: str = "") -> tuple[str, ...] | None:
        if not self.player_database.is_file():
            return None
        excluded = str(excluding_user_id).strip()
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            rows = uow.query_all(
                "SELECT user_id FROM dufang_sharing_preferences "
                "WHERE user_id<>? ORDER BY user_id",
                (excluded,),
            )
        return tuple(str(row["user_id"]) for row in rows)

    def set_enabled(self, user_id: str, enabled: bool, updated_at: str) -> bool | None:
        user_id, updated_at = str(user_id).strip(), str(updated_at).strip()
        if not user_id or not updated_at or not self.player_database.is_file():
            return None
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return None
            if enabled:
                changed = uow.execute(
                    "INSERT OR IGNORE INTO dufang_sharing_preferences(user_id,enabled_at) VALUES(?,?)",
                    (user_id, updated_at),
                ).rowcount
            else:
                changed = uow.execute(
                    "DELETE FROM dufang_sharing_preferences WHERE user_id=?",
                    (user_id,),
                ).rowcount
        return changed == 1

    def import_enabled_users(self, user_ids: tuple[str, ...] | list[str], updated_at: str) -> int | None:
        updated_at = str(updated_at).strip()
        users = tuple(dict.fromkeys(str(user_id).strip() for user_id in user_ids if str(user_id).strip()))
        if not updated_at or not self.player_database.is_file():
            return None
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return None
            inserted = 0
            for user_id in users:
                inserted += uow.execute(
                    "INSERT OR IGNORE INTO dufang_sharing_preferences(user_id,enabled_at) VALUES(?,?)",
                    (user_id, updated_at),
                ).rowcount
        return inserted


__all__ = ["DufangSharingPreferencesSqlRepository"]
