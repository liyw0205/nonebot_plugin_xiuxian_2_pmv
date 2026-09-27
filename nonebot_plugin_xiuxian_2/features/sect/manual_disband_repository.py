from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class SectManualDisbandSqlRepository:
    _COLUMNS = {"operation_id", "actor_id", "sect_id", "sect_name", "member_count", "created_at"}

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='sect_disband_operations'"
        )
        if table is None:
            return False
        columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(sect_disband_operations)")}
        return cls._COLUMNS.issubset(columns)

    def disband(
        self,
        operation_id: str,
        actor_id: str,
        *,
        expected_sect_id: int | None = None,
        owner_position: int = 0,
    ) -> dict[str, Any]:
        operation_id, actor_id = str(operation_id).strip(), str(actor_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return {"status": "schema_missing", "actor_id": actor_id}

            previous = uow.query_one(
                "SELECT sect_id,sect_name,member_count FROM sect_disband_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return {
                    "status": "duplicate",
                    "actor_id": actor_id,
                    "sect_id": int(previous["sect_id"]),
                    "sect_name": str(previous["sect_name"] or ""),
                    "member_count": int(previous["member_count"] or 0),
                }

            actor = uow.query_one(
                "SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?",
                (actor_id,),
            )
            if actor is None:
                return {"status": "actor_missing", "actor_id": actor_id}
            if actor["sect_id"] is None:
                return {"status": "actor_without_sect", "actor_id": actor_id}

            sect_id = int(actor["sect_id"])
            if expected_sect_id is not None and sect_id != int(expected_sect_id):
                return {"status": "sect_changed", "actor_id": actor_id, "sect_id": sect_id}

            sect = uow.query_one(
                "SELECT sect_owner,sect_name FROM sects WHERE sect_id=?",
                (sect_id,),
            )
            if sect is None:
                return {"status": "sect_missing", "actor_id": actor_id, "sect_id": sect_id}

            sect_name = str(sect["sect_name"] or "")
            if str(sect["sect_owner"]) != actor_id or int(actor["sect_position"]) != int(owner_position):
                return {"status": "not_owner", "actor_id": actor_id, "sect_id": sect_id, "sect_name": sect_name}

            member_count = int(
                uow.query_one("SELECT COUNT(*) AS count FROM user_xiuxian WHERE sect_id=?", (sect_id,))["count"]
            )
            members = uow.execute(
                "UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL,sect_contribution=0 WHERE sect_id=?",
                (sect_id,),
            )
            deleted = uow.execute(
                "DELETE FROM sects WHERE sect_id=? AND sect_owner=?",
                (sect_id, actor_id),
            )
            if members.rowcount != member_count or deleted.rowcount != 1:
                raise RuntimeError("sect ownership or membership changed")

            uow.execute(
                "INSERT INTO sect_disband_operations(operation_id,actor_id,sect_id,sect_name,member_count) "
                "VALUES(?,?,?,?,?)",
                (operation_id, actor_id, sect_id, sect_name, member_count),
            )
            return {
                "status": "disbanded",
                "actor_id": actor_id,
                "sect_id": sect_id,
                "sect_name": sect_name,
                "member_count": member_count,
            }


__all__ = ["SectManualDisbandSqlRepository"]
