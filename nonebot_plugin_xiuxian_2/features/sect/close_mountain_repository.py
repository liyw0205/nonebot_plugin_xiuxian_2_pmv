from __future__ import annotations
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork


class SectCloseMountainSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def ensure_schema(uow: DatabaseUnitOfWork) -> None:
        uow.execute("CREATE TABLE IF NOT EXISTS sect_close_mountain_operations(operation_id TEXT PRIMARY KEY,actor_id TEXT NOT NULL,sect_id INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

    def close(self, operation_id: str, actor_id: str, *, owner_position: int = 0, former_owner_position: int = 2, expected_sect_id: int | None = None) -> dict[str, Any]:
        operation_id, actor_id = str(operation_id).strip(), str(actor_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.ensure_schema(uow)
            previous = uow.query_one("SELECT o.sect_id,s.sect_name FROM sect_close_mountain_operations o LEFT JOIN sects s ON s.sect_id=o.sect_id WHERE o.operation_id=?", (operation_id,))
            if previous:
                return {"status":"duplicate","actor_id":actor_id,"sect_id":int(previous["sect_id"]),"sect_name":str(previous["sect_name"] or "")}
            actor = uow.query_one("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?", (actor_id,))
            if actor is None:
                return {"status":"actor_missing","actor_id":actor_id}
            if actor["sect_id"] is None:
                return {"status":"actor_without_sect","actor_id":actor_id}
            sect_id = int(actor["sect_id"])
            if expected_sect_id is not None and sect_id != int(expected_sect_id):
                return {"status":"sect_changed","actor_id":actor_id,"sect_id":sect_id}
            sect = uow.query_one("SELECT sect_owner,sect_name,closed FROM sects WHERE sect_id=?", (sect_id,))
            if sect is None:
                return {"status":"sect_missing","actor_id":actor_id,"sect_id":sect_id}
            result = {"actor_id":actor_id,"sect_id":sect_id,"sect_name":str(sect["sect_name"] or "")}
            if int(sect["closed"] or 0) == 1:
                return {"status":"already_closed",**result}
            if str(sect["sect_owner"]) != actor_id or int(actor["sect_position"] or 0) != int(owner_position):
                return {"status":"not_owner",**result}
            member = uow.execute("UPDATE user_xiuxian SET sect_position=? WHERE user_id=? AND sect_id=? AND sect_position=?", (int(former_owner_position), actor_id, sect_id, int(owner_position)))
            sect_update = uow.execute("UPDATE sects SET join_open=0,closed=1,sect_owner=NULL WHERE sect_id=? AND sect_owner=? AND COALESCE(closed,0)=0", (sect_id, actor_id))
            if member.rowcount != 1 or sect_update.rowcount != 1:
                raise RuntimeError("sect owner changed concurrently")
            uow.execute("INSERT INTO sect_close_mountain_operations(operation_id,actor_id,sect_id) VALUES(?,?,?)", (operation_id, actor_id, sect_id))
            return {"status":"closed",**result}
