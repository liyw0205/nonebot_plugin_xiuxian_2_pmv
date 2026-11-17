from __future__ import annotations
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork
from .schema import CLOSE_MOUNTAIN_COLUMNS, SCHEMA_MISSING, table_ready


class _CloseMountainRejected(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        self.reason = reason
        self.subject = subject


class SectCloseMountainSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _ready(uow: DatabaseUnitOfWork) -> bool:
        """Startup migration `sect.019` owns the receipt ledger; verify, never create."""
        return table_ready(uow, "sect_close_mountain_operations", CLOSE_MOUNTAIN_COLUMNS)

    def close(self, operation_id: str, actor_id: str, *, owner_position: int = 0, former_owner_position: int = 2, expected_sect_id: int | None = None) -> dict[str, Any]:
        operation_id, actor_id = str(operation_id).strip(), str(actor_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._ready(uow):
                return {"status": SCHEMA_MISSING}
            previous = uow.query_one("SELECT o.actor_id,o.sect_id,s.sect_name FROM sect_close_mountain_operations o LEFT JOIN sects s ON s.sect_id=o.sect_id WHERE o.operation_id=?", (operation_id,))
            if previous:
                if str(previous["actor_id"]) != actor_id:
                    return {"status":"operation_conflict","actor_id":actor_id,"sect_id":previous["sect_id"],"reason":"operation_conflict","subject":"actor"}
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
            try:
                with uow.savepoint("sect_close_mountain"):
                    member = uow.execute("UPDATE user_xiuxian SET sect_position=? WHERE user_id=? AND sect_id=? AND sect_position=?", (int(former_owner_position), actor_id, sect_id, int(owner_position)))
                    saved_member = uow.query_one("SELECT sect_position FROM user_xiuxian WHERE user_id=? AND sect_id=?", (actor_id, sect_id))
                    if member.rowcount != 1 or saved_member is None or int(saved_member["sect_position"]) != int(former_owner_position):
                        raise _CloseMountainRejected("sect_close_mountain_member_write_rejected", "actor")
                    sect_update = uow.execute("UPDATE sects SET join_open=0,closed=1,sect_owner=NULL WHERE sect_id=? AND sect_owner=? AND COALESCE(closed,0)=0", (sect_id, actor_id))
                    saved_sect = uow.query_one("SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=?", (sect_id,))
                    if sect_update.rowcount != 1 or saved_sect is None or saved_sect["sect_owner"] is not None or int(saved_sect["closed"] or 0) != 1 or int(saved_sect["join_open"] or 0) != 0:
                        raise _CloseMountainRejected("sect_close_mountain_state_write_rejected", "sect")
                    receipt = uow.execute("INSERT INTO sect_close_mountain_operations(operation_id,actor_id,sect_id) VALUES(?,?,?)", (operation_id, actor_id, sect_id))
                    if receipt.rowcount != 1:
                        raise _CloseMountainRejected("sect_close_mountain_receipt_write_rejected", operation_id)
                    saved_receipt = uow.query_one("SELECT actor_id,sect_id FROM sect_close_mountain_operations WHERE operation_id=?", (operation_id,))
                    expected = (actor_id, sect_id)
                    actual = None if saved_receipt is None else (str(saved_receipt["actor_id"]), int(saved_receipt["sect_id"]))
                    if actual != expected:
                        raise _CloseMountainRejected("sect_close_mountain_receipt_write_rejected", operation_id)
            except _CloseMountainRejected as exc:
                return {"status":"state_changed", "reason":exc.reason, "subject":exc.subject, **result}
            return {"status":"closed",**result}
