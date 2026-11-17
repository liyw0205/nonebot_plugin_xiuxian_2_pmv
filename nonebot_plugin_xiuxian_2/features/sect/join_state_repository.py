from __future__ import annotations
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork
from .schema import JOIN_STATE_COLUMNS, SCHEMA_MISSING, table_ready


class _JoinStateRejected(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        self.reason = reason
        self.subject = subject


class SectJoinStateSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def _ready(self, uow: DatabaseUnitOfWork, action: str) -> bool:
        """Startup migration `sect.019` owns the join receipts; verify, never create."""
        return table_ready(uow, f"sect_{action}_join_operations", JOIN_STATE_COLUMNS)

    def _change(self, action: str, operation_id: str, actor_id: str, *, owner_position: int = 0, expected_sect_id: int | None = None) -> dict[str, Any]:
        operation_id, actor_id = str(operation_id).strip(), str(actor_id)
        if not operation_id: raise ValueError("operation_id must not be empty")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._ready(uow, action):
                return {"status": SCHEMA_MISSING}
            previous = uow.query_one(f"SELECT o.actor_id,o.sect_id,s.sect_name FROM sect_{action}_join_operations o LEFT JOIN sects s ON s.sect_id=o.sect_id WHERE o.operation_id=?", (operation_id,))
            if previous:
                if str(previous["actor_id"]) != actor_id:
                    return {"status":"operation_conflict","actor_id":actor_id,"sect_id":int(previous["sect_id"]),"reason":"operation_conflict","subject":"actor"}
                return {"status":"duplicate","actor_id":actor_id,"sect_id":int(previous["sect_id"]),"sect_name":str(previous["sect_name"] or "")}
            actor = uow.query_one("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?", (actor_id,))
            if actor is None: return {"status":"actor_missing","actor_id":actor_id}
            if actor["sect_id"] is None: return {"status":"actor_without_sect","actor_id":actor_id}
            sect_id = int(actor["sect_id"])
            base = {"actor_id":actor_id,"sect_id":sect_id}
            if expected_sect_id is not None and sect_id != int(expected_sect_id): return {"status":"sect_changed",**base}
            sect = uow.query_one("SELECT sect_owner,sect_name,join_open,closed FROM sects WHERE sect_id=?", (sect_id,))
            if sect is None: return {"status":"sect_missing",**base}
            base["sect_name"] = str(sect["sect_name"] or "")
            if str(sect["sect_owner"]) != actor_id or int(actor["sect_position"] or 0) != int(owner_position): return {"status":"not_owner",**base}
            if int(sect["closed"] or 0) == 1: return {"status":"sect_closed",**base}
            desired = 1 if action == "open" else 0
            if int(sect["join_open"] or 0) == desired: return {"status":"already_open" if desired else "already_closed",**base}
            try:
                with uow.savepoint(f"sect_{action}_join"):
                    changed = uow.execute("UPDATE sects SET join_open=? WHERE sect_id=? AND sect_owner=? AND COALESCE(closed,0)=0 AND COALESCE(join_open,0)=?", (desired,sect_id,actor_id,1-desired))
                    saved_sect = uow.query_one("SELECT sect_owner,join_open,closed FROM sects WHERE sect_id=?", (sect_id,))
                    if (
                        changed.rowcount != 1
                        or saved_sect is None
                        or str(saved_sect["sect_owner"]) != actor_id
                        or int(saved_sect["join_open"] or 0) != desired
                        or int(saved_sect["closed"] or 0) != 0
                    ):
                        raise _JoinStateRejected(f"sect_{action}_join_state_write_rejected", "sect")
                    receipt = uow.execute(f"INSERT INTO sect_{action}_join_operations(operation_id,actor_id,sect_id) VALUES(?,?,?)", (operation_id,actor_id,sect_id))
                    if receipt.rowcount != 1:
                        raise _JoinStateRejected(f"sect_{action}_join_receipt_write_rejected", operation_id)
                    saved_receipt = uow.query_one(f"SELECT actor_id,sect_id FROM sect_{action}_join_operations WHERE operation_id=?", (operation_id,))
                    expected = (actor_id, sect_id)
                    actual = None if saved_receipt is None else (str(saved_receipt["actor_id"]), int(saved_receipt["sect_id"]))
                    if actual != expected:
                        raise _JoinStateRejected(f"sect_{action}_join_receipt_write_rejected", operation_id)
            except _JoinStateRejected as exc:
                return {"status":"state_changed", "reason":exc.reason, "subject":exc.subject, **base}
            return {"status":"opened" if desired else "closed",**base}

    def open(self, operation_id: str, actor_id: str, *, owner_position: int = 0, expected_sect_id: int | None = None) -> dict[str, Any]:
        return self._change("open", operation_id, actor_id, owner_position=owner_position, expected_sect_id=expected_sect_id)

    def close(self, operation_id: str, actor_id: str, *, owner_position: int = 0, expected_sect_id: int | None = None) -> dict[str, Any]:
        return self._change("close", operation_id, actor_id, owner_position=owner_position, expected_sect_id=expected_sect_id)
