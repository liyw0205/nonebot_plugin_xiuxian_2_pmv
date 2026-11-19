from __future__ import annotations
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork
from .schema import OWNER_INHERIT_COLUMNS, SCHEMA_MISSING, table_ready


class _OwnerInheritRejected(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        self.reason = reason
        self.subject = subject


class SectOwnerInheritSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _ready(uow: DatabaseUnitOfWork) -> bool:
        """Startup migration `sect.019` owns the receipt ledger; verify, never create."""
        return table_ready(uow, "sect_owner_inherit_operations", OWNER_INHERIT_COLUMNS)

    def inherit(self, operation_id: str, actor_id: str, *, expected_sect_id: int | None = None, eligible_positions=(1,2,6,7), eligible_user_ids=None, owner_position: int = 0) -> dict[str, Any]:
        operation_id, actor_id = str(operation_id).strip(), str(actor_id)
        positions = tuple(int(value) for value in eligible_positions)
        allowed_ids = None if eligible_user_ids is None else tuple(str(value) for value in eligible_user_ids)
        if not operation_id or not positions:
            raise ValueError("valid operation and eligible positions are required")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._ready(uow):
                return {"status": SCHEMA_MISSING}
            previous = uow.query_one("SELECT o.actor_id,o.sect_id,u.user_name,s.sect_name FROM sect_owner_inherit_operations o LEFT JOIN user_xiuxian u ON u.user_id=o.actor_id LEFT JOIN sects s ON s.sect_id=o.sect_id WHERE o.operation_id=?", (operation_id,))
            if previous:
                if str(previous["actor_id"]) != actor_id:
                    return {"status":"operation_conflict","actor_id":actor_id,"sect_id":previous["sect_id"],"reason":"operation_conflict","subject":"actor"}
                return {"status":"duplicate","actor_id":actor_id,"sect_id":int(previous["sect_id"]),"actor_name":str(previous["user_name"] or ""),"sect_name":str(previous["sect_name"] or "")}
            actor = uow.query_one("SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?", (actor_id,))
            if actor is None: return {"status":"actor_missing","actor_id":actor_id}
            if actor["sect_id"] is None: return {"status":"actor_without_sect","actor_id":actor_id}
            sect_id = int(actor["sect_id"])
            base = {"actor_id":actor_id,"sect_id":sect_id,"actor_name":str(actor["user_name"] or "")}
            if expected_sect_id is not None and sect_id != int(expected_sect_id): return {"status":"sect_changed",**base}
            sect = uow.query_one("SELECT sect_owner,sect_name,closed FROM sects WHERE sect_id=?", (sect_id,))
            if sect is None: return {"status":"sect_missing",**base}
            base["sect_name"] = str(sect["sect_name"] or "")
            if int(sect["closed"] or 0) != 1 or sect["sect_owner"] is not None: return {"status":"not_closed",**base}
            if int(actor["sect_position"] or 0) not in positions: return {"status":"ineligible",**base}
            if allowed_ids is not None and actor_id not in allowed_ids: return {"status":"ineligible",**base}
            marks = ",".join("?" for _ in positions)
            params = [sect_id, *positions]
            sql = f"SELECT user_id FROM user_xiuxian WHERE sect_id=? AND sect_position IN ({marks})"
            if allowed_ids is not None:
                if not allowed_ids: return {"status":"ineligible",**base}
                ids = ",".join("?" for _ in allowed_ids); sql += f" AND user_id IN ({ids})"; params.extend(allowed_ids)
            candidate = uow.query_one(sql + " ORDER BY sect_position ASC,COALESCE(sect_contribution,0) DESC,user_id ASC LIMIT 1", tuple(params))
            if candidate is None or str(candidate["user_id"]) != actor_id: return {"status":"higher_priority",**base}
            try:
                with uow.savepoint("sect_owner_inherit"):
                    member = uow.execute("UPDATE user_xiuxian SET sect_position=? WHERE user_id=? AND sect_id=? AND sect_position=?", (int(owner_position),actor_id,sect_id,int(actor["sect_position"])))
                    saved_member = uow.query_one("SELECT sect_position FROM user_xiuxian WHERE user_id=? AND sect_id=?", (actor_id,sect_id))
                    if member.rowcount != 1 or saved_member is None or int(saved_member["sect_position"]) != int(owner_position):
                        raise _OwnerInheritRejected("sect_owner_inherit_member_write_rejected", "actor")
                    sect_update = uow.execute("UPDATE sects SET sect_owner=?,closed=0,join_open=1 WHERE sect_id=? AND sect_owner IS NULL AND closed=1", (actor_id,sect_id))
                    saved_sect = uow.query_one("SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=?", (sect_id,))
                    if sect_update.rowcount != 1 or saved_sect is None or str(saved_sect["sect_owner"]) != actor_id or int(saved_sect["closed"] or 0) != 0 or int(saved_sect["join_open"] or 0) != 1:
                        raise _OwnerInheritRejected("sect_owner_inherit_state_write_rejected", "sect")
                    receipt = uow.execute("INSERT INTO sect_owner_inherit_operations(operation_id,actor_id,sect_id) VALUES(?,?,?)", (operation_id,actor_id,sect_id))
                    if receipt.rowcount != 1:
                        raise _OwnerInheritRejected("sect_owner_inherit_receipt_write_rejected", operation_id)
                    saved_receipt = uow.query_one("SELECT actor_id,sect_id FROM sect_owner_inherit_operations WHERE operation_id=?", (operation_id,))
                    expected = (actor_id, sect_id)
                    actual = None if saved_receipt is None else (str(saved_receipt["actor_id"]), int(saved_receipt["sect_id"]))
                    if actual != expected:
                        raise _OwnerInheritRejected("sect_owner_inherit_receipt_write_rejected", operation_id)
            except _OwnerInheritRejected as exc:
                return {"status":"state_changed", "reason":exc.reason, "subject":exc.subject, **base}
            return {"status":"inherited",**base}
