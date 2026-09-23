from __future__ import annotations
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork

class SectOwnerInheritSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def ensure_schema(uow: DatabaseUnitOfWork) -> None:
        uow.execute("CREATE TABLE IF NOT EXISTS sect_owner_inherit_operations(operation_id TEXT PRIMARY KEY,actor_id TEXT NOT NULL,sect_id INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

    def inherit(self, operation_id: str, actor_id: str, *, expected_sect_id: int | None = None, eligible_positions=(1,2,6,7), eligible_user_ids=None, owner_position: int = 0) -> dict[str, Any]:
        operation_id, actor_id = str(operation_id).strip(), str(actor_id)
        positions = tuple(int(value) for value in eligible_positions)
        allowed_ids = None if eligible_user_ids is None else tuple(str(value) for value in eligible_user_ids)
        if not operation_id or not positions:
            raise ValueError("valid operation and eligible positions are required")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.ensure_schema(uow)
            previous = uow.query_one("SELECT o.sect_id,u.user_name,s.sect_name FROM sect_owner_inherit_operations o LEFT JOIN user_xiuxian u ON u.user_id=o.actor_id LEFT JOIN sects s ON s.sect_id=o.sect_id WHERE o.operation_id=?", (operation_id,))
            if previous:
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
            member = uow.execute("UPDATE user_xiuxian SET sect_position=? WHERE user_id=? AND sect_id=? AND sect_position=?", (int(owner_position),actor_id,sect_id,int(actor["sect_position"])))
            sect_update = uow.execute("UPDATE sects SET sect_owner=?,closed=0,join_open=1 WHERE sect_id=? AND sect_owner IS NULL AND closed=1", (actor_id,sect_id))
            if member.rowcount != 1 or sect_update.rowcount != 1: raise RuntimeError("sect inheritance changed concurrently")
            uow.execute("INSERT INTO sect_owner_inherit_operations(operation_id,actor_id,sect_id) VALUES(?,?,?)", (operation_id,actor_id,sect_id))
            return {"status":"inherited",**base}
