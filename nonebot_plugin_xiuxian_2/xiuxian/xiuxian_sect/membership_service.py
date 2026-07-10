from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectOwnerTransfer:
    status: str
    actor_id: str
    target_id: str
    sect_id: int | None = None
    actor_name: str = ""
    target_name: str = ""
    sect_name: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"transferred", "duplicate"}


class SectMembershipService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_operations (
                operation_id TEXT PRIMARY KEY,
                operation_type TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def transfer_owner(
        self,
        operation_id,
        actor_id,
        target_id,
        *,
        owner_position: int = 0,
        former_owner_position: int | None = None,
    ) -> SectOwnerTransfer:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)
        target_id = str(target_id)
        former_owner_position = (
            int(owner_position) + 1
            if former_owner_position is None
            else int(former_owner_position)
        )

        if actor_id == target_id:
            return SectOwnerTransfer("self_transfer", actor_id, target_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT o.sect_id, a.user_name, t.user_name, s.sect_name "
                    "FROM sect_operations o "
                    "LEFT JOIN user_xiuxian a ON a.user_id=o.actor_id "
                    "LEFT JOIN user_xiuxian t ON t.user_id=o.target_id "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectOwnerTransfer(
                        "duplicate",
                        actor_id,
                        target_id,
                        int(previous[0]),
                        str(previous[1] or ""),
                        str(previous[2] or ""),
                        str(previous[3] or ""),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectOwnerTransfer("actor_missing", actor_id, target_id)
                target = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (target_id,),
                ).fetchone()
                if target is None:
                    conn.rollback()
                    return SectOwnerTransfer("target_missing", actor_id, target_id)

                actor_sect_id = actor[0]
                if actor_sect_id is None:
                    conn.rollback()
                    return SectOwnerTransfer("actor_without_sect", actor_id, target_id)
                sect = conn.execute(
                    "SELECT sect_owner, sect_name FROM sects WHERE sect_id=%s",
                    (actor_sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectOwnerTransfer("sect_missing", actor_id, target_id)

                result = SectOwnerTransfer(
                    "",
                    actor_id,
                    target_id,
                    int(actor_sect_id),
                    str(actor[2] or ""),
                    str(target[2] or ""),
                    str(sect[1] or ""),
                )
                if str(sect[0]) != actor_id or int(actor[1]) != int(owner_position):
                    conn.rollback()
                    return replace(result, status="not_owner")
                if target[0] != actor_sect_id:
                    conn.rollback()
                    return replace(result, status="target_not_member")
                if int(target[1]) == int(owner_position):
                    conn.rollback()
                    return replace(result, status="target_already_owner")

                former_owner = conn.execute(
                    "UPDATE user_xiuxian SET sect_position=%s "
                    "WHERE user_id=%s AND sect_id=%s AND sect_position=%s",
                    (former_owner_position, actor_id, actor_sect_id, owner_position),
                )
                new_owner = conn.execute(
                    "UPDATE user_xiuxian SET sect_position=%s "
                    "WHERE user_id=%s AND sect_id=%s",
                    (owner_position, target_id, actor_sect_id),
                )
                if former_owner.rowcount != 1 or new_owner.rowcount != 1:
                    raise db_backend.IntegrityError("sect membership changed concurrently")
                updated = conn.execute(
                    "UPDATE sects SET sect_owner=%s WHERE sect_id=%s AND sect_owner=%s",
                    (target_id, actor_sect_id, actor_id),
                )
                if updated.rowcount != 1:
                    raise db_backend.IntegrityError("sect owner changed concurrently")
                conn.execute(
                    "INSERT INTO sect_operations "
                    "(operation_id, operation_type, actor_id, target_id, sect_id) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, "transfer_owner", actor_id, target_id, actor_sect_id),
                )
                conn.commit()
                return replace(result, status="transferred")
            except Exception:
                conn.rollback()
                raise


__all__ = ["SectMembershipService", "SectOwnerTransfer"]
