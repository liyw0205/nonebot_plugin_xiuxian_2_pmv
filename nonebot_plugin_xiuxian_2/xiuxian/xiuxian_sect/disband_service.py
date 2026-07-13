from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectDisbandResult:
    status: str
    actor_id: str
    sect_id: int | None = None
    sect_name: str = ""
    member_count: int = 0

    @property
    def applied(self) -> bool:
        return self.status in {"disbanded", "duplicate"}


class SectDisbandService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_disband_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                sect_name TEXT NOT NULL DEFAULT '',
                member_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def disband(
        self,
        operation_id,
        actor_id,
        *,
        expected_sect_id: int | None = None,
        owner_position: int = 0,
    ) -> SectDisbandResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT sect_id, sect_name, member_count "
                    "FROM sect_disband_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectDisbandResult(
                        "duplicate",
                        actor_id,
                        int(previous[0]),
                        str(previous[1] or ""),
                        int(previous[2] or 0),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectDisbandResult("actor_missing", actor_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectDisbandResult("actor_without_sect", actor_id)

                sect_id = int(actor[0])
                if expected_sect_id is not None and sect_id != int(expected_sect_id):
                    conn.rollback()
                    return SectDisbandResult("sect_changed", actor_id, sect_id)

                sect = conn.execute(
                    "SELECT sect_owner, sect_name FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectDisbandResult("sect_missing", actor_id, sect_id)

                sect_name = str(sect[1] or "")
                if str(sect[0]) != actor_id or int(actor[1]) != int(owner_position):
                    conn.rollback()
                    return SectDisbandResult("not_owner", actor_id, sect_id, sect_name)

                member_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=%s", (sect_id,)
                    ).fetchone()[0]
                )
                members = conn.execute(
                    "UPDATE user_xiuxian SET sect_id=NULL, sect_position=NULL, "
                    "sect_contribution=0 WHERE sect_id=%s",
                    (sect_id,),
                )
                deleted = conn.execute(
                    "DELETE FROM sects WHERE sect_id=%s AND sect_owner=%s",
                    (sect_id, actor_id),
                )
                if members.rowcount != member_count or deleted.rowcount != 1:
                    raise db_backend.IntegrityError("sect ownership or membership changed")

                conn.execute(
                    "INSERT INTO sect_disband_operations "
                    "(operation_id, actor_id, sect_id, sect_name, member_count) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, actor_id, sect_id, sect_name, member_count),
                )
                conn.commit()
                return SectDisbandResult(
                    "disbanded", actor_id, sect_id, sect_name, member_count
                )
            except Exception:
                conn.rollback()
                raise
