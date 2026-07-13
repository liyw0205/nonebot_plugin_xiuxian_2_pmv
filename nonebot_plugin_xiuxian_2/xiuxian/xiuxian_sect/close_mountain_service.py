from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectCloseMountainResult:
    status: str
    actor_id: str
    sect_id: int | None = None
    sect_name: str = ""

    @property
    def applied(self) -> bool:
        return self.status in {"closed", "duplicate"}


class SectCloseMountainService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_close_mountain_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def close(
        self,
        operation_id,
        actor_id,
        *,
        owner_position: int = 0,
        former_owner_position: int = 2,
        expected_sect_id: int | None = None,
    ) -> SectCloseMountainResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT o.sect_id, s.sect_name "
                    "FROM sect_close_mountain_operations o "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectCloseMountainResult(
                        "duplicate", actor_id, int(previous[0]), str(previous[1] or "")
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectCloseMountainResult("actor_missing", actor_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectCloseMountainResult("actor_without_sect", actor_id)

                sect_id = int(actor[0])
                if expected_sect_id is not None and sect_id != int(expected_sect_id):
                    conn.rollback()
                    return SectCloseMountainResult("sect_changed", actor_id, sect_id)
                sect = conn.execute(
                    "SELECT sect_owner, sect_name, closed FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectCloseMountainResult("sect_missing", actor_id, sect_id)

                result = SectCloseMountainResult(
                    "", actor_id, sect_id, str(sect[1] or "")
                )
                if int(sect[2] or 0) == 1:
                    conn.rollback()
                    return SectCloseMountainResult(
                        "already_closed", actor_id, sect_id, result.sect_name
                    )
                if str(sect[0]) != actor_id or int(actor[1]) != int(owner_position):
                    conn.rollback()
                    return SectCloseMountainResult(
                        "not_owner", actor_id, sect_id, result.sect_name
                    )

                member = conn.execute(
                    "UPDATE user_xiuxian SET sect_position=%s "
                    "WHERE user_id=%s AND sect_id=%s AND sect_position=%s",
                    (former_owner_position, actor_id, sect_id, owner_position),
                )
                sect_update = conn.execute(
                    "UPDATE sects SET join_open=0, closed=1, sect_owner=NULL "
                    "WHERE sect_id=%s AND sect_owner=%s AND COALESCE(closed, 0)=0",
                    (sect_id, actor_id),
                )
                if member.rowcount != 1 or sect_update.rowcount != 1:
                    raise db_backend.IntegrityError("sect owner changed concurrently")
                conn.execute(
                    "INSERT INTO sect_close_mountain_operations "
                    "(operation_id, actor_id, sect_id) VALUES (%s, %s, %s)",
                    (operation_id, actor_id, sect_id),
                )
                conn.commit()
                return SectCloseMountainResult(
                    "closed", actor_id, sect_id, result.sect_name
                )
            except Exception:
                conn.rollback()
                raise
