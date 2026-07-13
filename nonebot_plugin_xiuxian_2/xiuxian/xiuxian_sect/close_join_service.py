from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectCloseJoinResult:
    status: str
    actor_id: str
    sect_id: int | None = None
    sect_name: str = ""

    @property
    def applied(self) -> bool:
        return self.status in {"closed", "duplicate"}


class SectCloseJoinService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_close_join_operations (
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
        expected_sect_id: int | None = None,
    ) -> SectCloseJoinResult:
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
                    "FROM sect_close_join_operations o "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectCloseJoinResult(
                        "duplicate", actor_id, int(previous[0]), str(previous[1] or "")
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectCloseJoinResult("actor_missing", actor_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectCloseJoinResult("actor_without_sect", actor_id)

                sect_id = int(actor[0])
                if expected_sect_id is not None and sect_id != int(expected_sect_id):
                    conn.rollback()
                    return SectCloseJoinResult("sect_changed", actor_id, sect_id)
                sect = conn.execute(
                    "SELECT sect_owner, sect_name, join_open, closed "
                    "FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectCloseJoinResult("sect_missing", actor_id, sect_id)

                sect_name = str(sect[1] or "")
                if str(sect[0]) != actor_id or int(actor[1]) != int(owner_position):
                    conn.rollback()
                    return SectCloseJoinResult("not_owner", actor_id, sect_id, sect_name)
                if int(sect[3] or 0) == 1:
                    conn.rollback()
                    return SectCloseJoinResult("sect_closed", actor_id, sect_id, sect_name)
                if int(sect[2] or 0) == 0:
                    conn.rollback()
                    return SectCloseJoinResult("already_closed", actor_id, sect_id, sect_name)

                changed = conn.execute(
                    "UPDATE sects SET join_open=0 "
                    "WHERE sect_id=%s AND sect_owner=%s "
                    "AND COALESCE(closed, 0)=0 AND COALESCE(join_open, 0)=1",
                    (sect_id, actor_id),
                )
                if changed.rowcount != 1:
                    raise db_backend.IntegrityError("sect join state changed concurrently")
                conn.execute(
                    "INSERT INTO sect_close_join_operations "
                    "(operation_id, actor_id, sect_id) VALUES (%s, %s, %s)",
                    (operation_id, actor_id, sect_id),
                )
                conn.commit()
                return SectCloseJoinResult("closed", actor_id, sect_id, sect_name)
            except Exception:
                conn.rollback()
                raise
