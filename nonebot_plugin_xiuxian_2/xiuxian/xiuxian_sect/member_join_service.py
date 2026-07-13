from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectMemberJoinResult:
    status: str
    user_id: str
    sect_id: int
    sect_name: str = ""
    member_count: int = 0
    member_limit: int = 0

    @property
    def applied(self) -> bool:
        return self.status in {"joined", "duplicate"}


class SectMemberJoinService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _member_limit(sect_scale: int) -> int:
        return min(20 + max(0, int(sect_scale)) // 50_000_000, 100)

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_member_join_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                member_count INTEGER NOT NULL,
                member_limit INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def join(
        self,
        operation_id,
        user_id,
        sect_id,
        *,
        member_position: int = 12,
    ) -> SectMemberJoinResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        sect_id = int(sect_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT o.user_id, o.sect_id, s.sect_name, "
                    "o.member_count, o.member_limit "
                    "FROM sect_member_join_operations o "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != user_id or int(previous[1]) != sect_id:
                        return SectMemberJoinResult("operation_conflict", user_id, sect_id)
                    return SectMemberJoinResult(
                        "duplicate",
                        user_id,
                        sect_id,
                        str(previous[2] or ""),
                        int(previous[3]),
                        int(previous[4]),
                    )

                user = conn.execute(
                    "SELECT sect_id FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return SectMemberJoinResult("user_missing", user_id, sect_id)
                if user[0] is not None:
                    conn.rollback()
                    return SectMemberJoinResult("already_in_sect", user_id, sect_id)

                sect = conn.execute(
                    "SELECT sect_name, sect_scale, join_open, closed "
                    "FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectMemberJoinResult("sect_missing", user_id, sect_id)
                sect_name = str(sect[0] or "")
                if int(sect[3] or 0) == 1:
                    conn.rollback()
                    return SectMemberJoinResult("sect_closed", user_id, sect_id, sect_name)
                if int(sect[2] or 0) != 1:
                    conn.rollback()
                    return SectMemberJoinResult("join_closed", user_id, sect_id, sect_name)

                member_limit = self._member_limit(int(sect[1] or 0))
                member_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=%s", (sect_id,)
                    ).fetchone()[0]
                )
                if member_count >= member_limit:
                    conn.rollback()
                    return SectMemberJoinResult(
                        "sect_full", user_id, sect_id, sect_name, member_count, member_limit
                    )

                changed = conn.execute(
                    "UPDATE user_xiuxian SET sect_id=%s, sect_position=%s "
                    "WHERE user_id=%s AND sect_id IS NULL",
                    (sect_id, member_position, user_id),
                )
                if changed.rowcount != 1:
                    raise db_backend.IntegrityError("user sect changed concurrently")
                member_count += 1
                conn.execute(
                    "INSERT INTO sect_member_join_operations "
                    "(operation_id, user_id, sect_id, member_count, member_limit) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, sect_id, member_count, member_limit),
                )
                conn.commit()
                return SectMemberJoinResult(
                    "joined", user_id, sect_id, sect_name, member_count, member_limit
                )
            except Exception:
                conn.rollback()
                raise
