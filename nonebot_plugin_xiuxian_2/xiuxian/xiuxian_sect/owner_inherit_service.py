from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectOwnerInheritResult:
    status: str
    actor_id: str
    sect_id: int | None = None
    actor_name: str = ""
    sect_name: str = ""

    @property
    def applied(self) -> bool:
        return self.status in {"inherited", "duplicate"}


class SectOwnerInheritService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_owner_inherit_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def inherit(
        self,
        operation_id,
        actor_id,
        *,
        expected_sect_id: int | None = None,
        eligible_positions: tuple[int, ...] = (1, 2, 6, 7),
        eligible_user_ids: tuple[str, ...] | list[str] | None = None,
        owner_position: int = 0,
    ) -> SectOwnerInheritResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)
        positions = tuple(int(value) for value in eligible_positions)
        if not positions:
            raise ValueError("eligible_positions must not be empty")
        allowed_ids = (
            None
            if eligible_user_ids is None
            else tuple(str(value) for value in eligible_user_ids)
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT o.sect_id, u.user_name, s.sect_name "
                    "FROM sect_owner_inherit_operations o "
                    "LEFT JOIN user_xiuxian u ON u.user_id=o.actor_id "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectOwnerInheritResult(
                        "duplicate",
                        actor_id,
                        int(previous[0]),
                        str(previous[1] or ""),
                        str(previous[2] or ""),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectOwnerInheritResult("actor_missing", actor_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectOwnerInheritResult("actor_without_sect", actor_id)
                sect_id = int(actor[0])
                if expected_sect_id is not None and sect_id != int(expected_sect_id):
                    conn.rollback()
                    return SectOwnerInheritResult("sect_changed", actor_id, sect_id)

                sect = conn.execute(
                    "SELECT sect_owner, sect_name, closed FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectOwnerInheritResult("sect_missing", actor_id, sect_id)
                result = SectOwnerInheritResult(
                    "", actor_id, sect_id, str(actor[2] or ""), str(sect[1] or "")
                )
                if int(sect[2] or 0) != 1 or sect[0] is not None:
                    conn.rollback()
                    return SectOwnerInheritResult(
                        "not_closed", actor_id, sect_id, result.actor_name, result.sect_name
                    )
                if int(actor[1]) not in positions:
                    conn.rollback()
                    return SectOwnerInheritResult(
                        "ineligible", actor_id, sect_id, result.actor_name, result.sect_name
                    )
                if allowed_ids is not None and actor_id not in allowed_ids:
                    conn.rollback()
                    return SectOwnerInheritResult(
                        "ineligible", actor_id, sect_id, result.actor_name, result.sect_name
                    )

                position_marks = ", ".join("%s" for _ in positions)
                params: list[object] = [sect_id, *positions]
                candidate_sql = (
                    "SELECT user_id FROM user_xiuxian WHERE sect_id=%s "
                    f"AND sect_position IN ({position_marks})"
                )
                if allowed_ids is not None:
                    if not allowed_ids:
                        conn.rollback()
                        return SectOwnerInheritResult(
                            "ineligible", actor_id, sect_id, result.actor_name, result.sect_name
                        )
                    id_marks = ", ".join("%s" for _ in allowed_ids)
                    candidate_sql += f" AND user_id IN ({id_marks})"
                    params.extend(allowed_ids)
                candidate_sql += (
                    " ORDER BY sect_position ASC, "
                    "COALESCE(sect_contribution, 0) DESC, user_id ASC LIMIT 1"
                )
                candidate = conn.execute(candidate_sql, tuple(params)).fetchone()
                if candidate is None or str(candidate[0]) != actor_id:
                    conn.rollback()
                    return SectOwnerInheritResult(
                        "higher_priority", actor_id, sect_id, result.actor_name, result.sect_name
                    )

                member = conn.execute(
                    "UPDATE user_xiuxian SET sect_position=%s "
                    "WHERE user_id=%s AND sect_id=%s AND sect_position=%s",
                    (owner_position, actor_id, sect_id, int(actor[1])),
                )
                sect_update = conn.execute(
                    "UPDATE sects SET sect_owner=%s, closed=0, join_open=1 "
                    "WHERE sect_id=%s AND sect_owner IS NULL AND closed=1",
                    (actor_id, sect_id),
                )
                if member.rowcount != 1 or sect_update.rowcount != 1:
                    raise db_backend.IntegrityError("sect inheritance changed concurrently")
                conn.execute(
                    "INSERT INTO sect_owner_inherit_operations "
                    "(operation_id, actor_id, sect_id) VALUES (%s, %s, %s)",
                    (operation_id, actor_id, sect_id),
                )
                conn.commit()
                return SectOwnerInheritResult(
                    "inherited", actor_id, sect_id, result.actor_name, result.sect_name
                )
            except Exception:
                conn.rollback()
                raise
