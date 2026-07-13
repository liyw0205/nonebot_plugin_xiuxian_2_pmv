from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
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


@dataclass(frozen=True)
class SectInactiveDisbandResult:
    status: str
    sect_id: int
    sect_name: str = ""
    reason: str = ""
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

    @staticmethod
    def _ensure_inactive_operations(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sect_inactive_disband_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,sect_id INTEGER NOT NULL,"
            "sect_name TEXT NOT NULL,reason TEXT NOT NULL,member_count INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _parse_timestamp(value) -> datetime | None:
        if isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None

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

    def disband_inactive(
        self,
        operation_id,
        sect_id,
        reason,
        *,
        expected_sect_name,
        expected_owner_id,
        expected_closed,
        expected_member_ids,
        expected_active_candidate_ids,
        checked_at,
        inactivity_days,
    ) -> SectInactiveDisbandResult:
        operation_id = str(operation_id).strip()
        sect_id = int(sect_id)
        reason = str(reason).strip()
        expected_sect_name = str(expected_sect_name)
        expected_owner_id = (
            None if expected_owner_id in (None, "") else str(expected_owner_id)
        )
        expected_closed = bool(expected_closed)
        member_ids = tuple(sorted({str(user_id) for user_id in expected_member_ids}))
        active_candidate_ids = tuple(
            sorted({str(user_id) for user_id in expected_active_candidate_ids})
        )
        checked_at_value = self._parse_timestamp(checked_at)
        inactivity_days = int(inactivity_days)
        if (
            not operation_id
            or sect_id <= 0
            or reason not in {"empty", "no_active_successor", "inactive_sole_owner"}
            or checked_at_value is None
            or inactivity_days <= 0
        ):
            raise ValueError("invalid inactive sect disband request")
        checked_at_text = checked_at_value.isoformat(sep=" ")
        payload = json.dumps(
            {
                "reason": reason,
                "sect": [
                    sect_id,
                    expected_sect_name,
                    expected_owner_id,
                    int(expected_closed),
                ],
                "member_ids": member_ids,
                "active_candidate_ids": active_candidate_ids,
                "checked_at": checked_at_text,
                "inactivity_days": inactivity_days,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_inactive_operations(conn)
                previous = conn.execute(
                    "SELECT payload,sect_name,reason,member_count "
                    "FROM sect_inactive_disband_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return SectInactiveDisbandResult(
                            "operation_conflict",
                            sect_id,
                            str(previous[1]),
                            str(previous[2]),
                            int(previous[3]),
                        )
                    return SectInactiveDisbandResult(
                        "duplicate",
                        sect_id,
                        str(previous[1]),
                        str(previous[2]),
                        int(previous[3]),
                    )

                sect = conn.execute(
                    "SELECT sect_name,sect_owner,COALESCE(closed,0) FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectInactiveDisbandResult("sect_missing", sect_id, reason=reason)
                sect_name = str(sect[0] or "")
                owner_id = None if sect[1] in (None, "") else str(sect[1])
                closed = bool(int(sect[2] or 0))
                if (
                    sect_name != expected_sect_name
                    or owner_id != expected_owner_id
                    or closed != expected_closed
                ):
                    conn.rollback()
                    return SectInactiveDisbandResult(
                        "sect_changed", sect_id, sect_name, reason
                    )

                members = conn.execute(
                    "SELECT user_id,sect_position FROM user_xiuxian "
                    "WHERE sect_id=%s ORDER BY user_id",
                    (sect_id,),
                ).fetchall()
                current_member_ids = tuple(str(row[0]) for row in members)
                if current_member_ids != member_ids:
                    conn.rollback()
                    return SectInactiveDisbandResult(
                        "members_changed", sect_id, sect_name, reason, len(members)
                    )

                last_active_by_user = {}
                if conn.table_exists("user_cd"):
                    last_active_by_user = {
                        str(row[0]): self._parse_timestamp(row[1])
                        for row in conn.execute(
                            "SELECT user_id,last_check_info_time FROM user_cd "
                            "WHERE user_id IN (SELECT user_id FROM user_xiuxian WHERE sect_id=%s)",
                            (sect_id,),
                        ).fetchall()
                    }
                current_active_candidates = tuple(
                    sorted(
                        str(row[0])
                        for row in members
                        if row[1] is not None
                        and int(row[1]) != 0
                        and last_active_by_user.get(str(row[0])) is not None
                        and (
                            checked_at_value - last_active_by_user[str(row[0])]
                        ).days
                        <= inactivity_days
                    )
                )
                if current_active_candidates != active_candidate_ids:
                    conn.rollback()
                    return SectInactiveDisbandResult(
                        "candidates_changed", sect_id, sect_name, reason, len(members)
                    )

                if reason == "empty":
                    valid_reason = closed and not members
                elif reason == "no_active_successor":
                    valid_reason = closed and bool(members) and not current_active_candidates
                else:
                    sole_owner = (
                        not closed
                        and owner_id is not None
                        and len(members) == 1
                        and str(members[0][0]) == owner_id
                        and int(members[0][1]) == 0
                    )
                    owner_last_active = last_active_by_user.get(owner_id or "")
                    valid_reason = (
                        sole_owner
                        and owner_last_active is not None
                        and (checked_at_value - owner_last_active).days
                        >= inactivity_days
                    )
                if not valid_reason:
                    conn.rollback()
                    return SectInactiveDisbandResult(
                        "condition_changed", sect_id, sect_name, reason, len(members)
                    )

                cleared = conn.execute(
                    "UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL,sect_contribution=0 "
                    "WHERE sect_id=%s",
                    (sect_id,),
                )
                deleted = conn.execute("DELETE FROM sects WHERE sect_id=%s", (sect_id,))
                if cleared.rowcount != len(members) or deleted.rowcount != 1:
                    raise db_backend.IntegrityError("inactive sect snapshot changed")
                conn.execute(
                    "INSERT INTO sect_inactive_disband_operations("
                    "operation_id,payload,sect_id,sect_name,reason,member_count) "
                    "VALUES(%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, sect_id, sect_name, reason, len(members)),
                )
                conn.commit()
                return SectInactiveDisbandResult(
                    "disbanded", sect_id, sect_name, reason, len(members)
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["SectDisbandResult", "SectInactiveDisbandResult", "SectDisbandService"]
