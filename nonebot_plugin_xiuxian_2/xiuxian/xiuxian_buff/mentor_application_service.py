from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
import time

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MentorApplication:
    invite_id: str
    mentor_id: str
    apprentice_id: str
    status: str
    created_at: float
    expires_at: float


@dataclass(frozen=True)
class MentorApplicationResult:
    status: str
    application: MentorApplication | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class MentorProtectionResult:
    status: str
    previous_status: str = "off"
    current_status: str = "off"
    rejected_invite_ids: tuple[str, ...] = ()
    rejected_apprentice_ids: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MentorApplicationService:
    """Persist and conditionally resolve the complete mentor application lifecycle."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None):
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS mentor_applications ("
            "invite_id TEXT PRIMARY KEY,mentor_id TEXT NOT NULL,apprentice_id TEXT NOT NULL,"
            "status TEXT NOT NULL,created_at REAL NOT NULL,expires_at REAL NOT NULL,resolved_at REAL)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS mentor_application_pending_apprentice "
            "ON mentor_applications(apprentice_id) WHERE status='pending'"
        )

    @staticmethod
    def _ensure_mentor_schema(conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS mentor (user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(mentor)").fetchall()
        }
        for field in ("mentor_protect", "mentor_apply_time", "mentor_apply_target"):
            if field not in columns:
                conn.execute(f"ALTER TABLE mentor ADD COLUMN {field} TEXT")

    @staticmethod
    def _ensure_create_operation_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS mentor_application_create_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,result_invite_id TEXT,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _ensure_resolution_operation_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS mentor_application_resolution_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,result_invite_id TEXT,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _table_exists(conn, table_name) -> bool:
        return conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=%s",
            (str(table_name),),
        ).fetchone() is not None

    @staticmethod
    def _require_protection(status) -> str:
        status = str(status).strip().lower()
        if status not in {"on", "off"}:
            raise ValueError("invalid mentor protection status")
        return status

    @staticmethod
    def _normalize_protection(status) -> str:
        status = str(status or "off").strip().lower()
        return status if status in {"on", "off"} else "off"

    @classmethod
    def _read_protection(cls, conn, mentor_id) -> str:
        cls._ensure_mentor_schema(conn)
        row = conn.execute(
            "SELECT mentor_protect FROM mentor WHERE user_id=%s",
            (str(mentor_id),),
        ).fetchone()
        return cls._normalize_protection(row[0] if row is not None else "off")

    @staticmethod
    def _application(row) -> MentorApplication | None:
        if row is None:
            return None
        return MentorApplication(str(row[0]), str(row[1]), str(row[2]), str(row[3]), float(row[4]), float(row[5]))

    @classmethod
    def _application_by_id(cls, conn, invite_id) -> MentorApplication | None:
        if not invite_id:
            return None
        row = conn.execute(
            "SELECT invite_id,mentor_id,apprentice_id,status,created_at,expires_at "
            "FROM mentor_applications WHERE invite_id=%s",
            (str(invite_id),),
        ).fetchone()
        return cls._application(row)

    @classmethod
    def _stored_result(cls, conn, row) -> MentorApplicationResult:
        status = "duplicate" if str(row[1]) == "applied" else str(row[1])
        return MentorApplicationResult(
            status, cls._application_by_id(conn, row[2])
        )

    @staticmethod
    def _expire_stale(conn, now: float) -> None:
        conn.execute(
            "UPDATE mentor_applications SET status='expired',resolved_at=%s "
            "WHERE status='pending' AND expires_at<=%s",
            (now, now),
        )

    def replay_create(
        self, operation_id, mentor_id, apprentice_id
    ) -> MentorApplicationResult | None:
        operation_id = str(operation_id).strip()
        mentor_id, apprentice_id = str(mentor_id), str(apprentice_id)
        if not operation_id:
            raise ValueError("invalid mentor application")
        payload = json.dumps(
            [mentor_id, apprentice_id], ensure_ascii=True, separators=(",", ":")
        )
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            if self._table_exists(conn, "mentor_application_create_operations"):
                previous = conn.execute(
                    "SELECT payload,result_status,result_invite_id "
                    "FROM mentor_application_create_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    if str(previous[0]) != payload:
                        return MentorApplicationResult("invite_conflict")
                    return self._stored_result(conn, previous)
            if not self._table_exists(conn, "mentor_applications"):
                return None
            application = self._application_by_id(conn, operation_id)
        if application is None:
            return None
        status = (
            "duplicate"
            if application.mentor_id == mentor_id
            and application.apprentice_id == apprentice_id
            else "invite_conflict"
        )
        return MentorApplicationResult(status, application)

    def create(self, invite_id, mentor_id, apprentice_id, *, ttl_seconds=60, now=None) -> MentorApplicationResult:
        invite_id, mentor_id, apprentice_id = str(invite_id), str(mentor_id), str(apprentice_id)
        created_at = float(time.time() if now is None else now)
        expires_at = created_at + max(1, int(ttl_seconds))
        if not invite_id or mentor_id == apprentice_id:
            raise ValueError("invalid mentor application")
        payload = json.dumps(
            [mentor_id, apprentice_id], ensure_ascii=True, separators=(",", ":")
        )
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._ensure_create_operation_schema(conn)
                operation = conn.execute(
                    "SELECT payload,result_status,result_invite_id "
                    "FROM mentor_application_create_operations WHERE operation_id=%s",
                    (invite_id,),
                ).fetchone()
                if operation is not None:
                    conn.rollback()
                    if str(operation[0]) != payload:
                        return MentorApplicationResult("invite_conflict")
                    return self._stored_result(conn, operation)
                previous = self._application_by_id(conn, invite_id)
                if previous is not None:
                    if previous.mentor_id != mentor_id or previous.apprentice_id != apprentice_id:
                        conn.rollback()
                        return MentorApplicationResult("invite_conflict", previous)
                    conn.execute(
                        "INSERT INTO mentor_application_create_operations("
                        "operation_id,payload,result_status,result_invite_id) "
                        "VALUES(%s,%s,'applied',%s)",
                        (invite_id, payload, invite_id),
                    )
                    conn.commit()
                    return MentorApplicationResult("duplicate", previous)
                self._expire_stale(conn, created_at)
                if self._read_protection(conn, mentor_id) == "on":
                    conn.execute(
                        "INSERT INTO mentor_application_create_operations("
                        "operation_id,payload,result_status,result_invite_id) "
                        "VALUES(%s,%s,'protected',NULL)",
                        (invite_id, payload),
                    )
                    conn.commit()
                    return MentorApplicationResult("protected")
                pending = conn.execute(
                    "SELECT invite_id,mentor_id,apprentice_id,status,created_at,expires_at "
                    "FROM mentor_applications WHERE apprentice_id=%s AND status='pending'", (apprentice_id,),
                ).fetchone()
                if pending is not None:
                    application = self._application(pending)
                    conn.execute(
                        "INSERT INTO mentor_application_create_operations("
                        "operation_id,payload,result_status,result_invite_id) "
                        "VALUES(%s,%s,'already_pending',%s)",
                        (invite_id, payload, application.invite_id),
                    )
                    conn.commit()
                    return MentorApplicationResult("already_pending", application)
                conn.execute(
                    "INSERT INTO mentor_applications VALUES(%s,%s,%s,'pending',%s,%s,NULL)",
                    (invite_id, mentor_id, apprentice_id, created_at, expires_at),
                )
                apply_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(created_at))
                changed = conn.execute(
                    "UPDATE mentor SET mentor_apply_time=%s,mentor_apply_target=%s WHERE user_id=%s",
                    (apply_time, mentor_id, apprentice_id),
                )
                if changed.rowcount == 0:
                    conn.execute(
                        "INSERT INTO mentor(user_id,mentor_apply_time,mentor_apply_target) VALUES(%s,%s,%s)",
                        (apprentice_id, apply_time, mentor_id),
                    )
                conn.execute(
                    "INSERT INTO mentor_application_create_operations("
                    "operation_id,payload,result_status,result_invite_id) "
                    "VALUES(%s,%s,'applied',%s)",
                    (invite_id, payload, invite_id),
                )
                conn.commit()
                return MentorApplicationResult(
                    "applied", MentorApplication(invite_id, mentor_id, apprentice_id, "pending", created_at, expires_at)
                )
            except Exception:
                conn.rollback()
                raise

    def get_protection(self, mentor_id) -> str:
        with self._lock, db_backend.transaction(self._player_database) as conn:
            return self._read_protection(conn, mentor_id)

    def set_protection(
        self, operation_id, mentor_id, expected_status, new_status, *, now=None
    ) -> MentorProtectionResult:
        operation_id = str(operation_id).strip()
        mentor_id = str(mentor_id).strip()
        expected_status = self._require_protection(expected_status)
        new_status = self._require_protection(new_status)
        if not operation_id or not mentor_id:
            raise ValueError("invalid mentor protection operation")
        changed_at = float(time.time() if now is None else now)
        payload = json.dumps(
            [mentor_id, new_status], ensure_ascii=True, separators=(",", ":")
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._expire_stale(conn, changed_at)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mentor_protection_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "previous_status TEXT NOT NULL,current_status TEXT NOT NULL,"
                    "rejected_applications TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,previous_status,current_status,rejected_applications "
                    "FROM mentor_protection_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MentorProtectionResult("operation_conflict")
                    rejected = json.loads(str(previous[3]))
                    return MentorProtectionResult(
                        "duplicate",
                        str(previous[1]),
                        str(previous[2]),
                        tuple(str(item[0]) for item in rejected),
                        tuple(str(item[1]) for item in rejected),
                    )

                current_status = self._read_protection(conn, mentor_id)
                if current_status != expected_status:
                    conn.rollback()
                    return MentorProtectionResult(
                        "state_changed", current_status, current_status
                    )
                changed = conn.execute(
                    "UPDATE mentor SET mentor_protect=%s WHERE user_id=%s",
                    (new_status, mentor_id),
                )
                if changed.rowcount == 0:
                    conn.execute(
                        "INSERT INTO mentor(user_id,mentor_protect) VALUES(%s,%s)",
                        (mentor_id, new_status),
                    )

                rejected = []
                if new_status == "on":
                    rejected = conn.execute(
                        "SELECT invite_id,apprentice_id FROM mentor_applications "
                        "WHERE mentor_id=%s AND status='pending' AND expires_at>%s "
                        "ORDER BY created_at,invite_id",
                        (mentor_id, changed_at),
                    ).fetchall()
                    rejected_count = conn.execute(
                        "UPDATE mentor_applications SET status='rejected',resolved_at=%s "
                        "WHERE mentor_id=%s AND status='pending' AND expires_at>%s",
                        (changed_at, mentor_id, changed_at),
                    ).rowcount
                    if rejected_count != len(rejected):
                        raise RuntimeError("mentor applications changed during protection")

                rejected_payload = [
                    [str(invite_id), str(apprentice_id)]
                    for invite_id, apprentice_id in rejected
                ]
                conn.execute(
                    "INSERT INTO mentor_protection_operations("
                    "operation_id,payload,previous_status,current_status,"
                    "rejected_applications) VALUES(%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        current_status,
                        new_status,
                        json.dumps(
                            rejected_payload,
                            ensure_ascii=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
                conn.commit()
                return MentorProtectionResult(
                    "applied",
                    current_status,
                    new_status,
                    tuple(item[0] for item in rejected_payload),
                    tuple(item[1] for item in rejected_payload),
                )
            except Exception:
                conn.rollback()
                raise

    def list_pending(self, mentor_id, *, now=None) -> list[MentorApplication]:
        current = float(time.time() if now is None else now)
        with self._lock, db_backend.transaction(self._player_database) as conn:
            self._ensure_schema(conn)
            self._expire_stale(conn, current)
            rows = conn.execute(
                "SELECT invite_id,mentor_id,apprentice_id,status,created_at,expires_at "
                "FROM mentor_applications WHERE mentor_id=%s AND status='pending' ORDER BY created_at",
                (str(mentor_id),),
            ).fetchall()
        return [self._application(row) for row in rows]

    def find_pending_by_apprentice(self, apprentice_id, *, now=None) -> MentorApplication | None:
        current = float(time.time() if now is None else now)
        with self._lock, db_backend.transaction(self._player_database) as conn:
            self._ensure_schema(conn)
            self._expire_stale(conn, current)
            row = conn.execute(
                "SELECT invite_id,mentor_id,apprentice_id,status,created_at,expires_at "
                "FROM mentor_applications WHERE apprentice_id=%s AND status='pending'",
                (str(apprentice_id),),
            ).fetchone()
        return self._application(row)

    def replay_resolution(
        self, operation_id, mentor_id, apprentice_id, status
    ) -> MentorApplicationResult | None:
        operation_id = str(operation_id).strip()
        mentor_id, apprentice_id = str(mentor_id), str(apprentice_id)
        if status not in {"rejected", "expired", "cancelled"}:
            raise ValueError("invalid mentor application status")
        if not operation_id:
            raise ValueError("invalid mentor application operation")
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            if not self._table_exists(
                conn, "mentor_application_resolution_operations"
            ):
                return None
            previous = conn.execute(
                "SELECT payload,result_status,result_invite_id "
                "FROM mentor_application_resolution_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            identity = json.loads(str(previous[0]))
            if [str(identity[1]), str(identity[2]), str(identity[3])] != [
                mentor_id, apprentice_id, status,
            ]:
                return MentorApplicationResult("operation_conflict")
            return self._stored_result(conn, previous)

    def resolve(
        self, invite_id, mentor_id, apprentice_id, status, *, operation_id=None,
        now=None,
    ) -> MentorApplicationResult:
        if status not in {"rejected", "expired", "cancelled"}:
            raise ValueError("invalid mentor application status")
        resolved_at = float(time.time() if now is None else now)
        operation_id = None if operation_id is None else str(operation_id).strip()
        payload = json.dumps(
            [str(invite_id), str(mentor_id), str(apprentice_id), status],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, db_backend.transaction(self._player_database) as conn:
            self._ensure_schema(conn)
            if operation_id:
                self._ensure_resolution_operation_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,result_invite_id "
                    "FROM mentor_application_resolution_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    if str(previous[0]) != payload:
                        return MentorApplicationResult("operation_conflict")
                    return self._stored_result(conn, previous)
            changed = conn.execute(
                "UPDATE mentor_applications SET status=%s,resolved_at=%s "
                "WHERE invite_id=%s AND mentor_id=%s AND apprentice_id=%s AND status='pending'",
                (status, resolved_at, str(invite_id), str(mentor_id), str(apprentice_id)),
            )
            row = conn.execute(
                "SELECT invite_id,mentor_id,apprentice_id,status,created_at,expires_at "
                "FROM mentor_applications WHERE invite_id=%s", (str(invite_id),),
            ).fetchone()
            app = self._application(row)
            if changed.rowcount == 1:
                result_status = "applied"
            elif app is not None and app.status == status:
                result_status = "duplicate"
            else:
                result_status = "state_changed"
            if operation_id:
                conn.execute(
                    "INSERT INTO mentor_application_resolution_operations("
                    "operation_id,payload,result_status,result_invite_id) "
                    "VALUES(%s,%s,%s,%s)",
                    (
                        operation_id, payload, result_status,
                        app.invite_id if app is not None else str(invite_id),
                    ),
                )
        return MentorApplicationResult(result_status, app)


__all__ = [
    "MentorApplication",
    "MentorApplicationResult",
    "MentorApplicationService",
    "MentorProtectionResult",
]
