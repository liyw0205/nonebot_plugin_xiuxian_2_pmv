from __future__ import annotations

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
    def _application(row) -> MentorApplication | None:
        if row is None:
            return None
        return MentorApplication(str(row[0]), str(row[1]), str(row[2]), str(row[3]), float(row[4]), float(row[5]))

    @staticmethod
    def _expire_stale(conn, now: float) -> None:
        conn.execute(
            "UPDATE mentor_applications SET status='expired',resolved_at=%s "
            "WHERE status='pending' AND expires_at<=%s",
            (now, now),
        )

    def create(self, invite_id, mentor_id, apprentice_id, *, ttl_seconds=60, now=None) -> MentorApplicationResult:
        invite_id, mentor_id, apprentice_id = str(invite_id), str(mentor_id), str(apprentice_id)
        created_at = float(time.time() if now is None else now)
        expires_at = created_at + max(1, int(ttl_seconds))
        if not invite_id or mentor_id == apprentice_id:
            raise ValueError("invalid mentor application")
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._expire_stale(conn, created_at)
                previous = conn.execute(
                    "SELECT invite_id,mentor_id,apprentice_id,status,created_at,expires_at "
                    "FROM mentor_applications WHERE invite_id=%s", (invite_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    app = self._application(previous)
                    status = "duplicate" if app.mentor_id == mentor_id and app.apprentice_id == apprentice_id else "invite_conflict"
                    return MentorApplicationResult(status, app)
                columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(mentor)").fetchall()}
                if "mentor_protect" not in columns:
                    conn.execute("ALTER TABLE mentor ADD COLUMN mentor_protect TEXT")
                if "mentor_apply_time" not in columns:
                    conn.execute("ALTER TABLE mentor ADD COLUMN mentor_apply_time TEXT")
                if "mentor_apply_target" not in columns:
                    conn.execute("ALTER TABLE mentor ADD COLUMN mentor_apply_target TEXT")
                protect = conn.execute(
                    "SELECT mentor_protect FROM mentor WHERE user_id=%s", (mentor_id,),
                ).fetchone()
                if protect is not None and str(protect[0] or "off") == "on":
                    conn.rollback()
                    return MentorApplicationResult("protected")
                pending = conn.execute(
                    "SELECT invite_id,mentor_id,apprentice_id,status,created_at,expires_at "
                    "FROM mentor_applications WHERE apprentice_id=%s AND status='pending'", (apprentice_id,),
                ).fetchone()
                if pending is not None:
                    conn.rollback()
                    return MentorApplicationResult("already_pending", self._application(pending))
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
                conn.commit()
                return MentorApplicationResult(
                    "applied", MentorApplication(invite_id, mentor_id, apprentice_id, "pending", created_at, expires_at)
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

    def resolve(self, invite_id, mentor_id, apprentice_id, status, *, now=None) -> MentorApplicationResult:
        if status not in {"rejected", "expired", "cancelled"}:
            raise ValueError("invalid mentor application status")
        resolved_at = float(time.time() if now is None else now)
        with self._lock, db_backend.transaction(self._player_database) as conn:
            self._ensure_schema(conn)
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
            return MentorApplicationResult("applied", app)
        if app is not None and app.status == status:
            return MentorApplicationResult("duplicate", app)
        return MentorApplicationResult("state_changed", app)


__all__ = ["MentorApplication", "MentorApplicationResult", "MentorApplicationService"]
