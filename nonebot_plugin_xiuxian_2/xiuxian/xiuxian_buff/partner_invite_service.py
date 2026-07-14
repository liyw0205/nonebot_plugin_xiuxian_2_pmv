from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
import time

from ..xiuxian_utils import db_backend
from .partner_protection_service import PartnerProtectionService


@dataclass(frozen=True)
class PartnerInvite:
    invite_id: str
    inviter_id: str
    target_id: str
    count: int
    status: str
    created_at: float
    expires_at: float


@dataclass(frozen=True)
class PartnerInviteResult:
    status: str
    invite: PartnerInvite | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PartnerInviteService:
    def __init__(self, player_database: str | Path, lock: RLock | None = None):
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def ensure_schema(conn, schema="") -> None:
        prefix = f"{schema}." if schema else ""
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}partner_cultivation_invites ("
            "invite_id TEXT PRIMARY KEY,inviter_id TEXT NOT NULL,target_id TEXT NOT NULL,count INTEGER NOT NULL,"
            "status TEXT NOT NULL,created_at REAL NOT NULL,expires_at REAL NOT NULL,resolved_at REAL)"
        )
        conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {prefix}partner_invite_pending_inviter "
            "ON partner_cultivation_invites(inviter_id) WHERE status='pending'"
        )
        conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {prefix}partner_invite_pending_target "
            "ON partner_cultivation_invites(target_id) WHERE status='pending'"
        )

    @staticmethod
    def from_row(row) -> PartnerInvite | None:
        if row is None:
            return None
        return PartnerInvite(str(row[0]), str(row[1]), str(row[2]), int(row[3]), str(row[4]), float(row[5]), float(row[6]))

    @staticmethod
    def expire_stale(conn, now: float) -> None:
        conn.execute(
            "UPDATE partner_cultivation_invites SET status='expired',resolved_at=%s "
            "WHERE status='pending' AND expires_at<=%s", (now, now),
        )

    def create(
        self, invite_id, inviter_id, target_id, count, *, ttl_seconds=60,
        now=None, expected_target_protection=None,
    ) -> PartnerInviteResult:
        invite_id, inviter_id, target_id = str(invite_id), str(inviter_id), str(target_id)
        count = int(count)
        created_at = float(time.time() if now is None else now)
        expires_at = created_at + max(1, int(ttl_seconds))
        expected_target_protection = (
            None if expected_target_protection is None
            else PartnerProtectionService.require_valid(expected_target_protection)
        )
        if not invite_id or inviter_id == target_id or count <= 0:
            raise ValueError("invalid partner invite")
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self.ensure_schema(conn)
                self.expire_stale(conn, created_at)
                previous = conn.execute(
                    "SELECT invite_id,inviter_id,target_id,count,status,created_at,expires_at "
                    "FROM partner_cultivation_invites WHERE invite_id=%s", (invite_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    invite = self.from_row(previous)
                    identical = invite.inviter_id == inviter_id and invite.target_id == target_id and invite.count == count
                    return PartnerInviteResult("duplicate" if identical else "invite_conflict", invite)
                if expected_target_protection is not None:
                    actual_protection = PartnerProtectionService.read_status(
                        conn, target_id
                    )
                    if actual_protection != expected_target_protection:
                        conn.rollback()
                        return PartnerInviteResult("protection_changed")
                busy = conn.execute(
                    "SELECT invite_id,inviter_id,target_id,count,status,created_at,expires_at "
                    "FROM partner_cultivation_invites WHERE status='pending' AND "
                    "(inviter_id IN (%s,%s) OR target_id IN (%s,%s)) LIMIT 1",
                    (inviter_id, target_id, inviter_id, target_id),
                ).fetchone()
                if busy is not None:
                    conn.rollback()
                    return PartnerInviteResult("busy", self.from_row(busy))
                conn.execute(
                    "INSERT INTO partner_cultivation_invites VALUES(%s,%s,%s,%s,'pending',%s,%s,NULL)",
                    (invite_id, inviter_id, target_id, count, created_at, expires_at),
                )
                conn.commit()
                return PartnerInviteResult("applied", PartnerInvite(invite_id, inviter_id, target_id, count, "pending", created_at, expires_at))
            except Exception:
                conn.rollback()
                raise

    def pending_for_user(self, user_id, *, now=None) -> PartnerInvite | None:
        current = float(time.time() if now is None else now)
        with self._lock, db_backend.transaction(self._player_database) as conn:
            self.ensure_schema(conn)
            self.expire_stale(conn, current)
            row = conn.execute(
                "SELECT invite_id,inviter_id,target_id,count,status,created_at,expires_at "
                "FROM partner_cultivation_invites WHERE status='pending' AND "
                "(inviter_id=%s OR target_id=%s) ORDER BY created_at LIMIT 1",
                (str(user_id), str(user_id)),
            ).fetchone()
        return self.from_row(row)

    def pending_for_target(self, target_id, *, now=None) -> PartnerInvite | None:
        current = float(time.time() if now is None else now)
        with self._lock, db_backend.transaction(self._player_database) as conn:
            self.ensure_schema(conn)
            self.expire_stale(conn, current)
            row = conn.execute(
                "SELECT invite_id,inviter_id,target_id,count,status,created_at,expires_at "
                "FROM partner_cultivation_invites WHERE status='pending' AND target_id=%s",
                (str(target_id),),
            ).fetchone()
        return self.from_row(row)

    def resolve(self, invite_id, target_id, status, *, now=None) -> PartnerInviteResult:
        if status not in {"rejected", "expired"}:
            raise ValueError("invalid partner invite status")
        resolved_at = float(time.time() if now is None else now)
        with self._lock, db_backend.transaction(self._player_database) as conn:
            self.ensure_schema(conn)
            changed = conn.execute(
                "UPDATE partner_cultivation_invites SET status=%s,resolved_at=%s "
                "WHERE invite_id=%s AND target_id=%s AND status='pending'",
                (status, resolved_at, str(invite_id), str(target_id)),
            )
            row = conn.execute(
                "SELECT invite_id,inviter_id,target_id,count,status,created_at,expires_at "
                "FROM partner_cultivation_invites WHERE invite_id=%s", (str(invite_id),),
            ).fetchone()
        invite = self.from_row(row)
        if changed.rowcount == 1:
            return PartnerInviteResult("applied", invite)
        if invite is not None and invite.status == status:
            return PartnerInviteResult("duplicate", invite)
        return PartnerInviteResult("state_changed", invite)


__all__ = ["PartnerInvite", "PartnerInviteResult", "PartnerInviteService"]
