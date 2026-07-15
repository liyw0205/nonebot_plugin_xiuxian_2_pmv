from __future__ import annotations

import json
import time
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend
from ..xiuxian_utils.fight_models import Entity
from ..xiuxian_utils.player_fight import BattleSystem, apply_player_buffs, get_players_attributes
from ..xiuxian_utils.numeric_bind import as_int_like as _as_int_like_num
from ..xiuxian_utils.numeric_bind import number_count, sql_num as _sql_num
from ..xiuxian_utils.numeric_bind import sql_num_nonneg as _sql_num_nonneg
from .relation_transaction_utils import (
    append_mentor_history,
    ensure_player_field,
    get_json_field,
    increment_stat,
    set_field,
)


# number_count / _sql_num* re-exported helpers live in numeric_bind (shared)
@dataclass(frozen=True)
class PartnerProtectionResult:
    status: str
    previous_status: str = "off"
    current_status: str = "off"

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PartnerProtectionService:
    """Own the protection state used by invite and settlement transactions."""

    VALID_STATUSES = {"on", "off", "refusal"}

    def __init__(self, player_database: str | Path, lock: RLock | None = None):
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @classmethod
    def normalize(cls, status) -> str:
        status = str(status or "off").strip().lower()
        return status if status in cls.VALID_STATUSES else "off"

    @classmethod
    def require_valid(cls, status) -> str:
        status = str(status).strip().lower()
        if status not in cls.VALID_STATUSES:
            raise ValueError("invalid partner protection status")
        return status

    @staticmethod
    def _table_ref(schema="") -> str:
        table = db_backend.quote_ident("status")
        return f"{schema}.{table}" if schema else table

    @classmethod
    def ensure_schema(cls, conn, schema="") -> None:
        table = cls._table_ref(schema)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (user_id TEXT PRIMARY KEY)")
        pragma = f"PRAGMA {schema + '.' if schema else ''}table_info(status)"
        columns = {str(row[1]) for row in conn.execute(pragma).fetchall()}
        if "two_exp_protect" not in columns:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN "
                f"{db_backend.quote_ident('two_exp_protect')} TEXT"
            )

    @classmethod
    def read_status(cls, conn, user_id, schema="") -> str:
        cls.ensure_schema(conn, schema)
        row = conn.execute(
            f"SELECT {db_backend.quote_ident('two_exp_protect')} "
            f"FROM {cls._table_ref(schema)} WHERE user_id=%s",
            (str(user_id),),
        ).fetchone()
        return cls.normalize(row[0] if row is not None else "off")

    def get_status(self, user_id) -> str:
        with self._lock, db_backend.transaction(self._player_database) as conn:
            return self.read_status(conn, user_id)

    def set_status(
        self, operation_id, user_id, expected_status, new_status
    ) -> PartnerProtectionResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        expected_status = self.require_valid(expected_status)
        new_status = self.require_valid(new_status)
        if not operation_id or not user_id:
            raise ValueError("invalid partner protection operation")
        payload = json.dumps(
            [user_id, new_status],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self.ensure_schema(conn)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_protection_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "previous_status TEXT NOT NULL,current_status TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,previous_status,current_status "
                    "FROM partner_protection_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    saved_payload = json.loads(str(previous[0]))
                    saved_identity = (
                        [str(saved_payload[0]), str(saved_payload[2])]
                        if len(saved_payload) == 3
                        else [str(value) for value in saved_payload]
                    )
                    if saved_identity != [user_id, new_status]:
                        return PartnerProtectionResult("operation_conflict")
                    return PartnerProtectionResult(
                        "duplicate", str(previous[1]), str(previous[2])
                    )

                current_status = self.read_status(conn, user_id)
                if current_status != expected_status:
                    conn.rollback()
                    return PartnerProtectionResult(
                        "state_changed", current_status, current_status
                    )
                field = db_backend.quote_ident("two_exp_protect")
                table = self._table_ref()
                changed = conn.execute(
                    f"UPDATE {table} SET {field}=%s WHERE user_id=%s",
                    (new_status, user_id),
                )
                if changed.rowcount == 0:
                    conn.execute(
                        f"INSERT INTO {table}(user_id,{field}) VALUES(%s,%s)",
                        (user_id, new_status),
                    )
                conn.execute(
                    "INSERT INTO partner_protection_operations("
                    "operation_id,payload,previous_status,current_status"
                    ") VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, current_status, new_status),
                )
                conn.commit()
                return PartnerProtectionResult(
                    "applied", current_status, new_status
                )
            except Exception:
                conn.rollback()
                raise

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

@dataclass(frozen=True)
class PartnerCultivationResult:
    status: str
    exp_1: int
    exp_2: int
    used_count: int
    affection_1: int
    affection_2: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PartnerCultivationService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(
        self, operation_id, user_id_1, user_id_2, *, expected_exp_1, expected_exp_2,
        exp_1, exp_2, used_count, power_1, power_2, hp_1, mp_1, atk_1, hp_2, mp_2, atk_2,
        level_rate_1=0, level_rate_2=0, expected_affection_1=None, expected_affection_2=None,
        affection_1=0, affection_2=0, invite_id=None,
        expected_used_count_1=None, expected_used_count_2=None,
        expected_target_protection=None,
    ) -> PartnerCultivationResult:
        operation_id = str(operation_id).strip()
        user_id_1, user_id_2 = str(user_id_1), str(user_id_2)
        # expected_exp concurrency snapshot + used_count/affection/rate stay int;
        # combat write fields may exceed SQLite INTEGER (high-realm dual cultivation).
        expected_exp_1 = int(expected_exp_1)
        expected_exp_2 = int(expected_exp_2)
        used_count = int(used_count)
        level_rate_1 = int(level_rate_1)
        level_rate_2 = int(level_rate_2)
        affection_1 = int(affection_1)
        affection_2 = int(affection_2)
        exp_1 = _sql_num_nonneg(exp_1)
        exp_2 = _sql_num_nonneg(exp_2)
        power_1 = _sql_num_nonneg(power_1)
        power_2 = _sql_num_nonneg(power_2)
        hp_1 = _sql_num_nonneg(hp_1)
        mp_1 = _sql_num_nonneg(mp_1)
        atk_1 = _sql_num_nonneg(atk_1)
        hp_2 = _sql_num_nonneg(hp_2)
        mp_2 = _sql_num_nonneg(mp_2)
        atk_2 = _sql_num_nonneg(atk_2)
        values = (
            expected_exp_1, expected_exp_2, exp_1, exp_2, used_count, power_1, power_2,
            hp_1, mp_1, atk_1, hp_2, mp_2, atk_2, level_rate_1, level_rate_2,
            affection_1, affection_2,
        )
        if not operation_id or values[4] <= 0 or float(values[2]) < 0 or float(values[3]) < 0:
            raise ValueError("invalid partner cultivation operation")
        expected_affection_1 = None if expected_affection_1 is None else int(expected_affection_1)
        expected_affection_2 = None if expected_affection_2 is None else int(expected_affection_2)
        invite_id = None if invite_id is None else str(invite_id)
        expected_used_count_1 = None if expected_used_count_1 is None else int(expected_used_count_1)
        expected_used_count_2 = None if expected_used_count_2 is None else int(expected_used_count_2)
        expected_target_protection = (
            None if expected_target_protection is None
            else PartnerProtectionService.require_valid(expected_target_protection)
        )
        if invite_id and (expected_used_count_1 is None or expected_used_count_2 is None):
            raise ValueError("invite settlement requires usage snapshots")
        payload = json.dumps(
            [user_id_1, user_id_2, *values, expected_affection_1, expected_affection_2,
             invite_id, expected_used_count_1, expected_used_count_2,
             expected_target_protection],
            separators=(",", ":"), ensure_ascii=True,
        )

        def result(status):
            return PartnerCultivationResult(status, _as_int_like_num(values[2]), _as_int_like_num(values[3]), int(values[4]), int(values[15]), int(values[16]))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_cultivation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,exp_1 INTEGER NOT NULL,"
                    "exp_2 INTEGER NOT NULL,used_count INTEGER NOT NULL,affection_1 INTEGER NOT NULL,"
                    "affection_2 INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,exp_1,exp_2,used_count,affection_1,affection_2 "
                    "FROM partner_cultivation_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("operation_conflict")
                    return PartnerCultivationResult("duplicate", *(_as_int_like_num(value) for value in previous[1:]))

                if expected_target_protection is not None:
                    actual_protection = PartnerProtectionService.read_status(
                        conn, user_id_2, "player_data"
                    )
                    if actual_protection != expected_target_protection:
                        conn.rollback()
                        return result("protection_changed")

                if invite_id:
                    PartnerInviteService.ensure_schema(conn, "player_data")
                    invite = conn.execute(
                        "SELECT inviter_id,target_id,count,status,expires_at FROM "
                        "player_data.partner_cultivation_invites WHERE invite_id=%s", (invite_id,),
                    ).fetchone()
                    if invite is None or str(invite[0]) != user_id_1 or str(invite[1]) != user_id_2:
                        conn.rollback()
                        return result("invitation_changed")
                    if str(invite[3]) != "pending" or float(invite[4]) <= __import__("time").time():
                        conn.rollback()
                        return result("invitation_changed")
                    if int(invite[2]) < values[4]:
                        conn.rollback()
                        return result("invitation_changed")
                    conn.execute(
                        "CREATE TABLE IF NOT EXISTS player_data.partner_two_exp_usage ("
                        "user_id TEXT PRIMARY KEY,used_count INTEGER NOT NULL DEFAULT 0,"
                        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                    )
                    for user_id, expected in (
                        (user_id_1, expected_used_count_1), (user_id_2, expected_used_count_2),
                    ):
                        row = conn.execute(
                            "SELECT used_count FROM player_data.partner_two_exp_usage WHERE user_id=%s", (user_id,),
                        ).fetchone()
                        current_used = int(row[0]) if row is not None else 0
                        if current_used != expected:
                            conn.rollback()
                            return result("state_changed")

                rows = conn.execute(
                    "SELECT user_id,exp FROM user_xiuxian WHERE user_id IN (%s,%s)",
                    (user_id_1, user_id_2),
                ).fetchall()
                current = {str(row[0]): int(float(row[1] or 0)) for row in rows}
                if current != {user_id_1: values[0], user_id_2: values[1]}:
                    conn.rollback()
                    return result("state_changed")

                if expected_affection_1 is not None:
                    for user_id, partner_id, expected in (
                        (user_id_1, user_id_2, expected_affection_1),
                        (user_id_2, user_id_1, expected_affection_2),
                    ):
                        row = conn.execute(
                            "SELECT partner_id,affection FROM player_data.partner WHERE user_id=%s", (user_id,),
                        ).fetchone()
                        if row is None or str(row[0]) != partner_id or int(row[1] or 0) != expected:
                            conn.rollback()
                            return result("state_changed")

                for user_id, expected_exp, gain, power, hp, mp, atk, rate in (
                    (user_id_1, values[0], values[2], values[5], values[7], values[8], values[9], values[13]),
                    (user_id_2, values[1], values[3], values[6], values[10], values[11], values[12], values[14]),
                ):
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET "
                        "exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL),"
                        "power=%s,hp=%s,mp=%s,atk=%s,"
                        "level_up_rate=COALESCE(level_up_rate,0)+%s "
                        "WHERE user_id=%s AND CAST(COALESCE(exp,0) AS REAL)=CAST(%s AS REAL)",
                        (gain, power, hp, mp, atk, rate, user_id, expected_exp),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return result("state_changed")
                    increment_stat(conn, user_id, "双修次数", values[4])

                if expected_affection_1 is not None:
                    set_field(conn, "partner", user_id_1, "affection", expected_affection_1 + values[15], "INTEGER")
                    set_field(conn, "partner", user_id_2, "affection", expected_affection_2 + values[16], "INTEGER")

                if invite_id:
                    for user_id, expected in (
                        (user_id_1, expected_used_count_1), (user_id_2, expected_used_count_2),
                    ):
                        changed = conn.execute(
                            "UPDATE player_data.partner_two_exp_usage SET used_count=used_count+%s "
                            "WHERE user_id=%s AND used_count=%s",
                            (values[4], user_id, expected),
                        )
                        if changed.rowcount == 0:
                            if expected != 0:
                                conn.rollback()
                                return result("state_changed")
                            conn.execute(
                                "INSERT INTO player_data.partner_two_exp_usage(user_id,used_count) VALUES(%s,%s)",
                                (user_id, values[4]),
                            )
                    changed = conn.execute(
                        "UPDATE player_data.partner_cultivation_invites SET status='accepted',"
                        "resolved_at=strftime('%s','now') WHERE invite_id=%s AND status='pending'",
                        (invite_id,),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return result("invitation_changed")

                conn.execute(
                    "INSERT INTO partner_cultivation_operations "
                    "(operation_id,payload,exp_1,exp_2,used_count,affection_1,affection_2) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, values[2], values[3], values[4], values[15], values[16]),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

@dataclass(frozen=True)
class PartnerBindResult:
    status: str
    bind_time: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PartnerBindService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(
        self,
        operation_id,
        invitee_id,
        inviter_id,
        *,
        bind_time,
        expected_invitee_partner=None,
        expected_inviter_partner=None,
    ) -> PartnerBindResult:
        operation_id = str(operation_id).strip()
        invitee_id, inviter_id = str(invitee_id), str(inviter_id)
        bind_time = str(bind_time).strip()
        expected_invitee_partner = self._partner_value(expected_invitee_partner)
        expected_inviter_partner = self._partner_value(expected_inviter_partner)
        if not operation_id or not bind_time or invitee_id == inviter_id:
            raise ValueError("invalid partner bind operation")
        payload = json.dumps(
            [invitee_id, inviter_id, bind_time, expected_invitee_partner, expected_inviter_partner],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_partner_table(conn)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_bind_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,bind_time TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,bind_time FROM partner_bind_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return PartnerBindResult(
                        "duplicate" if str(previous[0]) == payload else "operation_conflict", str(previous[1])
                    )
                users = conn.execute(
                    "SELECT user_id FROM user_xiuxian WHERE user_id IN (%s,%s)",
                    (invitee_id, inviter_id),
                ).fetchall()
                if {str(row[0]) for row in users} != {invitee_id, inviter_id}:
                    conn.rollback()
                    return PartnerBindResult("user_missing")
                actual = {
                    user_id: self._current_partner(conn, user_id)
                    for user_id in (invitee_id, inviter_id)
                }
                if actual != {
                    invitee_id: expected_invitee_partner,
                    inviter_id: expected_inviter_partner,
                }:
                    conn.rollback()
                    return PartnerBindResult("state_changed")
                for user_id, partner_id in ((invitee_id, inviter_id), (inviter_id, invitee_id)):
                    changed = conn.execute(
                        "UPDATE player_data.partner SET partner_id=%s,bind_time=%s,affection=0 "
                        "WHERE user_id=%s",
                        (partner_id, bind_time, user_id),
                    )
                    if changed.rowcount == 0:
                        conn.execute(
                            "INSERT INTO player_data.partner(user_id,partner_id,bind_time,affection) VALUES(%s,%s,%s,0)",
                            (user_id, partner_id, bind_time),
                        )
                conn.execute(
                    "INSERT INTO partner_bind_operations(operation_id,payload,bind_time) VALUES(%s,%s,%s)",
                    (operation_id, payload, bind_time),
                )
                conn.commit()
                return PartnerBindResult("applied", bind_time)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    @staticmethod
    def _partner_value(value):
        if value is None or str(value).strip().lower() in {"", "none", "null"}:
            return None
        return str(value)

    @classmethod
    def _current_partner(cls, conn, user_id):
        row = conn.execute(
            "SELECT partner_id FROM player_data.partner WHERE user_id=%s", (user_id,)
        ).fetchone()
        return None if row is None else cls._partner_value(row[0])

    @staticmethod
    def _ensure_partner_table(conn):
        ensure_player_field(conn, "partner", "partner_id", "TEXT")
        ensure_player_field(conn, "partner", "bind_time", "TEXT")
        ensure_player_field(conn, "partner", "affection", "INTEGER")

@dataclass(frozen=True)
class PartnerUnbindResult:
    status: str
    partner_id: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PartnerUnbindService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(
        self,
        operation_id,
        user_id,
        partner_id,
        *,
        expected_user_bind_time,
        expected_partner_bind_time,
        expected_user_affection,
        expected_partner_affection,
        checked_at,
        minimum_days=7,
    ) -> PartnerUnbindResult:
        operation_id = str(operation_id).strip()
        user_id, partner_id = str(user_id), str(partner_id)
        expected_user_bind_time = self._text_or_none(expected_user_bind_time)
        expected_partner_bind_time = self._text_or_none(expected_partner_bind_time)
        expected_user_affection = int(expected_user_affection or 0)
        expected_partner_affection = int(expected_partner_affection or 0)
        checked_at = str(checked_at).strip()
        minimum_days = int(minimum_days)
        if not operation_id or user_id == partner_id or not checked_at or minimum_days < 0:
            raise ValueError("invalid partner unbind operation")
        payload = json.dumps(
            [user_id, partner_id, expected_user_bind_time, expected_partner_bind_time,
             expected_user_affection, expected_partner_affection, checked_at, minimum_days],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_partner_table(conn)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_unbind_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,partner_id TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,partner_id FROM partner_unbind_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return PartnerUnbindResult(
                        "duplicate" if str(previous[0]) == payload else "operation_conflict", str(previous[1])
                    )
                rows = conn.execute(
                    "SELECT user_id,partner_id,bind_time,COALESCE(affection,0) FROM player_data.partner "
                    "WHERE user_id IN (%s,%s)",
                    (user_id, partner_id),
                ).fetchall()
                actual = {
                    str(row[0]): (self._text_or_none(row[1]), self._text_or_none(row[2]), int(row[3]))
                    for row in rows
                }
                expected = {
                    user_id: (partner_id, expected_user_bind_time, expected_user_affection),
                    partner_id: (user_id, expected_partner_bind_time, expected_partner_affection),
                }
                if actual != expected:
                    conn.rollback()
                    return PartnerUnbindResult("state_changed", partner_id)
                if self._within_minimum_period(expected_user_bind_time, checked_at, minimum_days):
                    conn.rollback()
                    return PartnerUnbindResult("too_early", partner_id)
                for owner_id, related_id, bind_time, affection in (
                    (user_id, partner_id, expected_user_bind_time, expected_user_affection),
                    (partner_id, user_id, expected_partner_bind_time, expected_partner_affection),
                ):
                    changed = conn.execute(
                        "UPDATE player_data.partner SET partner_id=NULL,bind_time=NULL,affection=0 "
                        "WHERE user_id=%s AND partner_id=%s AND "
                        "COALESCE(bind_time,'')=%s AND COALESCE(affection,0)=%s",
                        (owner_id, related_id, bind_time or "", affection),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return PartnerUnbindResult("state_changed", partner_id)
                conn.execute(
                    "INSERT INTO partner_unbind_operations(operation_id,payload,partner_id) VALUES(%s,%s,%s)",
                    (operation_id, payload, partner_id),
                )
                conn.commit()
                return PartnerUnbindResult("applied", partner_id)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    @staticmethod
    def _text_or_none(value):
        if value is None or str(value).strip().lower() in {"", "none", "null"}:
            return None
        return str(value)

    @staticmethod
    def _within_minimum_period(bind_time, checked_at, minimum_days):
        if not bind_time:
            return False
        try:
            bound = datetime.strptime(bind_time, "%Y-%m-%d %H:%M:%S")
            checked = datetime.strptime(checked_at, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return False
        return checked < bound + timedelta(days=minimum_days)

    @staticmethod
    def _ensure_partner_table(conn):
        ensure_player_field(conn, "partner", "partner_id", "TEXT")
        ensure_player_field(conn, "partner", "bind_time", "TEXT")
        ensure_player_field(conn, "partner", "affection", "INTEGER")

@dataclass(frozen=True)
class PartnerTokenUseResult:
    status: str
    used_tokens: int = 0
    used_count: int = 0
    item_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PartnerTokenUseService:
    """Atomically consume tokens and persist the authoritative two-exp usage count."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(
        self,
        operation_id,
        user_id,
        item_id,
        *,
        requested_count,
        expected_item_count,
        expected_used_count,
    ) -> PartnerTokenUseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        requested_count = int(requested_count)
        expected_item_count = int(expected_item_count)
        expected_used_count = int(expected_used_count)
        if not operation_id or requested_count <= 0 or expected_item_count < 0 or expected_used_count < 0:
            raise ValueError("invalid partner token operation")
        payload = json.dumps(
            [user_id, item_id, requested_count, expected_item_count, expected_used_count],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_token_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,used_tokens INTEGER NOT NULL,"
                    "used_count INTEGER NOT NULL,item_remaining INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS player_data.partner_two_exp_usage ("
                    "user_id TEXT PRIMARY KEY,used_count INTEGER NOT NULL DEFAULT 0,"
                    "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,used_tokens,used_count,item_remaining FROM partner_token_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    status = "duplicate" if str(previous[0]) == payload else "operation_conflict"
                    return PartnerTokenUseResult(status, int(previous[1]), int(previous[2]), int(previous[3]))

                usage = conn.execute(
                    "SELECT used_count FROM player_data.partner_two_exp_usage WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if usage is not None and int(usage[0]) != expected_used_count:
                    conn.rollback()
                    return PartnerTokenUseResult("state_changed")
                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None:
                    conn.rollback()
                    return PartnerTokenUseResult("item_missing")
                if int(item[0]) != expected_item_count:
                    conn.rollback()
                    return PartnerTokenUseResult("state_changed")

                used_tokens = min(requested_count, expected_used_count, expected_item_count)
                if used_tokens <= 0:
                    conn.rollback()
                    return PartnerTokenUseResult("limit_full", 0, expected_used_count, expected_item_count)
                new_used_count = expected_used_count - used_tokens
                item_remaining = expected_item_count - used_tokens
                bind_remaining = min(max(0, int(item[1]) - used_tokens), item_remaining)
                changed = conn.execute(
                    "UPDATE back SET goods_num=%s,bind_num=%s WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num,0)=%s",
                    (item_remaining, bind_remaining, user_id, item_id, expected_item_count),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return PartnerTokenUseResult("state_changed")
                if usage is None:
                    conn.execute(
                        "INSERT INTO player_data.partner_two_exp_usage(user_id,used_count) VALUES(%s,%s)",
                        (user_id, new_used_count),
                    )
                else:
                    changed = conn.execute(
                        "UPDATE player_data.partner_two_exp_usage SET used_count=%s "
                        "WHERE user_id=%s AND used_count=%s",
                        (new_used_count, user_id, expected_used_count),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return PartnerTokenUseResult("state_changed")
                conn.execute(
                    "INSERT INTO partner_token_operations(operation_id,payload,used_tokens,used_count,item_remaining) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, used_tokens, new_used_count, item_remaining),
                )
                conn.commit()
                return PartnerTokenUseResult("applied", used_tokens, new_used_count, item_remaining)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

@dataclass(frozen=True)
class PartnerBreakthroughResult:
    status: str
    reward_exp: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PartnerBreakthroughService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(self, operation_id, user_id, partner_id, new_level, *, expected_user_exp, expected_partner_exp, expected_affection, reward_exp, partner_power):
        operation_id, user_id, partner_id = str(operation_id).strip(), str(user_id), str(partner_id)
        expected_user_exp, expected_partner_exp = int(expected_user_exp), int(expected_partner_exp)
        expected_affection = int(expected_affection)
        reward_exp = _sql_num_nonneg(reward_exp)
        partner_power = _sql_num_nonneg(partner_power)
        if not operation_id or float(reward_exp) <= 0:
            raise ValueError("invalid partner breakthrough reward")
        payload = json.dumps(
            [user_id, partner_id, str(new_level), expected_user_exp, expected_partner_exp, expected_affection, reward_exp, partner_power],
            separators=(",", ":"), ensure_ascii=False,
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_breakthrough_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reward_exp INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,reward_exp FROM partner_breakthrough_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return PartnerBreakthroughResult("duplicate" if str(previous[0]) == payload else "operation_conflict", int(previous[1]))
                relation = conn.execute("SELECT partner_id,affection FROM player_data.partner WHERE user_id=%s", (user_id,)).fetchone()
                reciprocal = conn.execute("SELECT partner_id FROM player_data.partner WHERE user_id=%s", (partner_id,)).fetchone()
                if relation is None or reciprocal is None or str(relation[0]) != partner_id or str(reciprocal[0]) != user_id or int(relation[1] or 0) != expected_affection:
                    conn.rollback()
                    return PartnerBreakthroughResult("state_changed", _as_int_like_num(reward_exp))
                users = conn.execute("SELECT user_id,exp FROM user_xiuxian WHERE user_id IN (%s,%s)", (user_id, partner_id)).fetchall()
                if {str(row[0]): int(float(row[1] or 0)) for row in users} != {user_id: expected_user_exp, partner_id: expected_partner_exp}:
                    conn.rollback()
                    return PartnerBreakthroughResult("state_changed", _as_int_like_num(reward_exp))
                changed = conn.execute(
                    "UPDATE user_xiuxian SET "
                    "exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL),power=%s "
                    "WHERE user_id=%s AND CAST(COALESCE(exp,0) AS REAL)=CAST(%s AS REAL)",
                    (reward_exp, partner_power, partner_id, expected_partner_exp),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return PartnerBreakthroughResult("state_changed", _as_int_like_num(reward_exp))
                conn.execute("INSERT INTO partner_breakthrough_operations (operation_id,payload,reward_exp) VALUES (%s,%s,%s)", (operation_id, payload, reward_exp))
                conn.commit()
                return PartnerBreakthroughResult("applied", _as_int_like_num(reward_exp))
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

@dataclass(frozen=True)
class MentorBindResult:
    status: str
    bind_time: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MentorBindService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _operation_identity(payload) -> list:
        values = json.loads(str(payload))
        if len(values) == 10:
            return [*values[:3], *values[4:]]
        return values

    def replay(
        self, operation_id, mentor_id, apprentice_id
    ) -> MentorBindResult | None:
        operation_id = str(operation_id).strip()
        mentor_id = str(mentor_id)
        apprentice_id = str(apprentice_id)
        if not operation_id:
            raise ValueError("invalid mentor bind operation")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='mentor_bind_operations'"
            ).fetchone()
            if table is None:
                return None
            previous = conn.execute(
                "SELECT payload,bind_time FROM mentor_bind_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
        if previous is None:
            return None
        identity = self._operation_identity(previous[0])
        status = (
            "duplicate"
            if [str(identity[0]), str(identity[1])] == [mentor_id, apprentice_id]
            else "operation_conflict"
        )
        return MentorBindResult(status, str(previous[1]))

    def apply(
        self,
        operation_id,
        mentor_id,
        apprentice_id,
        invite_id,
        *,
        bind_time,
        expected_mentor_level,
        expected_apprentice_level,
        max_apprentices,
        history_limit,
        mentor_desc,
        apprentice_desc,
        invitation_validator: Callable[[str, str, str], bool] | None = None,
        now: datetime | None = None,
    ) -> MentorBindResult:
        operation_id = str(operation_id).strip()
        mentor_id, apprentice_id, invite_id = str(mentor_id), str(apprentice_id), str(invite_id)
        bind_time = str(bind_time)
        max_apprentices, history_limit = int(max_apprentices), int(history_limit)
        if not operation_id or mentor_id == apprentice_id or not invite_id or max_apprentices <= 0 or history_limit <= 0:
            raise ValueError("invalid mentor bind operation")
        identity = [
            mentor_id, apprentice_id, invite_id,
            str(expected_mentor_level), str(expected_apprentice_level),
            max_apprentices, history_limit, mentor_desc, apprentice_desc,
        ]
        payload = json.dumps(
            identity,
            ensure_ascii=False, separators=(",", ":"),
        )
        check_time = now or datetime.now()
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mentor_bind_operations (operation_id TEXT PRIMARY KEY,"
                    "payload TEXT NOT NULL,bind_time TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,bind_time FROM mentor_bind_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    status = (
                        "duplicate"
                        if self._operation_identity(previous[0]) == identity
                        else "operation_conflict"
                    )
                    return MentorBindResult(status, str(previous[1]))
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS player_data.mentor_applications ("
                    "invite_id TEXT PRIMARY KEY,mentor_id TEXT NOT NULL,apprentice_id TEXT NOT NULL,"
                    "status TEXT NOT NULL,created_at REAL NOT NULL,expires_at REAL NOT NULL,resolved_at REAL)"
                )
                application = conn.execute(
                    "SELECT status,expires_at FROM player_data.mentor_applications "
                    "WHERE invite_id=%s AND mentor_id=%s AND apprentice_id=%s",
                    (invite_id, mentor_id, apprentice_id),
                ).fetchone()
                if application is None:
                    valid_legacy = invitation_validator and invitation_validator(mentor_id, apprentice_id, invite_id)
                    if not valid_legacy:
                        conn.rollback()
                        return MentorBindResult("invitation_changed", bind_time)
                elif str(application[0]) != "pending" or float(application[1]) <= check_time.timestamp():
                    conn.rollback()
                    return MentorBindResult("invitation_changed", bind_time)
                users = conn.execute(
                    "SELECT user_id,level FROM user_xiuxian WHERE user_id IN (%s,%s)", (mentor_id, apprentice_id)
                ).fetchall()
                levels = {str(row[0]): str(row[1]) for row in users}
                if levels != {mentor_id: str(expected_mentor_level), apprentice_id: str(expected_apprentice_level)}:
                    conn.rollback()
                    return MentorBindResult("state_changed", bind_time)
                mentor_parent = conn.execute(
                    "SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (mentor_id,)
                ).fetchone()
                apprentice_parent = conn.execute(
                    "SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)
                ).fetchone()
                if apprentice_parent is not None and apprentice_parent[0] not in (None, ""):
                    conn.rollback()
                    return MentorBindResult("already_bound", bind_time)
                if mentor_parent is not None and str(mentor_parent[0]) == apprentice_id:
                    conn.rollback()
                    return MentorBindResult("state_changed", bind_time)
                ensure_player_field(conn, "mentor", "mentor_cd_until")
                ensure_player_field(conn, "mentor", "apprentice_cd_until")
                mentor_cd_row = conn.execute(
                    "SELECT mentor_cd_until FROM player_data.mentor WHERE user_id=%s", (mentor_id,)
                ).fetchone()
                apprentice_cd_row = conn.execute(
                    "SELECT apprentice_cd_until FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)
                ).fetchone()
                mentor_cd = mentor_cd_row[0] if mentor_cd_row is not None else None
                apprentice_cd = apprentice_cd_row[0] if apprentice_cd_row is not None else None
                rebind = get_json_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", {})
                if _active(mentor_cd, check_time) or _active(apprentice_cd, check_time) or _active(rebind.get(mentor_id), check_time):
                    conn.rollback()
                    return MentorBindResult("cooldown_active", bind_time)
                apprentices = [str(value) for value in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                valid = []
                for user_id in apprentices:
                    row = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (user_id,)).fetchone()
                    if row is not None and str(row[0]) == mentor_id:
                        valid.append(user_id)
                if apprentice_id not in valid and len(valid) >= max_apprentices:
                    conn.rollback()
                    return MentorBindResult("capacity_reached", bind_time)
                set_field(conn, "mentor", mentor_id, "apprentice_ids", [*valid, apprentice_id])
                set_field(conn, "mentor", apprentice_id, "mentor_id", mentor_id)
                set_field(conn, "mentor", apprentice_id, "bind_time", bind_time)
                set_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", 0, "INTEGER")
                increment_stat(conn, mentor_id, "收徒次数", 1)
                increment_stat(conn, apprentice_id, "拜师次数", 1)
                append_mentor_history(conn, mentor_id, "bind", apprentice_id, mentor_desc, history_limit)
                append_mentor_history(conn, apprentice_id, "bind", mentor_id, apprentice_desc, history_limit)
                if application is not None:
                    consumed = conn.execute(
                        "UPDATE player_data.mentor_applications SET status='accepted',resolved_at=%s "
                        "WHERE invite_id=%s AND status='pending'",
                        (check_time.timestamp(), invite_id),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return MentorBindResult("invitation_changed", bind_time)
                conn.execute(
                    "INSERT INTO mentor_bind_operations (operation_id,payload,bind_time) VALUES (%s,%s,%s)",
                    (operation_id, payload, bind_time),
                )
                conn.commit()
                return MentorBindResult("applied", bind_time)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

def _active(value, now: datetime) -> bool:
    if value in (None, ""):
        return False
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S") > now
    except (TypeError, ValueError):
        return True

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

@dataclass(frozen=True)
class MentorExpelResult:
    status: str
    mentor_cd_until: str
    apprentice_cd_until: str
    pair_rebind_until: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MentorExpelService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(self, operation_id, mentor_id, apprentice_id, *, occurred_at, mentor_cd_until,
              apprentice_cd_until, pair_rebind_until, history_limit, mentor_desc, apprentice_desc):
        operation_id, mentor_id, apprentice_id = str(operation_id).strip(), str(mentor_id), str(apprentice_id)
        values = tuple(str(v) for v in (occurred_at, mentor_cd_until, apprentice_cd_until, pair_rebind_until))
        history_limit = int(history_limit)
        if not operation_id or mentor_id == apprentice_id or history_limit <= 0:
            raise ValueError("invalid mentor expel operation")
        payload = json.dumps([mentor_id, apprentice_id, *values, history_limit, mentor_desc, apprentice_desc],
                             ensure_ascii=False, separators=(",", ":"))
        result = lambda status: MentorExpelResult(status, values[1], values[2], values[3])
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS mentor_expel_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload FROM mentor_expel_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback(); return result("duplicate" if str(previous[0]) == payload else "operation_conflict")
                apprentices = [str(v) for v in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                parent = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)).fetchone()
                if apprentice_id not in apprentices or parent is None or str(parent[0]) != mentor_id:
                    conn.rollback(); return result("state_changed")
                set_field(conn, "mentor", mentor_id, "apprentice_ids", [v for v in apprentices if v != apprentice_id])
                set_field(conn, "mentor", mentor_id, "mentor_cd_until", values[1])
                set_field(conn, "mentor", apprentice_id, "mentor_id", None)
                set_field(conn, "mentor", apprentice_id, "bind_time", None)
                set_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", 0, "INTEGER")
                set_field(conn, "mentor", apprentice_id, "apprentice_cd_until", values[2])
                rebind = get_json_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", {})
                rebind[mentor_id] = values[3]
                set_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", rebind)
                increment_stat(conn, mentor_id, "逐出徒弟次数", 1)
                increment_stat(conn, apprentice_id, "被逐出师门次数", 1)
                append_mentor_history(conn, mentor_id, "expel", apprentice_id, mentor_desc, history_limit)
                append_mentor_history(conn, apprentice_id, "expel", mentor_id, apprentice_desc, history_limit)
                conn.execute("INSERT INTO mentor_expel_operations (operation_id,payload) VALUES (%s,%s)", (operation_id, payload))
                conn.commit(); return result("applied")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass

@dataclass(frozen=True)
class MentorBreakthroughRewardResult:
    status: str
    reward_exp: int
    reward_count: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MentorBreakthroughRewardService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(self, operation_id, mentor_id, apprentice_id, new_level, business_event_id, *,
              expected_mentor_exp, expected_apprentice_exp, expected_reward_count, reward_limit,
              reward_exp, max_mentor_exp, mentor_power, history_limit, mentor_desc, apprentice_desc):
        operation_id, mentor_id, apprentice_id = str(operation_id).strip(), str(mentor_id), str(apprentice_id)
        new_level, business_event_id = str(new_level), str(business_event_id).strip()
        expected_mentor_exp = int(expected_mentor_exp)
        expected_apprentice_exp = int(expected_apprentice_exp)
        expected_reward_count = int(expected_reward_count)
        reward_limit = int(reward_limit)
        history_limit = int(history_limit)
        max_mentor_exp = int(max_mentor_exp)
        reward_exp = _sql_num_nonneg(reward_exp)
        mentor_power = _sql_num_nonneg(mentor_power)
        values = (
            expected_mentor_exp, expected_apprentice_exp, expected_reward_count,
            reward_limit, reward_exp, max_mentor_exp, mentor_power, history_limit,
        )
        if not operation_id or not business_event_id or mentor_id == apprentice_id or float(values[4]) <= 0 or values[2] < 0 or values[2] >= values[3]:
            raise ValueError("invalid mentor breakthrough reward")
        if expected_mentor_exp + float(reward_exp) > max_mentor_exp:
            raise ValueError("mentor exp cap exceeded")
        payload = json.dumps([mentor_id, apprentice_id, new_level, business_event_id, *values, mentor_desc, apprentice_desc],
                             ensure_ascii=False, separators=(",", ":"))
        result = lambda status, count=values[2] + 1: MentorBreakthroughRewardResult(status, _as_int_like_num(values[4]), count)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS mentor_breakthrough_reward_operations (operation_id TEXT PRIMARY KEY,business_event_id TEXT UNIQUE NOT NULL,payload TEXT NOT NULL,reward_exp INTEGER NOT NULL,reward_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload,reward_exp,reward_count FROM mentor_breakthrough_reward_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return MentorBreakthroughRewardResult("duplicate" if str(previous[0]) == payload else "operation_conflict", _as_int_like_num(previous[1]), int(previous[2]))
                event = conn.execute("SELECT payload,reward_exp,reward_count FROM mentor_breakthrough_reward_operations WHERE business_event_id=%s", (business_event_id,)).fetchone()
                if event is not None:
                    conn.rollback(); return MentorBreakthroughRewardResult("event_duplicate", _as_int_like_num(event[1]), int(event[2]))
                apprentices = [str(v) for v in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                parent = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)).fetchone()
                count = get_json_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", 0)
                users = conn.execute("SELECT user_id,exp FROM user_xiuxian WHERE user_id IN (%s,%s)", (mentor_id, apprentice_id)).fetchall()
                exps = {str(row[0]): int(float(row[1] or 0)) for row in users}
                if apprentice_id not in apprentices or parent is None or str(parent[0]) != mentor_id or int(count or 0) != values[2] or exps != {mentor_id: values[0], apprentice_id: values[1]}:
                    conn.rollback(); return result("state_changed")
                changed = conn.execute(
                    "UPDATE user_xiuxian SET "
                    "exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL),power=%s "
                    "WHERE user_id=%s AND CAST(COALESCE(exp,0) AS REAL)=CAST(%s AS REAL) "
                    "AND CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL)<=%s",
                    (values[4], values[6], mentor_id, values[0], values[4], values[5]),
                )
                if changed.rowcount != 1:
                    conn.rollback(); return result("state_changed")
                set_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", values[2] + 1, "INTEGER")
                increment_stat(conn, mentor_id, "师父突破返修", values[4])
                increment_stat(conn, apprentice_id, "徒弟突破回馈", values[4])
                append_mentor_history(conn, mentor_id, "breakthrough_reward", apprentice_id, mentor_desc, values[7])
                append_mentor_history(conn, apprentice_id, "breakthrough_reward", mentor_id, apprentice_desc, values[7])
                conn.execute("INSERT INTO mentor_breakthrough_reward_operations (operation_id,business_event_id,payload,reward_exp,reward_count) VALUES (%s,%s,%s,%s,%s)",
                             (operation_id, business_event_id, payload, values[4], values[2] + 1))
                conn.commit(); return result("applied")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass

@dataclass(frozen=True)
class ApprenticeLeaveResult:
    status: str
    apprentice_cd_until: str
    pair_rebind_until: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ApprenticeLeaveService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(self, operation_id, mentor_id, apprentice_id, *, occurred_at, expected_apprentice_level,
              graduation_eligible, apprentice_cd_until, pair_rebind_until, history_limit,
              mentor_desc, apprentice_desc):
        operation_id, mentor_id, apprentice_id = str(operation_id).strip(), str(mentor_id), str(apprentice_id)
        values = (str(occurred_at), str(expected_apprentice_level), bool(graduation_eligible),
                  str(apprentice_cd_until), str(pair_rebind_until), int(history_limit))
        if not operation_id or mentor_id == apprentice_id or values[2] or values[5] <= 0:
            raise ValueError("invalid apprentice leave operation")
        payload = json.dumps([mentor_id, apprentice_id, *values, mentor_desc, apprentice_desc],
                             ensure_ascii=False, separators=(",", ":"))
        result = lambda status: ApprenticeLeaveResult(status, values[3], values[4])
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS apprentice_leave_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload FROM apprentice_leave_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback(); return result("duplicate" if str(previous[0]) == payload else "operation_conflict")
                level = conn.execute("SELECT level FROM user_xiuxian WHERE user_id=%s", (apprentice_id,)).fetchone()
                apprentices = [str(v) for v in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                parent = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)).fetchone()
                if level is None or str(level[0]) != values[1] or apprentice_id not in apprentices or parent is None or str(parent[0]) != mentor_id:
                    conn.rollback(); return result("state_changed")
                set_field(conn, "mentor", mentor_id, "apprentice_ids", [v for v in apprentices if v != apprentice_id])
                set_field(conn, "mentor", apprentice_id, "mentor_id", None)
                set_field(conn, "mentor", apprentice_id, "bind_time", None)
                set_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", 0, "INTEGER")
                set_field(conn, "mentor", apprentice_id, "apprentice_cd_until", values[3])
                rebind = get_json_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", {})
                rebind[mentor_id] = values[4]
                set_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", rebind)
                increment_stat(conn, apprentice_id, "离开师门次数", 1)
                append_mentor_history(conn, mentor_id, "leave", apprentice_id, mentor_desc, values[5])
                append_mentor_history(conn, apprentice_id, "leave", mentor_id, apprentice_desc, values[5])
                conn.execute("INSERT INTO apprentice_leave_operations (operation_id,payload) VALUES (%s,%s)", (operation_id, payload))
                conn.commit(); return result("applied")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass

@dataclass(frozen=True)
class MentorGraduationResult:
    status: str
    apprentice_stone: int
    mentor_stone: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MentorGraduationService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(self, operation_id, mentor_id, apprentice_id, *, expected_mentor_stone, expected_apprentice_stone, apprentice_reward, mentor_reward, cooldown_days, history_limit, mentor_desc, apprentice_desc, apprentice_title_ids=(), mentor_title_ids=()):
        operation_id, mentor_id, apprentice_id = str(operation_id).strip(), str(mentor_id), str(apprentice_id)
        values = tuple(int(v) for v in (expected_mentor_stone, expected_apprentice_stone, apprentice_reward, mentor_reward, cooldown_days, history_limit))
        if not operation_id or values[2] < 0 or values[3] < 0:
            raise ValueError("invalid graduation operation")
        apprentice_title_ids = tuple(sorted(str(v) for v in apprentice_title_ids))
        mentor_title_ids = tuple(sorted(str(v) for v in mentor_title_ids))
        payload = json.dumps([mentor_id, apprentice_id, *values, mentor_desc, apprentice_desc, apprentice_title_ids, mentor_title_ids], separators=(",", ":"), ensure_ascii=False)
        result = lambda status: MentorGraduationResult(status, values[2], values[3])
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mentor_graduation_operations (operation_id TEXT PRIMARY KEY,"
                    "payload TEXT NOT NULL,apprentice_stone INTEGER NOT NULL,mentor_stone INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute("SELECT payload,apprentice_stone,mentor_stone FROM mentor_graduation_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return MentorGraduationResult("duplicate" if str(previous[0]) == payload else "operation_conflict", int(previous[1]), int(previous[2]))
                mentor_apprentices = [str(v) for v in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                apprentice_mentor = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)).fetchone()
                if apprentice_id not in mentor_apprentices or apprentice_mentor is None or str(apprentice_mentor[0]) != mentor_id:
                    conn.rollback()
                    return result("state_changed")
                stones = conn.execute("SELECT user_id,stone FROM user_xiuxian WHERE user_id IN (%s,%s)", (mentor_id, apprentice_id)).fetchall()
                if {str(row[0]): int(row[1]) for row in stones} != {mentor_id: values[0], apprentice_id: values[1]}:
                    conn.rollback()
                    return result("state_changed")
                for uid, expected, reward in ((mentor_id, values[0], values[3]), (apprentice_id, values[1], values[2])):
                    if conn.execute("UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s AND stone=%s", (reward, uid, expected)).rowcount != 1:
                        conn.rollback()
                        return result("state_changed")
                    increment_stat(conn, uid, "灵石获取", reward)
                increment_stat(conn, apprentice_id, "正常出师次数", 1)
                increment_stat(conn, mentor_id, "培养出师徒弟", 1)
                for uid, title_ids in ((apprentice_id, apprentice_title_ids), (mentor_id, mentor_title_ids)):
                    unlocked = {str(v) for v in get_json_field(conn, "title", uid, "unlocked", [])}
                    unlocked.update(title_ids)
                    set_field(conn, "title", uid, "unlocked", sorted(unlocked))
                set_field(conn, "mentor", mentor_id, "apprentice_ids", [uid for uid in mentor_apprentices if uid != apprentice_id])
                set_field(conn, "mentor", apprentice_id, "mentor_id", None)
                set_field(conn, "mentor", apprentice_id, "bind_time", None)
                set_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", 0, "INTEGER")
                rebind = get_json_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", {})
                rebind[mentor_id] = (datetime.now() + timedelta(days=values[4])).strftime("%Y-%m-%d %H:%M:%S")
                set_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", rebind)
                append_mentor_history(conn, mentor_id, "graduate", apprentice_id, mentor_desc, values[5])
                append_mentor_history(conn, apprentice_id, "graduate", mentor_id, apprentice_desc, values[5])
                conn.execute("INSERT INTO mentor_graduation_operations (operation_id,payload,apprentice_stone,mentor_stone) VALUES (%s,%s,%s,%s)", (operation_id, payload, values[2], values[3]))
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

@dataclass(frozen=True)
class MentorTransmissionResult:
    status: str
    reward_exp: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MentorTransmissionService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database); self._player_database = Path(player_database); self._lock = lock or RLock()

    def apply(self, operation_id, mentor_id, apprentice_id, *, expected_apprentice_exp, reward_exp, power, hp, mp, atk, mentor_used, apprentice_used, daily_limit, history_limit, mentor_desc, apprentice_desc):
        operation_id, mentor_id, apprentice_id = str(operation_id).strip(), str(mentor_id), str(apprentice_id)
        expected_apprentice_exp = int(expected_apprentice_exp)
        mentor_used = int(mentor_used)
        apprentice_used = int(apprentice_used)
        daily_limit = int(daily_limit)
        history_limit = int(history_limit)
        reward_exp = _sql_num_nonneg(reward_exp)
        power = _sql_num_nonneg(power)
        hp = _sql_num_nonneg(hp)
        mp = _sql_num_nonneg(mp)
        atk = _sql_num_nonneg(atk)
        values = (
            expected_apprentice_exp, reward_exp, power, hp, mp, atk,
            mentor_used, apprentice_used, daily_limit, history_limit,
        )
        if not operation_id or float(values[1]) <= 0 or values[6] >= values[8] or values[7] >= values[8]:
            raise ValueError("invalid mentor transmission operation")
        payload = json.dumps([mentor_id, apprentice_id, *values, mentor_desc, apprentice_desc], separators=(",", ":"), ensure_ascii=False)
        result = lambda status: MentorTransmissionResult(status, _as_int_like_num(values[1]))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True; conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS mentor_transmission_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reward_exp INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload,reward_exp FROM mentor_transmission_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return MentorTransmissionResult("duplicate" if str(previous[0]) == payload else "operation_conflict", _as_int_like_num(previous[1]))
                apprentices = [str(v) for v in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                apprentice_row = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)).fetchone()
                if apprentice_id not in apprentices or apprentice_row is None or str(apprentice_row[0]) != mentor_id:
                    conn.rollback(); return result("state_changed")
                changed = conn.execute(
                    "UPDATE user_xiuxian SET "
                    "exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL),"
                    "power=%s,hp=%s,mp=%s,atk=%s "
                    "WHERE user_id=%s AND CAST(COALESCE(exp,0) AS REAL)=CAST(%s AS REAL)",
                    (values[1], values[2], values[3], values[4], values[5], apprentice_id, values[0]),
                )
                if changed.rowcount != 1:
                    conn.rollback(); return result("state_changed")
                increment_stat(conn, mentor_id, "师徒传功次数", 1)
                increment_stat(conn, apprentice_id, "接受传功次数", 1)
                increment_stat(conn, apprentice_id, "传功获得修为", values[1])
                append_mentor_history(conn, mentor_id, "transmission", apprentice_id, mentor_desc, values[9])
                append_mentor_history(conn, apprentice_id, "transmission", mentor_id, apprentice_desc, values[9])
                conn.execute("INSERT INTO mentor_transmission_operations (operation_id,payload,reward_exp) VALUES (%s,%s,%s)", (operation_id, payload, values[1]))
                conn.commit(); return result("applied")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass

@dataclass(frozen=True)
class BlessedSpotResult:
    status: str
    user_id: str
    stone_cost: int = 0
    name: str = ""
    previous_level: int = 0
    current_level: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class BlessedSpotService:
    MIX_DEFAULTS = {
        "收取时间": "",
        "收取等级": 0,
        "灵田数量": 1,
        "药材速度": 0,
        "灵田傀儡": 0,
        "丹药控火": 0,
        "丹药耐药性": 0,
        "炼丹记录": {},
        "炼丹经验": 0,
    }

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operation_table(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blessed_spot_operations ("
            "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @classmethod
    def _ensure_mix_table(cls, conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.mix_elixir_info (user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(mix_elixir_info)").fetchall()
        }
        for field in cls.MIX_DEFAULTS:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE player_data.mix_elixir_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )

    @staticmethod
    def _payload(values) -> str:
        return json.dumps(values, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    def open(self, operation_id, user_id, stone_cost, default_name, harvest_time) -> BlessedSpotResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        stone_cost = int(stone_cost)
        default_name = str(default_name)
        harvest_time = str(harvest_time)
        if not operation_id or stone_cost <= 0 or not default_name or not harvest_time:
            raise ValueError("valid operation, cost, name and harvest time are required")
        # Request identity only — harvest_time is an outcome of first apply, not the request key.
        payload = self._payload([user_id, stone_cost, default_name])

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operation_table(conn)
                self._ensure_mix_table(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM blessed_spot_operations WHERE operation_id=%s AND action=%s",
                    (operation_id, "open"),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return BlessedSpotResult("state_changed", user_id)
                    saved = json.loads(str(previous[1]))
                    return BlessedSpotResult("duplicate", user_id, saved["stone_cost"], saved["name"])

                user = conn.execute(
                    "SELECT COALESCE(stone,0),COALESCE(blessed_spot_flag,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return BlessedSpotResult("user_missing", user_id)
                if int(user[1]) != 0:
                    conn.rollback()
                    return BlessedSpotResult("already_owned", user_id)
                if int(user[0]) < stone_cost:
                    conn.rollback()
                    return BlessedSpotResult("stone_insufficient", user_id, stone_cost)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s,blessed_spot_flag=1,blessed_spot_name=%s "
                    "WHERE user_id=%s AND stone>=%s AND COALESCE(blessed_spot_flag,0)=0",
                    (stone_cost, default_name, user_id, stone_cost),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return BlessedSpotResult("state_changed", user_id)

                values = dict(self.MIX_DEFAULTS)
                values["收取时间"] = harvest_time
                columns = ["user_id", *values]
                params = [user_id]
                for value in values.values():
                    params.append(json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value))
                quoted = ",".join(db_backend.quote_ident(column) for column in columns)
                placeholders = ",".join("%s" for _ in columns)
                updates = ",".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}" for field in values
                )
                conn.execute(
                    f"INSERT INTO player_data.mix_elixir_info ({quoted}) VALUES ({placeholders}) "
                    f"ON CONFLICT(user_id) DO UPDATE SET {updates}",
                    tuple(params),
                )
                result_json = json.dumps({"stone_cost": stone_cost, "name": default_name}, ensure_ascii=False)
                conn.execute(
                    "INSERT INTO blessed_spot_operations (operation_id,action,payload,result_json) VALUES (%s,%s,%s,%s)",
                    (operation_id, "open", payload, result_json),
                )
                conn.commit()
                return BlessedSpotResult("applied", user_id, stone_cost, default_name)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

    def upgrade_field(self, operation_id, user_id, expected_level, stone_cost, max_level=10) -> BlessedSpotResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, stone_cost, max_level = int(expected_level), int(stone_cost), int(max_level)
        if not operation_id or expected_level <= 0 or stone_cost <= 0 or max_level <= 1:
            raise ValueError("valid operation, level and cost are required")
        payload = self._payload([user_id, expected_level, stone_cost, max_level])
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operation_table(conn)
                self._ensure_mix_table(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM blessed_spot_operations WHERE operation_id=%s AND action=%s",
                    (operation_id, "upgrade"),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return BlessedSpotResult("state_changed", user_id)
                    saved = json.loads(str(previous[1]))
                    return BlessedSpotResult("duplicate", user_id, saved["stone_cost"], previous_level=saved["previous_level"], current_level=saved["current_level"])
                user = conn.execute("SELECT COALESCE(stone,0),COALESCE(blessed_spot_flag,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                level = conn.execute(f"SELECT {db_backend.quote_ident('灵田数量')} FROM player_data.mix_elixir_info WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or level is None or int(user[1]) == 0:
                    conn.rollback()
                    return BlessedSpotResult("blessed_spot_missing", user_id)
                current = int(level[0] or 0)
                if current != expected_level:
                    conn.rollback()
                    return BlessedSpotResult("state_changed", user_id, previous_level=current, current_level=current)
                if current >= max_level:
                    conn.rollback()
                    return BlessedSpotResult("max_level", user_id, previous_level=current, current_level=current)
                if int(user[0]) < stone_cost:
                    conn.rollback()
                    return BlessedSpotResult("stone_insufficient", user_id, stone_cost, previous_level=current, current_level=current)
                conn.execute("UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s", (stone_cost, user_id, stone_cost))
                changed = conn.execute(f"UPDATE player_data.mix_elixir_info SET {db_backend.quote_ident('灵田数量')}=%s WHERE user_id=%s AND CAST({db_backend.quote_ident('灵田数量')} AS INTEGER)=%s", (str(current + 1), user_id, current))
                if changed.rowcount != 1:
                    conn.rollback()
                    return BlessedSpotResult("state_changed", user_id)
                saved = {"stone_cost": stone_cost, "previous_level": current, "current_level": current + 1}
                conn.execute("INSERT INTO blessed_spot_operations (operation_id,action,payload,result_json) VALUES (%s,%s,%s,%s)", (operation_id, "upgrade", payload, json.dumps(saved)))
                conn.commit()
                return BlessedSpotResult("applied", user_id, stone_cost, previous_level=current, current_level=current + 1)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

    def rename(self, operation_id, user_id, expected_name, new_name) -> BlessedSpotResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_name, new_name = str(expected_name), str(new_name).strip()
        if not operation_id or not new_name or len(new_name) > 9:
            raise ValueError("valid operation and name up to 9 characters are required")
        # Request identity only — expected_name is a concurrency check.
        payload = self._payload([user_id, new_name])
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operation_table(conn)
                previous = conn.execute("SELECT payload,result_json FROM blessed_spot_operations WHERE operation_id=%s AND action=%s", (operation_id, "rename")).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return BlessedSpotResult("state_changed", user_id)
                    return BlessedSpotResult("duplicate", user_id, name=json.loads(str(previous[1]))["name"])
                row = conn.execute("SELECT COALESCE(blessed_spot_flag,0),COALESCE(blessed_spot_name,'') FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if row is None or int(row[0]) == 0:
                    conn.rollback()
                    return BlessedSpotResult("blessed_spot_missing", user_id)
                if str(row[1]) != expected_name:
                    conn.rollback()
                    return BlessedSpotResult("state_changed", user_id)
                changed = conn.execute("UPDATE user_xiuxian SET blessed_spot_name=%s WHERE user_id=%s AND COALESCE(blessed_spot_name,'')=%s AND COALESCE(blessed_spot_flag,0)<>0", (new_name, user_id, expected_name))
                if changed.rowcount != 1:
                    conn.rollback()
                    return BlessedSpotResult("state_changed", user_id)
                conn.execute("INSERT INTO blessed_spot_operations (operation_id,action,payload,result_json) VALUES (%s,%s,%s,%s)", (operation_id, "rename", payload, json.dumps({"name": new_name}, ensure_ascii=False)))
                conn.commit()
                return BlessedSpotResult("applied", user_id, name=new_name)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class ClosingSettlementResult:
    status: str
    exp_gain: int = 0
    stone_cost: int = 0
    hp: int = 0
    mp: int = 0
    atk: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ClosingSettlementService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def settle(self, operation_id, user_id, expected_create_time, exp_gain, stone_cost, hp, mp, atk, power) -> ClosingSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_create_time = str(expected_create_time)
        # stone_cost must stay integer (spend compare); combat stats may exceed INTEGER
        stone_cost = int(stone_cost)
        exp_gain = number_count(exp_gain)
        hp = number_count(hp)
        mp = number_count(mp)
        atk = number_count(atk)
        power = number_count(power)
        # identity payload uses string forms so scientific values are stable
        values = (exp_gain, stone_cost, hp, mp, atk, power)
        if not operation_id:
            raise ValueError("valid operation and non-negative settlement values are required")
        for value in (stone_cost,):
            if int(value) < 0:
                raise ValueError("valid operation and non-negative settlement values are required")
        for value in (exp_gain, hp, mp, atk, power):
            try:
                if float(value) < 0:
                    raise ValueError("valid operation and non-negative settlement values are required")
            except (TypeError, ValueError) as exc:
                raise ValueError("valid operation and non-negative settlement values are required") from exc
        payload = json.dumps(
            [user_id, expected_create_time, str(exp_gain), stone_cost, str(hp), str(mp), str(atk), str(power)],
            separators=(",", ":"),
        )
        # result_json keeps numeric-friendly values for duplicate replay
        saved_nums = [
            int(float(exp_gain)) if not isinstance(exp_gain, str) else exp_gain,
            stone_cost,
            int(float(hp)) if not isinstance(hp, str) else hp,
            int(float(mp)) if not isinstance(mp, str) else mp,
            int(float(atk)) if not isinstance(atk, str) else atk,
        ]
        result = ClosingSettlementResult("", *[_as_int_like(v) for v in saved_nums])
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS closing_settlement_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload,result_json FROM closing_settlement_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ClosingSettlementResult("state_changed")
                    saved = json.loads(str(previous[1]))
                    return ClosingSettlementResult("duplicate", *[_as_int_like(v) for v in saved])
                user = conn.execute("SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                cd = conn.execute("SELECT type,create_time FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or cd is None:
                    conn.rollback()
                    return ClosingSettlementResult("user_missing")
                if int(cd[0] or 0) != 1 or str(cd[1]) != expected_create_time:
                    conn.rollback()
                    return ClosingSettlementResult("state_changed")
                if int(user[0]) < stone_cost:
                    conn.rollback()
                    return ClosingSettlementResult("stone_insufficient")
                # exp may already be TEXT scientific; use CAST for arithmetic
                changed = conn.execute(
                    "UPDATE user_xiuxian SET "
                    "exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL),"
                    "stone=stone-%s,"
                    "hp=%s,mp=%s,atk=%s,power=%s "
                    "WHERE user_id=%s AND stone>=%s",
                    (exp_gain, stone_cost, hp, mp, atk, power, user_id, stone_cost),
                )
                cleared = conn.execute("UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s AND type=1 AND CAST(create_time AS TEXT)=%s", (user_id, expected_create_time))
                if changed.rowcount != 1 or cleared.rowcount != 1:
                    conn.rollback()
                    return ClosingSettlementResult("state_changed")
                conn.execute(
                    "INSERT INTO closing_settlement_operations (operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved_nums, ensure_ascii=True)),
                )
                conn.commit()
                return ClosingSettlementResult("applied", *[_as_int_like(v) for v in saved_nums])
            except Exception:
                conn.rollback()
                raise

    def get_result(self, operation_id: str):
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS closing_settlement_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            row = conn.execute(
                "SELECT result_json FROM closing_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            saved = json.loads(str(row[0]))
            return ClosingSettlementResult("duplicate", *[_as_int_like(v) for v in saved])


def _as_int_like(value):
    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return 0
@dataclass(frozen=True)
class NormalTrainingResult:
    status: str
    kind: str = ""
    create_time: str = ""
    exp_gain: int = 0
    stone_gain: int = 0
    hp_gain: int = 0
    mp_gain: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"started", "applied", "duplicate"}

class NormalTrainingLifecycleService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(values) -> str:
        return json.dumps(values, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _ensure_tables(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS normal_training_operations ("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
            "kind TEXT NOT NULL,create_time TEXT NOT NULL,scheduled_time TEXT NOT NULL,"
            "status TEXT NOT NULL,result_json TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def start(
        self,
        operation_id,
        user_id,
        *,
        kind,
        expected_exp,
        expected_stone,
        reward,
        exp_cap,
        power_multiplier,
        duration_seconds=60,
        now: datetime | None = None,
    ) -> NormalTrainingResult:
        operation_id, user_id, kind = str(operation_id).strip(), str(user_id), str(kind).strip()
        expected_exp, expected_stone, reward, exp_cap = (
            int(expected_exp), int(expected_stone), int(reward), int(exp_cap)
        )
        duration_seconds = int(duration_seconds)
        power_multiplier = float(power_multiplier)
        if not operation_id or kind not in {"cultivation", "mining"} or min(
            expected_exp, expected_stone, reward, exp_cap, duration_seconds
        ) < 0 or power_multiplier < 0:
            raise ValueError("invalid normal training lifecycle arguments")
        payload = self._payload([
            user_id, kind, expected_exp, expected_stone, reward, exp_cap,
            power_multiplier, duration_seconds,
        ])
        current = now or datetime.now()
        create_time = current.strftime("%Y-%m-%d %H:%M:%S.%f")
        scheduled_time = (current + timedelta(seconds=duration_seconds)).strftime("%Y-%m-%d %H:%M:%S.%f")

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_tables(conn)
                previous = conn.execute(
                    "SELECT payload,kind,create_time,status,result_json FROM normal_training_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return NormalTrainingResult("operation_conflict")
                    saved = json.loads(str(previous[4])) if previous[4] else {}
                    return NormalTrainingResult(
                        "duplicate" if str(previous[3]) == "applied" else "started",
                        str(previous[1]), str(previous[2]), int(saved.get("exp_gain", 0)),
                        int(saved.get("stone_gain", 0)), int(saved.get("hp_gain", 0)),
                        int(saved.get("mp_gain", 0)),
                    )
                user = conn.execute(
                    "SELECT COALESCE(exp,0),COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                cd = conn.execute("SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or cd is None:
                    conn.rollback()
                    return NormalTrainingResult("user_missing")
                if int(cd[0]) != 0:
                    conn.rollback()
                    return NormalTrainingResult("state_changed")
                if (int(user[0]), int(user[1])) != (expected_exp, expected_stone):
                    conn.rollback()
                    return NormalTrainingResult("state_changed")
                changed = conn.execute(
                    "UPDATE user_cd SET type=5,create_time=%s,scheduled_time=%s WHERE user_id=%s AND COALESCE(type,0)=0",
                    (create_time, scheduled_time, user_id),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return NormalTrainingResult("state_changed")
                conn.execute(
                    "INSERT INTO normal_training_operations "
                    "(operation_id,user_id,payload,kind,create_time,scheduled_time,status) VALUES (%s,%s,%s,%s,%s,%s,'pending')",
                    (operation_id, user_id, payload, kind, create_time, scheduled_time),
                )
                conn.commit()
                return NormalTrainingResult("started", kind, create_time)
            except Exception:
                conn.rollback()
                raise

    def complete(self, operation_id, *, task_period: str) -> NormalTrainingResult:
        operation_id = str(operation_id).strip()
        task_period = str(task_period).strip()
        if not operation_id or not task_period:
            raise ValueError("operation and task period are required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_tables(conn)
                row = conn.execute(
                    "SELECT user_id,payload,kind,create_time,status,result_json FROM normal_training_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return NormalTrainingResult("operation_missing")
                user_id, payload, kind, create_time, status = map(str, row[:5])
                if status == "applied":
                    conn.rollback()
                    saved = json.loads(str(row[5]))
                    return NormalTrainingResult("duplicate", kind, create_time, **saved)
                values = json.loads(payload)
                expected_exp, expected_stone, reward, exp_cap = map(int, values[2:6])
                power_multiplier = float(values[6])
                user = conn.execute(
                    "SELECT COALESCE(exp,0),COALESCE(stone,0),COALESCE(hp,0),COALESCE(mp,0) "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                cd = conn.execute("SELECT type,create_time FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or cd is None:
                    conn.rollback()
                    return NormalTrainingResult("user_missing")
                if (int(user[0]), int(user[1])) != (expected_exp, expected_stone) or int(cd[0] or 0) != 5 or str(cd[1]) != create_time:
                    conn.rollback()
                    return NormalTrainingResult("state_changed", kind, create_time)

                exp_gain = min(reward, max(0, exp_cap - expected_exp)) if kind == "cultivation" else 0
                stone_gain = reward if kind == "mining" else 0
                hp_gain = mp_gain = 0
                if kind == "cultivation":
                    new_exp = expected_exp + exp_gain
                    old_hp, old_mp = int(user[2]), int(user[3])
                    new_hp = min(new_exp // 2, old_hp + expected_exp // 10)
                    new_mp = min(new_exp, old_mp + expected_exp // 20)
                    hp_gain, mp_gain = max(0, new_hp - old_hp), max(0, new_mp - old_mp)
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET exp=%s,hp=%s,mp=%s,atk=%s,power=ROUND(%s*%s,0) "
                        "WHERE user_id=%s AND COALESCE(exp,0)=%s AND COALESCE(stone,0)=%s",
                        (new_exp, new_hp, new_mp, expected_exp // 10, new_exp, power_multiplier,
                         user_id, expected_exp, expected_stone),
                    )
                    increment_stat(conn, user_id, "修炼次数", 1)
                    increment_stat(conn, user_id, "修炼修为", exp_gain)
                    self._increment_training_task(conn, user_id, task_period)
                else:
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s AND COALESCE(exp,0)=%s AND COALESCE(stone,0)=%s",
                        (stone_gain, user_id, expected_exp, expected_stone),
                    )
                    increment_stat(conn, user_id, "凡人挖矿次数", 1)
                    increment_stat(conn, user_id, "灵石获取", stone_gain)
                cleared = conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s AND type=5 AND CAST(create_time AS TEXT)=%s",
                    (user_id, create_time),
                )
                if changed.rowcount != 1 or cleared.rowcount != 1:
                    conn.rollback()
                    return NormalTrainingResult("state_changed", kind, create_time)
                saved = {"exp_gain": exp_gain, "stone_gain": stone_gain, "hp_gain": hp_gain, "mp_gain": mp_gain}
                conn.execute(
                    "UPDATE normal_training_operations SET status='applied',result_json=%s WHERE operation_id=%s AND status='pending'",
                    (json.dumps(saved, separators=(",", ":")), operation_id),
                )
                conn.commit()
                return NormalTrainingResult("applied", kind, create_time, **saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    @staticmethod
    def _increment_training_task(conn, user_id: str, task_period: str) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.xiuxian_tasks (user_id TEXT PRIMARY KEY)")
        columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(xiuxian_tasks)").fetchall()}
        for field in ("weekly_period", "weekly_progress"):
            if field not in columns:
                conn.execute(f"ALTER TABLE player_data.xiuxian_tasks ADD COLUMN {field} TEXT")
        row = conn.execute(
            "SELECT weekly_period,weekly_progress FROM player_data.xiuxian_tasks WHERE user_id=%s", (user_id,)
        ).fetchone()
        progress = {}
        if row is not None and str(row[0] or "") == task_period:
            try:
                progress = json.loads(str(row[1] or "{}"))
            except (TypeError, ValueError):
                progress = {}
        progress["weekly_out_closing"] = min(7200, int(progress.get("weekly_out_closing", 0) or 0) + 1)
        encoded = json.dumps(progress, ensure_ascii=False, separators=(",", ":"))
        changed = conn.execute(
            "UPDATE player_data.xiuxian_tasks SET weekly_period=%s,weekly_progress=%s WHERE user_id=%s",
            (task_period, encoded, user_id),
        )
        if changed.rowcount == 0:
            conn.execute(
                "INSERT INTO player_data.xiuxian_tasks (user_id,weekly_period,weekly_progress) VALUES (%s,%s,%s)",
                (user_id, task_period, encoded),
            )

@dataclass(frozen=True)
class NormalPvpResult:
    status: str
    winner_id: str = ""
    winner_name: str = "没有人"
    battle_messages: list = field(default_factory=list)
    challenger_hp: int = 0
    challenger_mp: int = 0
    opponent_hp: int = 0
    opponent_mp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class NormalPvpSettlementService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(challenger_id, opponent_id, stamina_cost) -> str:
        # Request identity only — HP/MP/exp snapshots and battle results are outcomes.
        return json.dumps(
            {
                "challenger_id": str(challenger_id),
                "opponent_id": str(opponent_id),
                "stamina_cost": int(stamina_cost),
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )

    @staticmethod
    def _saved_result(row, status="duplicate") -> NormalPvpResult:
        saved = json.loads(str(row))
        return NormalPvpResult(status=status, **saved)

    def replay(self, operation_id, challenger_id, opponent_id) -> NormalPvpResult | None:
        with closing(db_backend.connect(self._game_database)) as conn:
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='normal_pvp_operations'"
            ).fetchone()
            if exists is None:
                return None
            row = conn.execute(
                "SELECT payload,result_json FROM normal_pvp_operations WHERE operation_id=%s",
                (str(operation_id),),
            ).fetchone()
            if row is None:
                return None
            payload = json.loads(str(row[0]))
            if isinstance(payload, dict):
                participants = [payload.get("challenger_id"), payload.get("opponent_id")]
            else:
                # Keep previously persisted operations replayable during rollout.
                participants = payload[:2]
            if participants != [str(challenger_id), str(opponent_id)]:
                return NormalPvpResult("operation_conflict")
            return self._saved_result(row[1])

    @staticmethod
    def calculate_battle(challenger_id, opponent_id, bot_id=0):
        players = [get_players_attributes(challenger_id), get_players_attributes(opponent_id)]
        entities = []
        for team_id, player in enumerate(players):
            attributes = player["属性"]
            attributes["natal_data"] = player.get("本命法宝")
            entity = Entity(attributes, team_id=team_id)
            apply_player_buffs(entity, player)
            entities.append(entity)
        messages, winner, statuses = BattleSystem([entities[0]], [entities[1]], bot_id).run_battle()
        final = {}
        for item in statuses:
            for attributes in item.values():
                hp_multiplier = float(attributes.get("hp_multiplier", 1) or 1)
                mp_multiplier = float(attributes.get("mp_multiplier", 1) or 1)
                final[str(attributes["user_id"])] = (
                    max(1, int(float(attributes.get("hp", 1)) / hp_multiplier)),
                    max(1, int(float(attributes.get("mp", 1)) / mp_multiplier)),
                )
        winner_id = "" if winner == 2 else str((challenger_id, opponent_id)[winner])
        winner_name = "没有人" if winner == 2 else str(players[winner]["属性"]["nickname"])
        return messages, winner_id, winner_name, final

    def settle(
        self,
        operation_id,
        challenger_id,
        opponent_id,
        *,
        expected_challenger_hp,
        expected_challenger_mp,
        expected_challenger_stamina,
        expected_challenger_exp,
        expected_opponent_hp,
        expected_opponent_mp,
        expected_opponent_stamina,
        expected_opponent_exp,
        challenger_final_hp,
        challenger_final_mp,
        opponent_final_hp,
        opponent_final_mp,
        winner_id="",
        winner_name="没有人",
        battle_messages=None,
        stamina_cost=1,
    ) -> NormalPvpResult:
        operation_id = str(operation_id).strip()
        challenger_id, opponent_id = str(challenger_id), str(opponent_id)
        snapshots = (
            expected_challenger_hp,
            expected_challenger_mp,
            expected_challenger_stamina,
            expected_challenger_exp,
            expected_opponent_hp,
            expected_opponent_mp,
            expected_opponent_stamina,
            expected_opponent_exp,
        )
        snapshots = tuple(int(value) for value in snapshots)
        finals = tuple(
            max(1, int(value))
            for value in (challenger_final_hp, challenger_final_mp, opponent_final_hp, opponent_final_mp)
        )
        stamina_cost = int(stamina_cost)
        winner_id = str(winner_id or "")
        battle_messages = list(battle_messages or [])
        if not operation_id or challenger_id == opponent_id or stamina_cost < 0:
            raise ValueError("invalid normal pvp settlement arguments")
        if winner_id not in {"", challenger_id, opponent_id}:
            raise ValueError("invalid normal pvp winner")
        payload = self._payload(
            challenger_id,
            opponent_id,
            stamina_cost,
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS normal_pvp_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM normal_pvp_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return NormalPvpResult("operation_conflict")
                    return self._saved_result(previous[1])

                rows = conn.execute(
                    "SELECT user_id,COALESCE(hp,0),COALESCE(mp,0),COALESCE(user_stamina,0),COALESCE(exp,0) "
                    "FROM user_xiuxian WHERE user_id IN (%s,%s)",
                    (challenger_id, opponent_id),
                ).fetchall()
                current = {str(row[0]): tuple(int(value) for value in row[1:]) for row in rows}
                if challenger_id not in current or opponent_id not in current:
                    conn.rollback()
                    return NormalPvpResult("user_missing")
                challenger_snapshot, opponent_snapshot = snapshots[:4], snapshots[4:]
                if current[challenger_id] != challenger_snapshot or current[opponent_id] != opponent_snapshot:
                    conn.rollback()
                    return NormalPvpResult("state_changed")
                if snapshots[0] <= snapshots[3] / 10:
                    conn.rollback()
                    return NormalPvpResult("challenger_injured")
                if snapshots[2] < stamina_cost:
                    conn.rollback()
                    return NormalPvpResult("stamina_insufficient")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s,user_stamina=user_stamina-%s "
                    "WHERE user_id=%s AND hp=%s AND mp=%s AND user_stamina=%s AND exp=%s AND user_stamina>=%s",
                    (finals[0], finals[1], stamina_cost, challenger_id, *challenger_snapshot, stamina_cost),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return NormalPvpResult("state_changed")
                changed = conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s WHERE user_id=%s AND hp=%s AND mp=%s AND user_stamina=%s AND exp=%s",
                    (finals[2], finals[3], opponent_id, *opponent_snapshot),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return NormalPvpResult("state_changed")

                if winner_id:
                    loser_id = opponent_id if winner_id == challenger_id else challenger_id
                    increment_stat(conn, winner_id, "切磋胜利", 1)
                    increment_stat(conn, loser_id, "切磋失败", 1)
                saved = {
                    "winner_id": winner_id,
                    "winner_name": str(winner_name),
                    "battle_messages": battle_messages,
                    "challenger_hp": finals[0],
                    "challenger_mp": finals[1],
                    "opponent_hp": finals[2],
                    "opponent_mp": finals[3],
                }
                conn.execute(
                    "INSERT INTO normal_pvp_operations (operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, ensure_ascii=False, separators=(",", ":"))),
                )
                conn.commit()
                return NormalPvpResult("applied", **saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

@dataclass(frozen=True)
class StoneTrainingResult:
    status: str
    exp_gain: int = 0
    stone_cost: int = 0
    power: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class StoneTrainingSettlementService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, user_id, *, requested_stone, expected_exp, expected_stone, exp_cap, power_multiplier) -> StoneTrainingResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        requested_stone, expected_exp, expected_stone, exp_cap = map(
            int, (requested_stone, expected_exp, expected_stone, exp_cap)
        )
        power_multiplier = float(power_multiplier)
        if not operation_id or requested_stone <= 0 or min(expected_exp, expected_stone, exp_cap) < 0 or power_multiplier < 0:
            raise ValueError("invalid stone training settlement arguments")
        possible_exp = requested_stone // 10
        exp_gain = min(possible_exp, max(0, exp_cap - expected_exp))
        stone_cost = exp_gain * 10 if possible_exp >= max(0, exp_cap - expected_exp) else requested_stone
        # Request identity only — exp/stone snapshots are concurrency checks.
        payload = json.dumps(
            [user_id, requested_stone, exp_cap, power_multiplier],
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS stone_training_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM stone_training_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return StoneTrainingResult("operation_conflict")
                    saved = json.loads(str(previous[1]))
                    return StoneTrainingResult("duplicate", **saved)
                user = conn.execute(
                    "SELECT COALESCE(exp,0),COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return StoneTrainingResult("user_missing")
                if (int(user[0]), int(user[1])) != (expected_exp, expected_stone):
                    conn.rollback()
                    return StoneTrainingResult("state_changed")
                if exp_gain <= 0:
                    conn.rollback()
                    return StoneTrainingResult("exp_capped")
                if expected_stone < stone_cost:
                    conn.rollback()
                    return StoneTrainingResult("stone_insufficient", exp_gain, stone_cost)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET exp=exp+%s,stone=stone-%s,power=ROUND((exp+%s)*%s,0) "
                    "WHERE user_id=%s AND exp=%s AND stone=%s AND stone>=%s",
                    (exp_gain, stone_cost, exp_gain, power_multiplier, user_id, expected_exp, expected_stone, stone_cost),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return StoneTrainingResult("state_changed")
                increment_stat(conn, user_id, "灵石修炼", stone_cost)
                increment_stat(conn, user_id, "灵石修炼修为", exp_gain)
                power = int(conn.execute("SELECT power FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()[0])
                saved = {"exp_gain": exp_gain, "stone_cost": stone_cost, "power": power}
                conn.execute(
                    "INSERT INTO stone_training_operations (operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return StoneTrainingResult("applied", **saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

__all__ = [
    "PartnerProtectionResult",
    "PartnerProtectionService",
    "PartnerInvite",
    "PartnerInviteResult",
    "PartnerInviteService",
    "PartnerCultivationResult",
    "PartnerCultivationService",
    "PartnerBindResult",
    "PartnerBindService",
    "PartnerUnbindResult",
    "PartnerUnbindService",
    "PartnerTokenUseResult",
    "PartnerTokenUseService",
    "PartnerBreakthroughResult",
    "PartnerBreakthroughService",
    "MentorBindResult",
    "MentorBindService",
    "MentorApplication",
    "MentorApplicationResult",
    "MentorProtectionResult",
    "MentorApplicationService",
    "MentorExpelResult",
    "MentorExpelService",
    "MentorBreakthroughRewardResult",
    "MentorBreakthroughRewardService",
    "ApprenticeLeaveResult",
    "ApprenticeLeaveService",
    "MentorGraduationResult",
    "MentorGraduationService",
    "MentorTransmissionResult",
    "MentorTransmissionService",
    "BlessedSpotResult",
    "BlessedSpotService",
    "ClosingSettlementResult",
    "ClosingSettlementService",
    "NormalTrainingResult",
    "NormalTrainingLifecycleService",
    "NormalPvpResult",
    "NormalPvpSettlementService",
    "StoneTrainingResult",
    "StoneTrainingSettlementService",
]
