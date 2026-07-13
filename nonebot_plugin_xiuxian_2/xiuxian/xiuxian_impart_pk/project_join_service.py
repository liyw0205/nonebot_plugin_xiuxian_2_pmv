from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


_LOCKS_GUARD = RLock()
_DATABASE_LOCKS = {}


def _database_lock(path):
    key = Path(path).expanduser().resolve()
    with _LOCKS_GUARD:
        return _DATABASE_LOCKS.setdefault(key, RLock())


@dataclass(frozen=True)
class ImpartProjectJoinResult:
    status: str
    pk_num: int
    member_count: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ImpartProjectJoinService:
    """Keep virtual-world projection membership in one authoritative transaction."""

    def __init__(self, player_database, capacity=40, lock=None):
        self._player_database = Path(player_database)
        self._capacity = int(capacity)
        self._lock = lock or _database_lock(self._player_database)

    @staticmethod
    def _ensure_schema(conn):
        conn.execute(
            "CREATE TABLE IF NOT EXISTS impart_pk_state("
            "user_id TEXT PRIMARY KEY,pk_num INTEGER NOT NULL DEFAULT 7,"
            "win_num INTEGER NOT NULL DEFAULT 0)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS impart_project_members("
            "user_id TEXT PRIMARY KEY,joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS impart_project_meta("
            "meta_key TEXT PRIMARY KEY,meta_value TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS impart_project_join_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _import_legacy_members(conn, legacy_members):
        migrated = conn.execute(
            "SELECT 1 FROM impart_project_meta WHERE meta_key='legacy_members_imported'"
        ).fetchone()
        if migrated is not None:
            return
        members = sorted({str(user_id) for user_id in (legacy_members or ()) if str(user_id)})
        conn.executemany(
            "INSERT OR IGNORE INTO impart_project_members(user_id) VALUES(%s)",
            ((user_id,) for user_id in members),
        )
        conn.execute(
            "INSERT INTO impart_project_meta(meta_key,meta_value) VALUES('legacy_members_imported',%s)",
            (str(len(members)),),
        )

    @staticmethod
    def _increment_projection_stat(conn, user_id):
        conn.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
        columns = {str(row[1]) for row in conn.execute('PRAGMA table_info("statistics")').fetchall()}
        if "虚神界投影次数" not in columns:
            conn.execute('ALTER TABLE statistics ADD COLUMN "虚神界投影次数" INTEGER')
        changed = conn.execute(
            'UPDATE statistics SET "虚神界投影次数"=COALESCE("虚神界投影次数",0)+1 '
            "WHERE user_id=%s",
            (user_id,),
        )
        if changed.rowcount == 0:
            conn.execute(
                'INSERT INTO statistics(user_id,"虚神界投影次数") VALUES(%s,1)',
                (user_id,),
            )

    def join(self, operation_id, user_id, *, legacy_pk_num=7, legacy_members=None):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        legacy_pk_num = int(legacy_pk_num)
        if not operation_id or not user_id or legacy_pk_num < 0 or self._capacity <= 0:
            raise ValueError("invalid impart project join")
        payload = json.dumps([user_id, legacy_pk_num], ensure_ascii=True, separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._import_legacy_members(conn, legacy_members)
                old = conn.execute(
                    "SELECT payload,result_json FROM impart_project_join_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return ImpartProjectJoinResult("operation_conflict", 0, 0)
                    saved = json.loads(str(old[1]))
                    return ImpartProjectJoinResult("duplicate", int(saved[0]), int(saved[1]))

                state = conn.execute(
                    "SELECT pk_num FROM impart_pk_state WHERE user_id=%s", (user_id,)
                ).fetchone()
                if state is None:
                    conn.execute(
                        "INSERT INTO impart_pk_state(user_id,pk_num,win_num) VALUES(%s,%s,0)",
                        (user_id, legacy_pk_num),
                    )
                    pk_num = legacy_pk_num
                else:
                    pk_num = int(state[0] or 0)

                member_count = int(conn.execute("SELECT COUNT(*) FROM impart_project_members").fetchone()[0])
                if conn.execute(
                    "SELECT 1 FROM impart_project_members WHERE user_id=%s", (user_id,)
                ).fetchone() is not None:
                    conn.commit()
                    return ImpartProjectJoinResult("already_joined", pk_num, member_count)
                if pk_num <= 0:
                    conn.commit()
                    return ImpartProjectJoinResult("pk_exhausted", pk_num, member_count)
                if member_count >= self._capacity:
                    conn.commit()
                    return ImpartProjectJoinResult("capacity_full", pk_num, member_count)

                conn.execute("INSERT INTO impart_project_members(user_id) VALUES(%s)", (user_id,))
                self._increment_projection_stat(conn, user_id)
                member_count += 1
                saved = [pk_num, member_count]
                conn.execute(
                    "INSERT INTO impart_project_join_operations(operation_id,payload,result_json) "
                    "VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartProjectJoinResult("applied", pk_num, member_count)
            except Exception:
                conn.rollback()
                raise

    def members(self, legacy_members=None):
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._import_legacy_members(conn, legacy_members)
                rows = conn.execute(
                    "SELECT user_id FROM impart_project_members ORDER BY joined_at,user_id"
                ).fetchall()
                conn.commit()
                return [str(row[0]) for row in rows]
            except Exception:
                conn.rollback()
                raise

    def contains(self, user_id, legacy_members=None):
        return str(user_id) in set(self.members(legacy_members))

    def remove(self, user_id):
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            changed = conn.execute(
                "DELETE FROM impart_project_members WHERE user_id=%s", (str(user_id),)
            )
            conn.commit()
            return changed.rowcount == 1

    def reset_daily(self, legacy_members=None):
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._import_legacy_members(conn, legacy_members)
                conn.execute("DELETE FROM impart_project_members")
                conn.execute("DELETE FROM impart_pk_state")
                conn.commit()
            except Exception:
                conn.rollback()
                raise


__all__ = ["ImpartProjectJoinResult", "ImpartProjectJoinService"]
