from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
from .settlement_state import increment_stat, load_daily_state
from ..xiuxian_utils.numeric_bind import as_int_like, number_count

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

    def get_result(self, operation_id: str) -> ImpartProjectJoinResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            old = conn.execute(
                "SELECT payload,result_json FROM impart_project_join_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            saved = json.loads(str(old[1]))
            return ImpartProjectJoinResult("duplicate", int(saved[0]), int(saved[1]))

    def join(self, operation_id, user_id, *, legacy_pk_num=7, legacy_members=None):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        legacy_pk_num = int(legacy_pk_num)
        if not operation_id or not user_id or legacy_pk_num < 0 or self._capacity <= 0:
            raise ValueError("invalid impart project join")
        # Request identity only — legacy_pk_num is init seed, not request key.
        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))
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

@dataclass(frozen=True)
class ImpartClosingEnterResult:
    status: str
    started_at: str = ""
    entry_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

def _increment_entry_stat(conn, user_id: str) -> int:
    conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)")
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA player_data.table_info(statistics)").fetchall()
    }
    key = "虚神界闭关次数"
    if key not in columns:
        conn.execute(
            f"ALTER TABLE player_data.statistics ADD COLUMN {db_backend.quote_ident(key)} "
            "INTEGER DEFAULT 0"
        )
    field = db_backend.quote_ident(key)
    changed = conn.execute(
        f"UPDATE player_data.statistics SET {field}=COALESCE({field},0)+1 WHERE user_id=%s",
        (user_id,),
    )
    if changed.rowcount == 0:
        conn.execute(
            f"INSERT INTO player_data.statistics(user_id,{field}) VALUES(%s,1)",
            (user_id,),
        )
    return int(
        conn.execute(
            f"SELECT {field} FROM player_data.statistics WHERE user_id=%s", (user_id,)
        ).fetchone()[0]
    )

class ImpartClosingEnterService:
    """Atomically enter virtual-world closing from the authoritative idle state."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> ImpartClosingEnterResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_closing_enter_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            old = conn.execute(
                "SELECT payload,result_json FROM impart_closing_enter_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            saved = json.loads(str(old[1]))
            return ImpartClosingEnterResult("duplicate", str(saved[0]), int(saved[1]))

    def enter(self, operation_id, user_id, started_at) -> ImpartClosingEnterResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        started_at = str(started_at).strip()
        if not operation_id or not user_id or not started_at:
            raise ValueError("operation, user and start time are required")
        # Request identity only — started_at is outcome, stored in result_json.
        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_closing_enter_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,result_json FROM impart_closing_enter_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return ImpartClosingEnterResult("operation_conflict")
                    saved = json.loads(str(old[1]))
                    return ImpartClosingEnterResult("duplicate", str(saved[0]), int(saved[1]))

                user = conn.execute(
                    "SELECT root_type FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                cd = conn.execute(
                    "SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None or cd is None:
                    conn.rollback()
                    return ImpartClosingEnterResult("user_missing")
                if str(user[0] or "") == "伪灵根":
                    conn.rollback()
                    return ImpartClosingEnterResult("ineligible")
                if int(cd[0]) != 0:
                    conn.rollback()
                    return ImpartClosingEnterResult("busy")

                changed = conn.execute(
                    "UPDATE user_cd SET type=4,create_time=%s,scheduled_time=NULL "
                    "WHERE user_id=%s AND COALESCE(type,0)=0",
                    (started_at, user_id),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return ImpartClosingEnterResult("state_changed")

                entry_count = _increment_entry_stat(conn, user_id)
                saved = [started_at, entry_count]
                conn.execute(
                    "INSERT INTO impart_closing_enter_operations(operation_id,payload,result_json) "
                    "VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, ensure_ascii=True, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartClosingEnterResult("applied", started_at, entry_count)
            except Exception:
                conn.rollback()
                raise

SQLITE_MAX_INT = 2**63 - 1

@dataclass(frozen=True)
class ImpartClosingSettlementResult:
    status: str
    exp_gain: int = 0
    blessing_cost: int = 0
    exp_day_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

def _increment_stat(conn, user_id: str, key: str, amount: int) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)")
    columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(statistics)").fetchall()}
    if key not in columns:
        conn.execute(
            f"ALTER TABLE player_data.statistics ADD COLUMN {db_backend.quote_ident(key)} INTEGER DEFAULT 0"
        )
    field = db_backend.quote_ident(key)
    changed = conn.execute(
        f"UPDATE player_data.statistics SET {field}=COALESCE({field},0)+%s WHERE user_id=%s",
        (as_int_like(amount), user_id),
    )
    if changed.rowcount == 0:
        conn.execute(
            f"INSERT INTO player_data.statistics(user_id,{field}) VALUES(%s,%s)",
            (user_id, as_int_like(amount)),
        )

class ImpartClosingSettlementService:
    def __init__(self, game_db, impart_db, player_db, lock=None):
        self.game_db = Path(game_db)
        self.impart_db = Path(impart_db)
        self.player_db = Path(player_db)
        self.lock = lock or RLock()

    def get_result(self, operation_id: str) -> ImpartClosingSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_closing_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            old = conn.execute(
                "SELECT payload,result_json FROM impart_closing_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            saved = json.loads(str(old[1]))
            return ImpartClosingSettlementResult("duplicate", *saved)

    def settle(
        self,
        operation_id,
        user_id,
        expected_create_time,
        expected_exp,
        expected_exp_day,
        exp_gain,
        blessing_cost,
        closing_minutes,
        hp,
        mp,
        atk,
        power,
    ) -> ImpartClosingSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_create_time = str(expected_create_time)
        # Snapshots / counters: use as_int_like (no SQLITE_MAX clamp — that caused
        # high-realm users to always hit state_changed on 虚神界出关).
        expected_exp = as_int_like(expected_exp)
        expected_exp_day = as_int_like(expected_exp_day)
        exp_gain = number_count(max(0, as_int_like(exp_gain)))
        blessing_cost = as_int_like(blessing_cost)
        closing_minutes = as_int_like(closing_minutes)
        hp = number_count(max(0, as_int_like(hp)))
        mp = number_count(max(0, as_int_like(mp)))
        atk = number_count(max(0, as_int_like(atk)))
        power = number_count(max(0, as_int_like(power)))
        if (
            not operation_id
            or expected_exp < 0
            or expected_exp_day < 0
            or blessing_cost < 0
            or closing_minutes < 0
            or blessing_cost > expected_exp_day
        ):
            raise ValueError("invalid impart closing settlement")
        # Request identity only — exp/hp rolls live in result_json; create_time identifies the closing session.
        payload = json.dumps(
            [user_id, expected_create_time], ensure_ascii=False, separators=(",", ":")
        )
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self.impart_db),))
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_closing_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,result_json FROM impart_closing_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return ImpartClosingSettlementResult("operation_conflict")
                    saved = json.loads(str(old[1]))
                    return ImpartClosingSettlementResult("duplicate", *saved)

                user = conn.execute(
                    "SELECT COALESCE(exp,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                cd = conn.execute(
                    "SELECT type,create_time FROM user_cd WHERE user_id=%s", (user_id,)
                ).fetchone()
                impart = conn.execute(
                    "SELECT COALESCE(exp_day,0) FROM impart_data.xiuxian_impart WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None or cd is None or impart is None:
                    conn.rollback()
                    return ImpartClosingSettlementResult("user_missing")
                # Session identity is type=4 (+ create_time when real). Blank/garbage create_time
                # must not trap high-realm players already in type=4.
                from ..xiuxian_utils.cd_time import (
                    cd_time_matches,
                    is_blank_cd_time,
                    normalize_cd_time_token,
                )

                expected_create_time = normalize_cd_time_token(expected_create_time)
                actual_exp_day = as_int_like(impart[0])
                if (
                    int(cd[0] or 0) != 4
                    or not cd_time_matches(cd[1], expected_create_time)
                    or actual_exp_day != expected_exp_day
                ):
                    conn.rollback()
                    return ImpartClosingSettlementResult("state_changed")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET "
                    "exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL),"
                    "hp=%s,mp=%s,atk=%s,power=%s "
                    "WHERE user_id=%s",
                    (exp_gain, hp, mp, atk, power, user_id),
                )
                if is_blank_cd_time(cd[1]) or is_blank_cd_time(expected_create_time):
                    cleared = conn.execute(
                        "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL "
                        "WHERE user_id=%s AND type=4",
                        (user_id,),
                    )
                else:
                    cleared = conn.execute(
                        "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL "
                        "WHERE user_id=%s AND type=4 AND CAST(create_time AS TEXT)=%s",
                        (user_id, expected_create_time),
                    )
                blessed = conn.execute(
                    "UPDATE impart_data.xiuxian_impart SET exp_day=exp_day-%s "
                    "WHERE user_id=%s AND exp_day=%s AND exp_day>=%s",
                    (blessing_cost, user_id, expected_exp_day, blessing_cost),
                )
                if changed.rowcount != 1 or cleared.rowcount != 1 or blessed.rowcount != 1:
                    conn.rollback()
                    return ImpartClosingSettlementResult("state_changed")

                _increment_stat(conn, user_id, "虚神界闭关时长", closing_minutes)
                _increment_stat(conn, user_id, "虚神界闭关修为", as_int_like(exp_gain))
                _increment_stat(conn, user_id, "虚神界闭关祝福时长", blessing_cost)
                remaining = expected_exp_day - blessing_cost
                saved = [as_int_like(exp_gain), blessing_cost, remaining]
                conn.execute(
                    "INSERT INTO impart_closing_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartClosingSettlementResult("applied", *saved)
            except Exception:
                conn.rollback()
                raise

SQLITE_MAX_INT = 2**63 - 1

from .settlement_state import increment_stat, load_daily_state

@dataclass(frozen=True)
class ImpartTrainingSettlementResult:
    status: str
    exp_day: int
    exp: int
    exp_used: int
    exp_count: int
    exp_load: int
    exp_gain: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ImpartTrainingSettlementService:
    """Settle one virtual-world cultivation action across all authoritative stores."""

    def __init__(self, game_database, impart_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._impart_database = Path(impart_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_daily_state(self, user_id, legacy_state=None) -> dict[str, int]:
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                state = load_daily_state(conn, str(user_id), legacy_state)
                conn.commit()
                return state
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

    def reset_daily(self) -> None:
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            conn.execute("DELETE FROM impart_pk_daily") if conn.table_exists("impart_pk_daily") else None
            conn.commit()

    def get_result(self, operation_id: str) -> ImpartTrainingSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_training_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,result_json FROM impart_training_operations WHERE operation_id=%s", (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return ImpartTrainingSettlementResult("duplicate", *json.loads(str(previous[1])))

    def settle(
        self, operation_id, user_id, *, expected_exp, expected_exp_day, expected_daily,
        exp_cost, exp_gain, exp_load_gain, power, legacy_state=None,
    ) -> ImpartTrainingSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_daily = {key: int(expected_daily[key]) for key in ("exp_used", "exp_count", "exp_load", "exp_gain")}
        expected_exp, expected_exp_day, exp_cost, exp_gain, exp_load_gain, power = map(
            int, (expected_exp, expected_exp_day, exp_cost, exp_gain, exp_load_gain, power)
        )
        power = max(0, min(power, SQLITE_MAX_INT))
        expected_exp = max(0, min(expected_exp, SQLITE_MAX_INT))
        exp_gain = max(0, min(exp_gain, SQLITE_MAX_INT))
        if not operation_id or exp_cost <= 0 or exp_gain <= 0 or exp_load_gain < 0 or power < 0:
            raise ValueError("invalid impart training settlement")
        # Request identity only — exp/daily snapshots are concurrency checks; roll outcome in result_json.
        payload = json.dumps([user_id, exp_cost], ensure_ascii=True, separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached_impart = attached_player = False
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self._impart_database),)); attached_impart = True
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached_player = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_training_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM impart_training_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ImpartTrainingSettlementResult("operation_conflict", 0, 0, 0, 0, 0, 0)
                    return ImpartTrainingSettlementResult("duplicate", *json.loads(str(previous[1])))

                user = conn.execute("SELECT exp FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                impart = conn.execute("SELECT exp_day FROM impart_data.xiuxian_impart WHERE user_id=%s", (user_id,)).fetchone()
                daily = load_daily_state(conn, user_id, legacy_state)
                if user is None or impart is None or int(user[0] or 0) != expected_exp or int(impart[0] or 0) != expected_exp_day:
                    conn.rollback(); return ImpartTrainingSettlementResult("state_changed", 0, 0, 0, 0, 0, 0)
                if any(daily[key] != value for key, value in expected_daily.items()):
                    conn.rollback(); return ImpartTrainingSettlementResult("state_changed", 0, 0, 0, 0, 0, 0)
                if expected_exp_day < exp_cost:
                    conn.rollback(); return ImpartTrainingSettlementResult("time_insufficient", 0, 0, 0, 0, 0, 0)

                new_exp_day = expected_exp_day - exp_cost
                new_exp = min(SQLITE_MAX_INT, expected_exp + exp_gain)
                new_used = expected_daily["exp_used"] + exp_cost
                new_count = expected_daily["exp_count"] + 1
                new_load = min(100, expected_daily["exp_load"] + exp_load_gain)
                new_gain = expected_daily["exp_gain"] + exp_gain
                conn.execute("UPDATE impart_data.xiuxian_impart SET exp_day=%s WHERE user_id=%s", (new_exp_day, user_id))
                changed = conn.execute(
                    "UPDATE user_xiuxian SET exp=%s,power=%s WHERE user_id=%s AND exp=%s",
                    (new_exp, power, user_id, expected_exp),
                )
                if changed.rowcount != 1:
                    conn.rollback(); return ImpartTrainingSettlementResult("state_changed", 0, 0, 0, 0, 0, 0)
                conn.execute(
                    "UPDATE player_data.impart_pk_daily SET exp_used=%s,exp_count=%s,exp_load=%s,exp_gain=%s WHERE user_id=%s",
                    (new_used, new_count, new_load, new_gain, user_id),
                )
                for key, amount in (
                    ("虚神界修炼", exp_cost), ("虚神界修炼次数", 1),
                    ("虚神界修炼修为", exp_gain), ("虚神界修炼承载", exp_load_gain),
                ):
                    increment_stat(conn, user_id, key, amount)
                saved = [new_exp_day, new_exp, new_used, new_count, new_load, new_gain]
                conn.execute(
                    "INSERT INTO impart_training_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartTrainingSettlementResult("applied", *saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached_player:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass
                if attached_impart:
                    try: conn.execute("DETACH DATABASE impart_data")
                    except Exception: pass

@dataclass(frozen=True)
class ImpartExploreSettlementResult:
    status: str
    exp_day: int
    impart_lv: int
    impart_num: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ImpartExploreSettlementService:
    def __init__(self, game_database, impart_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._impart_database = Path(impart_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> ImpartExploreSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_explore_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,result_json FROM impart_explore_operations WHERE operation_id=%s", (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return ImpartExploreSettlementResult("duplicate", *json.loads(str(previous[1])))

    def settle(
        self, operation_id, user_id, *, event_type, expected_exp_day, expected_impart_lv,
        expected_impart_num, time_cost, new_impart_lv, legacy_state=None,
    ) -> ImpartExploreSettlementResult:
        operation_id, user_id, event_type = str(operation_id).strip(), str(user_id), str(event_type)
        expected_exp_day, expected_impart_lv, expected_impart_num, time_cost, new_impart_lv = map(
            int, (expected_exp_day, expected_impart_lv, expected_impart_num, time_cost, new_impart_lv)
        )
        if not operation_id or event_type not in {"stay", "fail", "down", "up", "down_rate", "up_rate"}:
            raise ValueError("invalid impart exploration settlement")
        if expected_impart_num <= 0 or time_cost < 0 or not 0 <= new_impart_lv <= 30:
            raise ValueError("invalid impart exploration values")
        # Request identity only — event_type/time/lv rolls live in result_json.
        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached_impart = attached_player = False
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self._impart_database),)); attached_impart = True
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached_player = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_explore_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM impart_explore_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ImpartExploreSettlementResult("operation_conflict", 0, 0, 0)
                    return ImpartExploreSettlementResult("duplicate", *json.loads(str(previous[1])))

                impart = conn.execute(
                    "SELECT exp_day,impart_lv FROM impart_data.xiuxian_impart WHERE user_id=%s", (user_id,)
                ).fetchone()
                daily = load_daily_state(conn, user_id, legacy_state)
                if impart is None or (int(impart[0] or 0), int(impart[1] or 0)) != (expected_exp_day, expected_impart_lv):
                    conn.rollback(); return ImpartExploreSettlementResult("state_changed", 0, 0, 0)
                if daily["impart_num"] != expected_impart_num:
                    conn.rollback(); return ImpartExploreSettlementResult("state_changed", 0, 0, 0)
                if expected_exp_day < time_cost:
                    conn.rollback(); return ImpartExploreSettlementResult("time_insufficient", 0, 0, 0)

                new_exp_day = expected_exp_day - time_cost
                new_impart_num = expected_impart_num - 1
                conn.execute(
                    "UPDATE impart_data.xiuxian_impart SET exp_day=%s,impart_lv=%s WHERE user_id=%s",
                    (new_exp_day, new_impart_lv, user_id),
                )
                conn.execute(
                    "UPDATE player_data.impart_pk_daily SET impart_num=%s WHERE user_id=%s",
                    (new_impart_num, user_id),
                )
                increment_stat(conn, user_id, "虚神界探索次数", 1)
                if time_cost:
                    increment_stat(conn, user_id, "虚神界探索消耗时间", time_cost)
                if event_type in {"up", "up_rate"}:
                    increment_stat(conn, user_id, "虚神界探索上升", 1)
                elif event_type in {"down", "down_rate"}:
                    increment_stat(conn, user_id, "虚神界探索下降", 1)
                saved = [new_exp_day, new_impart_lv, new_impart_num]
                conn.execute(
                    "INSERT INTO impart_explore_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartExploreSettlementResult("applied", *saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached_player:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass
                if attached_impart:
                    try: conn.execute("DETACH DATABASE impart_data")
                    except Exception: pass

@dataclass(frozen=True)
class ImpartBattleBatchResult:
    status: str
    challenger_pk_num: int = 0
    opponent_pk_num: int | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ImpartBattleBatchService:
    def __init__(self, impart_db, player_db, lock=None):
        self.impart_db = Path(impart_db)
        self.player_db = Path(player_db)
        self.lock = lock or RLock()

    def get_pk_num(self, user_id, legacy_pk_num):
        user_id, legacy_pk_num = str(user_id), int(legacy_pk_num)
        with self.lock, closing(db_backend.connect(self.player_db)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_pk_state("
                "user_id TEXT PRIMARY KEY,pk_num INTEGER NOT NULL DEFAULT 7,"
                "win_num INTEGER NOT NULL DEFAULT 0)"
            )
            row = conn.execute("SELECT pk_num FROM impart_pk_state WHERE user_id=%s", (user_id,)).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO impart_pk_state(user_id,pk_num,win_num) VALUES(%s,%s,0)",
                    (user_id, legacy_pk_num),
                )
                conn.commit()
                return legacy_pk_num
            return int(row[0])

    def get_result(self, operation_id: str) -> ImpartBattleBatchResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self.lock, closing(db_backend.connect(self.impart_db)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_battle_batch_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            old = conn.execute(
                "SELECT payload,result_json FROM impart_battle_batch_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            saved = json.loads(str(old[1]))
            return ImpartBattleBatchResult("duplicate", saved[0], saved[1])

    def settle(
        self,
        operation_id,
        challenger_id,
        expected_challenger_pk_num,
        challenger_wins,
        challenger_losses,
        challenger_stones,
        opponent_id=None,
        expected_opponent_pk_num=None,
        opponent_wins=0,
        opponent_losses=0,
        opponent_stones=0,
    ) -> ImpartBattleBatchResult:
        operation_id = str(operation_id).strip()
        challenger_id = str(challenger_id)
        opponent_id = None if opponent_id is None else str(opponent_id)
        values = tuple(
            int(value)
            for value in (
                expected_challenger_pk_num,
                challenger_wins,
                challenger_losses,
                challenger_stones,
                opponent_wins,
                opponent_losses,
                opponent_stones,
            )
        )
        expected_challenger_pk_num, challenger_wins, challenger_losses, challenger_stones, opponent_wins, opponent_losses, opponent_stones = values
        if expected_opponent_pk_num is not None:
            expected_opponent_pk_num = int(expected_opponent_pk_num)
        if (
            not operation_id
            or min(values) < 0
            or challenger_losses > expected_challenger_pk_num
            or (opponent_id is None) != (expected_opponent_pk_num is None)
            or (expected_opponent_pk_num is not None and opponent_losses > expected_opponent_pk_num)
        ):
            raise ValueError("invalid impart battle batch")
        # Request identity only — win/loss/stone rolls + pk snapshots are concurrency checks.
        payload = json.dumps(
            [challenger_id, opponent_id], ensure_ascii=False, separators=(",", ":"),
        )
        with self.lock, closing(db_backend.connect(self.impart_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_battle_batch_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS player_data.impart_pk_state("
                    "user_id TEXT PRIMARY KEY,pk_num INTEGER NOT NULL DEFAULT 7,"
                    "win_num INTEGER NOT NULL DEFAULT 0)"
                )
                old = conn.execute(
                    "SELECT payload,result_json FROM impart_battle_batch_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return ImpartBattleBatchResult("operation_conflict")
                    saved = json.loads(str(old[1]))
                    return ImpartBattleBatchResult("duplicate", saved[0], saved[1])

                participants = [
                    (
                        challenger_id,
                        expected_challenger_pk_num,
                        challenger_wins,
                        challenger_losses,
                        challenger_stones,
                    )
                ]
                if opponent_id is not None:
                    participants.append(
                        (opponent_id, expected_opponent_pk_num, opponent_wins, opponent_losses, opponent_stones)
                    )
                remaining = []
                for user_id, expected_pk, wins, losses, stones in participants:
                    row = conn.execute(
                        "SELECT pk_num FROM player_data.impart_pk_state WHERE user_id=%s", (user_id,)
                    ).fetchone()
                    if row is None:
                        conn.execute(
                            "INSERT INTO player_data.impart_pk_state(user_id,pk_num,win_num) VALUES(%s,%s,0)",
                            (user_id, expected_pk),
                        )
                    elif int(row[0]) != expected_pk:
                        conn.rollback()
                        return ImpartBattleBatchResult("state_changed")
                    impart = conn.execute(
                        "SELECT 1 FROM xiuxian_impart WHERE user_id=%s", (user_id,)
                    ).fetchone()
                    if impart is None:
                        conn.rollback()
                        return ImpartBattleBatchResult("user_missing")
                    changed = conn.execute(
                        "UPDATE player_data.impart_pk_state SET pk_num=pk_num-%s,win_num=win_num+%s "
                        "WHERE user_id=%s AND pk_num=%s AND pk_num>=%s",
                        (losses, wins, user_id, expected_pk, losses),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return ImpartBattleBatchResult("state_changed")
                    conn.execute(
                        "UPDATE xiuxian_impart SET stone_num=CAST(COALESCE(stone_num,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                        (stones, user_id),
                    )
                    _increment_stat(conn, user_id, "虚神界对决次数", wins + losses)
                    _increment_stat(conn, user_id, "虚神界对决胜利", wins)
                    _increment_stat(conn, user_id, "虚神界对决失败", losses)
                    _increment_stat(conn, user_id, "思恋结晶获取", stones)
                    remaining.append(expected_pk - losses)

                saved = [remaining[0], remaining[1] if len(remaining) > 1 else None]
                conn.execute(
                    "INSERT INTO impart_battle_batch_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartBattleBatchResult("applied", saved[0], saved[1])
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "ImpartProjectJoinResult",
    "ImpartProjectJoinService",
    "ImpartClosingEnterResult",
    "ImpartClosingEnterService",
    "ImpartClosingSettlementResult",
    "ImpartClosingSettlementService",
    "ImpartTrainingSettlementResult",
    "ImpartTrainingSettlementService",
    "ImpartExploreSettlementResult",
    "ImpartExploreSettlementService",
    "ImpartBattleBatchResult",
    "ImpartBattleBatchService",
    "SQLITE_MAX_INT",
]
