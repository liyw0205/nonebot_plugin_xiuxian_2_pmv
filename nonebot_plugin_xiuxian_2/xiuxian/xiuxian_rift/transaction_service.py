from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
import hashlib
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.numeric_bind import operation_payload_matches
from datetime import datetime

def _canonical(value) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

def _normalise_rift_data(value) -> tuple[dict, tuple[str, ...]]:
    data = dict(value or {})
    raw_participants = data.pop("l_user_id", []) or []
    participants = tuple(
        dict.fromkeys(str(user_id) for user_id in raw_participants)
    )
    return json.loads(_canonical(data)), participants

@dataclass(frozen=True)
class RiftWorldState:
    rift_key: str
    generation_id: str
    rift_data: dict
    participants: tuple[str, ...] = ()
    revision: int = 0

    def as_dict(self) -> dict:
        value = dict(self.rift_data)
        value["l_user_id"] = list(self.participants)
        return value

@dataclass(frozen=True)
class RiftGenerationResult:
    status: str
    state: RiftWorldState | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

@dataclass(frozen=True)
class RiftEntryResult:
    status: str
    entries: int = 0
    rift_data: dict = field(default_factory=dict)
    world: RiftWorldState | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class RiftEntryService:
    """Persist global rift generations and player entry state atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_column(conn, table: str, column: str, definition: str) -> None:
        if not conn.column_exists(table, column):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rift_world_state("
            "rift_key TEXT PRIMARY KEY,generation_id TEXT NOT NULL,"
            "rift_data TEXT NOT NULL,participants TEXT NOT NULL,"
            "revision INTEGER NOT NULL,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rift_generation_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "rift_key TEXT NOT NULL,generation_id TEXT NOT NULL,"
            "rift_data TEXT NOT NULL,revision INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rift_entries("
            "user_id TEXT PRIMARY KEY,rift_key TEXT NOT NULL,rift_data TEXT NOT NULL,"
            "status TEXT NOT NULL,duration INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "generation_id TEXT NOT NULL DEFAULT '')"
        )
        cls._ensure_column(
            conn, "rift_entries", "generation_id", "TEXT NOT NULL DEFAULT ''"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rift_entry_counts("
            "user_id TEXT PRIMARY KEY,entry_count INTEGER NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rift_entry_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "entry_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "generation_id TEXT NOT NULL DEFAULT '',rift_data TEXT NOT NULL DEFAULT '{}',"
            "global_revision INTEGER NOT NULL DEFAULT 0)"
        )
        cls._ensure_column(
            conn,
            "rift_entry_operations",
            "generation_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        cls._ensure_column(
            conn,
            "rift_entry_operations",
            "rift_data",
            "TEXT NOT NULL DEFAULT '{}'",
        )
        cls._ensure_column(
            conn,
            "rift_entry_operations",
            "global_revision",
            "INTEGER NOT NULL DEFAULT 0",
        )

    @staticmethod
    def _read_world(conn, rift_key: str) -> RiftWorldState | None:
        row = conn.execute(
            "SELECT rift_key,generation_id,rift_data,participants,revision "
            "FROM rift_world_state WHERE rift_key=%s",
            (rift_key,),
        ).fetchone()
        if row is None:
            return None
        return RiftWorldState(
            rift_key=str(row[0]),
            generation_id=str(row[1]),
            rift_data=json.loads(str(row[2])),
            participants=tuple(str(value) for value in json.loads(str(row[3]))),
            revision=int(row[4]),
        )

    def get_current(self, rift_key: str) -> RiftWorldState | None:
        rift_key = str(rift_key).strip()
        if not rift_key:
            raise ValueError("rift_key is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                state = self._read_world(conn, rift_key)
                conn.commit()
                return state
            except Exception:
                conn.rollback()
                raise

    def bootstrap(self, rift_key: str, legacy_snapshot: dict) -> RiftWorldState:
        """Import the legacy JSON snapshot only when no database state exists."""
        rift_key = str(rift_key).strip()
        rift_data, participants = _normalise_rift_data(legacy_snapshot)
        if not rift_key or not rift_data:
            raise ValueError("rift_key and legacy snapshot are required")
        digest = hashlib.sha256(
            _canonical([rift_key, rift_data, participants]).encode("utf-8")
        ).hexdigest()[:24]
        generation_id = f"legacy:{digest}"

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                current = self._read_world(conn, rift_key)
                if current is not None:
                    conn.rollback()
                    return current
                conn.execute(
                    "INSERT INTO rift_world_state("
                    "rift_key,generation_id,rift_data,participants,revision) "
                    "VALUES(%s,%s,%s,%s,1)",
                    (
                        rift_key,
                        generation_id,
                        _canonical(rift_data),
                        _canonical(participants),
                    ),
                )
                conn.commit()
                return RiftWorldState(
                    rift_key,
                    generation_id,
                    rift_data,
                    participants,
                    1,
                )
            except Exception:
                conn.rollback()
                raise

    def generate(
        self,
        operation_id,
        rift_key,
        rift_plan,
    ) -> RiftGenerationResult:
        operation_id = str(operation_id).strip()
        rift_key = str(rift_key).strip()
        rift_data, _ = _normalise_rift_data(rift_plan)
        if (
            not operation_id
            or not rift_key
            or not str(rift_data.get("name", "")).strip()
            or int(rift_data.get("time", 0)) <= 0
        ):
            raise ValueError("valid operation, rift key and fixed plan are required")
        payload = _canonical([rift_key, rift_data])

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,generation_id FROM rift_generation_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    current = self._read_world(conn, rift_key)
                    if not operation_payload_matches(previous[0], payload):
                        status = "state_changed"
                    elif current is None or current.generation_id != str(previous[1]):
                        status = "superseded"
                    else:
                        status = "duplicate"
                    conn.rollback()
                    return RiftGenerationResult(status, current)

                current = self._read_world(conn, rift_key)
                revision = (current.revision if current is not None else 0) + 1
                conn.execute(
                    "INSERT INTO rift_world_state("
                    "rift_key,generation_id,rift_data,participants,revision,updated_at) "
                    "VALUES(%s,%s,%s,'[]',%s,CURRENT_TIMESTAMP) "
                    "ON CONFLICT(rift_key) DO UPDATE SET "
                    "generation_id=EXCLUDED.generation_id,rift_data=EXCLUDED.rift_data,"
                    "participants='[]',revision=EXCLUDED.revision,"
                    "updated_at=EXCLUDED.updated_at",
                    (rift_key, operation_id, _canonical(rift_data), revision),
                )
                conn.execute(
                    "INSERT INTO rift_generation_operations("
                    "operation_id,payload,rift_key,generation_id,rift_data,revision) "
                    "VALUES(%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        rift_key,
                        operation_id,
                        _canonical(rift_data),
                        revision,
                    ),
                )
                conn.commit()
                return RiftGenerationResult(
                    "applied",
                    RiftWorldState(rift_key, operation_id, rift_data, (), revision),
                )
            except Exception:
                conn.rollback()
                raise

    def enter(
        self,
        operation_id,
        user_id,
        rift_key,
        rift_data,
        duration,
        ticket_id=0,
        *,
        expected_generation_id,
        expected_revision,
        stamina_cost=0,
        expected_stamina=None,
    ) -> RiftEntryResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        rift_key = str(rift_key).strip()
        expected_generation_id = str(expected_generation_id).strip()
        expected_revision = int(expected_revision)
        duration = int(duration)
        ticket_id = int(ticket_id)
        stamina_cost = int(stamina_cost)
        expected_stamina = (
            None if expected_stamina is None else int(expected_stamina)
        )
        expected_rift, _ = _normalise_rift_data(rift_data)
        if (
            not operation_id
            or not user_id
            or not rift_key
            or not expected_generation_id
            or expected_revision <= 0
            or duration <= 0
            or ticket_id < 0
            or stamina_cost < 0
            or (stamina_cost > 0 and expected_stamina is None)
        ):
            raise ValueError("valid operation, user, generation and duration are required")
        payload = _canonical(
            [
                user_id,
                rift_key,
                expected_generation_id,
                expected_rift,
                duration,
                ticket_id,
                stamina_cost,
            ]
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,entry_count,generation_id,rift_data "
                    "FROM rift_entry_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    if not operation_payload_matches(previous[0], payload):
                        conn.rollback()
                        return RiftEntryResult("state_changed")
                    world = self._read_world(conn, rift_key)
                    result = RiftEntryResult(
                        "duplicate",
                        int(previous[1]),
                        json.loads(str(previous[3])),
                        world,
                    )
                    conn.rollback()
                    return result

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return RiftEntryResult("user_missing")

                world = self._read_world(conn, rift_key)
                if (
                    world is None
                    or world.generation_id != expected_generation_id
                    or _canonical(world.rift_data) != _canonical(expected_rift)
                    or int(world.rift_data.get("time", 0)) != duration
                ):
                    conn.rollback()
                    return RiftEntryResult("rift_changed", world=world)
                if user_id in world.participants and not ticket_id:
                    # 普通进入：本轮已进过不可重复。
                    # 秘藏令(ticket)是额外进入，允许绕过「本轮已参加」限制。
                    conn.rollback()
                    return RiftEntryResult("already_joined", world=world)

                active = conn.execute(
                    "SELECT 1 FROM rift_entries WHERE user_id=%s AND status='active'",
                    (user_id,),
                ).fetchone()
                if active is not None:
                    conn.rollback()
                    return RiftEntryResult("already_active", world=world)
                cd = conn.execute(
                    "SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if cd is None or int(cd[0]) != 0:
                    conn.rollback()
                    return RiftEntryResult("busy", world=world)

                if stamina_cost:
                    stamina = conn.execute(
                        "SELECT COALESCE(user_stamina,0) FROM user_xiuxian "
                        "WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    if stamina is None or int(stamina[0]) != expected_stamina:
                        conn.rollback()
                        return RiftEntryResult("state_changed", world=world)
                    if expected_stamina < stamina_cost:
                        conn.rollback()
                        return RiftEntryResult("stamina_missing", world=world)

                if ticket_id:
                    bind_update = ""
                    if conn.column_exists("back", "bind_num"):
                        bind_update = (
                            ",bind_num=MIN("
                            "MAX(COALESCE(bind_num,0)-1,0),goods_num-1)"
                        )
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-1" + bind_update + " "
                        "WHERE user_id=%s AND goods_id=%s AND COALESCE(goods_num,0)>=1",
                        (user_id, ticket_id),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return RiftEntryResult("ticket_missing", world=world)

                if stamina_cost:
                    stamina_updated = conn.execute(
                        "UPDATE user_xiuxian SET user_stamina=user_stamina-%s "
                        "WHERE user_id=%s AND COALESCE(user_stamina,0)=%s "
                        "AND COALESCE(user_stamina,0)>=%s",
                        (
                            stamina_cost,
                            user_id,
                            expected_stamina,
                            stamina_cost,
                        ),
                    )
                    if stamina_updated.rowcount != 1:
                        conn.rollback()
                        return RiftEntryResult("state_changed", world=world)

                snapshot = _canonical(expected_rift)
                conn.execute(
                    "INSERT INTO rift_entries("
                    "user_id,rift_key,rift_data,status,duration,created_at,generation_id) "
                    "VALUES(%s,%s,%s,'active',%s,CURRENT_TIMESTAMP,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET "
                    "rift_key=EXCLUDED.rift_key,rift_data=EXCLUDED.rift_data,"
                    "status=EXCLUDED.status,duration=EXCLUDED.duration,"
                    "created_at=EXCLUDED.created_at,generation_id=EXCLUDED.generation_id",
                    (
                        user_id,
                        rift_key,
                        snapshot,
                        duration,
                        expected_generation_id,
                    ),
                )
                cd_updated = conn.execute(
                    "UPDATE user_cd SET type=3,create_time=CURRENT_TIMESTAMP,"
                    "scheduled_time=%s WHERE user_id=%s AND COALESCE(type,0)=0",
                    (duration, user_id),
                )
                if cd_updated.rowcount != 1:
                    conn.rollback()
                    return RiftEntryResult("state_changed", world=world)

                participants = (*world.participants, user_id)
                next_revision = world.revision + 1
                world_updated = conn.execute(
                    "UPDATE rift_world_state SET participants=%s,revision=%s,"
                    "updated_at=CURRENT_TIMESTAMP WHERE rift_key=%s "
                    "AND generation_id=%s AND revision=%s",
                    (
                        _canonical(participants),
                        next_revision,
                        rift_key,
                        expected_generation_id,
                        world.revision,
                    ),
                )
                if world_updated.rowcount != 1:
                    conn.rollback()
                    return RiftEntryResult("rift_changed", world=world)

                conn.execute(
                    "INSERT INTO rift_entry_counts(user_id,entry_count) VALUES(%s,1) "
                    "ON CONFLICT(user_id) DO UPDATE SET "
                    "entry_count=rift_entry_counts.entry_count+1",
                    (user_id,),
                )
                count = int(
                    conn.execute(
                        "SELECT entry_count FROM rift_entry_counts WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()[0]
                )
                conn.execute(
                    "INSERT INTO rift_entry_operations("
                    "operation_id,payload,entry_count,generation_id,rift_data,global_revision) "
                    "VALUES(%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        count,
                        expected_generation_id,
                        snapshot,
                        next_revision,
                    ),
                )
                conn.commit()
                return RiftEntryResult(
                    "applied",
                    count,
                    expected_rift,
                    RiftWorldState(
                        rift_key,
                        expected_generation_id,
                        world.rift_data,
                        participants,
                        next_revision,
                    ),
                )
            except Exception:
                conn.rollback()
                raise

    def replay(self, operation_id, rift_key) -> RiftEntryResult | None:
        operation_id = str(operation_id).strip()
        rift_key = str(rift_key).strip()
        if not operation_id or not rift_key:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            if not conn.table_exists("rift_entry_operations"):
                return None
            previous = conn.execute(
                "SELECT entry_count,rift_data FROM rift_entry_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            world = self._read_world(conn, rift_key)
            return RiftEntryResult(
                "duplicate",
                int(previous[0]),
                json.loads(str(previous[1])),
                world,
            )

    def read_entry(self, user_id, *, active_only=False) -> dict | None:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                query = "SELECT rift_data,status FROM rift_entries WHERE user_id=%s"
                row = conn.execute(query, (user_id,)).fetchone()
            except db_backend.OperationalError:
                return None
        if row is None or (active_only and str(row[1]) != "active"):
            return None
        return json.loads(str(row[0]))

@dataclass(frozen=True)
class RiftTerminationResult:
    status: str
    rift_name: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class RiftTerminationService:
    """Atomically abandon an active rift and release the user's busy state."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def replay(self, operation_id, user_id) -> RiftTerminationResult | None:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            if not conn.table_exists("rift_termination_operations"):
                return None
            row = conn.execute(
                "SELECT payload FROM rift_termination_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            stored_user_id, snapshot = json.loads(str(row[0]))
            if str(stored_user_id) != user_id:
                return RiftTerminationResult("state_changed")
            rift_data = json.loads(str(snapshot))
            return RiftTerminationResult(
                "duplicate", str(rift_data.get("name", ""))
            )

    def terminate(self, operation_id, user_id, rift_data) -> RiftTerminationResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        snapshot = json.dumps(rift_data, ensure_ascii=False, sort_keys=True)
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        payload = json.dumps([user_id, snapshot], ensure_ascii=True)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS rift_termination_operations "
                    "(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload FROM rift_termination_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return RiftTerminationResult(
                        "duplicate" if operation_payload_matches(previous[0], payload) else "state_changed",
                        str(rift_data.get("name", "")),
                    )
                entry = conn.execute(
                    "SELECT rift_data,status FROM rift_entries WHERE user_id=%s", (user_id,)
                ).fetchone()
                if entry is None or str(entry[1]) != "active":
                    conn.rollback()
                    return RiftTerminationResult("not_active")
                if json.loads(str(entry[0])) != json.loads(snapshot):
                    conn.rollback()
                    return RiftTerminationResult("state_changed")
                cd = conn.execute("SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                if cd is None or int(cd[0]) != 3:
                    conn.rollback()
                    return RiftTerminationResult("state_changed")
                conn.execute("UPDATE rift_entries SET status='terminated' WHERE user_id=%s", (user_id,))
                conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s AND type=3", (user_id,)
                )
                conn.execute(
                    "INSERT INTO rift_termination_operations VALUES (%s,%s,CURRENT_TIMESTAMP)",
                    (operation_id, payload),
                )
                conn.commit()
                return RiftTerminationResult(
                    "applied", str(rift_data.get("name", ""))
                )
            except Exception:
                conn.rollback()
                raise

def _ensure_player_field(conn, table, field, data_type="TEXT"):
    table_sql, field_sql = db_backend.quote_ident(table), db_backend.quote_ident(field)
    conn.execute(f"CREATE TABLE IF NOT EXISTS player_data.{table_sql}(user_id TEXT PRIMARY KEY)")
    columns = {str(row[1]) for row in conn.execute(f"PRAGMA player_data.table_info({table_sql})").fetchall()}
    if field not in columns:
        conn.execute(f"ALTER TABLE player_data.{table_sql} ADD COLUMN {field_sql} {data_type}")

def _set_player_field(conn, table, user_id, field, value, data_type="TEXT"):
    _ensure_player_field(conn, table, field, data_type)
    table_sql, field_sql = db_backend.quote_ident(table), db_backend.quote_ident(field)
    changed = conn.execute(f"UPDATE player_data.{table_sql} SET {field_sql}=%s WHERE user_id=%s", (value, user_id))
    if changed.rowcount == 0:
        conn.execute(f"INSERT INTO player_data.{table_sql}(user_id,{field_sql}) VALUES(%s,%s)", (user_id, value))

def _increment_stat(conn, user_id, key, amount):
    _ensure_player_field(conn, "statistics", key, "INTEGER")
    field_sql = db_backend.quote_ident(key)
    changed = conn.execute(f"UPDATE player_data.statistics SET {field_sql}=COALESCE({field_sql},0)+%s WHERE user_id=%s", (amount, user_id))
    if changed.rowcount == 0:
        conn.execute(f"INSERT INTO player_data.statistics(user_id,{field_sql}) VALUES(%s,%s)", (user_id, amount))

def _normalise_progress_reward(expected_count, progress_reward):
    expected_count = int(expected_count)
    if not 0 <= expected_count <= 9:
        raise ValueError("expected explore count must be between 0 and 9")
    if expected_count != 9:
        if progress_reward is not None:
            raise ValueError("progress reward must match the tenth completion")
        return None
    if not isinstance(progress_reward, dict):
        raise ValueError("progress reward must match the tenth completion")
    try:
        reward = (
            int(progress_reward["id"]),
            str(progress_reward["name"]).strip(),
            str(progress_reward["type"]).strip(),
            int(progress_reward.get("amount", 1)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("progress reward must be complete") from exc
    if reward[0] <= 0 or not reward[1] or not reward[2] or reward[3] <= 0:
        raise ValueError("progress reward must be complete and positive")
    return reward

@dataclass(frozen=True)
class RiftKeyEventSettlementResult:
    status: str
    explore_count: int = 0
    message: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class RiftKeyEventSettlementService:
    """Consume a rift item and commit a pre-rolled event across both databases."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
        operation_table: str = "rift_key_event_operations",
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._operation_table = operation_table

    def replay(self, operation_id) -> RiftKeyEventSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            table = db_backend.quote_ident(self._operation_table)
            if not conn.table_exists(self._operation_table):
                return None
            row = conn.execute(f"SELECT explore_count,message FROM {table} WHERE operation_id=%s", (operation_id,)).fetchone()
            if row is None:
                return None
            return RiftKeyEventSettlementResult("duplicate", int(row[0]), str(row[1]))

    def settle(
        self,
        operation_id,
        user_id,
        item_id,
        expected_rift,
        expected_user,
        expected_explore_count,
        outcome,
        max_goods_num,
    ) -> RiftKeyEventSettlementResult:
        operation_id, user_id, item_id = str(operation_id).strip(), str(user_id), int(item_id)
        expected_count, max_goods_num = int(expected_explore_count), int(max_goods_num)
        rift_snapshot = json.dumps(expected_rift, ensure_ascii=False, sort_keys=True)
        expected = tuple(int(expected_user.get(key, 0)) for key in ("stone", "exp", "hp", "mp"))
        delta = tuple(int(outcome.get("delta", {}).get(key, 0)) for key in ("stone", "exp", "hp", "mp"))
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item.get("amount", 1)))
            for item in outcome.get("items", ())
            if int(item.get("amount", 1)) > 0
        )
        progress_reward = _normalise_progress_reward(
            expected_count, outcome.get("progress_reward")
        )
        if progress_reward is not None:
            rewards += (progress_reward,)
        statistics = tuple(sorted((str(key), int(value)) for key, value in outcome.get("statistics", {}).items() if int(value)))
        message = str(outcome.get("message", ""))
        if not operation_id or not user_id or item_id <= 0 or expected_count < 0 or max_goods_num < 0:
            raise ValueError("valid operation, user, item and snapshots are required")
        payload = json.dumps(
            [user_id, item_id, rift_snapshot, expected, expected_count, delta, rewards, statistics, message, max_goods_num],
            ensure_ascii=True,
            sort_keys=True,
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                table = db_backend.quote_ident(self._operation_table)
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table} ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,explore_count INTEGER NOT NULL,"
                    "message TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    f"SELECT payload,explore_count,message FROM {table} WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) == payload:
                        return RiftKeyEventSettlementResult("duplicate", int(old[1]), str(old[2]))
                    return RiftKeyEventSettlementResult("state_changed")

                entry = conn.execute("SELECT rift_data,status FROM rift_entries WHERE user_id=%s", (user_id,)).fetchone()
                cd = conn.execute("SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                user = conn.execute(
                    "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if entry is None or str(entry[1]) != "active" or cd is None or int(cd[0]) != 3:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("not_active")
                if json.loads(str(entry[0])) != json.loads(rift_snapshot) or user is None or tuple(map(int, user)) != expected:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("state_changed")
                item = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                ).fetchone()
                if item is None or int(item[0]) < 1:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("item_missing")

                rift_columns = {
                    str(row[1]) for row in conn.execute('PRAGMA player_data.table_info("rift")').fetchall()
                }
                count_row = conn.execute(
                    'SELECT "explore_count" FROM player_data."rift" WHERE user_id=%s', (user_id,)
                ).fetchone() if "explore_count" in rift_columns else None
                current_count = int(count_row[0] or 0) if count_row else 0
                if current_count != expected_count:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("state_changed")
                if expected[0] + delta[0] < 0:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("resource_missing")

                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                for reward_id, name, item_type, amount in rewards:
                    totals[reward_id] = totals.get(reward_id, 0) + amount
                    old_metadata = metadata.setdefault(reward_id, (name, item_type))
                    if old_metadata != (name, item_type):
                        raise ValueError("conflicting reward metadata")
                for reward_id, amount in totals.items():
                    row = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, reward_id)
                    ).fetchone()
                    current_amount = int(row[0]) if row else 0
                    consumed_amount = 1 if reward_id == item_id else 0
                    if current_amount - consumed_amount + amount > max_goods_num:
                        conn.rollback()
                        return RiftKeyEventSettlementResult("inventory_full")

                bind_update = ""
                if conn.column_exists("back", "bind_num"):
                    bind_update = (
                        ",bind_num=MIN("
                        "MAX(COALESCE(bind_num,0)-1,0),goods_num-1)"
                    )
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-1" + bind_update + " "
                    "WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num,0)>=1",
                    (user_id, item_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("item_missing")
                final_exp = max(0, expected[1] + delta[1])
                final_hp = max(1, expected[2] + delta[2])
                final_mp = max(1, expected[3] + delta[3])
                conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL),exp=%s,hp=%s,mp=%s WHERE user_id=%s",
                    (delta[0], final_exp, final_hp, final_mp, user_id),
                )
                now = datetime.now()
                for reward_id, amount in totals.items():
                    name, item_type = metadata[reward_id]
                    conn.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                        (user_id, reward_id, name, item_type, amount, now, now, amount),
                    )
                new_count = 0 if expected_count + 1 >= 10 else expected_count + 1
                _set_player_field(conn, "rift", user_id, "explore_count", new_count, "INTEGER")
                for key, amount in statistics:
                    _increment_stat(conn, user_id, key, amount)
                conn.execute("UPDATE rift_entries SET status='settled' WHERE user_id=%s", (user_id,))
                conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s AND type=3", (user_id,)
                )
                conn.execute(
                    f"INSERT INTO {table}(operation_id,payload,explore_count,message) VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, new_count, message),
                )
                conn.commit()
                return RiftKeyEventSettlementResult("applied", new_count, message)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

class RiftDemonTokenBattleSettlementService(RiftKeyEventSettlementService):
    """Commit a pre-rolled demon-token Boss battle as one rift transaction."""

    def __init__(self, game_database, player_database, lock=None) -> None:
        super().__init__(
            game_database,
            player_database,
            lock=lock,
            operation_table="rift_demon_token_battle_operations",
        )

RiftDemonTokenBattleSettlementResult = RiftKeyEventSettlementResult

@dataclass(frozen=True)
class RiftSpeedupResult:
    status: str
    new_time: int = 0
    rift_data: dict | None = None
    create_time: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class RiftSpeedupService:
    """Atomically consume a speedup item and shorten an active rift."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def apply(
        self,
        operation_id,
        user_id,
        item_id,
        expected_rift=None,
        expected_cd=None,
        remaining_ratio=None,
    ) -> RiftSpeedupResult:
        if isinstance(expected_rift, (int, float)) and isinstance(expected_cd, (int, float)) and remaining_ratio is None:
            return self._apply_legacy(operation_id, user_id, item_id, int(expected_rift), int(expected_cd))
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        if remaining_ratio is None:
            raise ValueError("remaining ratio is required")
        remaining_ratio = int(remaining_ratio)
        if not operation_id or not user_id or item_id <= 0 or not 0 < remaining_ratio < 100:
            raise ValueError("valid operation, user, item and remaining ratio are required")

        expected_time = 0
        expected_snapshot = None
        if expected_rift is None:
            payload = json.dumps([user_id, item_id, remaining_ratio], ensure_ascii=True)
        else:
            expected_rift = dict(expected_rift)
            expected_time = int(expected_rift.get("time", 0))
            if expected_time <= 0 or expected_cd is None:
                raise ValueError("active rift duration and cooldown state are required")
            expected_cd = {
                "type": int(expected_cd.get("type", 0)),
                "create_time": expected_cd.get("create_time"),
                "scheduled_time": expected_cd.get("scheduled_time"),
            }
            expected_snapshot = json.dumps(expected_rift, ensure_ascii=False, sort_keys=True)
            payload = json.dumps(
                [user_id, item_id, expected_snapshot, expected_cd, remaining_ratio],
                ensure_ascii=True,
                sort_keys=True,
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS rift_speedup_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,new_time INTEGER NOT NULL,"
                    "rift_data TEXT NOT NULL,create_time TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                if not conn.column_exists("rift_speedup_operations", "rift_data"):
                    conn.execute("ALTER TABLE rift_speedup_operations ADD COLUMN rift_data TEXT NOT NULL DEFAULT '{}'")
                if not conn.column_exists("rift_speedup_operations", "create_time"):
                    conn.execute("ALTER TABLE rift_speedup_operations ADD COLUMN create_time TEXT")
                old = conn.execute(
                    "SELECT payload,new_time,rift_data,create_time FROM rift_speedup_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return RiftSpeedupResult("state_changed", expected_time, expected_rift)
                    return RiftSpeedupResult("duplicate", int(old[1]), json.loads(str(old[2])), old[3])

                entry = conn.execute(
                    "SELECT rift_data,status,duration FROM rift_entries WHERE user_id=%s", (user_id,)
                ).fetchone()
                cd = conn.execute(
                    "SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=%s", (user_id,)
                ).fetchone()
                if entry is None or str(entry[1]) != "active" or cd is None or int(cd[0]) != 3:
                    conn.rollback()
                    return RiftSpeedupResult("not_active", expected_time, expected_rift)
                current_rift = json.loads(str(entry[0]))
                current_time = int(entry[2])
                current_cd = {
                    "type": int(cd[0]),
                    "create_time": cd[1],
                    "scheduled_time": cd[2],
                }
                if (
                    int(current_rift.get("time", 0)) != current_time
                    or (
                        expected_snapshot is not None
                        and (
                            current_rift != json.loads(expected_snapshot)
                            or current_time != expected_time
                            or current_cd != expected_cd
                        )
                    )
                ):
                    conn.rollback()
                    return RiftSpeedupResult("state_changed", expected_time, expected_rift)
                if current_time <= 10:
                    conn.rollback()
                    return RiftSpeedupResult("not_needed", current_time, current_rift, current_cd["create_time"])

                new_time = max(1, current_time * remaining_ratio // 100)
                updated_rift = dict(current_rift)
                updated_rift["time"] = new_time
                updated_snapshot = json.dumps(updated_rift, ensure_ascii=False, sort_keys=True)
                bind_update = ""
                if conn.column_exists("back", "bind_num"):
                    bind_update = (
                        ",bind_num=MIN("
                        "MAX(COALESCE(bind_num,0)-1,0),goods_num-1)"
                    )
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-1" + bind_update + " "
                    "WHERE user_id=%s AND goods_id=%s AND COALESCE(goods_num,0)>=1",
                    (user_id, item_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return RiftSpeedupResult("item_missing", current_time, current_rift, current_cd["create_time"])
                entry_updated = conn.execute(
                    "UPDATE rift_entries SET rift_data=%s,duration=%s WHERE user_id=%s AND status='active'",
                    (updated_snapshot, new_time, user_id),
                )
                cd_updated = conn.execute(
                    "UPDATE user_cd SET scheduled_time=%s WHERE user_id=%s AND type=3",
                    (new_time, user_id),
                )
                if entry_updated.rowcount != 1 or cd_updated.rowcount != 1:
                    conn.rollback()
                    return RiftSpeedupResult("state_changed", current_time, current_rift, current_cd["create_time"])
                conn.execute(
                    "INSERT INTO rift_speedup_operations(operation_id,payload,new_time,rift_data,create_time) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, new_time, updated_snapshot, current_cd["create_time"]),
                )
                conn.commit()
                return RiftSpeedupResult("applied", new_time, updated_rift, current_cd["create_time"])
            except Exception:
                conn.rollback()
                raise

    def _apply_legacy(self, operation_id, user_id, item_id, expected_time, new_time) -> RiftSpeedupResult:
        """Keep the pre-transaction-service API usable for old callers and tests."""
        operation_id, user_id, item_id = str(operation_id), str(user_id), int(item_id)
        payload = json.dumps([user_id, item_id, expected_time, new_time])
        if not operation_id or not 0 < new_time < expected_time:
            raise ValueError("invalid speedup")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS rift_speedup_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,new_time INTEGER NOT NULL,"
                    "rift_data TEXT NOT NULL DEFAULT '{}',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,new_time FROM rift_speedup_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    return RiftSpeedupResult(
                        "duplicate" if str(old[0]) == payload else "state_changed",
                        int(old[1]) if str(old[0]) == payload else expected_time,
                    )
                row = conn.execute(
                    "SELECT duration FROM rift_entries WHERE user_id=%s AND status='active'", (user_id,)
                ).fetchone()
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                ).fetchone()
                if row is None or int(row[0]) != expected_time:
                    conn.rollback()
                    return RiftSpeedupResult("state_changed", expected_time)
                if item is None or int(item[0]) < 1:
                    conn.rollback()
                    return RiftSpeedupResult("item_missing", expected_time)
                bind_update = ""
                if conn.column_exists("back", "bind_num"):
                    bind_update = (
                        ",bind_num=MIN("
                        "MAX(COALESCE(bind_num,0)-1,0),goods_num-1)"
                    )
                conn.execute(
                    "UPDATE back SET goods_num=goods_num-1" + bind_update + " "
                    "WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                )
                conn.execute("UPDATE rift_entries SET duration=%s WHERE user_id=%s", (new_time, user_id))
                conn.execute("UPDATE user_cd SET scheduled_time=%s WHERE user_id=%s", (new_time, user_id))
                conn.execute(
                    "INSERT INTO rift_speedup_operations(operation_id,payload,new_time) VALUES(%s,%s,%s)",
                    (operation_id, payload, new_time),
                )
                conn.commit()
                return RiftSpeedupResult("applied", new_time)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class RiftSettlementResult:
    status: str
    explore_count: int = 0
    message: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class RiftSettlementService:
    """Commit one pre-rolled ordinary rift event across both databases."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rift_settlement_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "explore_count INTEGER NOT NULL,message TEXT NOT NULL DEFAULT '',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        if not conn.column_exists("rift_settlement_operations", "message"):
            conn.execute(
                "ALTER TABLE rift_settlement_operations "
                "ADD COLUMN message TEXT NOT NULL DEFAULT ''"
            )

    def replay(self, operation_id) -> RiftSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(
            db_backend.connect(self._game_database)
        ) as conn:
            if not conn.table_exists("rift_settlement_operations"):
                return None
            columns = {
                str(row[1])
                for row in conn.execute(
                    "PRAGMA table_info(rift_settlement_operations)"
                ).fetchall()
            }
            message_sql = "message" if "message" in columns else "''"
            row = conn.execute(
                "SELECT explore_count," + message_sql + " "
                "FROM rift_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return RiftSettlementResult("duplicate", int(row[0]), str(row[1]))

    def settle(
        self,
        operation_id,
        user_id,
        expected_rift,
        expected_user,
        expected_explore_count,
        outcome,
        max_goods_num,
    ) -> RiftSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        expected_count = int(expected_explore_count)
        max_goods_num = int(max_goods_num)
        rift_snapshot = json.dumps(
            expected_rift, ensure_ascii=False, sort_keys=True
        )
        expected = tuple(
            int(expected_user.get(key, 0))
            for key in ("stone", "exp", "hp", "mp")
        )
        delta = tuple(
            int(outcome.get("delta", {}).get(key, 0))
            for key in ("stone", "exp", "hp", "mp")
        )
        rewards = tuple(
            (
                int(item["id"]),
                str(item["name"]),
                str(item["type"]),
                int(item.get("amount", 1)),
            )
            for item in outcome.get("items", ())
            if int(item.get("amount", 1)) > 0
        )
        progress_reward = _normalise_progress_reward(
            expected_count, outcome.get("progress_reward")
        )
        if progress_reward is not None:
            rewards += (progress_reward,)
        statistics = tuple(
            sorted(
                (str(key), int(value))
                for key, value in outcome.get("statistics", {}).items()
                if int(value)
            )
        )
        message = str(outcome.get("message", ""))
        if (
            not operation_id
            or not user_id
            or expected_count < 0
            or max_goods_num < 0
            or not message
        ):
            raise ValueError("valid operation, user, outcome and snapshots are required")
        payload = json.dumps(
            [
                user_id,
                rift_snapshot,
                expected,
                expected_count,
                delta,
                rewards,
                statistics,
                message,
                max_goods_num,
            ],
            ensure_ascii=True,
            sort_keys=True,
        )

        with self._lock, closing(
            db_backend.connect(self._game_database)
        ) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data",
                    (str(self._player_database),),
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute(
                    "SELECT payload,explore_count,message "
                    "FROM rift_settlement_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) == payload:
                        return RiftSettlementResult(
                            "duplicate", int(old[1]), str(old[2])
                        )
                    return RiftSettlementResult("state_changed")

                entry = conn.execute(
                    "SELECT rift_data,status FROM rift_entries WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                cd = conn.execute(
                    "SELECT COALESCE(type,0),create_time,scheduled_time "
                    "FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                user = conn.execute(
                    "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if (
                    entry is None
                    or str(entry[1]) != "active"
                    or cd is None
                    or int(cd[0]) != 3
                ):
                    conn.rollback()
                    return RiftSettlementResult("not_active")
                try:
                    expected_duration = int(expected_rift.get("time", 0))
                    scheduled_duration = int(cd[2])
                except (TypeError, ValueError):
                    conn.rollback()
                    return RiftSettlementResult("state_changed")
                elapsed_row = conn.execute(
                    "SELECT (julianday('now')-julianday(%s))*1440.0",
                    (cd[1],),
                ).fetchone()
                if (
                    expected_duration <= 0
                    or scheduled_duration != expected_duration
                    or elapsed_row is None
                    or elapsed_row[0] is None
                ):
                    conn.rollback()
                    return RiftSettlementResult("state_changed")
                if max(0, int(float(elapsed_row[0]))) < expected_duration:
                    conn.rollback()
                    return RiftSettlementResult("not_ready")
                if (
                    json.loads(str(entry[0])) != json.loads(rift_snapshot)
                    or user is None
                    or tuple(map(int, user)) != expected
                ):
                    conn.rollback()
                    return RiftSettlementResult("state_changed")

                _ensure_player_field(conn, "rift", "explore_count", "INTEGER")
                count_row = conn.execute(
                    'SELECT "explore_count" FROM player_data."rift" '
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_count = int(count_row[0] or 0) if count_row else 0
                if current_count != expected_count:
                    conn.rollback()
                    return RiftSettlementResult("state_changed")
                if expected[0] + delta[0] < 0:
                    conn.rollback()
                    return RiftSettlementResult("resource_missing")

                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                for reward_id, name, item_type, amount in rewards:
                    totals[reward_id] = totals.get(reward_id, 0) + amount
                    old_metadata = metadata.setdefault(
                        reward_id, (name, item_type)
                    )
                    if old_metadata != (name, item_type):
                        raise ValueError("conflicting reward metadata")
                for reward_id, amount in totals.items():
                    row = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (user_id, reward_id),
                    ).fetchone()
                    if (int(row[0]) if row else 0) + amount > max_goods_num:
                        conn.rollback()
                        return RiftSettlementResult("inventory_full")

                final_exp = max(0, expected[1] + delta[1])
                final_hp = max(1, expected[2] + delta[2])
                final_mp = max(1, expected[3] + delta[3])
                conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL),exp=%s,hp=%s,mp=%s "
                    "WHERE user_id=%s",
                    (delta[0], final_exp, final_hp, final_mp, user_id),
                )
                now = datetime.now()
                for reward_id, amount in totals.items():
                    name, item_type = metadata[reward_id]
                    conn.execute(
                        "INSERT INTO back("
                        "user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name,"
                        "goods_type=EXCLUDED.goods_type,"
                        "goods_num=back.goods_num+EXCLUDED.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,"
                        "update_time=EXCLUDED.update_time",
                        (
                            user_id,
                            reward_id,
                            name,
                            item_type,
                            amount,
                            now,
                            now,
                            amount,
                        ),
                    )

                new_count = (
                    0 if expected_count + 1 >= 10 else expected_count + 1
                )
                _set_player_field(
                    conn,
                    "rift",
                    user_id,
                    "explore_count",
                    new_count,
                    "INTEGER",
                )
                for key, amount in statistics:
                    _increment_stat(conn, user_id, key, amount)
                conn.execute(
                    "UPDATE rift_entries SET status='settled' WHERE user_id=%s",
                    (user_id,),
                )
                conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL "
                    "WHERE user_id=%s AND type=3",
                    (user_id,),
                )
                conn.execute(
                    "INSERT INTO rift_settlement_operations("
                    "operation_id,payload,explore_count,message) "
                    "VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, new_count, message),
                )
                conn.commit()
                return RiftSettlementResult("applied", new_count, message)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

__all__ = [
    "RiftWorldState",
    "RiftGenerationResult",
    "RiftEntryResult",
    "RiftEntryService",
    "RiftTerminationResult",
    "RiftTerminationService",
    "RiftKeyEventSettlementResult",
    "RiftKeyEventSettlementService",
    "RiftDemonTokenBattleSettlementService",
    "RiftSpeedupResult",
    "RiftSpeedupService",
    "RiftSettlementResult",
    "RiftSettlementService",
]
