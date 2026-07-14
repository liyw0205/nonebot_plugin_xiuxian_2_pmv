from __future__ import annotations

import hashlib
import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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
                    if str(previous[0]) != payload:
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
                    if str(previous[0]) != payload:
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
                if user_id in world.participants:
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


__all__ = [
    "RiftEntryResult",
    "RiftEntryService",
    "RiftGenerationResult",
    "RiftWorldState",
]
