from __future__ import annotations

import json
from contextlib import closing
from dataclasses import asdict, dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
from datetime import datetime
from ..xiuxian_utils.numeric_bind import as_int_like, number_count

STATE_FIELDS = (
    "active", "status", "event_id", "event_type", "name", "period", "manual",
    "bosses", "participants", "claimed", "started_at", "ends_at", "last_result",
)
JSON_FIELDS = {"bosses", "participants", "claimed"}
INTEGER_FIELDS = {"active", "manual"}

def _decode(field: str, value):
    if field in JSON_FIELDS:
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
    if field in INTEGER_FIELDS:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0
    return str(value or "")

def _encode(field: str, value):
    if field in JSON_FIELDS:
        return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)
    if field in INTEGER_FIELDS:
        return int(value or 0)
    return str(value or "")

@dataclass(frozen=True)
class DemonWaveRefreshResult:
    status: str
    refreshed_realms: tuple[str, ...] = ()
    state: dict | None = None

class DemonWaveRefreshService:
    def __init__(self, player_db: str | Path):
        self.player_db = Path(player_db)

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS world_event_state (user_id TEXT PRIMARY KEY)")
        columns = set(conn.column_names("world_event_state"))
        for field in STATE_FIELDS:
            if field not in columns:
                data_type = "INTEGER" if field in INTEGER_FIELDS else "TEXT"
                conn.execute(f'ALTER TABLE world_event_state ADD COLUMN "{field}" {data_type}')
        conn.execute(
            "CREATE TABLE IF NOT EXISTS demon_wave_refresh_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
        )

    @staticmethod
    def _read_state(conn, event_key: str) -> dict | None:
        fields = ",".join(f'"{field}"' for field in STATE_FIELDS)
        row = conn.execute(f"SELECT {fields} FROM world_event_state WHERE user_id=%s", (event_key,)).fetchone()
        if row is None:
            return None
        return {field: _decode(field, row[index]) for index, field in enumerate(STATE_FIELDS)}

    @staticmethod
    def _write_state(conn, event_key: str, state: dict) -> None:
        assignments = ",".join(f'"{field}"=%s' for field in STATE_FIELDS)
        values = [_encode(field, state.get(field)) for field in STATE_FIELDS]
        changed = conn.execute(
            f"UPDATE world_event_state SET {assignments} WHERE user_id=%s",
            (*values, event_key),
        )
        if changed.rowcount == 0:
            fields = ",".join(["user_id", *[f'"{field}"' for field in STATE_FIELDS]])
            marks = ",".join("%s" for _ in range(len(STATE_FIELDS) + 1))
            conn.execute(f"INSERT INTO world_event_state ({fields}) VALUES ({marks})", (event_key, *values))

    @classmethod
    def _verify_state(cls, conn, event_key: str, expected: dict) -> None:
        if cls._read_state(conn, event_key) != expected:
            raise RuntimeError("demon wave refresh state verification failed")

    def replay(self, operation_id: str) -> DemonWaveRefreshResult | None:
        conn = db_backend.connect(self.player_db)
        try:
            self._ensure_schema(conn)
            row = conn.execute(
                "SELECT result_json FROM demon_wave_refresh_operations WHERE operation_id=%s",
                (str(operation_id),),
            ).fetchone()
            if row is None:
                return None
            data = json.loads(str(row[0]))
            data["refreshed_realms"] = tuple(data.get("refreshed_realms") or ())
            return DemonWaveRefreshResult(**data)
        finally:
            conn.close()

    def refresh(
        self,
        operation_id: str,
        event_key: str,
        expected_state: dict,
        next_bosses: dict[str, dict],
        last_result: str,
    ) -> DemonWaveRefreshResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        expected = {field: _decode(field, expected_state.get(field)) for field in STATE_FIELDS}
        payload = json.dumps(
            {"event_key": str(event_key), "expected_state": expected, "next_bosses": next_bosses, "last_result": last_result},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        conn = db_backend.connect(self.player_db)
        try:
            self._ensure_schema(conn)
            conn.execute("BEGIN IMMEDIATE")
            previous = conn.execute(
                "SELECT payload,result_json FROM demon_wave_refresh_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous:
                if str(previous[0]) != payload:
                    conn.rollback()
                    return DemonWaveRefreshResult("operation_conflict")
                data = json.loads(str(previous[1]))
                data["refreshed_realms"] = tuple(data.get("refreshed_realms") or ())
                conn.commit()
                return DemonWaveRefreshResult(**data)

            current = self._read_state(conn, str(event_key))
            if current != expected or current is None or current.get("status") != "active":
                conn.rollback()
                return DemonWaveRefreshResult("state_changed")

            bosses = dict(current.get("bosses") or {})
            participants = {key: dict(value) for key, value in (current.get("participants") or {}).items()}
            defeated = []
            for realm, boss in bosses.items():
                if int(boss.get("boss_hp") or 0) > 0:
                    continue
                wave = max(int(boss.get("wave") or 1), 1)
                replacement = next_bosses.get(realm)
                if not replacement or int(replacement.get("wave") or 0) != wave + 1:
                    conn.rollback()
                    return DemonWaveRefreshResult("invalid_plan")
                reward_base_hp = max(int(boss.get("boss_max_hp") or 0), 1)
                for record in participants.values():
                    if record.get("realm") != realm or int(record.get("wave") or 1) != wave or int(record.get("damage") or 0) <= 0:
                        continue
                    record["reward_ready"] = 1
                    record["reward_wave"] = wave
                    record["reward_base_hp"] = max(int(record.get("reward_base_hp") or 0), reward_base_hp)
                    record["reward_total_damage"] = record["reward_base_hp"]
                    if "reward_contribution" not in record:
                        base = min(max(int(record.get("damage") or 0) / record["reward_base_hp"], 0.0), 1.0)
                        record["base_contribution"] = base
                        record["reward_contribution"] = min(base * max(float(record.get("reward_multiplier") or 1.0), 1.0), 1.0)
                bosses[realm] = replacement
                defeated.append(realm)

            if set(next_bosses) != set(defeated):
                conn.rollback()
                return DemonWaveRefreshResult("invalid_plan")
            updated = dict(current)
            updated["bosses"] = bosses
            updated["participants"] = participants
            if defeated:
                updated["last_result"] = str(last_result)
                self._write_state(conn, str(event_key), updated)
                self._verify_state(conn, str(event_key), updated)
            result = DemonWaveRefreshResult("applied", tuple(defeated), updated)
            conn.execute(
                "INSERT INTO demon_wave_refresh_operations VALUES (%s,%s,%s,CURRENT_TIMESTAMP)",
                (operation_id, payload, json.dumps(asdict(result), ensure_ascii=False, sort_keys=True)),
            )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

@dataclass(frozen=True)
class DemonEventLifecycleResult:
    status: str
    action: str = ""
    state: dict | None = None

class DemonEventLifecycleService:
    def __init__(self, player_db: str | Path):
        self.player_db = Path(player_db)

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS world_event_state (user_id TEXT PRIMARY KEY)")
        columns = set(conn.column_names("world_event_state"))
        for field in STATE_FIELDS:
            if field not in columns:
                data_type = "INTEGER" if field in INTEGER_FIELDS else "TEXT"
                conn.execute(f'ALTER TABLE world_event_state ADD COLUMN "{field}" {data_type}')
        conn.execute(
            "CREATE TABLE IF NOT EXISTS demon_event_lifecycle_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
        )

    @staticmethod
    def _read_state(conn, event_key: str) -> dict | None:
        fields = ",".join(f'"{field}"' for field in STATE_FIELDS)
        row = conn.execute(f"SELECT {fields} FROM world_event_state WHERE user_id=%s", (event_key,)).fetchone()
        if row is None:
            return None
        return {field: _decode(field, row[index]) for index, field in enumerate(STATE_FIELDS)}

    @staticmethod
    def _write_state(conn, event_key: str, state: dict) -> None:
        assignments = ",".join(f'"{field}"=%s' for field in STATE_FIELDS)
        values = [_encode(field, state.get(field)) for field in STATE_FIELDS]
        changed = conn.execute(f"UPDATE world_event_state SET {assignments} WHERE user_id=%s", (*values, event_key))
        if changed.rowcount == 0:
            fields = ",".join(["user_id", *[f'"{field}"' for field in STATE_FIELDS]])
            marks = ",".join("%s" for _ in range(len(STATE_FIELDS) + 1))
            conn.execute(f"INSERT INTO world_event_state ({fields}) VALUES ({marks})", (event_key, *values))

    @classmethod
    def _verify_state(cls, conn, event_key: str, expected: dict) -> None:
        if cls._read_state(conn, event_key) != expected:
            raise RuntimeError("demon lifecycle state verification failed")

    def replay(self, operation_id: str) -> DemonEventLifecycleResult | None:
        conn = db_backend.connect(self.player_db)
        try:
            self._ensure_schema(conn)
            row = conn.execute(
                "SELECT result_json FROM demon_event_lifecycle_operations WHERE operation_id=%s", (str(operation_id),),
            ).fetchone()
            return None if row is None else DemonEventLifecycleResult(**json.loads(str(row[0])))
        finally:
            conn.close()

    def transition(self, operation_id, event_key, action, expected_state, target_state):
        operation_id, action = str(operation_id).strip(), str(action).strip()
        if not operation_id or action not in {"auto_start", "manual_start", "auto_finish", "manual_finish"}:
            raise ValueError("invalid lifecycle operation")
        expected = None if expected_state is None else {field: _decode(field, expected_state.get(field)) for field in STATE_FIELDS}
        target = {field: _decode(field, target_state.get(field)) for field in STATE_FIELDS}
        payload = json.dumps(
            {"event_key": str(event_key), "action": action, "expected_state": expected, "target_state": target},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        conn = db_backend.connect(self.player_db)
        try:
            self._ensure_schema(conn)
            conn.execute("BEGIN IMMEDIATE")
            previous = conn.execute(
                "SELECT payload,result_json FROM demon_event_lifecycle_operations WHERE operation_id=%s", (operation_id,),
            ).fetchone()
            if previous:
                if str(previous[0]) != payload:
                    conn.rollback()
                    return DemonEventLifecycleResult("operation_conflict", action)
                conn.commit()
                return DemonEventLifecycleResult(**json.loads(str(previous[1])))
            current = self._read_state(conn, str(event_key))
            first_start = current is None and action.endswith("start") and expected is not None
            first_start = first_start and expected.get("status") == "idle" and not expected.get("event_id")
            if current != expected and not first_start:
                conn.rollback()
                return DemonEventLifecycleResult("state_changed", action, current)
            if action.endswith("start"):
                valid = target.get("status") == "active" and int(target.get("active") or 0) == 1 and bool(target.get("event_id"))
                valid = valid and not (current and current.get("status") == "active")
            else:
                valid = current is not None and current.get("status") == "active"
                valid = valid and target.get("event_id") == current.get("event_id")
                valid = valid and target.get("status") == "finished" and int(target.get("active") or 0) == 0
            if not valid:
                conn.rollback()
                return DemonEventLifecycleResult("invalid_transition", action, current)
            self._write_state(conn, str(event_key), target)
            self._verify_state(conn, str(event_key), target)
            result = DemonEventLifecycleResult("applied", action, target)
            conn.execute(
                "INSERT INTO demon_event_lifecycle_operations VALUES (%s,%s,%s,CURRENT_TIMESTAMP)",
                (operation_id, payload, json.dumps(asdict(result), ensure_ascii=False, sort_keys=True)),
            )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

@dataclass(frozen=True)
class SpiritVeinLifecycleResult:
    status: str
    action: str = ""
    state: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status not in {"operation_conflict", "state_changed", "invalid_transition"}

class SpiritVeinLifecycleService:
    _ACTIONS = {
        "auto_start",
        "auto_skip",
        "auto_miss",
        "manual_start",
        "manual_start_skip",
        "manual_finish",
        "manual_finish_skip",
        "expire",
    }
    _NOOP_ACTIONS = {
        "auto_skip",
        "auto_miss",
        "manual_start_skip",
        "manual_finish_skip",
    }
    _RESULT_STATUS = {
        "auto_start": "applied",
        "auto_skip": "already_active",
        "auto_miss": "not_triggered",
        "manual_start": "applied",
        "manual_start_skip": "already_active",
        "manual_finish": "applied",
        "manual_finish_skip": "already_finished",
        "expire": "applied",
    }

    def __init__(self, player_db: str | Path) -> None:
        self.player_db = Path(player_db)

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS world_event_state (user_id TEXT PRIMARY KEY)")
        columns = set(conn.column_names("world_event_state"))
        for field in STATE_FIELDS:
            if field not in columns:
                data_type = "INTEGER" if field in INTEGER_FIELDS else "TEXT"
                conn.execute(f'ALTER TABLE world_event_state ADD COLUMN "{field}" {data_type}')
        conn.execute(
            "CREATE TABLE IF NOT EXISTS spirit_vein_lifecycle_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
        )

    @staticmethod
    def _normalize(state: dict | None) -> dict | None:
        if state is None:
            return None
        return {field: _decode(field, state.get(field)) for field in STATE_FIELDS}

    @staticmethod
    def _read_state(conn, event_key: str) -> dict | None:
        fields = ",".join(f'"{field}"' for field in STATE_FIELDS)
        row = conn.execute(
            f"SELECT {fields} FROM world_event_state WHERE user_id=%s",
            (event_key,),
        ).fetchone()
        if row is None:
            return None
        return {
            field: _decode(field, row[index])
            for index, field in enumerate(STATE_FIELDS)
        }

    @staticmethod
    def _write_state(conn, event_key: str, state: dict) -> None:
        assignments = ",".join(f'"{field}"=%s' for field in STATE_FIELDS)
        values = [_encode(field, state.get(field)) for field in STATE_FIELDS]
        changed = conn.execute(
            f"UPDATE world_event_state SET {assignments} WHERE user_id=%s",
            (*values, event_key),
        )
        if changed.rowcount == 0:
            fields = ",".join(["user_id", *[f'"{field}"' for field in STATE_FIELDS]])
            marks = ",".join("%s" for _ in range(len(STATE_FIELDS) + 1))
            conn.execute(
                f"INSERT INTO world_event_state ({fields}) VALUES ({marks})",
                (event_key, *values),
            )

    @classmethod
    def _verify_state(cls, conn, event_key: str, expected: dict) -> None:
        if cls._read_state(conn, event_key) != expected:
            raise RuntimeError("spirit vein lifecycle state verification failed")

    @staticmethod
    def _parse_time(value) -> datetime | None:
        try:
            return datetime.fromisoformat(str(value)) if value else None
        except ValueError:
            return None

    @classmethod
    def _valid_transition(cls, action: str, current: dict | None, target: dict) -> bool:
        current_state = current or target
        if action in cls._NOOP_ACTIONS:
            if target != current_state:
                return False
            if action in {"auto_skip", "manual_start_skip"}:
                return current is not None and current.get("status") == "active"
            if action == "auto_miss":
                return current is None or current.get("status") != "active"
            return current is None or current.get("status") != "active"

        if action in {"auto_start", "manual_start"}:
            started_at = cls._parse_time(target.get("started_at"))
            ends_at = cls._parse_time(target.get("ends_at"))
            return (
                (current is None or current.get("status") != "active")
                and target.get("status") == "active"
                and int(target.get("active") or 0) == 1
                and target.get("event_type") == "spirit_vein"
                and bool(target.get("event_id"))
                and started_at is not None
                and ends_at is not None
                and started_at < ends_at
            )

        return (
            current is not None
            and current.get("status") == "active"
            and target.get("event_id") == current.get("event_id")
            and target.get("started_at") == current.get("started_at")
            and target.get("ends_at") == current.get("ends_at")
            and target.get("status") == "finished"
            and int(target.get("active") or 0) == 0
        )

    def replay(self, operation_id: str) -> SpiritVeinLifecycleResult | None:
        conn = db_backend.connect(self.player_db)
        try:
            self._ensure_schema(conn)
            row = conn.execute(
                "SELECT result_json FROM spirit_vein_lifecycle_operations "
                "WHERE operation_id=%s",
                (str(operation_id),),
            ).fetchone()
            if row is None:
                return None
            return SpiritVeinLifecycleResult(**json.loads(str(row[0])))
        finally:
            conn.close()

    def transition(
        self,
        operation_id,
        event_key,
        action,
        expected_state,
        target_state,
    ) -> SpiritVeinLifecycleResult:
        operation_id = str(operation_id).strip()
        event_key = str(event_key).strip()
        action = str(action).strip()
        if not operation_id or not event_key or action not in self._ACTIONS:
            raise ValueError("invalid spirit vein lifecycle operation")
        expected = self._normalize(expected_state)
        target = self._normalize(target_state)
        if target is None:
            raise ValueError("target state is required")
        payload = json.dumps(
            {
                "event_key": event_key,
                "action": action,
                "expected_state": expected,
                "target_state": target,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        conn = db_backend.connect(self.player_db)
        try:
            self._ensure_schema(conn)
            conn.execute("BEGIN IMMEDIATE")
            previous = conn.execute(
                "SELECT payload,result_json FROM spirit_vein_lifecycle_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is not None:
                if str(previous[0]) != payload:
                    conn.rollback()
                    return SpiritVeinLifecycleResult("operation_conflict", action)
                result = SpiritVeinLifecycleResult(**json.loads(str(previous[1])))
                conn.commit()
                return result

            current = self._read_state(conn, event_key)
            first_idle_state = (
                current is None
                and expected is not None
                and expected.get("status") == "idle"
                and not expected.get("event_id")
            )
            if current != expected and not first_idle_state:
                conn.rollback()
                return SpiritVeinLifecycleResult("state_changed", action, current)
            effective_current = current if current is not None else expected
            if not self._valid_transition(action, effective_current, target):
                conn.rollback()
                return SpiritVeinLifecycleResult(
                    "invalid_transition",
                    action,
                    effective_current,
                )

            if action not in self._NOOP_ACTIONS:
                self._write_state(conn, event_key, target)
                self._verify_state(conn, event_key, target)
            result = SpiritVeinLifecycleResult(
                self._RESULT_STATUS[action],
                action,
                target,
            )
            conn.execute(
                "INSERT INTO spirit_vein_lifecycle_operations "
                "VALUES(%s,%s,%s,CURRENT_TIMESTAMP)",
                (
                    operation_id,
                    payload,
                    json.dumps(
                        asdict(result),
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

def _json_value(value, default):
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return default

def _integer(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

@dataclass(frozen=True)
class DemonAttackSettlementResult:
    status: str
    real_damage: int = 0
    boss_now_hp: int = 0
    boss_all_hp: int = 1
    killed: bool = False
    pursuit_mode: bool = False
    contribution_ratio: float = 0.0
    reward_multiplier: float = 1.0
    total_contribution: float = 0.0

class DemonAttackSettlementService:
    def __init__(self, player_db):
        self.player_db = player_db

    def get_result(self, operation_id: str) -> DemonAttackSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        conn = db_backend.connect(self.player_db)
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS demon_attack_settlement_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
            )
            previous = conn.execute(
                "SELECT result_json FROM demon_attack_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            data = json.loads(str(previous[0]))
            data["status"] = "duplicate"
            return DemonAttackSettlementResult(**data)
        finally:
            conn.close()

    @staticmethod
    def _participant_key(user_id, realm, wave):
        return f"{realm}:{max(_integer(wave, 1), 1)}:{user_id}"

    @staticmethod
    def _count_attacks(participants, user_id):
        return sum(
            max(_integer(record.get("attacks")), 0)
            for record in participants.values()
            if str(record.get("user_id")) == user_id
        )

    @staticmethod
    def _claimed(claimed, user_id, record_key):
        if claimed.get(user_id) or claimed.get(record_key):
            return True
        return any(value and str(key).endswith(f":{user_id}") for key, value in claimed.items())

    @staticmethod
    def _increment_stat(conn, user_id, key, amount):
        key_sql = db_backend.quote_ident(key)
        conn.execute("CREATE TABLE IF NOT EXISTS statistics (user_id TEXT PRIMARY KEY)")
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(statistics)").fetchall()}
        if key not in columns:
            conn.execute(f"ALTER TABLE statistics ADD COLUMN {key_sql} INTEGER")
        changed = conn.execute(
            f"UPDATE statistics SET {key_sql}=COALESCE({key_sql},0)+%s WHERE user_id=%s",
            (as_int_like(amount), user_id),
        )
        if changed.rowcount == 0:
            conn.execute(
                f"INSERT INTO statistics (user_id,{key_sql}) VALUES (%s,%s)",
                (user_id, as_int_like(amount)),
            )

    def settle(
        self,
        operation_id,
        event_key,
        user_id,
        user_name,
        realm,
        total_damage,
        expected_event,
        expected_boss,
        expected_participants,
        *,
        attack_limit,
        real_hp_multiplier,
        max_damage_ratio,
        max_pursuit_ratio,
    ):
        operation_id = str(operation_id).strip()
        event_key, user_id, realm = str(event_key), str(user_id), str(realm)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        # Request identity only; mutable boss/participants snapshots are concurrency checks.
        payload = json.dumps(
            {
                "event_key": event_key,
                "user_id": user_id,
                "realm": realm,
                "event_id": str(expected_event.get("event_id") or ""),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        conn = db_backend.connect(self.player_db)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS demon_attack_settlement_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
            )
            previous = conn.execute(
                "SELECT payload,result_json FROM demon_attack_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous:
                if str(previous[0]) != payload:
                    conn.rollback()
                    return DemonAttackSettlementResult("operation_conflict")
                conn.commit()
                data = json.loads(str(previous[1]))
                data["status"] = "duplicate"
                return DemonAttackSettlementResult(**data)

            row = conn.execute(
                "SELECT status,event_id,bosses,participants,claimed FROM world_event_state WHERE user_id=%s",
                (event_key,),
            ).fetchone()
            if row is None:
                conn.rollback()
                return DemonAttackSettlementResult("state_changed")
            status, event_id = str(row[0] or ""), str(row[1] or "")
            bosses = _json_value(row[2], {})
            participants = _json_value(row[3], {})
            claimed = _json_value(row[4], {})
            boss = bosses.get(realm)
            if (
                status != str(expected_event.get("status") or "")
                or event_id != str(expected_event.get("event_id") or "")
                or status != "active"
                or boss != expected_boss
                or participants != expected_participants
            ):
                conn.rollback()
                return DemonAttackSettlementResult("state_changed")

            wave = max(_integer(boss.get("wave"), 1), 1)
            record_key = self._participant_key(user_id, realm, wave)
            if self._claimed(claimed, user_id, record_key) or self._count_attacks(participants, user_id) >= int(attack_limit):
                conn.rollback()
                return DemonAttackSettlementResult("already_settled")

            boss_all_hp = max(_integer(boss.get("boss_max_hp")), 1)
            reward_multiplier = max(float(boss.get("reward_multiplier") or 1.0), 1.0)
            current_hp = max(_integer(boss.get("boss_hp")), 0)
            pursuit_mode = current_hp <= 0
            ratio = float(max_pursuit_ratio if pursuit_mode else max_damage_ratio)
            maximum = max(int(boss_all_hp * ratio), 1)
            raw_damage = max(int(total_damage), 0) * int(real_hp_multiplier)
            real_damage = min(raw_damage, maximum) if pursuit_mode else min(raw_damage, maximum, current_hp)
            boss_now_hp = current_hp if pursuit_mode else max(current_hp - real_damage, 0)
            killed = not pursuit_mode and boss_now_hp <= 0
            contribution_ratio = min(max(real_damage / boss_all_hp, 0.0), 1.0)

            boss = dict(boss)
            boss["boss_hp"] = boss_now_hp
            battle_hp = boss.get("battle_max_hp", boss.get("battle_hp", 1))
            boss["battle_hp"] = battle_hp
            boss["气血"] = battle_hp
            boss["总血量"] = boss.get("battle_max_hp", boss.get("总血量", battle_hp))
            if pursuit_mode:
                boss["last_result"] = f"{user_name or user_id}追击了{realm}魔修。"
            elif killed:
                boss["battle_hp"] = 0
                boss["气血"] = 0
                boss["last_result"] = f"{user_name or user_id}击退了{realm}魔修。"
            bosses = dict(bosses)
            bosses[realm] = boss

            participants = dict(participants)
            record = dict(participants.get(record_key) or {})
            record.update({"user_id": user_id, "realm": realm, "wave": wave, "name": user_name or user_id})
            record["damage"] = _integer(record.get("damage")) + real_damage
            record["attacks"] = _integer(record.get("attacks")) + 1
            record["reward_base_hp"] = max(_integer(record.get("reward_base_hp")), boss_all_hp, 1)
            record["reward_total_damage"] = record["reward_base_hp"]
            settlement_contribution = contribution_ratio * reward_multiplier
            record["reward_multiplier"] = max(float(record.get("reward_multiplier") or 1.0), reward_multiplier)
            record["base_contribution"] = min(float(record.get("base_contribution") or 0.0) + contribution_ratio, 1.0)
            record["reward_contribution"] = min(float(record.get("reward_contribution") or 0.0) + settlement_contribution, 1.0)
            mode = "pursuit" if pursuit_mode else "normal"
            record[f"{mode}_damage"] = _integer(record.get(f"{mode}_damage")) + real_damage
            record[f"{mode}_contribution"] = min(float(record.get(f"{mode}_contribution") or 0.0) + settlement_contribution, 1.0)
            record[f"{mode}_attacks"] = _integer(record.get(f"{mode}_attacks")) + 1
            if killed:
                record["last_hit"] = 1
            participants[record_key] = record

            if pursuit_mode or killed:
                for item in participants.values():
                    if item.get("realm") == realm and max(_integer(item.get("wave"), 1), 1) == wave and _integer(item.get("damage")) > 0:
                        item["reward_ready"] = 1
                        item["reward_wave"] = wave
                        item["reward_base_hp"] = max(_integer(item.get("reward_base_hp")), boss_all_hp)
                        item["reward_total_damage"] = item["reward_base_hp"]

            total_contribution = min(
                sum(
                    max(float(item.get("reward_contribution") or 0.0), 0.0)
                    for item in participants.values()
                    if str(item.get("user_id")) == user_id and _integer(item.get("damage")) > 0
                ),
                1.0,
            )
            result = DemonAttackSettlementResult(
                "applied", real_damage, boss_now_hp, boss_all_hp, killed, pursuit_mode,
                contribution_ratio, reward_multiplier, total_contribution,
            )
            conn.execute(
                "UPDATE world_event_state SET bosses=%s,participants=%s WHERE user_id=%s",
                (json.dumps(bosses, ensure_ascii=False), json.dumps(participants, ensure_ascii=False), event_key),
            )
            self._increment_stat(conn, user_id, "魔修入侵参与", 1)
            if real_damage > 0:
                self._increment_stat(conn, user_id, "魔修入侵伤害", real_damage)
            if killed:
                self._increment_stat(conn, user_id, "魔修入侵击退", 1)
            conn.execute(
                "INSERT INTO demon_attack_settlement_operations VALUES (%s,%s,%s,CURRENT_TIMESTAMP)",
                (operation_id, payload, json.dumps(asdict(result), ensure_ascii=False)),
            )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

@dataclass(frozen=True)
class DemonClaimResult:
    status: str
    stone: int = 0
    exp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DemonClaimService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def _ensure_ops(self, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS demon_claim_operations ("
            "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
            "stone INTEGER NOT NULL DEFAULT 0, exp INTEGER NOT NULL DEFAULT 0, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(demon_claim_operations)").fetchall()}
        if "stone" not in cols:
            try:
                conn.execute("ALTER TABLE demon_claim_operations ADD COLUMN stone INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass
        if "exp" not in cols:
            try:
                conn.execute("ALTER TABLE demon_claim_operations ADD COLUMN exp INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass

    def get_result(self, operation_id: str) -> DemonClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_ops(conn)
            old = conn.execute(
                "SELECT COALESCE(stone,0),COALESCE(exp,0) FROM demon_claim_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            return DemonClaimResult("duplicate", as_int_like(old[0]), as_int_like(old[1]))

    def claim(
        self,
        operation_id,
        event_key,
        event_id,
        user_id,
        expected_claimed,
        stone,
        exp,
        items,
        max_goods_num,
    ):
        operation_id, event_key, event_id, user_id = map(str, (operation_id, event_key, event_id, user_id))
        # as_int_like: high-realm reward may exceed SQLite INTEGER; number_count for bind.
        stone = max(0, as_int_like(stone))
        exp = max(0, as_int_like(exp))
        max_goods_num = int(max_goods_num)
        claimed = dict(expected_claimed)
        rewards = tuple(
            (int(x["id"]), str(x["name"]), str(x["type"]), int(x["amount"]))
            for x in items
            if int(x.get("amount", 0)) > 0
        )
        if not operation_id or max_goods_num < 0:
            raise ValueError("valid claim and rewards are required")
        # Request identity only; claimed map / reward amounts are concurrency/outcome.
        payload = json.dumps(
            [event_key, event_id, user_id],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        stone_bind = number_count(stone)
        exp_bind = number_count(exp)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_ops(conn)
                old = conn.execute(
                    "SELECT payload,COALESCE(stone,0),COALESCE(exp,0) FROM demon_claim_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return DemonClaimResult("state_changed")
                    return DemonClaimResult("duplicate", as_int_like(old[1]), as_int_like(old[2]))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return DemonClaimResult("user_missing")
                row = conn.execute(
                    "SELECT event_id, claimed FROM player_data.world_event_state WHERE user_id=%s",
                    (event_key,),
                ).fetchone()
                if row is None or str(row[0]) != event_id:
                    conn.rollback()
                    return DemonClaimResult("state_changed")
                try:
                    current = json.loads(str(row[1])) if row[1] else {}
                except (TypeError, ValueError):
                    conn.rollback()
                    return DemonClaimResult("state_changed")
                if current != claimed:
                    conn.rollback()
                    return DemonClaimResult("state_changed")
                if current.get(user_id):
                    conn.rollback()
                    return DemonClaimResult("already_claimed")
                for item_id, _, _, amount in rewards:
                    inv = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(inv[0]) if inv else 0) + amount > max_goods_num:
                        conn.rollback()
                        return DemonClaimResult("inventory_full")
                current[user_id] = True
                conn.execute(
                    "UPDATE player_data.world_event_state SET claimed=%s WHERE user_id=%s",
                    (json.dumps(current, ensure_ascii=False), event_key),
                )
                # REAL cast: CAST(... AS INTEGER) clamps >2**63-1 to max int then +reward
                # wipes high-realm exp (e.g. 5654京 → 978京 for 无敌 after 领取魔修奖励).
                conn.execute(
                    "UPDATE user_xiuxian SET "
                    "stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL), "
                    "exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL) "
                    "WHERE user_id=%s",
                    (stone_bind, exp_bind, user_id),
                )
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_num=back.goods_num+EXCLUDED.goods_num, "
                        "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, "
                        "update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                conn.execute(
                    "INSERT INTO demon_claim_operations(operation_id,payload,stone,exp) "
                    "VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, stone_bind, exp_bind),
                )
                conn.commit()
                return DemonClaimResult("applied", stone, exp)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

__all__ = [
    "DemonWaveRefreshResult",
    "DemonWaveRefreshService",
    "DemonEventLifecycleResult",
    "DemonEventLifecycleService",
    "SpiritVeinLifecycleResult",
    "SpiritVeinLifecycleService",
    "DemonAttackSettlementResult",
    "DemonAttackSettlementService",
    "DemonClaimResult",
    "DemonClaimService",
    "STATE_FIELDS",
    "JSON_FIELDS",
    "INTEGER_FIELDS",
    "_decode",
    "_encode",
]
