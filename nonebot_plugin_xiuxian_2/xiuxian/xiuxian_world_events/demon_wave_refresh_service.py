from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from ..xiuxian_utils import db_backend


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


__all__ = ["DemonWaveRefreshResult", "DemonWaveRefreshService", "STATE_FIELDS"]
