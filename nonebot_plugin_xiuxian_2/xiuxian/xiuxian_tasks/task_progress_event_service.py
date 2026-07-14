from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Iterable, Mapping

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class TaskProgressEventResult:
    status: str
    completed: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class TaskProgressEventService:
    """Apply one gameplay event to every matching daily/weekly task atomically."""

    _cycles = ("daily", "weekly")

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiuxian_tasks(user_id TEXT PRIMARY KEY)"
        )
        columns = set(conn.column_names("xiuxian_tasks"))
        for cycle in cls._cycles:
            for suffix in ("period", "progress", "claimed"):
                field = f"{cycle}_{suffix}"
                if field not in columns:
                    conn.execute(
                        f"ALTER TABLE xiuxian_tasks ADD COLUMN "
                        f"{db_backend.quote_ident(field)} TEXT"
                    )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS task_progress_event_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _decode(value, default):
        if value in (None, ""):
            return default
        try:
            decoded = json.loads(str(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return default
        return decoded if isinstance(decoded, type(default)) else default

    @staticmethod
    def _json(value) -> str:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def _normalize_periods(
        cls, periods: Mapping[str, str], required_cycles: Iterable[str]
    ) -> dict[str, str]:
        required = set(required_cycles)
        normalized = {
            cycle: str(periods.get(cycle, "")).strip()
            for cycle in cls._cycles
            if cycle in required
        }
        if set(normalized) != required or any(not value for value in normalized.values()):
            raise ValueError("current task periods are required")
        return normalized

    @staticmethod
    def _normalize_events(events: Iterable[tuple[str, int]]) -> tuple[dict, ...]:
        normalized = []
        seen = set()
        for raw_key, raw_amount in events:
            key = str(raw_key).strip()
            amount = int(raw_amount)
            if not key or amount <= 0:
                raise ValueError("positive task event updates are required")
            if key in seen:
                raise ValueError("task event keys must be unique")
            seen.add(key)
            normalized.append({"key": key, "amount": amount})
        if not normalized:
            raise ValueError("at least one task event update is required")
        return tuple(normalized)

    @classmethod
    def _normalize_tasks(cls, tasks: Iterable[Mapping]) -> tuple[dict, ...]:
        normalized = []
        seen = set()
        for raw in tasks:
            task = dict(raw)
            key = str(task.get("key", "")).strip()
            cycle = str(task.get("cycle", "")).strip()
            name = str(task.get("name", "")).strip()
            target = int(task.get("target", 0))
            amount = int(task.get("amount", 0))
            identity = (cycle, key)
            if (
                not key
                or cycle not in cls._cycles
                or not name
                or target <= 0
                or amount <= 0
                or identity in seen
            ):
                raise ValueError("valid unique task updates are required")
            seen.add(identity)
            normalized.append(
                {
                    "key": key,
                    "cycle": cycle,
                    "name": name,
                    "target": target,
                    "amount": amount,
                }
            )
        return tuple(normalized)

    @classmethod
    def _load_row(cls, conn, user_id: str) -> dict[str, object]:
        fields = [
            f"{cycle}_{suffix}"
            for cycle in cls._cycles
            for suffix in ("period", "progress", "claimed")
        ]
        row = conn.execute(
            "SELECT " + ",".join(fields) + " FROM xiuxian_tasks WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO xiuxian_tasks(user_id) VALUES(%s)", (user_id,)
            )
            return {field: None for field in fields}
        return dict(zip(fields, row))

    @classmethod
    def _cycle_state(cls, values: Mapping[str, object], cycle: str, period: str):
        if str(values.get(f"{cycle}_period") or "") != period:
            return {}, []
        progress = cls._decode(values.get(f"{cycle}_progress"), {})
        claimed = cls._decode(values.get(f"{cycle}_claimed"), [])
        return progress, [str(key) for key in claimed]

    @classmethod
    def _write_states(cls, conn, user_id: str, states: Mapping[str, tuple[str, dict, list]]) -> None:
        if not states:
            return
        assignments = []
        parameters = []
        for cycle in cls._cycles:
            state = states.get(cycle)
            if state is None:
                continue
            period, progress, claimed = state
            assignments.extend(
                (
                    f"{cycle}_period=%s",
                    f"{cycle}_progress=%s",
                    f"{cycle}_claimed=%s",
                )
            )
            parameters.extend((period, cls._json(progress), cls._json(claimed)))
        parameters.append(user_id)
        conn.execute(
            "UPDATE xiuxian_tasks SET " + ",".join(assignments) + " WHERE user_id=%s",
            tuple(parameters),
        )

    def get_states(
        self, user_id: str, periods: Mapping[str, str]
    ) -> dict[str, tuple[dict[str, int], list[str], str]]:
        """Load selected cycles and reset every stale cycle in one transaction."""
        user_id = str(user_id).strip()
        selected = tuple(cycle for cycle in self._cycles if cycle in periods)
        normalized_periods = self._normalize_periods(periods, selected)
        if not user_id or not selected:
            raise ValueError("user and selected task periods are required")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                values = self._load_row(conn, user_id)
                states = {}
                result = {}
                for cycle in selected:
                    period = normalized_periods[cycle]
                    progress, claimed = self._cycle_state(values, cycle, period)
                    states[cycle] = (period, progress, claimed)
                    result[cycle] = (progress, claimed, period)
                self._write_states(conn, user_id, states)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def record(
        self,
        operation_id: str,
        user_id: str,
        events: Iterable[tuple[str, int]],
        periods: Mapping[str, str],
        tasks: Iterable[Mapping],
    ) -> TaskProgressEventResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        normalized_events = self._normalize_events(events)
        normalized_tasks = self._normalize_tasks(tasks)
        cycles = tuple(
            cycle
            for cycle in self._cycles
            if any(task["cycle"] == cycle for task in normalized_tasks)
        )
        normalized_periods = self._normalize_periods(periods, cycles)
        if not operation_id or not user_id:
            raise ValueError("operation and user are required")

        request = {"user_id": user_id, "events": list(normalized_events)}
        payload_data = {
            "request": request,
            "periods": normalized_periods,
            "tasks": normalized_tasks,
        }
        payload = self._json(payload_data)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM task_progress_event_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    try:
                        previous_payload = json.loads(str(previous[0]))
                        previous_result = json.loads(str(previous[1]))
                    except (TypeError, ValueError, json.JSONDecodeError) as exc:
                        raise RuntimeError("invalid stored task progress operation") from exc
                    if previous_payload.get("request") != request:
                        return TaskProgressEventResult("operation_conflict")
                    return TaskProgressEventResult(
                        "duplicate",
                        tuple(str(name) for name in previous_result.get("completed", [])),
                    )

                completed = []
                if normalized_tasks:
                    values = self._load_row(conn, user_id)
                    states = {}
                    for cycle in cycles:
                        period = normalized_periods[cycle]
                        progress, claimed = self._cycle_state(values, cycle, period)
                        claimed_set = set(claimed)
                        for task in normalized_tasks:
                            if task["cycle"] != cycle:
                                continue
                            old_value = max(0, int(progress.get(task["key"], 0) or 0))
                            new_value = min(old_value + task["amount"], task["target"])
                            if new_value != old_value:
                                progress[task["key"]] = new_value
                            if (
                                old_value < task["target"] <= new_value
                                and task["key"] not in claimed_set
                            ):
                                completed.append(task["name"])
                        states[cycle] = (period, progress, claimed)
                    self._write_states(conn, user_id, states)

                result_json = self._json({"completed": completed})
                conn.execute(
                    "INSERT INTO task_progress_event_operations("
                    "operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return TaskProgressEventResult("applied", tuple(completed))
            except Exception:
                conn.rollback()
                raise


__all__ = ["TaskProgressEventResult", "TaskProgressEventService"]
