from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


FIELDS = (
    "state", "stage", "revision", "alloc", "accumulated", "talent",
    "birth_scenario", "total_score", "score_breakdown", "event_indices",
    "event_snapshots", "early_death_rolls", "history", "last_run_time",
    "total_runs", "best_ending", "best_score", "endings_log",
    "achievement_points",
)
JSON_FIELDS = {
    "alloc", "accumulated", "score_breakdown", "event_indices",
    "event_snapshots", "early_death_rolls", "history", "endings_log",
}
INTEGER_FIELDS = {
    "state", "stage", "revision", "total_score", "total_runs",
    "best_score", "achievement_points",
}
DEFAULT_STATE = {
    "state": 0,
    "stage": 0,
    "revision": 0,
    "alloc": {},
    "accumulated": {},
    "talent": "",
    "birth_scenario": "",
    "total_score": 0,
    "score_breakdown": {},
    "event_indices": [],
    "event_snapshots": [],
    "early_death_rolls": {},
    "history": [],
    "last_run_time": None,
    "total_runs": 0,
    "best_ending": "",
    "best_score": 0,
    "endings_log": [],
    "achievement_points": 0,
}


@dataclass(frozen=True)
class PastLifeResetResult:
    status: str
    data: dict
    operation_id: str = ""
    mode: str = ""
    clear_history: bool = False
    task_status: str = ""
    user_id: str = ""
    total: int = 0
    processed: int = 0
    applied: int = 0
    conflicted: int = 0
    missing: int = 0
    last_error: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "created", "resumed", "duplicate"}

    @property
    def complete(self) -> bool:
        return self.task_status == "completed"


class PastLifeResetSqlRepository:
    """Feature-owned single and resumable all-player past-life reset writes."""

    def __init__(self, game_database: str | Path, player_database: str | Path | None = None):
        self.game_database = str(game_database)
        self.player_database = str(player_database or game_database)

    @staticmethod
    def _canonical(value) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _normalize(cls, value) -> dict:
        source = dict(value or {})
        state = {}
        for field in FIELDS:
            raw = source.get(field, DEFAULT_STATE[field])
            if raw is None:
                raw = DEFAULT_STATE[field]
            if field in JSON_FIELDS and isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (TypeError, ValueError, json.JSONDecodeError):
                    pass
            if field in INTEGER_FIELDS:
                try:
                    raw = int(raw)
                except (TypeError, ValueError):
                    raw = DEFAULT_STATE[field]
            state[field] = raw
        return state

    @classmethod
    def _encode(cls, field: str, value):
        return cls._canonical(value) if field in JSON_FIELDS else value

    @classmethod
    def _reset_state(cls, current: dict, clear_history: bool) -> dict:
        final = dict(current)
        for field in (
            "state", "stage", "alloc", "accumulated", "talent", "birth_scenario",
            "total_score", "score_breakdown", "event_indices", "event_snapshots",
            "early_death_rolls", "history", "last_run_time",
        ):
            final[field] = DEFAULT_STATE[field]
        final["revision"] = int(current.get("revision", 0) or 0) + 1
        if clear_history:
            for field in ("total_runs", "best_ending", "best_score", "endings_log", "achievement_points"):
                final[field] = DEFAULT_STATE[field]
        return cls._normalize(final)

    @classmethod
    def _payload(cls, mode: str, clear_history: bool, user_id: str = "") -> str:
        return cls._canonical({"mode": mode, "clear_history": bool(clear_history), "user_id": user_id})

    @classmethod
    def _ensure_schema(cls, uow: DatabaseUnitOfWork) -> set[str]:
        uow.execute("CREATE TABLE IF NOT EXISTS player_data.past_life(user_id TEXT PRIMARY KEY)")
        columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(past_life)")}
        for field in FIELDS:
            if field not in columns:
                kind = "INTEGER" if field in INTEGER_FIELDS else "TEXT"
                uow.execute(f'ALTER TABLE player_data.past_life ADD COLUMN "{field}" {kind} DEFAULT NULL')
        uow.execute(
            "CREATE TABLE IF NOT EXISTS past_life_reset_operations("
            "operation_id TEXT PRIMARY KEY,mode TEXT NOT NULL DEFAULT 'single',"
            "user_id TEXT NOT NULL DEFAULT '',clear_history INTEGER NOT NULL DEFAULT 0,"
            "payload TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'completed',"
            "total INTEGER NOT NULL DEFAULT 0,processed INTEGER NOT NULL DEFAULT 0,"
            "applied INTEGER NOT NULL DEFAULT 0,conflicted INTEGER NOT NULL DEFAULT 0,"
            "missing INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',"
            "created_at TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL DEFAULT '')"
        )
        operation_columns = {
            str(row["name"]) for row in uow.query_all("PRAGMA table_info(past_life_reset_operations)")
        }
        additions = {
            "mode": "TEXT NOT NULL DEFAULT 'single'",
            "user_id": "TEXT NOT NULL DEFAULT ''",
            "clear_history": "INTEGER NOT NULL DEFAULT 0",
            "payload": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT 'completed'",
            "total": "INTEGER NOT NULL DEFAULT 0",
            "processed": "INTEGER NOT NULL DEFAULT 0",
            "applied": "INTEGER NOT NULL DEFAULT 0",
            "conflicted": "INTEGER NOT NULL DEFAULT 0",
            "missing": "INTEGER NOT NULL DEFAULT 0",
            "last_error": "TEXT NOT NULL DEFAULT ''",
            "updated_at": "TEXT NOT NULL DEFAULT ''",
        }
        for field, definition in additions.items():
            if field not in operation_columns:
                uow.execute(f'ALTER TABLE past_life_reset_operations ADD COLUMN "{field}" {definition}')
        uow.execute(
            "CREATE TABLE IF NOT EXISTS past_life_reset_targets("
            "operation_id TEXT NOT NULL,ordinal INTEGER NOT NULL,user_id TEXT NOT NULL,"
            "expected_json TEXT NOT NULL,final_json TEXT NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'pending',error_text TEXT NOT NULL DEFAULT '',"
            "updated_at TEXT NOT NULL,PRIMARY KEY(operation_id,user_id))"
        )
        return operation_columns | set(additions)

    @classmethod
    def _read_state(cls, uow: DatabaseUnitOfWork, user_id: str) -> tuple[dict, bool]:
        row = uow.query_one("SELECT * FROM player_data.past_life WHERE user_id=?", (user_id,))
        return (cls._normalize(dict(row)), True) if row is not None else (cls._normalize(None), False)

    @classmethod
    def _write_state(cls, uow: DatabaseUnitOfWork, user_id: str, state: dict, exists: bool) -> None:
        values = [cls._encode(field, state[field]) for field in FIELDS]
        if exists:
            changed = uow.execute(
                "UPDATE player_data.past_life SET "
                + ",".join(f'"{field}"=?' for field in FIELDS)
                + " WHERE user_id=?",
                (*values, user_id),
            )
            if changed.rowcount != 1:
                raise RuntimeError("past life reset target disappeared")
            return
        uow.execute(
            "INSERT INTO player_data.past_life(user_id,"
            + ",".join(f'"{field}"' for field in FIELDS)
            + ") VALUES("
            + ",".join("?" for _ in range(len(FIELDS) + 1))
            + ")",
            (user_id, *values),
        )

    @classmethod
    def _result(cls, uow: DatabaseUnitOfWork, operation_id: str, status: str) -> PastLifeResetResult:
        row = uow.query_one(
            "SELECT mode,clear_history,status,user_id,total,processed,applied,"
            "conflicted,missing,last_error FROM past_life_reset_operations WHERE operation_id=?",
            (operation_id,),
        )
        if row is None:
            return PastLifeResetResult(status, {}, operation_id=operation_id)
        data = {
            "operation_id": operation_id,
            "mode": str(row["mode"] or ""),
            "clear_history": bool(int(row["clear_history"] or 0)),
            "task_status": str(row["status"] or ""),
            "total": int(row["total"] or 0),
            "processed": int(row["processed"] or 0),
            "applied": int(row["applied"] or 0),
            "conflicted": int(row["conflicted"] or 0),
            "missing": int(row["missing"] or 0),
            "last_error": str(row["last_error"] or ""),
        }
        return PastLifeResetResult(
            status, data, operation_id=operation_id, mode=data["mode"],
            clear_history=data["clear_history"], task_status=data["task_status"],
            user_id=str(row["user_id"] or ""), total=data["total"],
            processed=data["processed"], applied=data["applied"],
            conflicted=data["conflicted"], missing=data["missing"],
            last_error=data["last_error"],
        )

    @staticmethod
    def _insert_operation(uow: DatabaseUnitOfWork, columns: set[str], values: dict) -> None:
        # Older feature tests created a result_json-only table; include it when present.
        if "result_json" in columns:
            values = {**values, "result_json": values.get("result_json", "{}")}
        fields = [field for field in values if field in columns]
        uow.execute(
            "INSERT INTO past_life_reset_operations(" + ",".join(fields) + ") VALUES("
            + ",".join("?" for _ in fields) + ")",
            tuple(values[field] for field in fields),
        )

    def reset_one(self, operation_id, user_id, clear_history=False):
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        clear_history = bool(clear_history)
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        payload = self._payload("single", clear_history, user_id)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            columns = self._ensure_schema(uow)
            previous = uow.query_one(
                "SELECT payload FROM past_life_reset_operations WHERE operation_id=?", (operation_id,)
            )
            if previous is not None:
                status = "duplicate" if str(previous["payload"]) == payload else "operation_conflict"
                return self._result(uow, operation_id, status)
            current, exists = self._read_state(uow, user_id)
            final = self._reset_state(current, clear_history)
            self._write_state(uow, user_id, final, exists)
            result = {"revision": final["revision"], "clear_history": clear_history}
            self._insert_operation(
                uow, columns,
                {
                    "operation_id": operation_id, "mode": "single", "user_id": user_id,
                    "clear_history": int(clear_history), "payload": payload, "status": "completed",
                    "total": 1, "processed": 1, "applied": 1,
                    "created_at": "", "updated_at": "",
                    "result_json": self._canonical(result),
                },
            )
            return PastLifeResetResult(
                "applied", result, operation_id=operation_id, mode="single", user_id=user_id,
                total=1, processed=1, applied=1, task_status="completed",
            )

    def reset_all_create(self, operation_id, clear_history=False):
        operation_id = str(operation_id).strip()
        clear_history = bool(clear_history)
        if not operation_id:
            raise ValueError("operation_id is required")
        payload = self._payload("all", clear_history)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            columns = self._ensure_schema(uow)
            previous = uow.query_one(
                "SELECT payload,status FROM past_life_reset_operations WHERE operation_id=?", (operation_id,)
            )
            if previous is not None:
                status = "operation_conflict" if str(previous["payload"]) != payload else (
                    "duplicate" if str(previous["status"]) == "completed" else "resumed"
                )
                return self._result(uow, operation_id, status)
            running = uow.query_one(
                "SELECT operation_id,clear_history FROM past_life_reset_operations "
                "WHERE mode='all' AND status='running' ORDER BY created_at,operation_id LIMIT 1"
            )
            if running is not None:
                status = "resumed" if bool(int(running["clear_history"] or 0)) == clear_history else "operation_conflict"
                return self._result(uow, str(running["operation_id"]), status)
            targets = []
            for row in uow.query_all("SELECT * FROM player_data.past_life ORDER BY user_id"):
                state = self._normalize(row)
                user = str(row["user_id"])
                targets.append((user, self._canonical(state), self._canonical(self._reset_state(state, clear_history))))
            task_status = "completed" if not targets else "running"
            self._insert_operation(
                uow, columns,
                {
                    "operation_id": operation_id, "mode": "all", "clear_history": int(clear_history),
                    "payload": payload, "status": task_status, "total": len(targets),
                    "created_at": "", "updated_at": "",
                },
            )
            uow.executemany(
                "INSERT INTO past_life_reset_targets(operation_id,ordinal,user_id,expected_json,final_json,updated_at) VALUES(?,?,?,?,?,?)",
                [(operation_id, index, user, expected, final, "") for index, (user, expected, final) in enumerate(targets)],
            )
            return self._result(uow, operation_id, "created")

    def reset_all_batch(self, operation_id, *, batch_size=500):
        try:
            return self._reset_all_batch(operation_id, batch_size=batch_size)
        except Exception as exc:
            self._record_error(str(operation_id).strip(), str(exc))
            raise

    def _reset_all_batch(self, operation_id, *, batch_size=500):
        operation_id = str(operation_id).strip()
        batch_size = max(1, int(batch_size))
        if not operation_id:
            raise ValueError("operation_id is required")
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            self._ensure_schema(uow)
            operation = uow.query_one(
                "SELECT mode,status FROM past_life_reset_operations WHERE operation_id=?", (operation_id,)
            )
            if operation is None:
                return PastLifeResetResult("not_found", {}, operation_id=operation_id)
            if str(operation["mode"]) != "all":
                return self._result(uow, operation_id, "operation_conflict")
            if str(operation["status"]) == "completed":
                return self._result(uow, operation_id, "duplicate")
            targets = uow.query_all(
                "SELECT user_id,expected_json,final_json FROM past_life_reset_targets "
                "WHERE operation_id=? AND status='pending' ORDER BY ordinal LIMIT ?",
                (operation_id, batch_size),
            )
            for target in targets:
                user = str(target["user_id"])
                current, exists = self._read_state(uow, user)
                if not exists:
                    target_status, error = "missing", "state_missing"
                elif self._canonical(current) != str(target["expected_json"]):
                    target_status, error = "conflict", "state_changed"
                else:
                    self._write_state(uow, user, self._normalize(json.loads(str(target["final_json"]))), True)
                    target_status, error = "applied", ""
                uow.execute(
                    "UPDATE past_life_reset_targets SET status=?,error_text=?,updated_at=? "
                    "WHERE operation_id=? AND user_id=? AND status='pending'",
                    (target_status, error, "", operation_id, user),
                )
            counts = uow.query_one(
                "SELECT COUNT(*) AS total,"
                "COALESCE(SUM(CASE WHEN status!='pending' THEN 1 ELSE 0 END),0) AS processed,"
                "COALESCE(SUM(CASE WHEN status='applied' THEN 1 ELSE 0 END),0) AS applied,"
                "COALESCE(SUM(CASE WHEN status='conflict' THEN 1 ELSE 0 END),0) AS conflicted,"
                "COALESCE(SUM(CASE WHEN status='missing' THEN 1 ELSE 0 END),0) AS missing,"
                "COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) AS pending "
                "FROM past_life_reset_targets WHERE operation_id=?",
                (operation_id,),
            )
            status = "completed" if int(counts["pending"]) == 0 else "running"
            uow.execute(
                "UPDATE past_life_reset_operations SET status=?,total=?,processed=?,applied=?,conflicted=?,missing=?,last_error='',updated_at='' WHERE operation_id=?",
                (status, int(counts["total"]), int(counts["processed"]), int(counts["applied"]), int(counts["conflicted"]), int(counts["missing"]), operation_id),
            )
            return self._result(uow, operation_id, "applied")

    def _record_error(self, operation_id: str, error: str) -> None:
        if not operation_id:
            return
        try:
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                uow.execute(
                    "UPDATE past_life_reset_operations SET last_error=? WHERE operation_id=?",
                    (error, operation_id),
                )
        except Exception:
            pass

    def find_pending_all(self) -> PastLifeResetResult | None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            row = uow.query_one(
                "SELECT operation_id FROM past_life_reset_operations "
                "WHERE mode='all' AND status='running' ORDER BY created_at,operation_id LIMIT 1"
            )
            if row is None:
                return None
            return self._result(uow, str(row["operation_id"]), "resumed")


__all__ = ["PastLifeResetResult", "PastLifeResetSqlRepository"]
