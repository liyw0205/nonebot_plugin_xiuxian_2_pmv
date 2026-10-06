from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Iterator


_RUN_LOCK = RLock()
_TERMINAL = frozenset({"applied", "unchanged", "resolve_failed", "conflict"})
_REQUIRED_COLUMNS = {
    "admin_qqid_batches": {"batch_id", "operator_id", "status", "total", "created_at", "updated_at"},
    "admin_qqid_batch_requests": {"request_id", "batch_id"},
    "admin_qqid_batch_entries": {
        "batch_id", "source_id", "ordinal", "target_id", "child_operation_id",
        "attempt", "status", "result_json",
    },
}


class QqidBatchSchemaError(RuntimeError):
    code = "schema_missing"


class AdminQqidBatchRepository:
    """Durable orchestration only; ID mutation remains owned by the single writer."""

    def __init__(self, game_db: str | Path) -> None:
        self.database = Path(game_db)

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        if not self.database.is_file():
            raise QqidBatchSchemaError("QQID batch database missing")
        mode = "rw" if write else "ro"
        connection = sqlite3.connect(
            f"{self.database.resolve().as_uri()}?mode={mode}", uri=True, timeout=30,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            for table, required in _REQUIRED_COLUMNS.items():
                columns = {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")}
                if not required.issubset(columns):
                    raise QqidBatchSchemaError(f"QQID batch schema missing: {table}")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def exclusive_run(self) -> Iterator[None]:
        with self._connection():
            pass
        # This lock must never reuse the single-ID writer's flock path.
        with _RUN_LOCK:
            lock_path = self.database.with_name(".admin-qqid-batch.lock")
            with lock_path.open("a+b") as handle:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0, os.SEEK_END)
                    if not handle.tell():
                        handle.write(b"0")
                        handle.flush()
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    if os.name == "nt":
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _child_id(batch_id: str, ordinal: int, attempt: int = 0) -> str:
        digest = hashlib.sha256(batch_id.encode("utf-8")).hexdigest()
        return f"admin-qqid:{digest}:{ordinal}:{attempt}"

    @staticmethod
    def _entry(row) -> dict:
        if row is None:
            raise ValueError("QQID batch entry missing")
        entry = dict(row)
        entry["result"] = json.loads(entry.pop("result_json"))
        if not isinstance(entry["result"], dict):
            raise ValueError("QQID batch result must be an object")
        return entry

    @staticmethod
    def _active(connection) -> dict | None:
        row = connection.execute("SELECT * FROM admin_qqid_batches WHERE status='active'").fetchone()
        return dict(row) if row is not None else None

    def get_active(self) -> dict | None:
        with self._connection() as connection:
            return self._active(connection)

    @staticmethod
    def _get(connection, request_id: str) -> dict | None:
        row = connection.execute("SELECT * FROM admin_qqid_batches WHERE batch_id=?", (request_id,)).fetchone()
        if row is None:
            row = connection.execute(
                "SELECT b.* FROM admin_qqid_batches b JOIN admin_qqid_batch_requests r "
                "ON r.batch_id=b.batch_id WHERE r.request_id=?", (request_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def get(self, request_id: str) -> dict | None:
        with self._connection() as connection:
            return self._get(connection, request_id)

    def bind_request(self, request_id: str, batch_id: str) -> dict:
        request_id = str(request_id).strip()
        if not request_id:
            raise ValueError("QQID request identity missing")
        with self._connection(write=True) as connection:
            target = connection.execute("SELECT * FROM admin_qqid_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if target is None:
                raise ValueError("QQID batch missing")
            own = connection.execute("SELECT batch_id FROM admin_qqid_batches WHERE batch_id=?", (request_id,)).fetchone()
            alias = connection.execute(
                "SELECT batch_id FROM admin_qqid_batch_requests WHERE request_id=?", (request_id,),
            ).fetchone()
            if any(row is not None and row["batch_id"] != batch_id for row in (own, alias)):
                raise ValueError("QQID request is already bound to another batch")
            if alias is None:
                connection.execute(
                    "INSERT INTO admin_qqid_batch_requests(request_id,batch_id) VALUES (?,?)", (request_id, batch_id),
                )
            return dict(target)

    def create(self, batch_id: str, operator_id: str, candidates) -> dict:
        batch_id, operator_id = str(batch_id).strip(), str(operator_id).strip()
        if not batch_id or not operator_id:
            raise ValueError("QQID batch identity missing")
        with self._connection(write=True) as connection:
            previous = self._get(connection, batch_id)
            if previous is not None:
                return previous
            active = self._active(connection)
            if active is not None:
                return active
            frozen = tuple(sorted({str(value).strip() for value in candidates if str(value).strip()}))
            connection.execute(
                "INSERT INTO admin_qqid_batches(batch_id,operator_id,status,total) VALUES (?,?,'active',?)",
                (batch_id, operator_id, len(frozen)),
            )
            connection.executemany(
                "INSERT INTO admin_qqid_batch_entries "
                "(batch_id,source_id,ordinal,child_operation_id,status) VALUES (?,?,?,?,'pending')",
                ((batch_id, source, ordinal, self._child_id(batch_id, ordinal)) for ordinal, source in enumerate(frozen)),
            )
            return dict(connection.execute("SELECT * FROM admin_qqid_batches WHERE batch_id=?", (batch_id,)).fetchone())

    def entries(self, batch_id: str) -> list[dict]:
        with self._connection() as connection:
            return [self._entry(row) for row in connection.execute(
                "SELECT * FROM admin_qqid_batch_entries WHERE batch_id=? ORDER BY ordinal", (batch_id,),
            )]

    def _find_entry(self, connection, batch_id: str, source_id: str) -> dict:
        return self._entry(connection.execute(
            "SELECT * FROM admin_qqid_batch_entries WHERE batch_id=? AND source_id=?", (batch_id, source_id),
        ).fetchone())

    @staticmethod
    def _require_active(connection, batch_id: str) -> None:
        if connection.execute(
            "SELECT 1 FROM admin_qqid_batches WHERE batch_id=? AND status='active'", (batch_id,),
        ).fetchone() is None:
            raise ValueError("QQID batch is not active")

    def freeze_resolution(
        self, batch_id: str, source_id: str, target_id: str | None, error_code: str = "",
    ) -> dict:
        target_id = str(target_id).strip() if target_id is not None else None
        target_id = target_id or None
        with self._connection(write=True) as connection:
            self._require_active(connection, batch_id)
            entry = self._find_entry(connection, batch_id, source_id)
            if entry["status"] != "pending":
                if entry["target_id"] != target_id:
                    raise ValueError("QQID frozen resolution cannot change")
                return entry
            status = "resolved"
            if target_id is None:
                status, error_code = "resolve_failed", error_code or "resolution_failed"
            elif target_id == source_id:
                status = "unchanged"
            elif connection.execute(
                "SELECT 1 FROM admin_qqid_batch_entries WHERE batch_id=? AND source_id=?", (batch_id, target_id),
            ).fetchone() is not None:
                status, error_code = "conflict", "candidate_target_occupied"
            duplicates = [] if target_id is None else connection.execute(
                "SELECT source_id,status FROM admin_qqid_batch_entries "
                "WHERE batch_id=? AND target_id=? AND source_id<>?", (batch_id, target_id, source_id),
            ).fetchall()
            if duplicates:
                if any(row["status"] == "applied" for row in duplicates):
                    raise ValueError("QQID mapping conflicts with an applied entry")
                status, error_code = "conflict", "duplicate_target"
                connection.execute(
                    "UPDATE admin_qqid_batch_entries SET status='conflict',result_json=? "
                    "WHERE batch_id=? AND target_id=? AND source_id<>?",
                    (json.dumps({"code": error_code}), batch_id, target_id, source_id),
                )
            connection.execute(
                "UPDATE admin_qqid_batch_entries SET target_id=?,status=?,result_json=? "
                "WHERE batch_id=? AND source_id=?",
                (target_id, status, json.dumps({"code": error_code} if error_code else {}), batch_id, source_id),
            )
            return self._find_entry(connection, batch_id, source_id)

    def record_result(self, batch_id: str, source_id: str, status: str, result: dict) -> dict:
        if status not in _TERMINAL | {"failed"} or not isinstance(result, dict):
            raise ValueError("invalid QQID batch result")
        encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self._connection(write=True) as connection:
            self._require_active(connection, batch_id)
            entry = self._find_entry(connection, batch_id, source_id)
            if entry["status"] in _TERMINAL:
                if entry["status"] != status or entry["result"] != result:
                    raise ValueError("QQID terminal progress cannot change")
                return entry
            if entry["status"] not in {"resolved", "failed"}:
                raise ValueError("QQID resolution must be frozen before mutation")
            connection.execute(
                "UPDATE admin_qqid_batch_entries SET status=?,result_json=? WHERE batch_id=? AND source_id=?",
                (status, encoded, batch_id, source_id),
            )
            connection.execute("UPDATE admin_qqid_batches SET updated_at=CURRENT_TIMESTAMP WHERE batch_id=?", (batch_id,))
            return self._find_entry(connection, batch_id, source_id)

    def retry_rejected(self, batch_id: str, source_id: str) -> dict:
        with self._connection(write=True) as connection:
            self._require_active(connection, batch_id)
            entry = self._find_entry(connection, batch_id, source_id)
            if not (
                entry["status"] == "failed" and entry["result"].get("status") == "rejected"
                and entry["result"].get("code") == "reconcile_pending"
            ):
                raise ValueError("only a no-write reconcile_pending rejection may change attempt")
            attempt = int(entry["attempt"]) + 1
            connection.execute(
                "UPDATE admin_qqid_batch_entries SET attempt=?,child_operation_id=?,status='resolved',result_json='{}' "
                "WHERE batch_id=? AND source_id=?",
                (attempt, self._child_id(batch_id, entry["ordinal"], attempt), batch_id, source_id),
            )
            return self._find_entry(connection, batch_id, source_id)

    def complete(self, batch_id: str) -> dict:
        with self._connection(write=True) as connection:
            row = connection.execute("SELECT * FROM admin_qqid_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if row is None:
                raise ValueError("QQID batch missing")
            if row["status"] == "completed":
                return dict(row)
            count = connection.execute(
                "SELECT COUNT(*) AS total,SUM(CASE WHEN status IN ('pending','resolved','failed') THEN 1 ELSE 0 END) AS pending "
                "FROM admin_qqid_batch_entries WHERE batch_id=?", (batch_id,),
            ).fetchone()
            if count["total"] != row["total"] or count["pending"]:
                raise ValueError("QQID batch has unfinished entries")
            connection.execute(
                "UPDATE admin_qqid_batches SET status='completed',updated_at=CURRENT_TIMESTAMP WHERE batch_id=?", (batch_id,),
            )
            return dict(connection.execute("SELECT * FROM admin_qqid_batches WHERE batch_id=?", (batch_id,)).fetchone())


__all__ = ["AdminQqidBatchRepository", "QqidBatchSchemaError"]
