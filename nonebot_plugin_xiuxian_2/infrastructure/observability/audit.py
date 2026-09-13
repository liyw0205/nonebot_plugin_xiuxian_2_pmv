from __future__ import annotations

from pathlib import Path
from typing import Any

from ..clock import SystemClock
from ..database import DatabaseUnitOfWork
from .events import observation_fields


class AuditLogger:
    """Append-only audit records for adapter-level administrative actions."""

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    def record(
        self,
        *,
        request_id: str,
        method: str,
        path: str,
        status: int,
        actor: str = "",
        operation_id: str | None = None,
        job_id: str | None = None,
        user_scope: str | None = None,
        duration_ms: int | None = None,
    ) -> None:
        fields = observation_fields(
            request_id=request_id,
            operation_id=operation_id,
            job_id=job_id,
            user_scope=user_scope or actor,
            duration_ms=duration_ms,
        )
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE IF NOT EXISTS web_audit ("
                "audit_id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL, "
                "operation_id TEXT NOT NULL DEFAULT '', job_id TEXT NOT NULL DEFAULT '', "
                "user_scope TEXT NOT NULL DEFAULT '', method TEXT NOT NULL, path TEXT NOT NULL, "
                "status INTEGER NOT NULL, duration_ms INTEGER NOT NULL DEFAULT 0, "
                "actor TEXT NOT NULL, created_at TEXT NOT NULL)"
            )
            columns = {
                str(row["name"])
                for row in uow.query_all("PRAGMA table_info(web_audit)")
            }
            # The audit table predates the unified correlation contract in
            # existing deployments; upgrade it in place before appending.
            for name, definition in (
                ("operation_id", "TEXT NOT NULL DEFAULT ''"),
                ("job_id", "TEXT NOT NULL DEFAULT ''"),
                ("user_scope", "TEXT NOT NULL DEFAULT ''"),
                ("duration_ms", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if name not in columns:
                    uow.execute(f"ALTER TABLE web_audit ADD COLUMN {name} {definition}")
            uow.execute(
                "INSERT INTO web_audit(request_id, operation_id, job_id, user_scope, method, path, status, duration_ms, actor, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    fields["request_id"],
                    fields["operation_id"],
                    fields["job_id"],
                    fields["user_scope"],
                    str(method).upper(),
                    str(path),
                    int(status),
                    fields["duration_ms"],
                    str(actor)[:128],
                    self.clock.now().isoformat(),
                ),
            )

    def list(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='web_audit'")
            if row is None:
                return []
            return [dict(item) for item in uow.query_all("SELECT * FROM web_audit ORDER BY audit_id DESC LIMIT ?", (max(1, min(int(limit), 1000)),))]


__all__ = ["AuditLogger"]
