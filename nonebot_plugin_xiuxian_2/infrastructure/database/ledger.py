from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ..clock import SystemClock
from ..observability import emit
from .uow import DatabaseUnitOfWork


def request_hash(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _now(clock: Any | None = None) -> str:
    return (clock or SystemClock()).now().isoformat()


@dataclass(frozen=True)
class OperationRecord:
    operation_id: str
    action: str
    request_hash: str
    status: str
    result_json: str | None
    created_at: str
    updated_at: str

    def outcome(self) -> OperationOutcome[Any] | None:
        if not self.result_json:
            return None
        raw = json.loads(self.result_json)
        return OperationOutcome(
            status=raw["status"],
            operation_id=raw["operation_id"],
            action=raw["action"],
            data=raw.get("data"),
            code=raw.get("code"),
            message=raw.get("message", ""),
            before=raw.get("before", {}),
            after=raw.get("after", {}),
            consumed=raw.get("consumed", {}),
            granted=raw.get("granted", {}),
            audit_category=raw.get("audit_category", ""),
            occurred_at=raw.get("occurred_at", _now()),
            replayed=True,
        )


class OperationLedger:
    """Idempotency and audit state stored in the same primary transaction."""

    def __init__(self, *, clock: Any | None = None) -> None:
        self.clock = clock or SystemClock()

    def _now(self) -> str:
        return _now(self.clock)

    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            """
            CREATE TABLE IF NOT EXISTS operation_ledger (
                operation_id TEXT NOT NULL,
                action TEXT NOT NULL,
                request_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(operation_id, action)
            )
            """
        )
        uow.execute(
            """
            CREATE TABLE IF NOT EXISTS operation_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id TEXT NOT NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                before_json TEXT NOT NULL DEFAULT '{}',
                after_json TEXT NOT NULL DEFAULT '{}',
                consumed_json TEXT NOT NULL DEFAULT '{}',
                granted_json TEXT NOT NULL DEFAULT '{}',
                category TEXT NOT NULL DEFAULT '',
                occurred_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

    def get(self, uow: DatabaseUnitOfWork, operation_id: str, action: str) -> OperationRecord | None:
        row = uow.query_one(
            "SELECT operation_id, action, request_hash, status, result_json, created_at, updated_at "
            "FROM operation_ledger WHERE operation_id = ? AND action = ?",
            (operation_id, action),
        )
        return OperationRecord(**row) if row else None

    def begin(self, uow: DatabaseUnitOfWork, operation_id: str, action: str, payload: Any) -> OperationRecord | None:
        """Insert a started row; return an existing record for safe replay."""
        if not str(operation_id).strip() or not str(action).strip():
            raise ValueError("operation_id and action are required")
        digest = request_hash(payload)
        existing = self.get(uow, operation_id, action)
        if existing:
            if existing.request_hash != digest:
                raise OperationConflictError(operation_id, action)
            if existing.status in {"failed", "needs_reconcile"}:
                # A failed attempt is retryable with the same request.  The
                # original failure remains in audit history and the new
                # started row is committed with the caller's transaction.
                now = self._now()
                uow.execute(
                    "UPDATE operation_ledger SET status = 'started', result_json = NULL, updated_at = ? "
                    "WHERE operation_id = ? AND action = ?",
                    (now, operation_id, action),
                )
                return None
            return existing
        now = self._now()
        uow.execute(
            "INSERT INTO operation_ledger(operation_id, action, request_hash, status, created_at, updated_at) "
            "VALUES (?, ?, ?, 'started', ?, ?)",
            (operation_id, action, digest, now, now),
        )
        return None

    def finish(self, uow: DatabaseUnitOfWork, outcome: OperationOutcome[Any]) -> None:
        encoded = json.dumps(outcome.to_dict(), ensure_ascii=False, sort_keys=True, default=str)
        uow.execute(
            "UPDATE operation_ledger SET status = ?, result_json = ?, updated_at = ? "
            "WHERE operation_id = ? AND action = ?",
            (outcome.status, encoded, self._now(), outcome.operation_id, outcome.action),
        )
        occurred = outcome.occurred_at or self._now()
        uow.execute(
            "INSERT INTO operation_audit(operation_id, action, status, before_json, after_json, consumed_json, granted_json, category, occurred_at, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                outcome.operation_id,
                outcome.action,
                outcome.status,
                json.dumps(dict(outcome.before), ensure_ascii=False, default=str),
                json.dumps(dict(outcome.after), ensure_ascii=False, default=str),
                json.dumps(dict(outcome.consumed), ensure_ascii=False, default=str),
                json.dumps(dict(outcome.granted), ensure_ascii=False, default=str),
                outcome.audit_category,
                occurred,
                self._now(),
            ),
        )
        emit(
            "info" if outcome.ok else "warning",
            "operation outcome",
            operation_id=outcome.operation_id,
            operation_action=outcome.action,
            operation_status=outcome.status,
            audit_category=outcome.audit_category,
        )

    def record_failure(
        self,
        database: str | Path,
        operation_id: str,
        action: str,
        payload: Any,
        error: str,
    ) -> None:
        """Persist an exception after the business transaction rolled back."""
        digest = request_hash(payload)
        now = self._now()
        outcome = OperationOutcome.failed(
            operation_id,
            action,
            error,
            code="internal_error",
            audit_category="failure",
            occurred_at=now,
        )
        with DatabaseUnitOfWork(database) as uow:
            existing = self.get(uow, operation_id, action)
            if existing is not None:
                if existing.request_hash != digest:
                    raise OperationConflictError(operation_id, action)
                if existing.status in {"applied", "rejected"}:
                    return
            uow.execute(
                "INSERT INTO operation_ledger(operation_id, action, request_hash, status, result_json, created_at, updated_at) "
                "VALUES (?, ?, ?, 'failed', ?, ?, ?) "
                "ON CONFLICT(operation_id, action) DO UPDATE SET request_hash=excluded.request_hash, status='failed', result_json=excluded.result_json, updated_at=excluded.updated_at",
                (operation_id, action, digest, json.dumps(outcome.to_dict(), ensure_ascii=False, default=str), now, now),
            )
            self.finish(uow, outcome)

    def list_pending(self, uow: DatabaseUnitOfWork, *, limit: int = 100) -> list[Mapping[str, Any]]:
        return uow.query_all(
            "SELECT * FROM operation_ledger WHERE status IN ('started', 'failed', 'needs_reconcile') ORDER BY created_at LIMIT ?",
            (max(1, min(int(limit), 1000)),),
        )


class OutboxStore:
    def __init__(self, *, clock: Any | None = None) -> None:
        self.clock = clock or SystemClock()

    def _now(self) -> str:
        return _now(self.clock)

    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            """
            CREATE TABLE IF NOT EXISTS domain_outbox (
                event_id TEXT PRIMARY KEY,
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    def append(
        self,
        uow: DatabaseUnitOfWork,
        *,
        event_id: str,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> None:
        now = self._now()
        uow.execute(
            "INSERT OR IGNORE INTO domain_outbox "
            "(event_id, aggregate_type, aggregate_id, event_type, payload_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (event_id, aggregate_type, aggregate_id, event_type, json.dumps(dict(payload), ensure_ascii=False, default=str), now, now),
        )

    def pending(self, uow: DatabaseUnitOfWork, *, limit: int = 100) -> list[Mapping[str, Any]]:
        return uow.query_all(
            "SELECT * FROM domain_outbox WHERE status = 'pending' "
            "AND (next_attempt_at IS NULL OR next_attempt_at <= ?) ORDER BY created_at LIMIT ?",
            (self._now(), max(1, min(int(limit), 1000))),
        )

    def mark_sent(self, uow: DatabaseUnitOfWork, event_id: str) -> None:
        uow.execute("UPDATE domain_outbox SET status = 'sent', updated_at = ? WHERE event_id = ?", (self._now(), event_id))

    def mark_failed(self, uow: DatabaseUnitOfWork, event_id: str, *, next_attempt_at: str | None = None) -> None:
        uow.execute(
            "UPDATE domain_outbox SET status = 'pending', attempts = attempts + 1, next_attempt_at = ?, updated_at = ? WHERE event_id = ?",
            (next_attempt_at, self._now(), event_id),
        )

    def mark_dead(self, uow: DatabaseUnitOfWork, event_id: str) -> None:
        uow.execute(
            "UPDATE domain_outbox SET status = 'dead', updated_at = ? WHERE event_id = ?",
            (self._now(), event_id),
        )


__all__ = ["OperationLedger", "OperationRecord", "OutboxStore", "request_hash"]
