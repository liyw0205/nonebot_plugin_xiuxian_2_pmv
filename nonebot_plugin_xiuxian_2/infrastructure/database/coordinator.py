"""Best-effort coordinator for operations spanning SQLite files."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from ...core.result import OperationOutcome
from ..observability import trace_context
from .ledger import OperationLedger, OutboxStore
from .uow import DatabaseUnitOfWork


@dataclass(frozen=True)
class DatabaseStep:
    name: str
    database: str
    apply: Callable[[DatabaseUnitOfWork], Any]


class CrossDatabaseCoordinator:
    """Execute subordinate commits with one operation ID and reconciliation.

    SQLite cannot provide a transaction across files.  The coordinator makes
    that fact explicit: the primary ledger is prepared first, every step gets
    its own UoW, and a failed step leaves a ``needs_reconcile`` record rather
    than claiming success.
    """

    def __init__(self, catalog: Any, ledger: OperationLedger | None = None, outbox: OutboxStore | None = None) -> None:
        self.catalog = catalog
        self.ledger = ledger or OperationLedger()
        self.outbox = outbox or OutboxStore()

    def execute(
        self,
        *,
        operation_id: str,
        action: str,
        payload: Any,
        steps: Iterable[DatabaseStep],
        outcome: OperationOutcome[Any],
    ) -> OperationOutcome[Any]:
        with trace_context(operation_id=operation_id):
            return self._execute(operation_id=operation_id, action=action, payload=payload, steps=steps, outcome=outcome)

    def _execute(
        self,
        *,
        operation_id: str,
        action: str,
        payload: Any,
        steps: Iterable[DatabaseStep],
        outcome: OperationOutcome[Any],
    ) -> OperationOutcome[Any]:
        primary = self.catalog.path("game_db")
        with DatabaseUnitOfWork(primary) as uow:
            existing = self.ledger.begin(uow, operation_id, action, payload)
            if existing is not None:
                previous = existing.outcome()
                if previous is not None:
                    return previous.replay()
                raise RuntimeError("operation is already in progress")
            self.outbox.append(
                uow,
                event_id=f"{operation_id}:{action}",
                aggregate_type="operation",
                aggregate_id=operation_id,
                event_type=action,
                payload=dict(payload) if isinstance(payload, dict) else {"value": payload},
            )
        completed: list[str] = []
        current_step: DatabaseStep | None = None
        try:
            for step in steps:
                current_step = step
                with self.catalog.unit_of_work(step.database) as uow:
                    step.apply(uow)
                completed.append(step.name)
        except Exception as exc:
            failed = OperationOutcome.failed(
                operation_id,
                action,
                f"跨数据库步骤失败：{current_step.name if current_step else 'unknown'}: {exc}",
                code="needs_reconcile",
                data={"completed": completed, "failed_step": current_step.name if current_step else "unknown"},
                audit_category="cross_database_failure",
            )
            with DatabaseUnitOfWork(primary) as uow:
                self.ledger.finish(self._ensure_record(uow, operation_id, action, payload), failed)
            return failed
        with DatabaseUnitOfWork(primary) as uow:
            self.ledger.finish(self._ensure_record(uow, operation_id, action, payload), outcome)
            self.outbox.mark_sent(uow, f"{operation_id}:{action}")
        return outcome

    def _ensure_record(self, uow: DatabaseUnitOfWork, operation_id: str, action: str, payload: Any):
        record = self.ledger.get(uow, operation_id, action)
        if record is None:
            self.ledger.begin(uow, operation_id, action, payload)
        return uow


__all__ = ["CrossDatabaseCoordinator", "DatabaseStep"]
