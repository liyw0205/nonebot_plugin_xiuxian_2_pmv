from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable, Mapping
from typing import Any

from ...core.result import OperationOutcome
from .ledger import OperationLedger, OutboxStore
from .uow import DatabaseUnitOfWork


def _table_exists(uow: DatabaseUnitOfWork, table: str) -> bool:
    row = uow.query_one(
        "SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    )
    return row is not None


@dataclass(frozen=True)
class ReconcileReport:
    operations: int
    outbox_events: int
    details: tuple[dict[str, Any], ...] = ()
    dead_events: int = 0

    @property
    def clean(self) -> bool:
        return self.operations == 0 and self.outbox_events == 0 and self.dead_events == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "clean": self.clean,
            "operations": self.operations,
            "outbox_events": self.outbox_events,
            "dead_events": self.dead_events,
            "details": list(self.details),
        }


class ReconcileService:
    def __init__(self, ledger: OperationLedger | None = None, outbox: OutboxStore | None = None) -> None:
        self.ledger = ledger or OperationLedger()
        self.outbox = outbox or OutboxStore()

    def inspect(self, uow: DatabaseUnitOfWork) -> ReconcileReport:
        operations = self.ledger.list_pending(uow)
        events = self.outbox.pending(uow)
        dead = uow.query_all("SELECT * FROM domain_outbox WHERE status = 'dead' ORDER BY created_at LIMIT 1000") if _table_exists(uow, "domain_outbox") else []
        details = tuple(
            [{"kind": "operation", **dict(row)} for row in operations]
            + [{"kind": "outbox", "event_id": row["event_id"], "event_type": row["event_type"]} for row in events]
            + [{"kind": "dead_outbox", "event_id": row["event_id"], "event_type": row["event_type"]} for row in dead]
        )
        return ReconcileReport(len(operations), len(events), details, len(dead))

    def run(
        self,
        uow: DatabaseUnitOfWork,
        *,
        handlers: Mapping[str, Callable[[dict[str, Any]], Any]] | None = None,
        operation_handlers: Mapping[str, Callable[[dict[str, Any]], Any]] | None = None,
        max_attempts: int = 5,
    ) -> ReconcileReport:
        """Retry known outbox events and failed operations.

        Handlers are deliberately supplied by the composition root; the
        database layer never imports gameplay modules or sends messages.
        """
        handlers = handlers or {}
        operation_handlers = operation_handlers or {}
        # A failed cross-database operation is retried only by an explicit
        # action handler.  Unknown actions remain visible for manual repair.
        for row in self.ledger.list_pending(uow, limit=1000):
            action = str(row.get("action", ""))
            handler = operation_handlers.get(action)
            if handler is None:
                continue
            try:
                result = handler(dict(row))
                if hasattr(result, "__await__"):
                    raise TypeError("reconcile operation handlers must be synchronous")
                if isinstance(result, OperationOutcome):
                    outcome = result
                elif result is False:
                    raise RuntimeError("operation handler returned failure")
                else:
                    # A truthy/None result is accepted for simple repair
                    # handlers and is recorded as a successful reconciliation.
                    outcome = OperationOutcome.applied(
                        str(row["operation_id"]),
                        action,
                        data={"reconciled": True},
                        audit_category="reconcile",
                    )
                self.ledger.finish(uow, outcome)
                event_id = f"{row['operation_id']}:{action}"
                self.outbox.mark_sent(uow, event_id)
            except Exception:
                continue
        for row in self.outbox.pending(uow, limit=1000):
            handler = handlers.get(str(row["event_type"]))
            if handler is None:
                continue
            try:
                payload = __import__("json").loads(row["payload_json"])
                handler({**dict(row), "payload": payload})
            except Exception:
                if int(row.get("attempts", 0)) + 1 >= max(1, max_attempts):
                    self.outbox.mark_dead(uow, str(row["event_id"]))
                else:
                    self.outbox.mark_failed(uow, str(row["event_id"]))
            else:
                self.outbox.mark_sent(uow, str(row["event_id"]))
        return self.inspect(uow)


__all__ = ["ReconcileReport", "ReconcileService"]
