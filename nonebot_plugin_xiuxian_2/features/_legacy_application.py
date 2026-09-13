"""Shared application boundary for legacy transaction adapters.

The historical services still own game-specific algorithms and schemas.  This
module keeps their calls behind the same idempotency, audit and error boundary
used by migrated features.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from ..core.errors import ConflictError, DomainError, ValidationError
from ..core.result import OperationOutcome, ReplyPlan
from ..infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..infrastructure.observability import trace_context


def result_data(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value):
        return dict(asdict(value))
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], bool):
        ok, message = value
        return {"status": "applied" if ok else "rejected", "message": str(message)}
    if isinstance(value, bool):
        return {"status": "applied" if value else "rejected"}
    if isinstance(value, str):
        return {"status": "applied", "message": value}
    if value is None:
        return {}
    try:
        return dict(vars(value))
    except TypeError:
        return {"status": "applied", "result": value}


class LegacyApplication:
    """Coordinate one legacy service call as an idempotent use case."""

    success_statuses = frozenset(
        {
            "applied",
            "duplicate",
            "success",
            "succeeded",
            "completed",
            "created",
            "opened",
            "built",
            "changed",
            "used",
            "started",
            "pending",
            "updated",
            "purchased",
            "upgraded",
            "signed",
            "entered",
            "generated",
            "settled",
            "finished",
            "claimed",
            "trained",
            "renamed",
            "ok",
        }
    )

    def __init__(self, database: str | Path, *, repository: Any = None, feature: str = "legacy", ledger: OperationLedger | None = None) -> None:
        self.database = str(database)
        self.repository = repository
        self.feature = feature
        self.ledger = ledger or OperationLedger()

    def _execute(
        self,
        *,
        operation_id: str,
        user_id: str,
        action: str,
        payload: Mapping[str, Any],
        call: Callable[[], Any],
        success_statuses: frozenset[str] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, action, dict(payload))
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                execute_callback = getattr(self.repository, "execute_callback", None)
                if callable(execute_callback):
                    raw = result_data(execute_callback(operation_id, user_id, action, payload, call))
                else:
                    # Preserve source compatibility for simple test doubles
                    # while concrete feature ports adopt the callback method.
                    raw = result_data(call())
                status = str(raw.get("status", "failed")).casefold()
                raw.setdefault("status", status)
                accepted = success_statuses or self.success_statuses
                if status in accepted:
                    outcome = OperationOutcome.applied(
                        operation_id,
                        action,
                        data=raw,
                        consumed=dict(raw.get("consumed", {}) or {}),
                        granted=dict(raw.get("granted", {}) or {}),
                        audit_category=self.feature,
                    )
                else:
                    outcome = OperationOutcome.rejected(
                        operation_id,
                        action,
                        str(raw.get("message") or raw.get("response") or f"{self.feature} 操作未完成。"),
                        code=status or "rejected",
                        data=raw,
                        audit_category=self.feature,
                    )
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, action, dict(payload), str(exc))
                raise

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "execute"))
        result = getattr(self, action)(**kwargs)
        return ReplyPlan(result.to_dict() if isinstance(result, OperationOutcome) else result, reference=True)


__all__ = ["LegacyApplication", "result_data"]
