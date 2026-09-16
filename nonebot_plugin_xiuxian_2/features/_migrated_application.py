"""Application boundary shared by feature-specific legacy projections.

Each feature supplies its own repository module and action mapping.  This
coordinator only owns operation identity, replay and result normalization; it
does not discover modules or guess a business handler.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from ..core.errors import ConflictError, DomainError, ValidationError
from ..core.result import OperationOutcome, ReplyPlan
from ..infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..infrastructure.observability import trace_context


def _result_data(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value):
        return dict(asdict(value))
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], bool):
        return {"status": "applied" if value[0] else "rejected", "message": str(value[1])}
    if isinstance(value, bool):
        return {"status": "applied" if value else "rejected"}
    if isinstance(value, str):
        return {"status": "applied", "message": value}
    if value is None:
        return {"status": "applied"}
    try:
        return dict(vars(value))
    except TypeError:
        return {"status": "applied", "result": value}


class MigratedFeatureApplication:
    success_statuses = frozenset({
        "applied", "duplicate", "success", "succeeded", "completed", "created", "opened",
        "built", "changed", "used", "started", "updated", "purchased", "upgraded", "signed",
        "entered", "generated", "settled", "finished", "claimed", "trained", "renamed", "ok", "pending",
    })

    def __init__(self, database: str | Path, *, feature: str, repository: Any, ledger: OperationLedger | None = None) -> None:
        self.database = str(database)
        self.feature = str(feature)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def execute(
        self,
        *,
        operation_id: str,
        user_id: str,
        payload: Mapping[str, Any] | None = None,
        ledger_payload: Mapping[str, Any] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        request = dict(payload or {})
        action = str(request.pop("action", request.pop("operation", "execute")) or "execute")
        request["user_id"] = user_id
        ledger_request = dict(ledger_payload or request)
        ledger_request["user_id"] = user_id
        ledger_action = f"{self.feature}.{action}"
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                # Legacy services open their own SQLite connections.  Keep the
                # ledger reservation short so those connections never contend
                # with the reservation transaction itself.
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, ledger_action, ledger_request)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                raw = _result_data(self.repository.execute(operation_id, user_id, action, request))
                status = str(raw.get("status", "failed")).casefold()
                raw.setdefault("status", status)
                if status in self.success_statuses:
                    outcome = OperationOutcome.applied(
                        operation_id, ledger_action, data=raw,
                        consumed=dict(raw.get("consumed", {}) or {}),
                        granted=dict(raw.get("granted", {}) or {}),
                        audit_category=self.feature,
                    )
                else:
                    outcome = OperationOutcome.rejected(
                        operation_id, ledger_action,
                        str(raw.get("message") or raw.get("response") or f"{self.feature} 操作未完成。"),
                        code=status or "rejected", data=raw, audit_category=self.feature,
                    )
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, ledger_action, ledger_request, str(exc))
                raise

    def execute_legacy_call(
        self,
        *,
        operation_id: str,
        user_id: str,
        action: str,
        call: Callable[[], Any],
        payload: Mapping[str, Any] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        request = dict(payload or {})
        request["user_id"] = str(user_id)
        return self._execute_call(operation_id, user_id, action, request, call)

    def _execute_call(self, operation_id: str, user_id: str, action: str, request: Mapping[str, Any], call: Callable[[], Any]) -> OperationOutcome[dict[str, Any]]:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        ledger_action = f"{self.feature}.{action}"
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, ledger_action, dict(request))
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                execute_callback = getattr(self.repository, "execute_callback", None)
                if callable(execute_callback):
                    raw = _result_data(execute_callback(operation_id, user_id, action, request, call))
                else:
                    # Keep lightweight test doubles and third-party ports
                    # source-compatible while migrated repositories adopt the
                    # explicit callback boundary.
                    raw = _result_data(call())
                status = str(raw.get("status", "failed")).casefold()
                raw.setdefault("status", status)
                outcome = (
                    OperationOutcome.applied(operation_id, ledger_action, data=raw, audit_category=self.feature)
                    if status in self.success_statuses
                    else OperationOutcome.rejected(operation_id, ledger_action, str(raw.get("message") or raw.get("response") or f"{self.feature} 操作未完成。"), code=status or "rejected", data=raw, audit_category=self.feature)
                )
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, ledger_action, dict(request), str(exc))
                raise

    def inspect(self, *, user_id: str) -> Mapping[str, Any]:
        return self.repository.inspect(str(user_id))

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.execute(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["MigratedFeatureApplication"]
