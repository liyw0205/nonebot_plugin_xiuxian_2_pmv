from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, TypeVar

from ..errors import DomainError
from ..result import OperationOutcome
from ..ports import OperationLedgerPort, UnitOfWork


T = TypeVar("T")


class OperationRunner:
    """Reusable transaction template for asset-changing application actions."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork], *, ledger: OperationLedgerPort) -> None:
        self.uow_factory = uow_factory
        self.ledger = ledger

    def execute(
        self,
        operation_id: str,
        action: str,
        payload: Mapping[str, Any],
        handler: Callable[[UnitOfWork], T | OperationOutcome[T]],
        *,
        audit_category: str = "",
    ) -> OperationOutcome[T]:
        with self.uow_factory() as uow:
            previous = self.ledger.begin(uow, operation_id, action, payload)
            if previous is not None:
                outcome = previous.outcome()
                if outcome is not None:
                    return outcome.replay()
                raise RuntimeError("operation is already in progress")
            try:
                result = handler(uow)
                if isinstance(result, OperationOutcome):
                    outcome = result
                else:
                    outcome = OperationOutcome.applied(
                        operation_id,
                        action,
                        data=result,
                        audit_category=audit_category,
                    )
            except DomainError as exc:
                outcome = OperationOutcome.rejected(
                    operation_id,
                    action,
                    exc.message,
                    code=exc.code,
                    audit_category=audit_category,
                )
            self.ledger.finish(uow, outcome)
            return outcome


__all__ = ["OperationRunner"]
