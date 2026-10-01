from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Mapping

from ...core.errors import DomainError, ValidationError
from ...core.result import OperationOutcome
from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.observability import trace_context
from .._migrated_application import MigratedFeatureApplication
from .repository import DufangRepository


class DufangApplication(MigratedFeatureApplication):
    def __init__(
        self,
        database: str | Path,
        player_database: str | Path | None = None,
        *,
        repository: DufangRepository | None = None,
    ) -> None:
        super().__init__(
            database,
            feature="dufang",
            repository=repository or DufangRepository(database, player_database),
        )

    def bet(self, *, operation_id: str, user_id: str, **kwargs):
        return self.execute(
            operation_id=operation_id,
            user_id=user_id,
            payload={"action": "bet", **kwargs},
            ledger_payload={"cost": kwargs["cost"]},
        )

    def payout(self, *, operation_id: str, user_id: str, **kwargs):
        return self.execute(
            operation_id=operation_id,
            user_id=user_id,
            payload={"action": "payout", **kwargs},
            ledger_payload={"bet_id": kwargs["bet_id"]},
        )

    def payout_result(self, operation_id: str):
        return self.repository.payout_result(operation_id)

    def share_settle(self, *, operation_id: str, user_id: str, **kwargs):
        return self._execute_share(operation_id, user_id, "share_settle", kwargs)

    def resume_share(self, *, operation_id: str, user_id: str, settled_at: str = ""):
        return self._execute_share(
            operation_id, user_id, "share_resume", {"settled_at": str(settled_at)}
        )

    def share_exists(self, operation_id: str) -> bool:
        return bool(self.repository.share_exists(operation_id))

    def _execute_share(self, operation_id: str, user_id: str, action: str, payload: dict):
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        ledger_action = f"{self.feature}.share_settle"
        identity = {"user_id": user_id}
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, ledger_action, identity)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                raw = self.repository.execute(operation_id, user_id, action, payload)
                if is_dataclass(raw):
                    raw = asdict(raw)
                elif isinstance(raw, Mapping):
                    raw = dict(raw)
                elif raw is not None:
                    raw = dict(vars(raw))
                else:
                    raw = {"status": "applied"}
                status = str(raw.get("status", "failed")).casefold()
                raw.setdefault("status", status)
                if status in self.success_statuses:
                    outcome = OperationOutcome.applied(
                        operation_id, ledger_action, data=raw, audit_category=self.feature
                    )
                else:
                    outcome = OperationOutcome.rejected(
                        operation_id,
                        ledger_action,
                        str(raw.get("message") or f"{self.feature} 操作未完成。"),
                        code=status or "rejected",
                        data=raw,
                        audit_category=self.feature,
                    )
                if str(raw.get("task_status", "")).casefold() != "running":
                    with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                        self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, ledger_action, identity, str(exc))
                raise


__all__ = ["DufangApplication"]
