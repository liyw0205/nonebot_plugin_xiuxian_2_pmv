from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger


@dataclass(frozen=True)
class AdminNoviceResetResult:
    status: str
    operation_id: str
    reset_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminNoviceResetSqlRepository:
    ACTION = "admin.reset_novice"

    def __init__(self, database: str | Path, *, ledger: OperationLedger | None = None) -> None:
        self.database = Path(database)
        self.ledger = ledger or OperationLedger()

    @staticmethod
    def _has_columns(uow: DatabaseUnitOfWork, table: str, required: set[str]) -> bool:
        columns = {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA main.table_info("{table}")')
        }
        return required.issubset(columns)

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        ledger = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='operation_ledger'"
        )
        return (
            ledger is not None
            and cls._has_columns(
                uow,
                "operation_ledger",
                {
                    "operation_id", "action", "request_hash", "status", "result_json",
                    "created_at", "updated_at",
                },
            )
            and cls._has_columns(
                uow,
                "operation_audit",
                {
                    "operation_id", "action", "status", "before_json", "after_json",
                    "consumed_json", "granted_json", "category", "occurred_at", "created_at",
                },
            )
            and cls._has_columns(uow, "user_xiuxian", {"user_id", "is_novice"})
        )

    @staticmethod
    def _from_outcome(outcome: OperationOutcome[dict] | None, operation_id: str, status: str):
        data = dict(outcome.data or {}) if outcome is not None else {}
        return AdminNoviceResetResult(
            status=status,
            operation_id=operation_id,
            reset_count=int(data.get("reset_count", 0) or 0),
        )

    def reset(self, operation_id: str, operator_id: str) -> AdminNoviceResetResult:
        operation_id, operator_id = str(operation_id).strip(), str(operator_id).strip()
        if not operation_id or not operator_id:
            return AdminNoviceResetResult("invalid", operation_id)
        if not self.database.is_file():
            return AdminNoviceResetResult("schema_missing", operation_id)

        payload = {"operator_id": operator_id}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AdminNoviceResetResult("schema_missing", operation_id)
            try:
                existing = self.ledger.begin(uow, operation_id, self.ACTION, payload)
            except OperationConflictError:
                return AdminNoviceResetResult("operation_conflict", operation_id)
            if existing is not None:
                previous = existing.outcome()
                if previous is None:
                    return AdminNoviceResetResult("in_progress", operation_id)
                return self._from_outcome(previous, operation_id, "duplicate")

            row = uow.query_one(
                "SELECT COUNT(*) AS reset_count FROM user_xiuxian "
                "WHERE COALESCE(is_novice,0) <> 0"
            )
            reset_count = int(row["reset_count"] or 0)
            updated = uow.execute(
                "UPDATE user_xiuxian SET is_novice=0 WHERE COALESCE(is_novice,0) <> 0"
            )
            if updated.rowcount != reset_count:
                raise RuntimeError("novice reset target changed")
            outcome = OperationOutcome.applied(
                operation_id,
                self.ACTION,
                data={"status": "applied", "reset_count": reset_count},
                before={"claimed_users": reset_count},
                after={"claimed_users": 0},
                audit_category="admin_asset",
            )
            self.ledger.finish(uow, outcome)
            return AdminNoviceResetResult("applied", operation_id, reset_count)


__all__ = ["AdminNoviceResetResult", "AdminNoviceResetSqlRepository"]
