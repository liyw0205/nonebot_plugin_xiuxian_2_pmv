from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger


@dataclass(frozen=True)
class WorkAdminRefreshResetResult:
    status: str
    operation_id: str
    affected_rows: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorkAdminRefreshResetSqlRepository:
    ACTION = "admin.reset_work_refresh_count"

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
        return (
            cls._has_columns(
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
            and cls._has_columns(uow, "user_xiuxian", {"work_num"})
        )

    @staticmethod
    def _from_outcome(
        outcome: OperationOutcome[dict] | None,
        operation_id: str,
        status: str,
    ) -> WorkAdminRefreshResetResult:
        data = dict(outcome.data or {}) if outcome is not None else {}
        return WorkAdminRefreshResetResult(
            status=status,
            operation_id=operation_id,
            affected_rows=int(data.get("affected_rows", 0) or 0),
        )

    def reset_all(
        self,
        operation_id: str,
        operator_id: str,
        reset_count: int,
    ) -> WorkAdminRefreshResetResult:
        operation_id, operator_id = str(operation_id).strip(), str(operator_id).strip()
        reset_count = int(reset_count)
        if not operation_id or not operator_id:
            return WorkAdminRefreshResetResult("invalid", operation_id)
        if not self.database.is_file():
            return WorkAdminRefreshResetResult("schema_missing", operation_id)

        payload = {"operator_id": operator_id, "reset_count": reset_count}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return WorkAdminRefreshResetResult("schema_missing", operation_id)
            try:
                existing = self.ledger.begin(uow, operation_id, self.ACTION, payload)
            except OperationConflictError:
                return WorkAdminRefreshResetResult("operation_conflict", operation_id)
            if existing is not None:
                previous = existing.outcome()
                if previous is None:
                    return WorkAdminRefreshResetResult("in_progress", operation_id)
                return self._from_outcome(previous, operation_id, "duplicate")

            counts = uow.query_one(
                "SELECT COUNT(*) AS total_rows,"
                "COALESCE(SUM(CASE WHEN work_num IS NOT ? THEN 1 ELSE 0 END),0) AS changed_rows "
                "FROM user_xiuxian",
                (reset_count,),
            )
            updated = uow.execute(
                "UPDATE user_xiuxian SET work_num=? WHERE work_num IS NOT ?",
                (reset_count, reset_count),
            )
            affected_rows = int(updated.rowcount)
            if affected_rows != int(counts["changed_rows"] or 0):
                raise RuntimeError("work refresh reset target changed")
            outcome = OperationOutcome.applied(
                operation_id,
                self.ACTION,
                data={"status": "applied", "affected_rows": affected_rows},
                before={
                    "target_rows": int(counts["total_rows"] or 0),
                    "changed_rows": int(counts["changed_rows"] or 0),
                },
                after={"reset_count": reset_count},
                audit_category="work_maintenance",
            )
            self.ledger.finish(uow, outcome)
            return WorkAdminRefreshResetResult("applied", operation_id, affected_rows)


__all__ = ["WorkAdminRefreshResetResult", "WorkAdminRefreshResetSqlRepository"]
