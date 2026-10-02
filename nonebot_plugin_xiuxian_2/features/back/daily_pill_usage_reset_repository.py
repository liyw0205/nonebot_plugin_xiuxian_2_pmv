from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger


@dataclass(frozen=True)
class DailyPillUsageResetResult:
    status: str
    operation_id: str
    business_date: str
    affected_rows: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DailyPillUsageResetSqlRepository:
    ACTION = "back.reset_daily_pill_usage"
    REQUIRED_LEDGER_COLUMNS = {
        "operation_id", "action", "request_hash", "status", "result_json",
        "created_at", "updated_at",
    }
    REQUIRED_AUDIT_COLUMNS = {
        "operation_id", "action", "status", "before_json", "after_json",
        "consumed_json", "granted_json", "category", "occurred_at", "created_at",
    }

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
            cls._has_columns(uow, "operation_ledger", cls.REQUIRED_LEDGER_COLUMNS)
            and cls._has_columns(uow, "operation_audit", cls.REQUIRED_AUDIT_COLUMNS)
            and cls._has_columns(uow, "back", {"goods_type", "day_num"})
        )

    @staticmethod
    def _result(
        outcome: OperationOutcome[dict] | None,
        operation_id: str,
        business_date: str,
        status: str,
    ) -> DailyPillUsageResetResult:
        data = dict(outcome.data or {}) if outcome is not None else {}
        return DailyPillUsageResetResult(
            status=status,
            operation_id=operation_id,
            business_date=business_date,
            affected_rows=int(data.get("affected_rows", 0) or 0),
        )

    def reset(self, operation_id: str, business_date: str) -> DailyPillUsageResetResult:
        operation_id = str(operation_id).strip()
        business_date = date.fromisoformat(str(business_date)).isoformat()
        if not operation_id:
            return DailyPillUsageResetResult("invalid", operation_id, business_date)
        if not self.database.is_file():
            return DailyPillUsageResetResult("schema_missing", operation_id, business_date)

        payload = {"business_date": business_date}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return DailyPillUsageResetResult("schema_missing", operation_id, business_date)
            try:
                existing = self.ledger.begin(uow, operation_id, self.ACTION, payload)
            except OperationConflictError:
                return DailyPillUsageResetResult("operation_conflict", operation_id, business_date)
            if existing is not None:
                previous = existing.outcome()
                if previous is None:
                    return DailyPillUsageResetResult("in_progress", operation_id, business_date)
                return self._result(previous, operation_id, business_date, "duplicate")

            before = uow.query_one(
                "SELECT COUNT(*) AS affected_rows FROM back "
                "WHERE goods_type='丹药' AND day_num IS NOT 0"
            )
            affected_rows = int(before["affected_rows"] or 0)
            updated = uow.execute(
                "UPDATE back SET day_num=0 "
                "WHERE goods_type='丹药' AND day_num IS NOT 0"
            )
            if updated.rowcount != affected_rows:
                raise RuntimeError("daily pill usage reset target changed")

            outcome = OperationOutcome.applied(
                operation_id,
                self.ACTION,
                data={"status": "applied", "business_date": business_date, "affected_rows": affected_rows},
                before={"nonzero_or_null_rows": affected_rows},
                after={"goods_type": "丹药", "day_num": 0},
                audit_category="back_maintenance",
            )
            self.ledger.finish(uow, outcome)
            return self._result(outcome, operation_id, business_date, "applied")


__all__ = ["DailyPillUsageResetResult", "DailyPillUsageResetSqlRepository"]
