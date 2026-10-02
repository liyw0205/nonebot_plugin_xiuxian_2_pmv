from __future__ import annotations

from datetime import date
from pathlib import Path

from ...core.errors import ValidationError
from ...infrastructure.database import OperationLedger
from .daily_pill_usage_reset_repository import (
    DailyPillUsageResetResult,
    DailyPillUsageResetSqlRepository,
)


class DailyPillUsageResetApplication:
    def __init__(
        self,
        database: str | Path,
        *,
        repository: DailyPillUsageResetSqlRepository | None = None,
        ledger: OperationLedger | None = None,
    ) -> None:
        self.repository = repository or DailyPillUsageResetSqlRepository(database, ledger=ledger)

    def reset(self, business_date: str | date) -> DailyPillUsageResetResult:
        try:
            normalized_date = date.fromisoformat(str(business_date)).isoformat()
        except (TypeError, ValueError) as exc:
            raise ValidationError("invalid business_date") from exc
        operation_id = f"back.daily-pill-usage-reset:{normalized_date}"
        return self.repository.reset(operation_id, normalized_date)


__all__ = ["DailyPillUsageResetApplication"]
