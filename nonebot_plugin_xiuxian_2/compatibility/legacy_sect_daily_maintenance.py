from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian.xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectMaintenanceOutcome:
    sect_id: int
    sect_name: str
    status: str
    from_level: int
    to_level: int
    materials_cost: int

@dataclass(frozen=True)
class SectDailyResetResult:
    status: str
    business_date: str
    user_count: int = 0
    outcomes: tuple[SectMaintenanceOutcome, ...] = ()
    reason: str = ""
    subject: str = ""

    @property
    def applied(self) -> bool:
        return self.status in {"applied", "duplicate"}

class SectDailyResetMaintenanceService:
    """Reset daily sect counters and settle all elixir-room maintenance."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_daily_reset_operations (
                business_date TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                user_count INTEGER NOT NULL,
                outcomes TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _normalize_costs(costs_by_level) -> dict[int, int]:
        costs = {
            int(level): max(int(cost), 0)
            for level, cost in dict(costs_by_level).items()
        }
        if not costs or any(level <= 0 for level in costs):
            raise ValueError("positive elixir-room levels are required")
        return costs

    @staticmethod
    def _decode_outcomes(raw: str) -> tuple[SectMaintenanceOutcome, ...]:
        return tuple(SectMaintenanceOutcome(**item) for item in json.loads(raw))

    def settle(self, business_date, costs_by_level) -> SectDailyResetResult:
        from ..features.sect.daily_maintenance_repository import SectDailyMaintenanceSqlRepository

        business_date = str(business_date).strip()
        if not business_date:
            raise ValueError("business_date must not be empty")
        costs = self._normalize_costs(costs_by_level)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_operations(conn)
            conn.commit()
            result = SectDailyMaintenanceSqlRepository(self._database).settle(business_date, costs)
        return SectDailyResetResult(result.status, result.business_date, result.user_count,
                                    tuple(SectMaintenanceOutcome(**row.__dict__) for row in result.outcomes),
                                    result.reason, result.subject)


__all__ = ["SectMaintenanceOutcome", "SectDailyResetResult", "SectDailyResetMaintenanceService"]
