from __future__ import annotations

from pathlib import Path

from .repair_repository import BackpackRepairResult, BackpackRepairSqlRepository


class BackpackRepairApplication:
    """Application boundary for resumable administrator backpack repair."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: BackpackRepairSqlRepository | None = None,
    ) -> None:
        self.repository = repository or BackpackRepairSqlRepository(database)

    def run(self, operation_id: str, **kwargs) -> BackpackRepairResult:
        return self.repository.run(operation_id, **kwargs)


__all__ = ["BackpackRepairApplication", "BackpackRepairResult"]
