from __future__ import annotations

from pathlib import Path
from typing import Mapping

from .work_item_use_repository import WorkItemUseResult, WorkItemUseSqlRepository


class WorkItemUseApplication:
    """Application boundary for accelerating an accepted work order."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: WorkItemUseSqlRepository | None = None,
    ) -> None:
        self.repository = repository or WorkItemUseSqlRepository(database)

    def accelerate(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        expected_item_count: int,
        expected_work: Mapping[str, object],
        accelerated_at: str,
    ) -> WorkItemUseResult:
        return self.repository.accelerate(
            operation_id,
            user_id,
            item_id,
            expected_item_count,
            expected_work,
            accelerated_at,
        )


__all__ = ["WorkItemUseApplication", "WorkItemUseResult"]
