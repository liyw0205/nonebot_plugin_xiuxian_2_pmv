from __future__ import annotations

from pathlib import Path
from typing import Mapping

from .work_item_use_repository import WorkItemUseResult, WorkItemUseSqlRepository


class WorkItemUseApplication:
    """Application boundary for acceleration and capture work items."""

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

    def capture(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        expected_item_count: int,
        expected_work_type: int,
        new_offer: Mapping[str, object],
        reward_multiplier: int | None = None,
    ) -> WorkItemUseResult:
        return self.repository.capture(
            operation_id,
            user_id,
            item_id,
            expected_item_count,
            expected_work_type,
            new_offer,
            reward_multiplier,
        )


__all__ = ["WorkItemUseApplication", "WorkItemUseResult"]
