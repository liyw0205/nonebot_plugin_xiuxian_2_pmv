from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

from .work_item_use_repository import WorkItemUseResult, WorkItemUseSqlRepository


class WorkItemUseApplication:
    """Application boundary for acceleration and capture work items."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: WorkItemUseSqlRepository | None = None,
        legacy_projection_writer: Callable[[str, Mapping[str, Any]], None] | None = None,
    ) -> None:
        self.repository = repository or WorkItemUseSqlRepository(database)
        self.legacy_projection_writer = legacy_projection_writer

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
        result = self.repository.capture(
            operation_id,
            user_id,
            item_id,
            expected_item_count,
            expected_work_type,
            new_offer,
            reward_multiplier,
        )
        snapshot = result.result_snapshot
        offer = snapshot.get("offer") if isinstance(snapshot, Mapping) else None
        if result.status in {"applied", "duplicate"} and isinstance(offer, Mapping):
            self._project_legacy(user_id, offer)
        return result

    def _project_legacy(self, user_id: str, offer: Mapping[str, Any]) -> None:
        if self.legacy_projection_writer is not None:
            self.legacy_projection_writer(str(user_id), dict(offer))


__all__ = ["WorkItemUseApplication", "WorkItemUseResult"]
