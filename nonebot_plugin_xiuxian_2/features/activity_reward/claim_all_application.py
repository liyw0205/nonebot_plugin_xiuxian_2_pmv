from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .claim_all_repository import ActivityClaimAllRepository, ActivityClaimAllResult


class ActivityClaimAllApplication:
    def __init__(self, database: str | Path, *, repository: ActivityClaimAllRepository | None = None) -> None:
        self.repository = repository or ActivityClaimAllRepository(database)

    @classmethod
    def step_names(cls) -> tuple[str, ...]:
        return ActivityClaimAllRepository.step_names()

    def run(self, operation_id: str, user_id: str, runners: Mapping[str, Any]) -> ActivityClaimAllResult:
        return self.repository.run(operation_id, user_id, runners)


__all__ = ["ActivityClaimAllApplication"]
