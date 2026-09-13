from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WorkClaimResult:
    status: str
    operation_id: str
    task_name: str
    started_at: str
    remaining_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "operation_id": self.operation_id,
            "task_name": self.task_name,
            "started_at": self.started_at,
            "remaining_count": self.remaining_count,
        }


__all__ = ["WorkClaimResult"]
