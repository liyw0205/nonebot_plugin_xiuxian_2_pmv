from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class GameEventStatisticsRequest:
    """Validated input for one idempotent statistics projection."""

    event_id: str
    user_id: str
    increments: Mapping[str, int]
    occurred_at: str

    def validate(self) -> None:
        if not self.event_id.strip() or not self.user_id.strip():
            raise ValueError("event_id and user_id are required")
        if not self.occurred_at.strip():
            raise ValueError("occurred_at is required")
        if not self.increments:
            raise ValueError("increments are required")
        for key, value in self.increments.items():
            if not str(key).strip() or isinstance(value, bool) or int(value) < 0:
                raise ValueError("statistics increments must be nonnegative integers")


@dataclass(frozen=True)
class GameEventStatisticsResult:
    event_id: str
    user_id: str
    status: str
    changed: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "user_id": self.user_id,
            "status": self.status,
            "changed": self.changed,
        }


__all__ = ["GameEventStatisticsRequest", "GameEventStatisticsResult"]
