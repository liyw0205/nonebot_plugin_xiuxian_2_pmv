from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TiantiSettlementRequest:
    operation_id: str
    user_id: str
    settled_at: datetime
    sect_fairyland_level: int = 0

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if self.sect_fairyland_level < 0:
            raise ValueError("sect_fairyland_level must not be negative")

    def payload(self) -> dict[str, object]:
        return {
            "user_id": self.user_id,
            "settled_at": self.settled_at.isoformat(),
            "sect_fairyland_level": self.sect_fairyland_level,
        }


__all__ = ["TiantiSettlementRequest"]
