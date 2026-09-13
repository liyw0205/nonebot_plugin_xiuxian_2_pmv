from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ActivityClaimResult:
    status: str
    operation_id: str
    user_id: str
    text: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "operation_id": self.operation_id,
            "user_id": self.user_id,
            "text": self.text,
        }
