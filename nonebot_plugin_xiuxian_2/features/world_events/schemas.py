from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class DemonClaimResult:
    status: str
    operation_id: str
    stone: int
    exp: int
    items: tuple[Mapping[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "operation_id": self.operation_id,
            "stone": self.stone,
            "exp": self.exp,
            "items": [dict(item) for item in self.items],
        }


__all__ = ["DemonClaimResult"]
