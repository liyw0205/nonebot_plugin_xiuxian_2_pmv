from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class FairylandClaimResult:
    """Stable response DTO for a sect fairyland claim."""

    status: str
    operation_id: str
    user_id: str
    sect_id: str
    detail: Mapping[str, Any]

    @classmethod
    def from_data(cls, operation_id: str, data: Mapping[str, Any]) -> "FairylandClaimResult":
        return cls(
            status=str(data.get("status", "failed")),
            operation_id=str(operation_id),
            user_id=str(data.get("user_id", "")),
            sect_id=str(data.get("sect_id", "")),
            detail=dict(data.get("detail") or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "operation_id": self.operation_id,
            "user_id": self.user_id,
            "sect_id": self.sect_id,
            "detail": dict(self.detail),
        }


__all__ = ["FairylandClaimResult"]
