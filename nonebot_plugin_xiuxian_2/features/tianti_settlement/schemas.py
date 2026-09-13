from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class TiantiSettlementResult:
    status: str
    operation_id: str
    user_id: str
    detail: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "operation_id": self.operation_id,
            "user_id": self.user_id,
            "detail": dict(self.detail),
        }


__all__ = ["TiantiSettlementResult"]
