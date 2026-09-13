from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MixelixirResult:
    status: str
    operation_id: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status, "operation_id": self.operation_id, **self.data}


__all__ = ["MixelixirResult"]
