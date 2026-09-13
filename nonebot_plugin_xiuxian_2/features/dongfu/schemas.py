from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class DongfuRequest:
    operation_id: str
    user_id: str
    payload: Mapping[str, Any]

    def validate(self) -> None:
        if not self.operation_id.strip() or not self.user_id.strip():
            raise ValueError("operation_id and user_id are required")


__all__ = ["DongfuRequest"]
