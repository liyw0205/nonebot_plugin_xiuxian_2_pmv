from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AccessoryDecomposeChange:
    status: str
    action: str
    user_id: str
    affected: int = 0
    stone_delta: int = 0
    accessory: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


__all__ = ["AccessoryDecomposeChange"]
