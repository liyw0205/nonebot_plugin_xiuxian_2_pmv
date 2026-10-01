from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PlayerStateResult:
    """Outcome of a player vital-state write or compatibility fallback."""

    status: str
    user_id: str
    hp: Any = None
    mp: Any = None
    atk: Any = None
    exp: Any = None

    @property
    def changed(self) -> bool:
        return self.status == "applied"


__all__ = ["PlayerStateResult"]
