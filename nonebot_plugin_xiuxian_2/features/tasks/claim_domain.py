from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TaskClaimResult:
    status: str
    tasks: tuple[dict, ...] = ()
    message: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


__all__ = ["TaskClaimResult"]
