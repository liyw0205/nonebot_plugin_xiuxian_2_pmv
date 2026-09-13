from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PuppetOperationResult:
    status: str
    data: dict[str, Any]


__all__ = ["PuppetOperationResult"]
