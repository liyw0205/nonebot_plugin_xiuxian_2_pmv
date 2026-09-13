from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BankOperationResult:
    status: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status, **self.data}


__all__ = ["BankOperationResult"]
