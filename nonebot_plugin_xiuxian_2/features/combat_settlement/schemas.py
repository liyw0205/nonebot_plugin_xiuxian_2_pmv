from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CombatSettlementResult:
    status: str
    operation_id: str
    user_id: str
    stone: int = 0
    rewards: tuple[tuple[int, int], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "operation_id": self.operation_id,
            "user_id": self.user_id,
            "stone": self.stone,
            "rewards": [{"id": item_id, "amount": amount} for item_id, amount in self.rewards],
        }


__all__ = ["CombatSettlementResult"]
