from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class PuppetPurchaseRequest:
    operation_id: str
    user_id: str
    stone_cost: int

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or self.stone_cost < 0:
            raise ValueError("valid operation, user and cost are required")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "stone_cost": self.stone_cost}


@dataclass(frozen=True)
class PuppetUpgradeRequest:
    operation_id: str
    user_id: str
    upgrade_costs: Mapping[int, int]
    max_level: int

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or self.max_level <= 0:
            raise ValueError("valid operation, user and max level are required")
        if any(int(level) < 0 or int(cost) < 0 for level, cost in self.upgrade_costs.items()):
            raise ValueError("upgrade costs must not be negative")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "upgrade_costs": {str(k): int(v) for k, v in self.upgrade_costs.items()}, "max_level": self.max_level}


__all__ = ["PuppetPurchaseRequest", "PuppetUpgradeRequest"]
