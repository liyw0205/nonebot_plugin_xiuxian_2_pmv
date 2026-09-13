from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class StoneTrainingRequest:
    operation_id: str
    user_id: str
    requested_stone: int

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if self.requested_stone <= 0:
            raise ValueError("requested_stone must be positive")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "requested_stone": self.requested_stone}


@dataclass(frozen=True)
class MedicineBathRequest:
    operation_id: str
    user_id: str
    consume_plan: tuple[Mapping[str, Any], ...]
    effect: float
    slot_name: str
    started_at: datetime
    duration_minutes: int
    sect_fairyland_level: int = 0

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if not self.consume_plan or any(int(item.get("amount", 0)) <= 0 for item in self.consume_plan):
            raise ValueError("consume_plan must contain positive amounts")
        if self.effect <= 0 or self.duration_minutes <= 0:
            raise ValueError("effect and duration_minutes must be positive")
        if self.sect_fairyland_level < 0:
            raise ValueError("sect_fairyland_level must not be negative")

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "consume_plan": [dict(item) for item in self.consume_plan],
            "effect": self.effect,
            "slot_name": self.slot_name,
            "started_at": self.started_at.isoformat(),
            "duration_minutes": self.duration_minutes,
            "sect_fairyland_level": self.sect_fairyland_level,
        }


@dataclass(frozen=True)
class BreakthroughRequest:
    operation_id: str
    user_id: str
    cultivation_rank: int
    roll_success: bool

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if self.cultivation_rank < 0:
            raise ValueError("cultivation_rank must not be negative")

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "cultivation_rank": self.cultivation_rank,
            "roll_success": self.roll_success,
        }


@dataclass(frozen=True)
class QiaoxueRequest:
    operation_id: str
    user_id: str
    roll: int

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if self.roll < 0:
            raise ValueError("roll must not be negative")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "roll": self.roll}


def normalize_plan(value: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    return tuple(
        {"item_id": int(item["item_id"]), "name": str(item["name"]), "amount": int(item["amount"])}
        for item in value
    )


__all__ = [
    "BreakthroughRequest",
    "MedicineBathRequest",
    "QiaoxueRequest",
    "StoneTrainingRequest",
    "normalize_plan",
]
