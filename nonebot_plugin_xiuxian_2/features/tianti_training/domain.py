from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
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
class StoneTrainingDecision:
    status: str
    stone_cost: int = 0
    hp_gain: int = 0
    new_hp: int = 0


def decide_stone_training(*, old_hp: int, requested_stone: int, hp_cap: int) -> StoneTrainingDecision:
    """Calculate the asset exchange without database or configuration access."""
    old_hp = max(0, int(old_hp))
    requested_stone = int(requested_stone)
    hp_cap = max(old_hp, int(hp_cap))
    requested_gain = requested_stone // 10
    new_hp = min(hp_cap, old_hp + requested_gain)
    hp_gain = max(0, new_hp - old_hp)
    stone_cost = hp_gain * 10
    if stone_cost <= 0:
        return StoneTrainingDecision("at_cap", new_hp=old_hp)
    return StoneTrainingDecision("trained", stone_cost, hp_gain, new_hp)


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
class BreakthroughDecision:
    status: str
    old_level: str = ""
    new_level: str = ""
    hp_cost: int = 0
    new_hp: int = 0


def decide_breakthrough(*, old_level: str, next_level: str | None, cultivation_rank: int, required_rank: int, old_hp: int, required_hp: int, roll_success: bool) -> BreakthroughDecision:
    old_hp = max(0, int(old_hp))
    if not next_level:
        return BreakthroughDecision("max_level", old_level, old_level, new_hp=old_hp)
    if int(cultivation_rank) > int(required_rank):
        return BreakthroughDecision("cultivation_insufficient", old_level, old_level, new_hp=old_hp)
    if old_hp < int(required_hp):
        return BreakthroughDecision("hp_insufficient", old_level, old_level, new_hp=old_hp)
    hp_cost = max(1, int(old_hp * 0.05))
    return BreakthroughDecision("completed", old_level, next_level if roll_success else old_level, hp_cost, max(0, old_hp - hp_cost))


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


def decide_medicine_bath_activation(
    *,
    current_end_time: datetime | None,
    now: datetime,
    duration_minutes: int,
    effect: float,
) -> tuple[str, datetime | None]:
    if duration_minutes <= 0 or effect <= 0:
        raise ValueError("duration_minutes and effect must be positive")
    if effect > 1 and current_end_time is not None and now <= current_end_time:
        return "bath_active", None
    return "applied", now + timedelta(minutes=duration_minutes)


@dataclass(frozen=True)
class TiantiSettlementWindow:
    status: str
    minutes: int
    next_settlement_time: datetime | None


def decide_tianti_settlement_window(*, last_settlement: datetime | None, now: datetime) -> TiantiSettlementWindow:
    if last_settlement is None:
        return TiantiSettlementWindow("init", 0, now)
    minutes = max(0, int((now - last_settlement).total_seconds() // 60))
    if minutes <= 0:
        return TiantiSettlementWindow("empty", minutes, last_settlement)
    return TiantiSettlementWindow("settle", minutes, now)


@dataclass(frozen=True)
class TiantiGainDecision:
    gain: int
    new_hp: int
    real_gain: int


def decide_tianti_gain(*, minutes: int, base_per_min: int, base_ratio: float, gain_pct: float, bath_effect: float, sect_bonus: float, spirit_vein_multiplier: float, old_hp: int, hp_cap: int) -> TiantiGainDecision:
    if minutes < 0 or base_per_min < 0 or old_hp < 0:
        raise ValueError("minutes, base_per_min and old_hp must not be negative")
    if bath_effect <= 0 or spirit_vein_multiplier <= 0:
        raise ValueError("multipliers must be positive")
    gain = int(minutes * int(base_per_min * (1 + base_ratio)) * (1 + gain_pct) * bath_effect * (1 + sect_bonus) * spirit_vein_multiplier)
    new_hp = min(max(int(hp_cap), int(old_hp)), int(old_hp) + max(0, gain))
    return TiantiGainDecision(gain=max(0, gain), new_hp=new_hp, real_gain=max(0, new_hp - int(old_hp)))


__all__ = [
    "BreakthroughDecision",
    "BreakthroughRequest",
    "MedicineBathRequest",
    "QiaoxueRequest",
    "StoneTrainingRequest",
    "StoneTrainingDecision",
    "decide_breakthrough",
    "decide_medicine_bath_activation",
    "TiantiSettlementWindow",
    "decide_tianti_settlement_window",
    "TiantiGainDecision",
    "decide_tianti_gain",
    "decide_stone_training",
    "normalize_plan",
]
