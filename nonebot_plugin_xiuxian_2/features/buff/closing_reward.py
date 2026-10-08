"""Pure reward calculation for ordinary ``闭关`` settlement.

The legacy command still owns reading the current player snapshot and rendering
the reply.  This module owns only the deterministic calculation so it can be
tested without a database, world-event state, or the virtual-world flow.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...core.numeric import as_int_like


@dataclass(frozen=True)
class ClosingRewardSnapshot:
    """Calculated ordinary closing reward and resulting player attributes."""

    base_exp: int
    exp_gain: int
    stone_cost: int
    exp_time: int
    hp_gain: int
    mp_gain: int
    new_exp: int
    new_hp: int
    new_mp: int
    new_atk: int
    new_power: int
    reached_limit: bool
    efficiency: float
    spirit_vein_message: str = ""

    def as_data(self) -> dict[str, Any]:
        """Return the settlement fields consumed by adapters and tests."""
        return {
            "base_exp": self.base_exp,
            "exp_gain": self.exp_gain,
            "stone_cost": self.stone_cost,
            "exp_time": self.exp_time,
            "hp_gain": self.hp_gain,
            "mp_gain": self.mp_gain,
            "new_exp": self.new_exp,
            "new_hp": self.new_hp,
            "new_mp": self.new_mp,
            "new_atk": self.new_atk,
            "new_power": self.new_power,
            "reached_limit": self.reached_limit,
            "efficiency": self.efficiency,
            "spirit_vein_message": self.spirit_vein_message,
        }


class ClosingRewardCalculator:
    """Calculate normal and spirit-stone closing rewards without I/O."""

    @staticmethod
    def _boosted_exp(exp: int, cap: int, multiplier: float) -> int:
        value = max(0, int(exp))
        if multiplier > 1.0:
            value = int(value * multiplier)
        return min(value, max(0, int(cap)))

    @classmethod
    def calculate(
        cls,
        *,
        exp_time: Any,
        current_exp: Any,
        current_stone: Any,
        current_hp: Any,
        current_mp: Any,
        exp_cap: Any,
        closing_exp: Any,
        level_rate: Any,
        realm_rate: Any,
        main_rate: Any = 0.0,
        closing_rate: Any = 0.0,
        blessed_rate: Any = 0.0,
        stone_exit: bool = False,
        spirit_vein_multiplier: Any = 1.0,
        spirit_vein_message: str = "",
    ) -> ClosingRewardSnapshot:
        """Calculate an ordinary close from one frozen read snapshot.

        ``exp_cap`` is the remaining experience available to the player, matching
        the legacy handler's ``user_get_exp_max``.  A stone exit spends up to the
        unboosted base reward, capped by the currently available stone balance.
        """
        minutes = max(0, as_int_like(exp_time))
        old_exp = max(0, as_int_like(current_exp))
        old_stone = max(0, as_int_like(current_stone))
        old_hp = max(0, as_int_like(current_hp))
        old_mp = max(0, as_int_like(current_mp))
        cap = max(0, as_int_like(exp_cap))
        base_rate = float(closing_exp)
        root_rate = float(level_rate)
        rank_rate = float(realm_rate)
        main_bonus = float(main_rate)
        closing_bonus = float(closing_rate)
        blessed_bonus = float(blessed_rate)
        multiplier = float(spirit_vein_multiplier)

        base_exp = max(
            0,
            int(
                minutes
                * base_rate
                * root_rate
                * rank_rate
                * (1 + main_bonus)
                * (1 + closing_bonus)
                * (1 + blessed_bonus)
            ),
        )
        exp_gain = cls._boosted_exp(base_exp, cap, multiplier)
        reached_limit = exp_gain >= cap
        stone_cost = 0
        if reached_limit:
            exp_gain = cap
        elif stone_exit:
            stone_cost = min(base_exp, old_stone)
            exp_gain = cls._boosted_exp(base_exp + stone_cost, cap, multiplier)

        new_exp = old_exp + exp_gain
        hp_gain = int(old_exp / 10 * minutes)
        mp_gain = int(old_exp / 20 * minutes)
        new_hp = min(old_hp + hp_gain, int(new_exp / 2))
        new_mp = min(old_mp + mp_gain, new_exp)
        new_atk = int(new_exp / 10)
        new_power = int(new_exp * root_rate * rank_rate)
        efficiency = root_rate + main_bonus + closing_bonus + blessed_bonus

        return ClosingRewardSnapshot(
            base_exp=base_exp,
            exp_gain=exp_gain,
            stone_cost=stone_cost,
            exp_time=minutes,
            hp_gain=hp_gain,
            mp_gain=mp_gain,
            new_exp=new_exp,
            new_hp=new_hp,
            new_mp=new_mp,
            new_atk=new_atk,
            new_power=new_power,
            reached_limit=reached_limit,
            efficiency=efficiency,
            spirit_vein_message=(
                str(spirit_vein_message) if multiplier > 1.0 else ""
            ),
        )


__all__ = ["ClosingRewardCalculator", "ClosingRewardSnapshot"]
