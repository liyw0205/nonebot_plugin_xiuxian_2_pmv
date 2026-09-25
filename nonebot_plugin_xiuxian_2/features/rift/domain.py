from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol, Sequence


@dataclass(frozen=True)
class RiftOperation:
    operation_id: str
    user_id: str

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")


class RiftRandomSource(Protocol):
    def randint(self, start: int, end: int) -> int: ...

    def choice(self, values: Sequence[Any]) -> Any: ...


@dataclass(frozen=True)
class RiftDamageEvent:
    message: str
    delta: dict[str, int]

    def as_outcome(self) -> dict[str, Any]:
        return {"delta": dict(self.delta), "message": self.message}


class RiftDamageEventResolver:
    """Roll a damage event without reading or mutating player state."""

    def __init__(
        self,
        *,
        battle_config: Mapping[str, Any],
        exp_reward: Callable[..., int],
        format_number: Callable[[Any], str],
    ) -> None:
        self.battle_config = battle_config
        self.exp_reward = exp_reward
        self.format_number = format_number

    @staticmethod
    def _weighted_choice(
        values: Mapping[str, Mapping[str, Any]], random_source: RiftRandomSource
    ) -> str:
        rates = [
            (str(name), int(value.get("type_rate", 0)))
            for name, value in values.items()
        ]
        rates = [(name, rate) for name, rate in rates if rate > 0]
        total = sum(rate for _, rate in rates)
        if total <= 0:
            raise ValueError("damage event rates must contain a positive weight")
        draw = int(random_source.randint(1, total))
        for name, rate in rates:
            if draw <= rate:
                return name
            draw -= rate
        raise ValueError("damage event rate draw was out of range")

    def roll(
        self,
        event_type: str,
        user_info: Mapping[str, Any],
        *,
        random_source: RiftRandomSource,
    ) -> RiftDamageEvent:
        if event_type not in self.battle_config:
            raise ValueError("unknown rift damage event type")
        event = self.battle_config[event_type]
        costs = event.get("cost")
        descriptions = event.get("desc")
        if (
            not isinstance(costs, Mapping)
            or not isinstance(descriptions, Sequence)
            or not descriptions
        ):
            raise ValueError("rift damage event configuration is incomplete")
        cost_type = self._weighted_choice(costs, random_source)
        values = costs[cost_type].get("value")
        if not isinstance(values, Sequence) or not values:
            raise ValueError("rift damage event cost values are missing")
        value = random_source.choice(values)
        delta: dict[str, int]
        if cost_type == "exp":
            exp = self.exp_reward(
                user_info["exp"],
                float(value),
                user_info.get("level"),
                apply_rank_suppress=False,
                anchor="gap",
            )
            now_hp = user_info["hp"] - (exp / 2)
            now_mp = user_info["mp"] - exp
            now_hp = now_hp if now_hp > 0 else 1
            now_mp = now_mp if now_mp > 0 else 1
            delta = {
                "exp": -int(exp),
                "hp": int(now_hp) - int(user_info["hp"]),
                "mp": int(now_mp) - int(user_info["mp"]),
            }
            detail = f"修为减少了：{self.format_number(exp)}点！"
        elif cost_type == "hp":
            cost_hp = int((user_info["exp"] / 2) * value)
            now_hp = user_info["hp"] - cost_hp
            if now_hp < 0:
                now_hp = 1
            delta = {"hp": int(now_hp) - int(user_info["hp"])}
            detail = f"气血减少了：{self.format_number(cost_hp)}点！"
        elif cost_type == "stone":
            cost_stone = int(value)
            delta = {"stone": -cost_stone}
            detail = f"灵石减少了：{self.format_number(cost_stone)}枚！"
        else:
            raise ValueError("unsupported rift damage cost type")
        message = str(random_source.choice(descriptions)).format(detail)
        return RiftDamageEvent(message, delta)


__all__ = [
    "RiftDamageEvent",
    "RiftDamageEventResolver",
    "RiftOperation",
    "RiftRandomSource",
]
