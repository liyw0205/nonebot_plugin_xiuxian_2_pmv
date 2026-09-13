from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class PackageReward:
    """A resolved reward; random selection happens in the adapter layer."""

    item_id: int | None
    name: str
    item_type: str | None
    quantity: int

    def validate(self) -> None:
        if not str(self.name).strip():
            raise ValueError("reward name is required")
        if self.quantity == 0:
            raise ValueError("reward quantity must not be zero")
        if self.name == "灵石":
            if self.item_id is not None:
                raise ValueError("stone rewards must not contain item_id")
        elif self.quantity < 0 or self.item_id is None:
            raise ValueError("item rewards require a positive item_id and quantity")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_rewards(rewards: Iterable[PackageReward | tuple[Any, ...]]) -> tuple[PackageReward, ...]:
    normalized: list[PackageReward] = []
    for reward in rewards:
        if isinstance(reward, PackageReward):
            item = reward
        else:
            if len(reward) != 4:
                raise ValueError("reward must contain item_id, name, item_type and quantity")
            item = PackageReward(
                None if reward[0] is None else int(reward[0]),
                str(reward[1]),
                None if reward[2] is None else str(reward[2]),
                int(reward[3]),
            )
        item.validate()
        normalized.append(item)
    if not normalized:
        raise ValueError("rewards must not be empty")
    return tuple(normalized)


__all__ = ["PackageReward", "normalize_rewards"]
