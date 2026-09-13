from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .domain import AccessoryReward, normalize_accessories
from ..package_reward.domain import PackageReward, normalize_rewards


@dataclass(frozen=True)
class AccessoryPackageRequest:
    operation_id: str
    user_id: str
    package_id: int
    quantity: int
    rewards: tuple[PackageReward, ...]
    accessories: tuple[AccessoryReward, ...]
    max_goods_num: int
    accessory_limit: int

    @classmethod
    def build(
        cls,
        *,
        operation_id: str,
        user_id: str,
        package_id: int,
        quantity: int,
        rewards: Iterable[PackageReward | tuple[Any, ...]],
        accessories: Iterable[Mapping[str, Any] | AccessoryReward],
        max_goods_num: int,
        accessory_limit: int,
    ) -> "AccessoryPackageRequest":
        request = cls(
            str(operation_id).strip(),
            str(user_id).strip(),
            int(package_id),
            int(quantity),
            normalize_rewards(rewards),
            normalize_accessories(accessories),
            int(max_goods_num),
            int(accessory_limit),
        )
        request.validate()
        return request

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")
        if self.package_id <= 0 or self.quantity <= 0:
            raise ValueError("package_id and quantity must be positive")
        if self.max_goods_num <= 0 or self.accessory_limit <= 0:
            raise ValueError("limits must be positive")

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "package_id": self.package_id,
            "quantity": self.quantity,
            "max_goods_num": self.max_goods_num,
            "accessory_limit": self.accessory_limit,
            "rewards": [item.to_dict() for item in self.rewards],
            "accessories": [item.to_dict() for item in self.accessories],
        }


__all__ = ["AccessoryPackageRequest"]
