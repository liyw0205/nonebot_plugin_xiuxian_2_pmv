from __future__ import annotations

from dataclasses import dataclass

from .domain import PackageReward, normalize_rewards


@dataclass(frozen=True)
class PackageOpenRequest:
    operation_id: str
    user_id: str
    package_id: int
    quantity: int
    rewards: tuple[PackageReward, ...]
    max_goods_num: int

    def validate(self) -> None:
        if not self.operation_id.strip():
            raise ValueError("operation_id must not be empty")
        if not self.user_id.strip():
            raise ValueError("user_id must not be empty")
        if self.package_id <= 0:
            raise ValueError("package_id must be positive")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.max_goods_num <= 0:
            raise ValueError("max_goods_num must be positive")
        normalize_rewards(self.rewards)


__all__ = ["PackageOpenRequest"]
