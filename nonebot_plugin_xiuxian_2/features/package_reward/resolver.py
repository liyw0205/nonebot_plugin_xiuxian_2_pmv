from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ResolvedPackageReward:
    item_id: int | None
    name: str
    item_type: str | None
    quantity: int
    quality: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "buff": self.item_id,
            "name": self.name,
            "type": self.item_type,
            "amount": self.quantity,
            "quality": self.quality,
        }


@dataclass(frozen=True)
class ResolveResult:
    rewards: tuple[ResolvedPackageReward, ...]
    errors: tuple[str, ...] = ()


class PackageRewardResolver:
    """Pure reward configuration resolver with injected RandomSource."""

    def __init__(self, random_source: Any) -> None:
        self.random = random_source

    def resolve_once(self, package: dict[str, Any]) -> ResolveResult:
        name = str(package.get("name", "未知礼包"))
        if int(package.get("roll", 0) or 0) == 1:
            pool = package.get("roll_pool", [])
            if not isinstance(pool, list) or not pool:
                return ResolveResult((), (f"【失败】{name}：roll_pool为空或配置错误",))
            raw = self.random.choice(pool)
            if not isinstance(raw, dict):
                return ResolveResult((), (f"【失败】{name}：随机奖励配置错误",))
            return ResolveResult((self._from_raw(raw),))
        rewards: list[ResolvedPackageReward] = []
        index = 1
        while f"name_{index}" in package:
            rewards.append(self._from_raw({
                "buff": package.get(f"buff_{index}"),
                "name": package.get(f"name_{index}"),
                "type": package.get(f"type_{index}"),
                "amount": package.get(f"amount_{index}", 1),
                "quality": package.get(f"quality_{index}", 1),
            }))
            index += 1
        return ResolveResult(tuple(rewards))

    @staticmethod
    def _from_raw(raw: dict[str, Any]) -> ResolvedPackageReward:
        raw_name = str(raw.get("name", "未知物品"))
        try:
            quantity = int(raw.get("amount", 1) or 1)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"奖励数量配置错误: {raw_name}") from exc
        if quantity == 0:
            raise ValueError(f"奖励数量配置错误: {raw_name}")
        item_id = None if raw.get("buff") is None else int(raw["buff"])
        quality = max(1, min(5, int(raw.get("quality", 1) or 1)))
        return ResolvedPackageReward(item_id, raw_name, None if raw.get("type") is None else str(raw["type"]), quantity, quality)


__all__ = ["PackageRewardResolver", "ResolveResult", "ResolvedPackageReward"]
