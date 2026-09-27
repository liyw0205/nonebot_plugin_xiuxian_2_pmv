from dataclasses import dataclass
from typing import Any, TypedDict


class MapRequest(TypedDict, total=False):
    operation_id: str
    user_id: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class SeedPurchaseResult:
    status: str
    quantity: int
    cost: int
    stone: int
    inventory: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class MapDongfuBuildResult:
    status: str
    stone: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class MapInteractiveActionResult:
    status: str
    stamina: int = 0
    action: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class MapCombatLifecycleResult:
    status: str
    stamina: int = 0
    task: dict[str, Any] | None = None
    snapshot: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate", "pending"}


@dataclass(frozen=True)
class MapExploreStartResult:
    status: str
    stamina: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class MapExploreSettlementResult:
    status: str
    stone: int
    rewards: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class MapMissionClaimResult:
    status: str
    stone: int
    rewards: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


__all__ = [
    "MapCombatLifecycleResult",
    "MapDongfuBuildResult",
    "MapExploreSettlementResult",
    "MapExploreStartResult",
    "MapInteractiveActionResult",
    "MapMissionClaimResult",
    "MapRequest",
    "SeedPurchaseResult",
]
