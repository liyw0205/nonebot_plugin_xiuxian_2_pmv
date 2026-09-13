from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


def business_date(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)).isoformat()


@dataclass(frozen=True)
class ExpRewardResult:
    status: str
    granted: bool = False
    exp_reward: int = 0
    exp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status, "granted": self.granted, "exp_reward": self.exp_reward, "exp": self.exp}


@dataclass(frozen=True)
class StoneRewardResult:
    status: str
    granted: bool = False
    stone_reward: int = 0
    stone: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status, "granted": self.granted, "stone_reward": self.stone_reward, "stone": self.stone}


@dataclass(frozen=True)
class GreetingClaimResult:
    status: str
    kind: str = ""
    business_date: str = ""
    claimed: bool = False
    position: int = 0

    @property
    def succeeded(self) -> bool:
        return self.claimed and self.status in {"claimed", "duplicate"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "kind": self.kind,
            "business_date": self.business_date,
            "claimed": self.claimed,
            "position": self.position,
        }


@dataclass(frozen=True)
class DailyFortuneResult:
    status: str
    business_date: str = ""
    fortune_type: str = ""
    description: str = ""
    stars: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"generated", "existing", "duplicate"}

    @property
    def fortune(self) -> dict[str, str]:
        return {"type": self.fortune_type, "description": self.description, "stars": self.stars}

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "business_date": self.business_date,
            "fortune_type": self.fortune_type,
            "description": self.description,
            "stars": self.stars,
        }


__all__ = [
    "business_date",
    "ExpRewardResult",
    "StoneRewardResult",
    "GreetingClaimResult",
    "DailyFortuneResult",
]
