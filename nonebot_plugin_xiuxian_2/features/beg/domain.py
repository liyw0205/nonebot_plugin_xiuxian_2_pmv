from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable


@dataclass(frozen=True)
class BegDailyRewardResult:
    status: str
    stone_reward: int = 0
    stone: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "stone_reward": self.stone_reward,
            "stone": self.stone,
        }


@dataclass(frozen=True)
class NoviceGiftClaimResult:
    status: str
    stone: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status, "stone": self.stone}


def parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError("create_time is required")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        for pattern in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(text, pattern)
            except ValueError:
                continue
    raise ValueError("invalid create_time")


def canonical_datetime(value: Any) -> str:
    return parse_datetime(value).isoformat(sep=" ")


def normalize_optional(value: Any) -> str | None:
    return None if value is None else str(value)


def normalized_rewards(rewards: Iterable[dict[str, Any]]) -> tuple[tuple[int, str, str, int], ...]:
    totals: dict[int, list[Any]] = {}
    for reward in rewards:
        item_id = int(reward["id"])
        amount = int(reward["amount"])
        if amount <= 0:
            continue
        metadata = [str(reward["name"]), str(reward["type"]), 0]
        if item_id not in totals:
            totals[item_id] = metadata
        elif totals[item_id][:2] != metadata[:2]:
            raise ValueError("conflicting reward metadata")
        totals[item_id][2] += amount
    return tuple(
        (item_id, values[0], values[1], values[2])
        for item_id, values in sorted(totals.items())
    )


__all__ = [
    "BegDailyRewardResult",
    "NoviceGiftClaimResult",
    "canonical_datetime",
    "normalize_optional",
    "normalized_rewards",
    "parse_datetime",
]
