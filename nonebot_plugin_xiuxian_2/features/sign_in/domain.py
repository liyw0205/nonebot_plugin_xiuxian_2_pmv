from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SignInRecord:
    user_id: str
    operation_id: str
    stone: int

    def to_dict(self) -> dict[str, object]:
        return {
            "user_id": self.user_id,
            "operation_id": self.operation_id,
            "stone": self.stone,
        }


def validate_limits(lower: int, upper: int) -> tuple[int, int]:
    lower, upper = int(lower), int(upper)
    if lower < 0 or upper < lower:
        raise ValueError("签到灵石区间无效")
    return lower, upper


def lottery_tier(number: int) -> str:
    """Classify a deterministic lottery number without persistence concerns."""
    number = int(number)
    if number in {6, 66, 666, 6666, 66666}:
        return "grand"
    six_count = str(number).count("6")
    return {3: "first", 2: "second", 1: "third"}.get(six_count, "none")


def lottery_prize(pool: int, tier: str) -> int:
    """Calculate the prize from a pool after the deposit is funded."""
    pool = max(0, int(pool))
    return {
        "grand": pool,
        "first": pool // 10,
        "second": pool // 100,
        "third": pool // 1000,
    }.get(str(tier), 0)
