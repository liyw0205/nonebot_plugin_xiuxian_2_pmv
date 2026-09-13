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
