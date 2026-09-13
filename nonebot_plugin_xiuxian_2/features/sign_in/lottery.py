from __future__ import annotations

from dataclasses import dataclass

from .domain import lottery_prize, lottery_tier


@dataclass(frozen=True)
class LotterySettlement:
    status: str
    operation_id: str
    user_id: str = ""
    user_name: str = ""
    business_date: str = ""
    lottery_number: int = 0
    prize_tier: str = "none"
    prize: int = 0
    deposit: int = 0
    pool_before: int = 0
    pool_after: int = 0
    participants: int = 0
    wallet_stone: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate", "already_participated"}


__all__ = ["LotterySettlement", "lottery_prize", "lottery_tier"]
