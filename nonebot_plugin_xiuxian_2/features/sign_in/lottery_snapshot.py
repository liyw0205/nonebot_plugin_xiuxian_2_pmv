from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LotteryWinner:
    user_id: str
    user_name: str
    won_at: str
    prize: int
    lottery_number: int
    prize_tier: str


@dataclass(frozen=True)
class LotterySnapshot:
    business_date: str
    pool: int
    participants: int
    last_winner: LotteryWinner | None = None


__all__ = ["LotterySnapshot", "LotteryWinner"]
