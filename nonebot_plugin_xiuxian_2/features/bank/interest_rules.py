from __future__ import annotations

from datetime import datetime

from dataclasses import dataclass


@dataclass(frozen=True)
class BankInterestDecision:
    wallet_after: int
    settled_at: str


def decide_interest(*, wallet: int, interest: int, settled_at: str) -> BankInterestDecision:
    wallet = int(wallet)
    interest = int(interest)
    settled_at = str(settled_at).strip()
    if wallet < 0 or interest < 0 or not settled_at:
        raise ValueError("bank interest values are invalid")
    return BankInterestDecision(wallet + interest, settled_at)


def calculate_interest(*, saved_stone: int, saved_at: str, settled_at: datetime, rate: float) -> tuple[int, float]:
    saved_stone = int(saved_stone)
    rate = float(rate)
    if saved_stone < 0 or rate < 0:
        raise ValueError("bank interest values are invalid")
    previous = datetime.strptime(str(saved_at), "%Y-%m-%d %H:%M:%S")
    hours = round((settled_at - previous).total_seconds() / 3600, 2)
    if hours < 0:
        raise ValueError("settlement time precedes account time")
    return int(saved_stone * hours * rate), hours


__all__ = ["BankInterestDecision", "calculate_interest", "decide_interest"]
