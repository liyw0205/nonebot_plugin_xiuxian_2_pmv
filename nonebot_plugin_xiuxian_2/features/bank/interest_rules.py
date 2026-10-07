from __future__ import annotations

import math
from datetime import datetime, timezone

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
    if saved_stone < 0 or not math.isfinite(rate) or rate < 0:
        raise ValueError("bank interest values are invalid")
    previous = datetime.fromisoformat(str(saved_at))
    if previous.tzinfo is None and settled_at.tzinfo is None:
        elapsed = (settled_at - previous).total_seconds()
    else:
        # Legacy naive timestamps used the process-local clock; ISO rows carry their offset.
        elapsed = (settled_at.astimezone(timezone.utc) - previous.astimezone(timezone.utc)).total_seconds()
    if elapsed < 0:
        raise ValueError("settlement time precedes account time")
    hours = round(elapsed / 3600, 2)
    return int(saved_stone * hours * rate), hours


__all__ = ["BankInterestDecision", "calculate_interest", "decide_interest"]
