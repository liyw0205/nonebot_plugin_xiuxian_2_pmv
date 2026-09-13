from __future__ import annotations

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


__all__ = ["BankInterestDecision", "decide_interest"]
