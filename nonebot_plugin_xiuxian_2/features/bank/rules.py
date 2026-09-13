from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BankDepositDecision:
    wallet_after: int
    saved_after: int
    interest: int


def decide_deposit(*, wallet: int, saved: int, amount: int, interest: int, limit: int) -> BankDepositDecision:
    wallet = int(wallet)
    saved = int(saved)
    amount = int(amount)
    interest = int(interest)
    limit = int(limit)
    if wallet < 0 or saved < 0 or amount <= 0 or interest < 0 or limit < 0:
        raise ValueError("bank deposit values are invalid")
    if wallet < amount:
        raise ValueError("stone_insufficient")
    if saved + amount > limit:
        raise ValueError("limit_exceeded")
    return BankDepositDecision(wallet - amount + interest, saved + amount, interest)


__all__ = ["BankDepositDecision", "decide_deposit"]
