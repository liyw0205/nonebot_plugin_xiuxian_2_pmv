from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BankWithdrawalDecision:
    wallet_after: int
    saved_after: int
    interest: int


def decide_withdraw(*, wallet: int, saved: int, amount: int, interest: int) -> BankWithdrawalDecision:
    wallet = int(wallet)
    saved = int(saved)
    amount = int(amount)
    interest = int(interest)
    if wallet < 0 or saved < 0 or amount <= 0 or interest < 0:
        raise ValueError("bank withdrawal values are invalid")
    if saved < amount:
        raise ValueError("saved_stone_insufficient")
    return BankWithdrawalDecision(wallet + amount + interest, saved - amount, interest)


__all__ = ["BankWithdrawalDecision", "decide_withdraw"]
