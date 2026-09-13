from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BankUpgradeDecision:
    wallet_after: int
    bank_level: str


def decide_upgrade(*, wallet: int, current_level: str, expected_level: str, next_level: str, cost: int) -> BankUpgradeDecision:
    wallet = int(wallet)
    cost = int(cost)
    if wallet < 0 or cost < 0 or not current_level or current_level != expected_level or not next_level or next_level == current_level:
        raise ValueError("bank upgrade values are invalid")
    if wallet < cost:
        raise ValueError("stone_insufficient")
    return BankUpgradeDecision(wallet - cost, next_level)


__all__ = ["BankUpgradeDecision", "decide_upgrade"]
