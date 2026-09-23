from __future__ import annotations

from datetime import datetime


STORED_CAP = 1_000_000_000
OP_AMOUNT_CAP = 1_000_000_000


def withdrawal_is_open(now: datetime) -> bool:
    """Guishi stone withdrawals are available on Saturday and Sunday."""
    return now.weekday() in {5, 6}


def withdrawal_fee(stored_balance: int, amount: int) -> int:
    """Keep the historical balance-based Guishi withdrawal fee schedule."""
    if stored_balance < 0:
        raise ValueError("stored_balance must not be negative")
    if amount <= 0:
        raise ValueError("amount must be positive")
    fee_rate = 0.2
    if stored_balance > 10_000_000_000:
        excess = stored_balance - 10_000_000_000
        fee_rate += (excess // 10_000_000_000) * 0.05
    return int(amount * min(fee_rate, 0.8))


__all__ = ["OP_AMOUNT_CAP", "STORED_CAP", "withdrawal_fee", "withdrawal_is_open"]
