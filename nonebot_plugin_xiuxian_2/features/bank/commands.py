from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class BankDepositCommand:
    operation_id: str
    user_id: str
    amount: int
    interest: int
    limit: int
    bank_level: str
    settled_at: str


def parse_first_use_deposit(*, user_id: str, text: str, operation_id: str, clock: Any, limit: int, bank_level: str = "1", interest: int = 0) -> BankDepositCommand:
    operation_id = str(operation_id).strip()
    user_id = str(user_id).strip()
    if not operation_id or not user_id:
        raise ValueError("operation_id and user_id are required")
    parts = str(text).strip().split()
    if len(parts) != 1:
        raise ValueError("deposit amount is required")
    amount = int(parts[0])
    if amount <= 0:
        raise ValueError("deposit amount must be positive")
    now = clock.now() if hasattr(clock, "now") else clock()
    if not isinstance(now, datetime):
        raise TypeError("clock must return datetime")
    return BankDepositCommand(operation_id, user_id, amount, int(interest), int(limit), str(bank_level), now.isoformat())


def parse_first_use_upgrade(*, user_id: str, text: str, operation_id: str, clock: Any) -> dict[str, Any]:
    parts = str(text).strip().split()
    if len(parts) != 2:
        raise ValueError("upgrade requires next level and cost")
    next_level, cost = parts
    if not next_level or int(cost) < 0:
        raise ValueError("upgrade values are invalid")
    now = clock.now() if hasattr(clock, "now") else clock()
    if not isinstance(now, datetime):
        raise TypeError("clock must return datetime")
    return {"operation_id": str(operation_id).strip(), "user_id": str(user_id).strip(), "expected_level": "1", "next_level": next_level, "cost": int(cost), "settled_at": now.isoformat()}


def parse_first_use_interest(*, user_id: str, text: str, operation_id: str, clock: Any) -> dict[str, Any]:
    value = int(str(text).strip())
    if value < 0:
        raise ValueError("interest must not be negative")
    now = clock.now() if hasattr(clock, "now") else clock()
    if not isinstance(now, datetime):
        raise TypeError("clock must return datetime")
    return {"operation_id": str(operation_id).strip(), "user_id": str(user_id).strip(), "interest": value, "bank_level": "1", "settled_at": now.isoformat()}


__all__ = ["BankDepositCommand", "parse_first_use_deposit", "parse_first_use_interest", "parse_first_use_upgrade"]
