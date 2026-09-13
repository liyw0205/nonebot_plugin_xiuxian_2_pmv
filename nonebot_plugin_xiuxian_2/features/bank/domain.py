from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BankDepositRequest:
    operation_id: str
    user_id: str
    amount: int
    expected_saved_stone: int
    expected_saved_at: str
    bank_level: str
    interest: int
    settled_at: str
    save_limit: int

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.settled_at:
            raise ValueError("operation_id, user_id and settled_at are required")
        if self.amount <= 0 or min(self.expected_saved_stone, self.interest, self.save_limit) < 0:
            raise ValueError("deposit values are invalid")
        if not self.bank_level:
            raise ValueError("bank_level is required")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "amount": self.amount, "expected_saved_stone": self.expected_saved_stone, "expected_saved_at": self.expected_saved_at, "bank_level": self.bank_level, "interest": self.interest, "settled_at": self.settled_at, "save_limit": self.save_limit}


@dataclass(frozen=True)
class BankWithdrawalRequest:
    operation_id: str
    user_id: str
    amount: int
    expected_saved_stone: int
    expected_saved_at: str
    bank_level: str
    interest: int
    settled_at: str

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.settled_at:
            raise ValueError("operation_id, user_id and settled_at are required")
        if self.amount <= 0 or min(self.expected_saved_stone, self.interest) < 0:
            raise ValueError("withdrawal values are invalid")
        if not self.bank_level:
            raise ValueError("bank_level is required")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "amount": self.amount, "expected_saved_stone": self.expected_saved_stone, "expected_saved_at": self.expected_saved_at, "bank_level": self.bank_level, "interest": self.interest, "settled_at": self.settled_at}


@dataclass(frozen=True)
class BankUpgradeRequest:
    operation_id: str
    user_id: str
    expected_level: str
    next_level: str
    cost: int

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.expected_level or not self.next_level:
            raise ValueError("operation_id, user_id and levels are required")
        if self.expected_level == self.next_level or self.cost < 0:
            raise ValueError("upgrade values are invalid")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "expected_level": self.expected_level, "next_level": self.next_level, "cost": self.cost}


@dataclass(frozen=True)
class BankInterestRequest:
    operation_id: str
    user_id: str
    expected_saved_stone: int
    expected_saved_at: str
    bank_level: str
    interest: int
    settled_at: str

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.settled_at:
            raise ValueError("operation_id, user_id and settled_at are required")
        if min(self.expected_saved_stone, self.interest) < 0 or not self.bank_level:
            raise ValueError("interest values are invalid")

    def payload(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "expected_saved_stone": self.expected_saved_stone, "expected_saved_at": self.expected_saved_at, "bank_level": self.bank_level, "interest": self.interest, "settled_at": self.settled_at}


__all__ = ["BankDepositRequest", "BankWithdrawalRequest", "BankUpgradeRequest", "BankInterestRequest"]
