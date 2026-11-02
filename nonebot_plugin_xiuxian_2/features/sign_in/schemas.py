from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SignInRequest:
    user_id: str
    operation_id: str
    lower_limit: int
    upper_limit: int

    def validate(self) -> None:
        if not self.user_id.strip():
            raise ValueError("user_id is required")
        if not self.operation_id.strip():
            raise ValueError("operation_id is required")


@dataclass(frozen=True)
class SignInMakeupRequest:
    """One retroactive check-in for a past business date in the current month."""

    user_id: str
    operation_id: str
    business_date: str
    cost: int

    def validate(self) -> None:
        if not self.user_id.strip():
            raise ValueError("user_id is required")
        if not self.operation_id.strip():
            raise ValueError("operation_id is required")
        if not self.business_date.strip():
            raise ValueError("business_date is required")
        if int(self.cost) < 0:
            raise ValueError("cost must not be negative")
