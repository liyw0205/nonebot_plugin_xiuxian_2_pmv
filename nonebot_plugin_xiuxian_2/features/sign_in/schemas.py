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
