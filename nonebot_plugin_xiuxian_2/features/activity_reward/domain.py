from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ActivityClaimRequest:
    operation_id: str
    user_id: str

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")

