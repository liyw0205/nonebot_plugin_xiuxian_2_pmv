from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StoneGiftRequest:
    operation_id: str
    sender_id: str
    recipient_id: str
    gross_amount: int
    fee_rate: float = 0.1

    def validate(self) -> None:
        from .domain import validate_transfer

        validate_transfer(self.operation_id, self.sender_id, self.recipient_id, self.gross_amount)
