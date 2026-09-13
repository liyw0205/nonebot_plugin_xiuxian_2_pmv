from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class StoneGiftRecord:
    operation_id: str
    sender_id: str
    recipient_id: str
    gross_amount: int
    net_amount: int
    fee_amount: int

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "sender_id": self.sender_id,
            "recipient_id": self.recipient_id,
            "gross_amount": self.gross_amount,
            "net_amount": self.net_amount,
            "fee_amount": self.fee_amount,
        }


def calculate_amounts(gross_amount: int, fee_rate: float) -> tuple[int, int]:
    amount = int(gross_amount)
    rate = float(fee_rate)
    if amount <= 0:
        raise ValueError("gross_amount must be positive")
    if not 0 <= rate < 1:
        raise ValueError("fee_rate must be in [0, 1)")
    fee = int(amount * rate)
    return amount - fee, fee


def validate_transfer(operation_id: str, sender_id: str, recipient_id: str, gross_amount: int) -> None:
    if not str(operation_id).strip():
        raise ValueError("operation_id must not be empty")
    if not str(sender_id).strip() or not str(recipient_id).strip():
        raise ValueError("sender_id and recipient_id are required")
    if str(sender_id) == str(recipient_id):
        raise ValueError("sender and recipient must differ")
    if int(gross_amount) <= 0:
        raise ValueError("gross_amount must be positive")


def validate_daily_limit(value: int | None, name: str) -> int | None:
    if value is None:
        return None
    normalized = int(value)
    if normalized < 0:
        raise ValueError(f"{name} must not be negative")
    return normalized


def normalize_transfer_date(value: str) -> str:
    try:
        return date.fromisoformat(str(value)).isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError("transfer_date must use YYYY-MM-DD") from exc
