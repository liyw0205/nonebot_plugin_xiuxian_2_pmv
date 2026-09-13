from __future__ import annotations

from dataclasses import dataclass
from datetime import date


RANK_NAMES = (
    "江湖好手", "感气境初期", "感气境中期", "感气境圆满", "练气境初期", "练气境中期", "练气境圆满",
    "筑基境初期", "筑基境中期", "筑基境圆满", "结丹境初期", "结丹境中期", "结丹境圆满",
    "金丹境初期", "金丹境中期", "金丹境圆满", "元神境初期", "元神境中期", "元神境圆满",
    "化神境初期", "化神境中期", "化神境圆满", "炼神境初期", "炼神境中期", "炼神境圆满",
    "返虚境初期", "返虚境中期", "返虚境圆满", "大乘境初期", "大乘境中期", "大乘境圆满",
    "虚道境初期", "虚道境中期", "虚道境圆满", "斩我境初期", "斩我境中期", "斩我境圆满",
    "遁一境初期", "遁一境中期", "遁一境圆满", "至尊境初期", "至尊境中期", "至尊境圆满",
    "微光境初期", "微光境中期", "微光境圆满", "星芒境初期", "星芒境中期", "星芒境圆满",
    "月华境初期", "月华境中期", "月华境圆满", "耀日境初期", "耀日境中期", "耀日境圆满",
    "祭道境初期", "祭道境中期", "祭道境圆满", "自在境初期", "自在境中期", "自在境圆满",
    "破虚境初期", "破虚境中期", "破虚境圆满", "无界境初期", "无界境中期", "无界境圆满",
    "混元境初期", "混元境中期", "混元境圆满", "造化境初期", "造化境中期", "造化境圆满",
    "永恒境初期", "永恒境中期", "永恒境圆满", "至高",
)


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


def rank_value(level: str) -> int:
    try:
        return 75 - RANK_NAMES.index(str(level)) * 3
    except ValueError:
        return 75


def daily_cap(level: str, reference_level: str = "江湖好手") -> int:
    return max(0, 100_000_000 + (rank_value(reference_level) - rank_value(level)) * 20_000_000)


def normalize_transfer_date(value: str) -> str:
    try:
        return date.fromisoformat(str(value)).isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError("transfer_date must use YYYY-MM-DD") from exc
