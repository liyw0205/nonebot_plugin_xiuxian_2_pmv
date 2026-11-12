from __future__ import annotations

import math
from datetime import datetime, timezone

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class BankInterestDecision:
    wallet_after: int
    settled_at: str


def decide_interest(*, wallet: int, interest: int, settled_at: str) -> BankInterestDecision:
    wallet = int(wallet)
    interest = int(interest)
    settled_at = str(settled_at).strip()
    if wallet < 0 or interest < 0 or not settled_at:
        raise ValueError("bank interest values are invalid")
    return BankInterestDecision(wallet + interest, settled_at)


def calculate_interest(*, saved_stone: int, saved_at: str, settled_at: datetime, rate: float) -> tuple[int, float]:
    saved_stone = int(saved_stone)
    rate = float(rate)
    if saved_stone < 0 or not math.isfinite(rate) or rate < 0:
        raise ValueError("bank interest values are invalid")
    text = str(saved_at).strip()
    if not text:
        # 旧库存在空 savetime（账户已建但从未结息）。空时间视为零时长，
        # 首存不产生利息，且绝不因为一个空字符串让整个灵庄指令失败。
        return 0, 0.0
    try:
        previous = datetime.fromisoformat(text)
    except ValueError:
        # 同属历史脏数据：无法解析的时间按零时长处理（不凭空生息、不崩溃）。
        return 0, 0.0
    if previous.tzinfo is None and settled_at.tzinfo is None:
        elapsed = (settled_at - previous).total_seconds()
    else:
        # Legacy naive timestamps used the process-local clock; ISO rows carry their offset.
        elapsed = (settled_at.astimezone(timezone.utc) - previous.astimezone(timezone.utc)).total_seconds()
    if elapsed < 0:
        raise ValueError("settlement time precedes account time")
    hours = round(elapsed / 3600, 2)
    hours_num, hours_den = Decimal(str(hours)).as_integer_ratio()
    rate_num, rate_den = Decimal(str(rate)).as_integer_ratio()
    return saved_stone * hours_num * rate_num // (hours_den * rate_den), hours


__all__ = ["BankInterestDecision", "calculate_interest", "decide_interest"]
