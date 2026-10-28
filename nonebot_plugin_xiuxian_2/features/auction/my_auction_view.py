"""Pure rendering for the player's personal auction view.

The command layer owns the large message assembly; this module only turns the
read model into stable lines so the same text can be asserted in unit tests
without importing NoneBot.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Mapping, Sequence


NumberFormatter = Callable[[int], str]


def _plain_number(value: int) -> str:
    return str(int(value))


def format_remaining(seconds: int) -> str:
    """Format a positive remaining duration, never re-granting a full window."""

    seconds = max(int(seconds), 0)
    if seconds >= 86400:
        days, rest = divmod(seconds, 86400)
        hours = rest // 3600
        return f"{days}天{hours}小时" if hours else f"{days}天"
    if seconds >= 3600:
        hours, rest = divmod(seconds, 3600)
        minutes = rest // 60
        return f"{hours}小时{minutes}分" if minutes else f"{hours}小时"
    if seconds >= 60:
        return f"{seconds // 60}分钟"
    return f"{seconds}秒"


def _timestamp_text(value: Any) -> str:
    try:
        moment = float(value or 0)
    except (TypeError, ValueError):
        moment = 0.0
    if moment <= 0:
        return "时间未知"
    return datetime.fromtimestamp(moment).strftime("%m-%d %H:%M")


def render_live_lots(
    lots: Sequence[Mapping[str, Any]], *, number: NumberFormatter = _plain_number
) -> tuple[str, ...]:
    """Render the player's lots in the running session."""

    lines: list[str] = []
    for lot in lots:
        line = (
            f"拍卖ID:{lot.get('auction_id', '')} {lot.get('item_name', '')} "
            f"起拍:{number(int(lot.get('start_price') or 0))} "
            f"当前价:{number(int(lot.get('current_price') or 0))} "
            f"出价:{int(lot.get('bid_count') or 0)}次/"
            f"{int(lot.get('bidder_count') or 0)}人"
        )
        remaining = lot.get("remaining_seconds")
        if remaining is not None:
            line += f" 剩余:{format_remaining(int(remaining))}"
        lines.append(line)
    if lines:
        lines.append("拍卖进行中，拍品暂不可下架")
    return tuple(lines)


def render_settlements(
    records: Sequence[Mapping[str, Any]], *, number: NumberFormatter = _plain_number
) -> tuple[str, ...]:
    """Render settled or parked auction records from the seller/winner view."""

    lines: list[str] = []
    for record in records:
        role = "seller" if str(record.get("role")) == "seller" else "winner"
        role_label = "卖出" if role == "seller" else "拍得"
        item_name = str(record.get("item_name") or "")
        status = str(record.get("status") or "")
        final_price = record.get("final_price")
        counterparty = str(record.get("counterparty") or "未知")
        occurred_at = _timestamp_text(record.get("end_time"))

        if status == "流拍":
            parts = [role_label, item_name, "流拍", "未售出，物品已退回背包", occurred_at]
        elif status == "待领取":
            parts = [role_label, item_name, "待领取"]
            if final_price is not None:
                parts.append(f"成交价:{number(int(final_price))}")
            parts.append("物品在拍卖待领取")
            parts.append(occurred_at)
        else:
            parts = [role_label, item_name, "成交"]
            if final_price is not None:
                parts.append(f"成交价:{number(int(final_price))}")
            if role == "seller":
                parts.append(f"手续费:{number(int(record.get('fee') or 0))}")
                parts.append(f"实收:{number(int(record.get('seller_earnings') or 0))}")
                parts.append(f"买家:{counterparty}")
            else:
                parts.append(f"卖家:{counterparty}")
            parts.append(occurred_at)
        lines.append("｜".join(parts))
    return tuple(lines)


__all__ = ["format_remaining", "render_live_lots", "render_settlements"]
