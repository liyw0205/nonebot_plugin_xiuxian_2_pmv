"""Player-visible Guishi order status lines.

``鬼市信息`` listed the id, item, unit price and remaining quantity of each order
but never said when it expires — the one fact the lifecycle rule turns on. This
module renders that line from data the caller already read, so the command stays a
thin adapter and the wording is pinned by unit tests.

Pure rendering: no I/O, no clock reads, no NoneBot.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from .guishi_lifecycle import (
    DEFAULT_GUISHI_END_HOUR,
    order_deadline,
    order_remaining_seconds,
    parse_created_at,
)

_QIUGOU_TYPES = ("qiugou", "求购")


def format_remaining(seconds: int) -> str:
    """Render a remainder as ``X小时Y分`` (``Y分`` under an hour)."""

    seconds = max(int(seconds), 0)
    hours, remainder = divmod(seconds, 3600)
    minutes = remainder // 60
    if hours:
        return f"{hours}小时{minutes}分"
    return f"{minutes}分"


def format_stone(value: Any) -> str:
    """Format a stone amount with thousands separators, like the command layer."""

    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return "0"


def order_status_line(
    order: Mapping[str, Any],
    *,
    created_at: Any = None,
    now: datetime,
    end_hour: int = DEFAULT_GUISHI_END_HOUR,
) -> str:
    """One status line: the historical fields plus the deadline or remaining time."""

    quantity = int(order.get("quantity") or 0)
    filled = int(order.get("filled_quantity") or 0)
    remaining = max(quantity - filled, 0)
    is_qiugou = str(order.get("item_type") or "") in _QIUGOU_TYPES
    label = "待购" if is_qiugou else "待售"
    marker = "到期" if is_qiugou else "截止"
    base = (
        f"ID:{order.get('id')} {order.get('item_name')} "
        f"{format_stone(order.get('price'))}灵石 x{quantity} ({label}:{remaining})"
    )

    created = parse_created_at(created_at)
    deadline = order_deadline(created, end_hour=end_hour)
    if deadline is None:
        return f"{base} {marker}：未知（历史订单）"
    if now >= deadline:
        return f"{base} {marker}：已{marker}（等待清理）"
    remaining_text = format_remaining(
        order_remaining_seconds(created, now, end_hour=end_hour)
    )
    return (
        f"{base} {marker}：{deadline.strftime('%m-%d %H:%M')}"
        f"（剩余 {remaining_text}）"
    )


def order_status_lines(
    orders: Sequence[Mapping[str, Any]] | None,
    created_at_by_order: Mapping[str, Any] | None = None,
    *,
    now: datetime,
    end_hour: int = DEFAULT_GUISHI_END_HOUR,
) -> list[str]:
    """Render every order of one player, preserving the query order."""

    anchors = created_at_by_order or {}
    return [
        order_status_line(
            order,
            created_at=anchors.get(str(order.get("id") or "")),
            now=now,
            end_hour=end_hour,
        )
        for order in orders or []
    ]


__all__ = [
    "format_remaining",
    "format_stone",
    "order_status_line",
    "order_status_lines",
]
