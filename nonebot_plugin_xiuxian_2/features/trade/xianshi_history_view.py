"""Pure rendering for a player's recent Xianshi trades."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Mapping, Sequence


NumberFormatter = Callable[[int], str]


def _plain_number(value: int) -> str:
    return str(int(value))


def _time_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "时间未知"
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], pattern).strftime("%m-%d %H:%M")
        except ValueError:
            continue
    return "时间未知"


def render_xianshi_trades(
    records: Sequence[Mapping[str, Any]], *, number: NumberFormatter = _plain_number
) -> tuple[str, ...]:
    """Render recent sales and purchases with a compact role summary."""

    lines: list[str] = []
    sold: list[Mapping[str, Any]] = []
    bought: list[Mapping[str, Any]] = []
    for record in records:
        seller = str(record.get("role")) == "seller"
        role_label = "卖出" if seller else "买入"
        (sold if seller else bought).append(record)
        counterparty = str(record.get("counterparty") or "未知")
        lines.append(
            f"{role_label}｜{record.get('name', '')} "
            f"x{int(record.get('quantity') or 0)}｜"
            f"{number(int(record.get('total_cost') or 0))}灵石｜"
            f"{'买家' if seller else '卖家'}:{counterparty}｜"
            f"{_time_text(record.get('created_at'))}"
        )
    if lines:
        sold_income = sum(int(record.get("total_cost") or 0) for record in sold)
        bought_spend = sum(int(record.get("total_cost") or 0) for record in bought)
        lines.append(
            f"最近 {len(records)} 笔：卖出 {len(sold)} 笔/收入 {number(sold_income)}｜"
            f"买入 {len(bought)} 笔/支出 {number(bought_spend)}"
        )
    return tuple(lines)


__all__ = ["render_xianshi_trades"]
