from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class BankOperationView:
    kind: str
    amount: int
    interest: int
    saved_after: int
    wallet_after: int
    created_at: str


def render_bank_history(operations: Sequence[BankOperationView]) -> str:
    lines = ["", "【最近灵庄记录】"]
    rows = list(operations or ())
    if not rows:
        lines.append("暂无记录。")
        return "\n".join(lines)
    for item in rows:
        stamp = item.created_at or "时间未知"
        if item.kind == "存入":
            lines.append(
                f"{stamp} 存入 {item.amount}，结息 {item.interest}，存款 {item.saved_after}"
            )
        elif item.kind == "取出":
            lines.append(
                f"{stamp} 取出 {item.amount}，结息 {item.interest}，存款 {item.saved_after}"
            )
        elif item.kind == "结息":
            lines.append(f"{stamp} 结息 {item.interest}，存款 {item.saved_after}")
        elif item.kind == "会员升级":
            lines.append(f"{stamp} 会员升级")
        else:
            lines.append(
                f"{stamp} 账户操作 {item.amount}，存款 {item.saved_after}"
            )
    return "\n".join(lines)


__all__ = ["BankOperationView", "render_bank_history"]
