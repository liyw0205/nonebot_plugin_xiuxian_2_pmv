from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class ActivityClaimReceiptView:
    created_at: str
    status: str
    ok: bool
    settled_labels: tuple[str, ...] = ()
    failed_labels: tuple[str, ...] = ()
    text: str = ""


def render_claim_receipts(
    receipts: Sequence[ActivityClaimReceiptView], *, limit: int = 5
) -> str:
    size = max(int(limit), 0)
    rows = list(receipts or ())[:size]
    if not rows:
        return ""
    lines = ["", "【最近一键领取】"]
    for item in rows:
        stamp = item.created_at or "时间未知"
        settled = "、".join(item.settled_labels) or "无来源"
        failed = "、".join(item.failed_labels)
        if item.status == "处理中":
            line = f"{stamp} 处理中（可重试）"
        elif item.ok:
            line = f"{stamp} 已领取：{settled}"
            if failed:
                line += f"；未完成：{failed}"
        else:
            line = f"{stamp} 未领取：{settled}"
            if failed:
                line = f"{stamp} 未领取：{failed}"
        lines.append(line)
    lines.append(f"仅显示最近最多 {max(size, 1)} 次一键领取；单来源领取请查看活动奖励。")
    return "\n".join(lines)


__all__ = ["ActivityClaimReceiptView", "render_claim_receipts"]
