"""Pure helpers for the activity rank boards.

Board resolution, reward-tier matching and value rendering stay free of IO so
the focused tests can exercise the player flow without a database or a running
bot.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

BOARD_VALUE_LABELS = {
    "sign": "累计签到",
    "points": "累计积分",
    "collect": "集字总数",
    "pass": "战令等级",
    "boss": "累计伤害",
}
BOARD_VALUE_SUFFIX = {
    "sign": "天",
    "points": "分",
    "collect": "字",
    "pass": "级",
    "boss": "点",
}


def board_by_query(
    boards: Sequence[Mapping[str, Any]], query: str
) -> dict[str, Any] | None:
    """Resolve a player supplied board name to one configured board."""
    target = str(query or "").strip()
    if not target:
        return None
    folded = target.casefold()
    for board in boards:
        candidates = (
            str(board.get("key") or ""),
            str(board.get("label") or ""),
            str(board.get("source") or ""),
        )
        if folded in {candidate.casefold() for candidate in candidates if candidate}:
            return dict(board)
    return None


def board_labels(boards: Sequence[Mapping[str, Any]]) -> str:
    """Comma separated label list used by the unknown-board failure message."""
    labels = [
        str(board.get("label") or board.get("key") or "")
        for board in boards
        if str(board.get("label") or board.get("key") or "")
    ]
    return "、".join(labels)


def rewards_for_board(
    rewards: Iterable[Mapping[str, Any]], board: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Reward tiers configured for one board, ordered by rank range."""
    keys = {str(board.get("key") or ""), str(board.get("source") or "")}
    rows = [dict(row) for row in rewards if str(row.get("board") or "") in keys]
    return sorted(
        rows, key=lambda row: (int(row.get("rank_min") or 1), int(row.get("rank_max") or 1))
    )


def tier_for_position(
    tiers: Sequence[Mapping[str, Any]], position: int
) -> dict[str, Any] | None:
    """Reward tier covering a 1-based position (None when unranked or outside)."""
    position = int(position or 0)
    if position <= 0:
        return None
    for tier in tiers:
        low = int(tier.get("rank_min") or 0)
        high = int(tier.get("rank_max") or 0)
        if low <= position <= high:
            return dict(tier)
    return None


def value_text(source: str, value: int) -> str:
    """Render a board value with its board-specific label and suffix."""
    source = str(source or "")
    label = BOARD_VALUE_LABELS.get(source, "数值")
    suffix = BOARD_VALUE_SUFFIX.get(source, "")
    return f"{label} {max(0, int(value))}{suffix}"


def position_text(position: int, total: int) -> str:
    """Render the player's own position, or the unranked marker."""
    position = int(position or 0)
    total = max(0, int(total or 0))
    if position <= 0:
        return "尚未上榜"
    if total > 0:
        return f"第 {position} 名 / 共 {total} 人"
    return f"第 {position} 名"


def display_name(user_id: str, user_name: str = "") -> str:
    """Fallback display name shared by every board."""
    name = str(user_name or "").strip()
    if name:
        return name
    uid = str(user_id or "")
    if len(uid) > 6:
        return f"修士·{uid[-4:]}"
    return uid or "无名修士"


__all__ = [
    "BOARD_VALUE_LABELS",
    "BOARD_VALUE_SUFFIX",
    "board_by_query",
    "board_labels",
    "display_name",
    "position_text",
    "rewards_for_board",
    "tier_for_position",
    "value_text",
]
