"""Shared constants and name normalization for the Xianshi market watch slice.

The market watch feature is read-only over the existing Xianshi ledgers
(``xianshi_operations`` / ``xianshi_item``) and owns exactly two additive
tables for the player's watch list and its operation receipts.
"""

from __future__ import annotations

# How many items one player may watch at a time.
WATCH_LIMIT = 10

# Longest accepted item name (the catalogue's longest names are well below).
WATCH_NAME_MAX = 24

# Aggregation window for trade quotes, in days.
MARKET_WINDOW_DAYS = 7

# Upper bound on names accepted by one snapshot request.
WATCH_SNAPSHOT_LIMIT = 64

_CONTROL_CHARS = frozenset(chr(code) for code in range(0x20) if chr(code) not in {" "})


def normalize_goods_name(value: object) -> str:
    """Trim and collapse internal whitespace; keep the player's casing.

    Returns an empty string for non-strings or whitespace-only input so callers
    can reject it as ``invalid_name`` before touching the database.
    """

    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    cleaned = " ".join(value.replace("\u3000", " ").split())
    return cleaned.strip()


def is_valid_goods_name(name: str) -> bool:
    """A watch name must be short, printable and free of control characters."""

    if not name or len(name) > WATCH_NAME_MAX:
        return False
    return not any(char in _CONTROL_CHARS or char in {"\n", "\r", "\t"} for char in name)


def name_key(name: str) -> str:
    """Case-insensitive lookup key used for local aggregation maps."""

    return normalize_goods_name(name).casefold()


__all__ = [
    "MARKET_WINDOW_DAYS",
    "WATCH_LIMIT",
    "WATCH_NAME_MAX",
    "WATCH_SNAPSHOT_LIMIT",
    "is_valid_goods_name",
    "name_key",
    "normalize_goods_name",
]
