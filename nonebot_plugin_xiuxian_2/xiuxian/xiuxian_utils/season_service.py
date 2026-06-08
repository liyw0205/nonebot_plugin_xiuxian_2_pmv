from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Union

from .periods import get_season_key

SeasonNow = Optional[Union[date, datetime]]

_SUPPORTED_SEASON_MODES = {"monthly", "weekly", "quarterly"}
_SEASON_MODE_NAMES = {
    "monthly": "月榜",
    "weekly": "周榜",
    "quarterly": "季度榜",
}

__all__ = [
    "SeasonInfo",
    "build_season_rank_key",
    "get_current_season",
    "normalize_season_mode",
]


@dataclass(frozen=True)
class SeasonInfo:
    mode: str
    key: str
    name: str


def normalize_season_mode(mode: str = "monthly") -> str:
    """Normalize season mode, falling back to monthly for unknown values."""
    normalized_mode = str(mode or "monthly").strip().lower()
    if normalized_mode in _SUPPORTED_SEASON_MODES:
        return normalized_mode
    return "monthly"


def get_current_season(mode: str = "monthly") -> SeasonInfo:
    """Return current season metadata for the requested mode."""
    normalized_mode = normalize_season_mode(mode)
    key = get_season_key(mode=normalized_mode)
    return SeasonInfo(
        mode=normalized_mode,
        key=key,
        name=f"{key}{_SEASON_MODE_NAMES[normalized_mode]}",
    )


def build_season_rank_key(
    rank_name: str,
    mode: str = "monthly",
    now: SeasonNow = None,
) -> str:
    """Build a stable rank key scoped by season mode and period."""
    normalized_mode = normalize_season_mode(mode)
    key = get_season_key(now=now, mode=normalized_mode)
    return f"season_rank:{normalized_mode}:{key}:{str(rank_name)}"
