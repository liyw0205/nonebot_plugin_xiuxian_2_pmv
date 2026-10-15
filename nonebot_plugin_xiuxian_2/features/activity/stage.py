"""Activity stage timeline, countdowns and reward rows (pure, read-only).

The stage window is the activity's own time progression: stages come from the
versioned config, resolve to 未开始/进行中/已结束 by time, and may declare
optional rewards that players claim after the stage ends.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from ...xiuxian.xiuxian_activity.activity_config import (
    _activity_stages_config,
    parse_time,
)
from ...xiuxian.xiuxian_activity.activity_utils import _clean_text

NOT_STARTED = "未开始"
IN_PROGRESS = "进行中"
ENDED = "已结束"


def stage_status(stage: Mapping[str, Any], now: datetime) -> str:
    """Lifecycle of one stage at ``now`` (time-driven, no writes)."""
    start = parse_time(stage.get("start_time"), is_start=True)
    end = parse_time(stage.get("end_time"), is_start=False)
    if start is not None and now < start:
        return NOT_STARTED
    if end is not None and now > end:
        return ENDED
    return IN_PROGRESS


def _format_span(seconds: float) -> str:
    total_hours = int(seconds // 3600)
    if total_hours < 24:
        return f"{max(1, total_hours)} 小时"
    return f"{total_hours // 24} 天"


def stage_remaining_text(stage: Mapping[str, Any], now: datetime) -> str:
    """Countdown text; empty for unlimited windows and ended stages."""
    status = stage_status(stage, now)
    if status == NOT_STARTED:
        start = parse_time(stage.get("start_time"), is_start=True)
        if start is None:
            return ""
        return _format_span((start - now).total_seconds()) + "后开启"
    if status == IN_PROGRESS:
        end = parse_time(stage.get("end_time"), is_start=False)
        if end is None:
            return ""
        remaining = (end - now).total_seconds()
        if remaining <= 0:
            return ""
        return "剩余 " + _format_span(remaining)
    return ""


def stage_reward_rows(stage: Mapping[str, Any]) -> list[dict[str, str]]:
    """Normalized reward rows of one stage (empty when none configured)."""
    rows = stage.get("rewards")
    if not isinstance(rows, list):
        return []
    return [
        {
            "key": _clean_text(row.get("key")),
            "name": _clean_text(row.get("name")),
            "reward": _clean_text(row.get("reward")),
        }
        for row in rows
        if isinstance(row, Mapping) and _clean_text(row.get("reward"))
    ]


def stage_timeline(config: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    """Resolved stage timeline: all stages plus active/ended/next picks."""
    stages: list[dict[str, Any]] = []
    for stage in _activity_stages_config(dict(config)):
        item = dict(stage)
        item["status"] = stage_status(stage, now)
        item["remaining_text"] = stage_remaining_text(stage, now)
        item["reward_rows"] = stage_reward_rows(stage)
        stages.append(item)
    return {
        "stages": stages,
        "active": next((s for s in stages if s["status"] == IN_PROGRESS), None),
        "ended": [s for s in stages if s["status"] == ENDED],
        "next": next((s for s in stages if s["status"] == NOT_STARTED), None),
    }


def stage_window_bounds(stage: Mapping[str, Any]) -> tuple[str | None, str | None]:
    """``(start_text, end_text)`` of a stage window in log timestamp format."""
    fmt = "%Y-%m-%d %H:%M:%S"
    start = parse_time(stage.get("start_time"), is_start=True)
    end = parse_time(stage.get("end_time"), is_start=False)
    return (
        start.strftime(fmt) if start else None,
        end.strftime(fmt) if end else None,
    )


def claimable_stage_rows(
    config: Mapping[str, Any],
    now: datetime,
    participated: set[str],
    claimed: set[str],
) -> list[dict[str, Any]]:
    """Ended stages the player took part in, has rewards for and not claimed."""
    rows: list[dict[str, Any]] = []
    for stage in stage_timeline(config, now)["ended"]:
        if stage["key"] in claimed or stage["key"] not in participated:
            continue
        rewards = stage["reward_rows"]
        if not rewards:
            continue
        rows.append({"key": stage["key"], "name": stage["name"], "rewards": rewards})
    return rows


__all__ = [
    "ENDED",
    "IN_PROGRESS",
    "NOT_STARTED",
    "claimable_stage_rows",
    "stage_remaining_text",
    "stage_reward_rows",
    "stage_status",
    "stage_timeline",
]
