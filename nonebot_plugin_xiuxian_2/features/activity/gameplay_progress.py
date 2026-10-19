"""Shared read-only gameplay progress computations.

The center card and the gameplay detail commands (活动背包/活动积分/活动首领)
must report the same numbers from the same snapshot. Keeping the computation in
one place is what makes that consistency enforceable and testable.
"""

from __future__ import annotations

from typing import Any, Mapping

from ...xiuxian.xiuxian_activity.activity_utils import _as_int, _clean_text
from ...xiuxian.xiuxian_activity.activity_views import ACTIVITY_EVENT_LABELS


def collect_pity_entries(
    activity: Mapping[str, Any],
    pity_state: Mapping[Any, Any],
) -> list[tuple[str, int, int]]:
    """``(label, miss, threshold)`` per configured drop event, in config order.

    ``miss`` is the stored miss count capped at the threshold, matching what
    the activity bag shows. Returns an empty list when the activity has no
    pity threshold or no configured drop events.
    """
    threshold = max(0, _as_int(activity.get("pity_threshold"), 0))
    if threshold <= 0:
        return []
    activity_key = str(activity.get("key") or "")
    entries: list[tuple[str, int, int]] = []
    for raw_event in activity.get("drop_events") or []:
        event = str(raw_event)
        if not event:
            continue
        label = ACTIVITY_EVENT_LABELS.get(event, event)
        miss = min(
            threshold,
            max(0, _as_int(pity_state.get((activity_key, event), 0), 0)),
        )
        entries.append((label, miss, threshold))
    return entries


def closest_pity_entry(
    entries: list[tuple[str, int, int]],
) -> tuple[str, int, int] | None:
    """The entry closest to pity: largest miss, config order breaking ties."""
    if not entries:
        return None
    return max(entries, key=lambda item: item[1])


def gameplay_status_text(activity: Mapping[str, Any]) -> str:
    """Status text for one gameplay activity, shared by card and detail views.

    Uses ``activity_state`` so the card cannot claim a different lifecycle than
    the detail commands (e.g. 已结束 vs 活动未开启).
    """
    from ...xiuxian.xiuxian_activity.activity_config import activity_state

    ok, reason = activity_state(dict(activity))
    return "进行中" if ok else (_clean_text(reason) or "未开放")


def point_event_daily_parts(
    activity: Mapping[str, Any],
    daily_totals: Mapping[tuple[str, str], int],
) -> list[str]:
    """Per-rule daily progress parts for one points activity, in config order.

    ``daily_totals`` maps ``(activity_key, event_key)`` to the points earned
    today (the same accumulation the settlement-side daily limit reads). Only
    rules with ``daily_limit > 0`` render progress; the part reads
    ``{label} 今日 {earned}/{limit}`` and appends ``已达上限`` when the cap is
    reached.
    """
    activity_key = str(activity.get("key") or "")
    parts: list[str] = []
    for rule in activity.get("event_rules") or []:
        if not isinstance(rule, Mapping):
            continue
        limit = max(0, _as_int(rule.get("daily_limit"), 0))
        if limit <= 0:
            continue
        event = str(rule.get("event") or "")
        label = ACTIVITY_EVENT_LABELS.get(event, event)
        earned = min(
            limit,
            max(0, _as_int(daily_totals.get((activity_key, event), 0), 0)),
        )
        part = f"{label} 今日 {earned}/{limit}"
        if earned >= limit:
            part += "（已达上限）"
        parts.append(part)
    return parts


def task_claimable(state: Mapping[str, Any], target: Any) -> bool:
    """A task reward is claimable when it is unclaimed and progress is met."""
    if bool(_as_int(state.get("claimed"), 0)):
        return False
    return _as_int(state.get("progress"), 0) >= max(1, _as_int(target, 1))


def pass_level(pass_config: Mapping[str, Any], total_exp: Any) -> int:
    """Current pass level for a total exp amount (clamped to max level)."""
    level_exp = max(1, _as_int(pass_config.get("level_exp"), 100))
    max_level = max(1, _as_int(pass_config.get("max_level"), 12))
    return min(max(0, _as_int(total_exp, 0)) // level_exp, max_level)


def claimable_pass_rewards(
    pass_config: Mapping[str, Any],
    total_exp: Any,
    claimed_levels: Any,
) -> list[int]:
    """Levels whose pass reward is claimable (reached and not yet claimed)."""
    if not pass_config.get("enabled", True):
        return []
    level = pass_level(pass_config, total_exp)
    claimed = {_as_int(item, 0) for item in (claimed_levels or ())}
    return [
        reward_level
        for reward in pass_config.get("level_rewards") or []
        if isinstance(reward, Mapping)
        and 0 < (reward_level := _as_int(reward.get("level"), 0)) <= level
        and reward_level not in claimed
    ]


__all__ = [
    "claimable_pass_rewards",
    "closest_pity_entry",
    "collect_pity_entries",
    "gameplay_status_text",
    "pass_level",
    "task_claimable",
]
