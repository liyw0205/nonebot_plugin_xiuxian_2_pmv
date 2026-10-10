from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from ...xiuxian.xiuxian_activity.activity_config import _activity_config_key, parse_time
from ...xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities
from ...xiuxian.xiuxian_activity.activity_utils import _as_int, _clean_text


NOT_STARTED = "未开始"
IN_PROGRESS = "进行中"
ENDED = "已结束"
REWARD_CLAIM = "奖励领取"


@dataclass(frozen=True)
class ActivityCenterItem:
    key: str
    name: str
    status: str
    description: str
    reward_hint: str
    commands: tuple[tuple[str, str], ...]


def resolve_status(
    start_time: Any,
    end_time: Any,
    now: datetime,
    *,
    enabled: bool = True,
    claimable: bool = False,
) -> str:
    """Resolve the user-facing lifecycle state without mutating activity data."""
    start = parse_time(start_time, is_start=True)
    end = parse_time(end_time, is_start=False)
    if start is not None and now < start:
        return NOT_STARTED
    if not enabled:
        return ENDED
    if claimable:
        return REWARD_CLAIM
    if end is not None and now > end:
        return ENDED
    return IN_PROGRESS


def _reward_hint(config: Mapping[str, Any], activities: list[dict[str, Any]]) -> str:
    rewards: list[str] = []
    for row in config.get("daily_rewards") or []:
        if isinstance(row, Mapping) and row.get("reward"):
            rewards.append(str(row["reward"]))
    for row in config.get("milestone_rewards") or []:
        if isinstance(row, Mapping) and row.get("reward"):
            rewards.append(str(row["reward"]))
    extensions = config.get("extensions")
    if isinstance(extensions, Mapping):
        pass_config = extensions.get("activity_pass")
        if isinstance(pass_config, Mapping):
            for row in pass_config.get("level_rewards") or []:
                if isinstance(row, Mapping) and row.get("reward"):
                    rewards.append(str(row["reward"]))
    for activity in activities:
        for field in ("phrases", "shop", "rank_rewards", "server_milestones"):
            for row in activity.get(field) or []:
                if isinstance(row, Mapping) and row.get("reward"):
                    rewards.append(str(row["reward"]))
    unique = list(dict.fromkeys(item for item in rewards if item.strip()))
    if not unique:
        return "暂无奖励说明"
    suffix = "……" if len(unique) > 3 else ""
    return "、".join(unique[:3]) + suffix


def _commands_for_activity(activity: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    configured = activity.get("commands")
    if isinstance(configured, list):
        commands = []
        for row in configured:
            if isinstance(row, Mapping):
                label = _clean_text(row.get("label"))
                command = _clean_text(row.get("command"))
                if label and command:
                    commands.append((label, command))
        if commands:
            return tuple(commands[:4])
    activity_type = _clean_text(activity.get("type"))
    defaults = {
        "collect_words": (("集字背包", "活动背包"), ("集字兑换", "活动兑换")),
        "event_points": (("活动积分", "活动积分"), ("活动商店", "活动商店")),
        "activity_boss": (("活动首领", "活动首领"), ("首领排行", "活动首领排行")),
    }
    return defaults.get(activity_type, (("活动玩法", "活动玩法"),))


def build_activity_center_items(
    config: Mapping[str, Any],
    now: datetime,
    *,
    claimable: bool = False,
) -> tuple[ActivityCenterItem, ...]:
    """Build center cards from the versioned config and normalized activities."""
    activities = get_gameplay_activities(dict(config))
    main = ActivityCenterItem(
        key=_activity_config_key(dict(config)),
        name=_clean_text(config.get("name"), "节日活动"),
        status=resolve_status(
            config.get("start_time"),
            config.get("end_time"),
            now,
            enabled=bool(config.get("enabled", False)),
            claimable=claimable,
        ),
        description=_clean_text(config.get("description"), ""),
        reward_hint=_reward_hint(config, activities),
        commands=(("活动签到", "活动签到"), ("活动任务", "活动任务"), ("活动领取", "活动领取")),
    )
    items = [main]
    for activity in activities:
        items.append(
            ActivityCenterItem(
                key=_clean_text(activity.get("key"), "activity"),
                name=_clean_text(activity.get("name"), "活动玩法"),
                status=resolve_status(
                    activity.get("start_time"),
                    activity.get("end_time"),
                    now,
                    enabled=bool(activity.get("enabled", False)),
                ),
                description=_clean_text(activity.get("description"), ""),
                reward_hint=_reward_hint({}, [activity]),
                commands=_commands_for_activity(activity),
            )
        )
    return tuple(items)


def claimable_reward_count(
    config: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    now: datetime,
) -> int:
    """Count claimable task/pass rewards from one read-model snapshot."""
    from ...xiuxian.xiuxian_activity.activity_rules import (
        _activity_pass_config,
        _task_scope_key,
    )

    count = 0
    tasks = snapshot.get("tasks") or {}
    for scope_type, rows in (
        ("daily", config.get("daily_tasks") or []),
        ("weekly", config.get("weekly_tasks") or []),
    ):
        scope_key = _task_scope_key(scope_type, now)
        for task in rows:
            if not isinstance(task, Mapping):
                continue
            task_key = _clean_text(task.get("key"))
            state = tasks.get((scope_type, scope_key, task_key), {})
            target = max(1, _as_int(task.get("target"), 1))
            if not state.get("claimed") and _as_int(state.get("progress"), 0) >= target:
                count += 1

    pass_config = _activity_pass_config(dict(config))
    if pass_config.get("enabled", True):
        level_exp = max(1, _as_int(pass_config.get("level_exp"), 100))
        max_level = max(1, _as_int(pass_config.get("max_level"), 12))
        level = min(max(0, _as_int(snapshot.get("pass_total_exp"), 0)) // level_exp, max_level)
        claimed = snapshot.get("pass_claimed_levels") or set()
        count += sum(
            1
            for reward in pass_config.get("level_rewards") or []
            if isinstance(reward, Mapping)
            and 0 < _as_int(reward.get("level"), 0) <= level
            and _as_int(reward.get("level"), 0) not in claimed
        )
    return count


__all__ = [
    "ActivityCenterItem",
    "ENDED",
    "IN_PROGRESS",
    "NOT_STARTED",
    "REWARD_CLAIM",
    "build_activity_center_items",
    "claimable_reward_count",
    "resolve_status",
]
