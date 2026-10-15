from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from ...xiuxian.xiuxian_activity.activity_config import _activity_config_key, parse_time
from ...xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities
from ...xiuxian.xiuxian_activity.activity_utils import _as_int, _clean_text
from .gameplay_progress import (
    claimable_pass_rewards,
    closest_pity_entry,
    collect_pity_entries,
    gameplay_status_text,
    pass_level,
    task_claimable,
)


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
    gameplay_summary: str = ""
    time_hint: str = ""
    gameplay_commands: tuple[tuple[str, str], ...] = ()


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


# 状态入口映射：只映射既有命令，不新增能力。未开始的玩法没有可参与的入口；
# 已结束只保留结算类入口（可领奖时由 REWARD_CLAIM 状态给出）。
MAIN_STATUS_COMMANDS: Mapping[str, tuple[tuple[str, str], ...]] = {
    NOT_STARTED: (("查看详情", "活动信息"),),
    IN_PROGRESS: (
        ("活动签到", "活动签到"),
        ("活动任务", "活动任务"),
        ("活动战令", "活动战令"),
    ),
    REWARD_CLAIM: (("领取奖励", "活动领取"), ("活动任务", "活动任务")),
    ENDED: (),
}


def commands_for_status(
    status: str,
    activity: Mapping[str, Any] | None = None,
) -> tuple[tuple[str, str], ...]:
    """Map one lifecycle state onto the existing entry commands it allows."""
    if activity is None:
        return MAIN_STATUS_COMMANDS.get(status, ())
    if status != IN_PROGRESS:
        return ()
    return _commands_for_activity(activity)


def _format_countdown(delta_seconds: float) -> str:
    total_hours = int(delta_seconds // 3600)
    if total_hours < 24:
        return f"{max(1, total_hours)} 小时"
    return f"{total_hours // 24} 天"


def time_hint(
    status: str,
    start_time: Any,
    end_time: Any,
    now: datetime,
) -> str:
    """Config-driven countdown text; empty for unlimited/ended states."""
    if status == NOT_STARTED:
        start = parse_time(start_time, is_start=True)
        if start is None:
            return ""
        return _format_countdown((start - now).total_seconds()) + "后开启"
    if status == IN_PROGRESS:
        end = parse_time(end_time, is_start=False)
        if end is None:
            return ""
        remaining = (end - now).total_seconds()
        if remaining <= 0:
            return ""
        return "剩余 " + _format_countdown(remaining)
    return ""


def _gameplay_progress_parts(
    config: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> list[str]:
    """Read-only collect/points progress segments from one overview snapshot.

    Only keys the snapshot actually carries are used, so callers that do not
    request collect/point state simply get no extra segment.
    """
    pity_state = snapshot.get("collect_pity")
    balances = snapshot.get("point_balances")
    parts: list[str] = []
    for activity in get_gameplay_activities(dict(config)):
        activity_key = str(activity.get("key") or "")
        if not activity_key:
            continue
        if activity.get("type") == "collect_words":
            if pity_state is None:
                continue
            entries = collect_pity_entries(activity, pity_state)
            closest = closest_pity_entry(entries)
            if closest is None:
                continue
            # 与活动背包同源：取最接近保底的事件，数字必须与背包一致。
            _, miss, threshold = closest
            parts.append(
                f"{_clean_text(activity.get('name'), '集字')} 保底 {miss}/{threshold}"
            )
        elif activity.get("type") == "event_points":
            if balances is None or activity_key not in balances:
                continue
            point_name = _clean_text(activity.get("point_name"), "活动积分")
            parts.append(
                f"{point_name} {max(0, _as_int(balances.get(activity_key), 0))}"
            )
    return parts


def personal_progress_summary(
    config: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    now: datetime,
) -> str:
    """Read-only personal progress line built from one read-model snapshot."""
    from ...xiuxian.xiuxian_activity.activity_rules import (
        _activity_pass_config,
        _task_scope_key,
    )

    parts: list[str] = []
    # 今日签到状态与结算侧同一语义：last_sign_date 等于当天日期即已签。
    sign_state = snapshot.get("sign") or {}
    signed_today = (
        _clean_text(sign_state.get("last_sign_date")) == now.strftime("%Y-%m-%d")
    )
    sign_part = "签到 已签" if signed_today else "签到 未签"
    rows: list[tuple[str, list[Any]]] = [
        ("日常", list(config.get("daily_tasks") or [])),
        ("周常", list(config.get("weekly_tasks") or [])),
    ]
    tasks = snapshot.get("tasks") or {}
    for scope_label, scope_rows in rows:
        if not scope_rows:
            continue
        scope_key = _task_scope_key("daily" if scope_label == "日常" else "weekly", now)
        done = 0
        for task in scope_rows:
            if not isinstance(task, Mapping):
                continue
            state = tasks.get(("daily" if scope_label == "日常" else "weekly", scope_key, _clean_text(task.get("key"))), {})
            target = max(1, _as_int(task.get("target"), 1))
            if _as_int(state.get("progress"), 0) >= target:
                done += 1
        parts.append(f"{scope_label}任务 {done}/{len(scope_rows)}")
    pass_config = _activity_pass_config(dict(config))
    claimable = claimable_reward_count(config, snapshot, now)
    line = "进度：" + sign_part + (
        "、" + "、".join(parts) if parts else "、暂无任务"
    )
    if pass_config.get("enabled", True):
        level = pass_level(pass_config, snapshot.get("pass_total_exp"))
        line += f"、战令 Lv{level}"
    gameplay_parts = _gameplay_progress_parts(config, snapshot)
    if gameplay_parts:
        line += "、" + "、".join(gameplay_parts)
    if claimable:
        line += f"、可领 {claimable} 项"
    return line


def gameplay_entry_commands(
    config: Mapping[str, Any],
    status: str,
    now: datetime,
) -> tuple[tuple[str, str], ...]:
    """Entries for the gameplay activities the card advertises.

    Only offered while the activity window is open (IN_PROGRESS) and only for
    gameplay activities that are themselves in progress; every target is an
    existing command, deduped in configuration order.
    """
    if status != IN_PROGRESS:
        return ()
    entries: list[tuple[str, str]] = []
    seen: set[str] = set()
    for activity in get_gameplay_activities(dict(config)):
        activity_status = resolve_status(
            activity.get("start_time"),
            activity.get("end_time"),
            now,
            enabled=bool(activity.get("enabled", False)),
        )
        if activity_status != IN_PROGRESS:
            continue
        for label, command in _commands_for_activity(activity):
            if command in seen:
                continue
            seen.add(command)
            entries.append((label, command))
    return tuple(entries[:4])


def build_activity_center_items(
    config: Mapping[str, Any],
    now: datetime,
    *,
    claimable: bool = False,
) -> tuple[ActivityCenterItem, ...]:
    """Build center cards from the versioned config and normalized activities.

    One card only: the main activity aggregates description, rewards and the
    entry commands its lifecycle state allows; gameplay states are listed as
    a short line instead of repeating identical cards/links (QQ short-message
    contract: no duplicated primary-level blocks).
    """
    activities = get_gameplay_activities(dict(config))
    main_status = resolve_status(
        config.get("start_time"),
        config.get("end_time"),
        now,
        enabled=bool(config.get("enabled", False)),
        claimable=claimable,
    )
    gameplay_lines = [
        f"{_clean_text(activity.get('name'), '活动玩法')}·{gameplay_status_text(activity)}"
        for activity in activities
    ]
    main_commands = commands_for_status(main_status)
    main_targets = {command for _, command in main_commands}
    gameplay_commands = tuple(
        (label, command)
        for label, command in gameplay_entry_commands(config, main_status, now)
        if command not in main_targets
    )
    main = ActivityCenterItem(
        key=_activity_config_key(dict(config)),
        name=_clean_text(config.get("name"), "节日活动"),
        status=main_status,
        description=_clean_text(config.get("description"), ""),
        reward_hint=_reward_hint(config, activities),
        commands=main_commands,
        gameplay_summary="；".join(gameplay_lines[:4]),
        time_hint=time_hint(
            main_status, config.get("start_time"), config.get("end_time"), now
        ),
        gameplay_commands=gameplay_commands,
    )
    return (main,)


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
            if task_claimable(state, task.get("target")):
                count += 1

    pass_config = _activity_pass_config(dict(config))
    count += len(
        claimable_pass_rewards(
            pass_config,
            snapshot.get("pass_total_exp"),
            snapshot.get("pass_claimed_levels") or set(),
        )
    )
    return count


__all__ = [
    "ActivityCenterItem",
    "ENDED",
    "IN_PROGRESS",
    "MAIN_STATUS_COMMANDS",
    "NOT_STARTED",
    "REWARD_CLAIM",
    "build_activity_center_items",
    "claimable_reward_count",
    "commands_for_status",
    "gameplay_entry_commands",
    "personal_progress_summary",
    "resolve_status",
    "time_hint",
]
