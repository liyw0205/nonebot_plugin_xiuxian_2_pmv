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
    # Per-activity entry groups: (activity name, its entries). The renderer
    # emits one compact link row per group so every entry stays reachable
    # without building one unbounded row.
    gameplay_entry_groups: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = ()


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
            # Configured overrides render in full; entry-count caps belong to
            # the delivery layer, not to entry mapping.
            return tuple(commands)
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


def _source_entry_commands(
    config: Mapping[str, Any], now: datetime
) -> tuple[tuple[str, str], ...]:
    """Entries for the configured non-gameplay sources (stages, rank boards).

    Stages and rank boards are not gameplay activities, so they never reach
    ``gameplay_entry_commands``. They contribute an entry only when the config
    actually defines them, and only existing commands are mapped.
    """
    from ...xiuxian.xiuxian_activity.activity_config import activity_rank_config
    from .stage import stage_timeline

    entries: list[tuple[str, str]] = []
    if stage_timeline(dict(config), now)["stages"]:
        entries.append(("活动阶段", "活动阶段"))
    if activity_rank_config(dict(config))["boards"]:
        entries.append(("活动排行", "活动排行"))
    return tuple(entries)


def _claim_source_entry_commands(
    config: Mapping[str, Any], snapshot: Mapping[str, Any], now: datetime
) -> tuple[tuple[str, str], ...]:
    """Map actually claimable sources to their existing focused commands.

    The one-click claim and this card both consume the same read-only snapshot.
    An absent source state therefore produces no focused link instead of
    guessing from configuration alone.
    """
    from ...xiuxian.xiuxian_activity.activity_rules import _activity_pass_config

    entries: list[tuple[str, str]] = []
    if _claimable_stage_count(config, snapshot, now):
        entries.append(("活动阶段领取", "活动阶段领取"))
    if _claimable_rank_count(config, snapshot):
        entries.append(("活动排行领取", "活动排行领取"))
    pass_config = _activity_pass_config(dict(config))
    if claimable_pass_rewards(
        pass_config,
        snapshot.get("pass_total_exp"),
        snapshot.get("pass_claimed_levels") or set(),
    ):
        entries.append(("活动战令领取", "活动战令领取"))
    if _claimable_boss_milestone_count(config, snapshot):
        entries.append(("活动首领领奖 进度", "活动首领领奖 进度"))
    if _claimable_boss_rank_count(config, snapshot):
        entries.append(("活动首领领奖 排行", "活动首领领奖 排行"))
    return tuple(entries)


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
    daily_totals = snapshot.get("point_event_daily_totals")
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
            from .gameplay_progress import point_event_daily_parts

            daily_parts = (
                point_event_daily_parts(activity, daily_totals)
                if daily_totals is not None
                else []
            )
            if daily_parts:
                parts[-1] += f"（今日 {'、'.join(daily_parts)}）"
        elif activity.get("type") == "activity_boss":
            progress = (snapshot.get("boss_progress") or {}).get(activity_key)
            if not progress:
                continue
            configured_max = max(1, _as_int(activity.get("max_hp"), 1))
            hp_left = max(0, _as_int(progress.get("hp_left"), configured_max))
            pct = 100.0 * hp_left / configured_max if configured_max else 0.0
            segment = (
                f"{_clean_text(activity.get('boss_name'), '活动首领')} 血量 {pct:.0f}%"
                f"、我的伤害 {max(0, _as_int(progress.get('damage'), 0))}"
            )
            unlocked = set(progress.get("milestones_unlocked") or ())
            claimed = set(progress.get("milestones_claimed") or ())
            claimable = len((unlocked & _boss_milestone_keys(config)) - claimed)
            if claimable:
                segment += f"、进度奖励 {claimable} 项可领"
            parts.append(segment)
    return parts


def _stage_progress_part(config: Mapping[str, Any], now: datetime) -> str:
    """Current stage name and countdown; empty when no stage is running."""
    from .stage import stage_timeline

    active = stage_timeline(dict(config), now).get("active")
    if not active:
        return ""
    name = _clean_text(active.get("name") or active.get("key"), "阶段")
    remaining = _clean_text(active.get("remaining_text"))
    return f"阶段 {name} 剩余 {remaining}" if remaining else f"阶段 {name}"


def _rank_progress_parts(
    config: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> list[str]:
    """Own position per configured rank board; empty when unranked."""
    from ...xiuxian.xiuxian_activity.activity_config import activity_rank_config
    from .rank import position_text

    positions = snapshot.get("board_positions") or {}
    if not positions:
        return []
    parts: list[str] = []
    for board in activity_rank_config(dict(config)).get("boards") or []:
        source = str(board.get("source") or "")
        position = _as_int(positions.get(source), 0)
        if position <= 0:
            continue
        label = _clean_text(board.get("label") or board.get("key") or source, source)
        parts.append(f"{label} {position_text(position, 0)}")
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
    stage_part = _stage_progress_part(config, now)
    if stage_part:
        line += "、" + stage_part
    rank_parts = _rank_progress_parts(config, snapshot)
    if rank_parts:
        line += "、" + "、".join(rank_parts)
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
    # No cap here: every in-progress gameplay keeps its entries on the card.
    # Message-length constraints belong to the delivery layer, not to entry
    # mapping — a cap here silently dropped boss entries once a third gameplay
    # type was configured.
    return tuple(entries)


def gameplay_entry_groups(
    config: Mapping[str, Any],
    status: str,
    now: datetime,
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    """Per-activity entry groups for the card's link rows.

    Same selection rules as :func:`gameplay_entry_commands` (in-progress
    activities only, existing commands, deduped against earlier groups), but
    grouped per activity so the renderer can emit one compact row per gameplay
    instead of a single unbounded row.
    """
    if status != IN_PROGRESS:
        return ()
    groups: list[tuple[str, tuple[tuple[str, str], ...]]] = []
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
        name = _clean_text(activity.get("name"), "活动玩法")
        entries: list[tuple[str, str]] = []
        for label, command in _commands_for_activity(activity):
            if command in seen:
                continue
            seen.add(command)
            entries.append((label, command))
        if entries:
            groups.append((name, tuple(entries)))
    return tuple(groups)


def build_activity_center_items(
    config: Mapping[str, Any],
    now: datetime,
    *,
    claimable: bool = False,
    snapshot: Mapping[str, Any] | None = None,
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
    if main_status == IN_PROGRESS:
        main_commands += _source_entry_commands(config, now)
    elif main_status == REWARD_CLAIM and snapshot is not None:
        main_commands += _claim_source_entry_commands(config, snapshot, now)
    main_targets = {command for _, command in main_commands}
    gameplay_commands = tuple(
        (label, command)
        for label, command in gameplay_entry_commands(config, main_status, now)
        if command not in main_targets
    )
    gameplay_entry_groups_value = tuple(
        (name, tuple((label, command) for label, command in entries if command not in main_targets))
        for name, entries in gameplay_entry_groups(config, main_status, now)
        if any(command not in main_targets for _, command in entries)
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
        gameplay_entry_groups=gameplay_entry_groups_value,
    )
    return (main,)


def _claimable_stage_count(config: Mapping[str, Any], snapshot: Mapping[str, Any], now: datetime) -> int:
    """Ended stages the player took part in, has rewards for and not claimed."""
    from .stage import claimable_stage_rows

    participated = set(snapshot.get("stage_participated") or ())
    claimed = set(snapshot.get("stage_claimed") or ())
    if not participated:
        return 0
    return len(claimable_stage_rows(dict(config), now, participated, claimed))


def _claimable_rank_count(config: Mapping[str, Any], snapshot: Mapping[str, Any]) -> int:
    """Configured rank tiers the player's own position reaches and has not claimed."""
    from ...xiuxian.xiuxian_activity.activity_config import activity_rank_config
    from .rank import rewards_for_board, tier_for_position

    rank_config = activity_rank_config(dict(config))
    rewards = rank_config.get("rewards") or []
    if not rewards:
        return 0
    positions = snapshot.get("board_positions") or {}
    claimed = set(snapshot.get("rank_claimed") or ())
    count = 0
    for board in rank_config.get("boards") or []:
        tiers = rewards_for_board(rewards, board)
        if not tiers:
            continue
        position = _as_int(positions.get(str(board.get("source") or "")), 0)
        tier = tier_for_position(tiers, position)
        if tier is None:
            continue
        board_key = str(board.get("key") or board.get("source") or "")
        tier_key = str(tier.get("key") or "")
        if (board_key, tier_key) in claimed:
            continue
        count += 1
    return count


def _boss_milestone_keys(config: Mapping[str, Any]) -> set[str]:
    """Configured server-progress milestone keys, matching the unlock writer."""
    keys: set[str] = set()
    for activity in get_gameplay_activities(dict(config)):
        if activity.get("type") != "activity_boss":
            continue
        for milestone in activity.get("server_milestones") or []:
            if not isinstance(milestone, Mapping):
                continue
            key = milestone.get("key") or f"p{int(float(milestone.get('hp_percent', 0)))}"
            if str(key):
                keys.add(str(key))
    return keys


def _boss_claim_state(snapshot: Mapping[str, Any]) -> tuple[set[str], set[str], set[str]]:
    """Boss claim state from whichever representation the snapshot carries.

    ``boss_progress`` is keyed by the boss gameplay activity's own key, which is
    the key the boss tables actually use; the flat keys are the older
    single-activity projection. Both are unioned so the count never depends on
    which one the caller populated.
    """
    unlocked = set(snapshot.get("boss_milestone_unlocked") or ())
    claimed = set(snapshot.get("boss_milestone_claimed") or ())
    rank_claimed = set(snapshot.get("boss_rank_claimed") or ())
    for progress in (snapshot.get("boss_progress") or {}).values():
        if not isinstance(progress, Mapping):
            continue
        unlocked |= set(progress.get("milestones_unlocked") or ())
        claimed |= set(progress.get("milestones_claimed") or ())
        rank_claimed |= set(progress.get("rank_claimed") or ())
    return unlocked, claimed, rank_claimed


def _boss_claim_state_by_activity(
    snapshot: Mapping[str, Any],
) -> dict[str, tuple[set[str], set[str], set[str]]]:
    """Per-boss-activity claim state.

    The claim receipts are keyed by ``(activity_key, user_id, key)``, so each
    boss's unlocked/claimed sets are independent; a flat single-activity
    snapshot falls back to the shared projection for every boss that has no
    ``boss_progress`` entry.
    """
    flat: tuple[set[str], set[str], set[str]] = (
        set(snapshot.get("boss_milestone_unlocked") or ()),
        set(snapshot.get("boss_milestone_claimed") or ()),
        set(snapshot.get("boss_rank_claimed") or ()),
    )
    states: dict[str, tuple[set[str], set[str], set[str]]] = {}
    has_progress = False
    for activity_key, progress in (snapshot.get("boss_progress") or {}).items():
        if not isinstance(progress, Mapping):
            continue
        has_progress = True
        states[str(activity_key)] = (
            set(progress.get("milestones_unlocked") or ()),
            set(progress.get("milestones_claimed") or ()),
            set(progress.get("rank_claimed") or ()),
        )
    if not has_progress:
        return {"": flat}
    return states


def _claimable_boss_milestone_count(config: Mapping[str, Any], snapshot: Mapping[str, Any]) -> int:
    """Unlocked server-progress milestones the player has not claimed yet.

    Each boss activity is judged by its own unlocked/claimed sets; a shared
    union would let one boss's claim suppress another boss's milestone.
    """
    from ...xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities as _all_activities

    states = _boss_claim_state_by_activity(snapshot)
    count = 0
    for activity in get_gameplay_activities(dict(config)):
        if activity.get("type") != "activity_boss":
            continue
        activity_key = str(activity.get("key") or "")
        state = states.get(activity_key)
        if state is None and "" in states:
            # Flat single-activity snapshot: every boss falls back to it.
            state = states[""]
        if state is None:
            continue
        unlocked, claimed, _ = state
        configured = {
            str(milestone.get("key") or f"p{int(float(milestone.get('hp_percent', 0)))}")
            for milestone in activity.get("server_milestones") or []
            if isinstance(milestone, Mapping)
        }
        count += len((unlocked & configured) - claimed)
    return count


def _claimable_boss_rank_count(config: Mapping[str, Any], snapshot: Mapping[str, Any]) -> int:
    """Boss rank tiers the player's own damage rank reaches and has not claimed.

    Each boss activity is judged by its own rank position; a single shared
    position would let one boss's rank match another boss's reward tiers.
    """
    from .rank import tier_for_position

    per_activity = snapshot.get("boss_board_positions") or {}
    legacy_position = _as_int((snapshot.get("board_positions") or {}).get("boss"), 0)
    states = _boss_claim_state_by_activity(snapshot)
    count = 0
    for activity in get_gameplay_activities(dict(config)):
        if activity.get("type") != "activity_boss":
            continue
        activity_key = str(activity.get("key") or "")
        if activity_key and activity_key in per_activity:
            position = _as_int(per_activity.get(activity_key), 0)
        else:
            # Older snapshots carry only the shared position; with a single
            # boss activity that is this activity's own rank.
            position = legacy_position
        if position <= 0:
            continue
        state = states.get(activity_key)
        if state is None and "" in states:
            state = states[""]
        claimed = state[2] if state is not None else set()
        tiers = [
            {"rank_min": tier.get("rank_min"), "rank_max": tier.get("rank_max")}
            for tier in activity.get("rank_rewards") or []
            if isinstance(tier, Mapping)
        ]
        matched = tier_for_position(tiers, position)
        if matched is None:
            continue
        tier_key = f"{_as_int(matched.get('rank_min'), 0)}-{_as_int(matched.get('rank_max'), 0)}"
        if tier_key in claimed:
            continue
        count += 1
    return count


def detail_action_tips(
    config: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    now: datetime,
    helpers: Mapping[str, Any],
) -> list[str]:
    """Action-summary tips shared by the center card and the detail view.

    Covers exactly the sources the one-click claim covers: tasks, the battle
    pass (including catch-up), collect pity, stage, rank and both boss claim
    sources. Every rule delegates to the same helpers the center card uses, so
    the two surfaces cannot drift; a snapshot without a source's claim state
    simply contributes no tip.
    """
    from ...xiuxian.xiuxian_activity.activity_rules import (
        _activity_pass_config,
        _task_scope_key,
    )

    as_int = helpers["_as_int"]
    as_float = helpers["_as_float"]
    tips: list[str] = []

    tasks_state = snapshot.get("tasks") or {}
    claimable_tasks = 0
    for scope_type, rows in (
        ("daily", config.get("daily_tasks") or []),
        ("weekly", config.get("weekly_tasks") or []),
    ):
        scope_key = _task_scope_key(scope_type, now)
        for task in rows:
            if not isinstance(task, Mapping):
                continue
            task_key = _clean_text(task.get("key"))
            state = tasks_state.get((scope_type, scope_key, task_key), {})
            if task_claimable(state, task.get("target")):
                claimable_tasks += 1
    if claimable_tasks:
        tips.append(f"{claimable_tasks}个任务奖励可领")

    pass_config = _activity_pass_config(dict(config))
    if pass_config.get("enabled"):
        claimable = claimable_pass_rewards(
            pass_config,
            snapshot.get("pass_total_exp"),
            snapshot.get("pass_claimed_levels") or set(),
        )
        if claimable:
            tips.append(f"{len(claimable)}档战令奖励可领")
        elapsed = helpers["_activity_elapsed_days"](config, now)
        highest_level = as_int(snapshot.get("pass_highest_level"), 0)
        level = pass_level(pass_config, snapshot.get("pass_total_exp"))
        gap = max(0, highest_level - level)
        multiplier = max(1.0, as_float(pass_config.get("catchup_multiplier"), 1.5))
        if (
            pass_config.get("catchup_enabled")
            and elapsed >= max(1, as_int(pass_config.get("catchup_start_day"), 5))
            and gap >= max(1, as_int(pass_config.get("catchup_level_gap"), 3))
            and multiplier > 1.0
        ):
            tips.append(f"战令追赶{multiplier:.2f}x生效")

    pity_state = snapshot.get("collect_pity")
    if pity_state:
        for activity in get_gameplay_activities(dict(config)):
            if activity.get("type") != "collect_words":
                continue
            threshold = max(0, as_int(activity.get("pity_threshold"), 0))
            if threshold <= 0:
                continue
            for event_key in activity.get("drop_events") or []:
                if (
                    pity_state.get((str(activity.get("key") or ""), str(event_key)), 0)
                    >= max(1, threshold - 1)
                ):
                    label = helpers["ACTIVITY_EVENT_LABELS"].get(event_key, event_key)
                    tips.append(f"{activity['name']}接近保底：{label}")
                    break
            else:
                continue
            break

    if _claimable_stage_count(config, snapshot, now):
        tips.append("阶段奖励可领：活动阶段领取")
    if _claimable_rank_count(config, snapshot):
        tips.append("排行奖励可领：活动排行领取")
    if _claimable_boss_milestone_count(config, snapshot):
        tips.append("首领进度奖励可领：活动首领领奖 进度")
    if _claimable_boss_rank_count(config, snapshot):
        tips.append("首领排行奖励可领：活动首领领奖 排行")
    return tips


def claimable_reward_count(
    config: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    now: datetime,
) -> int:
    """Count claimable rewards across every source the one-click claim covers.

    Task and pass rewards come straight from the snapshot; stage, rank and boss
    sources read the claim state the snapshot carries and contribute nothing when
    that state is absent, so older callers keep their previous count.
    """
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
    count += _claimable_stage_count(config, snapshot, now)
    count += _claimable_rank_count(config, snapshot)
    count += _claimable_boss_milestone_count(config, snapshot)
    count += _claimable_boss_rank_count(config, snapshot)
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
