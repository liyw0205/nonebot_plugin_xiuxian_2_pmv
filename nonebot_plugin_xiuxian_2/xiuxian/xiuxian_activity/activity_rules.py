from copy import deepcopy

from ..xiuxian_utils.activity_helpers import as_bool as _as_bool
from .activity_config import (
    DEFAULT_ACTIVITY_PASS,
    DEFAULT_ACTIVITY_STAGES,
    DEFAULT_PASS_EVENT_RULES,
    STAGE_TYPE_LABELS,
    _get_extensions,
    _runtime_allows,
    activity_state,
    load_config,
    parse_time,
)
from .activity_storage import DEFAULT_COLLECT_DROP_EVENTS, DEFAULT_POINT_EVENT_RULES, now_dt, today_str
from .activity_utils import (
    _as_float,
    _as_int,
    _clean_text,
    _drop_rate,
    _normalize_activity_key,
)
from .activity_views import ACTIVITY_EVENT_LABELS

def _normalize_event_list(value, default: list[str] | None = None) -> list[str]:
    if isinstance(value, str):
        source = value.replace("\n", ",").split(",")
    elif isinstance(value, list):
        source = value
    else:
        source = default or []

    events: list[str] = []
    seen_events: set[str] = set()
    for item in source:
        if isinstance(item, dict):
            event = _clean_text(item.get("value"))
        else:
            event = _clean_text(item)
        if not event or event in seen_events or event not in ACTIVITY_EVENT_LABELS:
            continue
        seen_events.add(event)
        events.append(event)
    return events


def _collect_phrases(activity: dict) -> list[dict]:
    rows = activity.get("phrases")
    if not isinstance(rows, list):
        return []

    phrases: list[dict] = []
    seen_phrases: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        phrase = _clean_text(row.get("phrase") or row.get("name"))
        if not phrase or phrase in seen_phrases:
            continue
        seen_phrases.add(phrase)
        phrases.append({
            "phrase": phrase,
            "name": _clean_text(row.get("name"), phrase),
            "reward": _clean_text(row.get("reward")),
            "limit": max(0, _as_int(row.get("limit", row.get("limit_per_user", 1)), 1)),
        })
    return phrases


def _collect_letters(activity: dict, phrases: list[dict] | None = None) -> list[dict]:
    rows = activity.get("letters")
    weights: dict[str, int] = {}
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                text = _clean_text(row.get("char") or row.get("word") or row.get("value"))
                weight = _as_int(row.get("weight"), 10)
            else:
                text = _clean_text(row)
                weight = 10
            if not text:
                continue
            word_char = text[0]
            weights[word_char] = weights.get(word_char, 0) + max(1, weight)

    for phrase_row in phrases or _collect_phrases(activity):
        for word_char in str(phrase_row.get("phrase") or ""):
            if word_char.strip() and word_char not in weights:
                weights[word_char] = 10

    return [
        {"char": word_char, "weight": weight}
        for word_char, weight in weights.items()
    ]


def _point_event_rules(activity: dict) -> list[dict]:
    rows = activity.get("event_rules")
    if isinstance(rows, dict):
        source = [
            {"event": event, "points": points}
            for event, points in rows.items()
        ]
    elif isinstance(rows, list):
        source = rows
    else:
        source = DEFAULT_POINT_EVENT_RULES

    rules: list[dict] = []
    seen_events: set[str] = set()
    for row in source:
        if isinstance(row, dict):
            event = _clean_text(row.get("event") or row.get("event_key") or row.get("value"))
            points = _as_int(row.get("points"), 0)
            daily_limit = _as_int(row.get("daily_limit"), 0)
        else:
            event = _clean_text(row)
            points = 0
            daily_limit = 0
        if not event or event in seen_events or event not in ACTIVITY_EVENT_LABELS:
            continue
        points = max(0, points)
        if points <= 0:
            continue
        seen_events.add(event)
        rules.append({
            "event": event,
            "points": points,
            "daily_limit": max(0, daily_limit),
        })
    return rules


def _normalize_activity_task_rows(rows, scope_type: str) -> list[dict]:
    if not isinstance(rows, list):
        return []

    tasks: list[dict] = []
    seen_keys: set[str] = set()
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            continue
        target = max(1, _as_int(row.get("target"), 1))
        events = _normalize_event_list(row.get("events"))
        if not events:
            continue
        task_key = _normalize_activity_key(
            row.get("key") or row.get("name") or f"{scope_type}_{index}",
            f"{scope_type}_{index}",
        )
        if task_key in seen_keys:
            task_key = f"{task_key}_{index}"
        seen_keys.add(task_key)
        tasks.append({
            "key": task_key,
            "name": _clean_text(row.get("name"), f"活动任务{index}"),
            "description": _clean_text(row.get("description") or row.get("desc")),
            "target": target,
            "events": events,
            "reward": _clean_text(row.get("reward")),
            "scope_type": scope_type,
        })
    return tasks


def get_activity_tasks(config: dict | None = None, scope_type: str | None = None) -> list[dict]:
    cfg = config or load_config()
    tasks = []
    if scope_type in (None, "daily"):
        tasks.extend(_normalize_activity_task_rows(cfg.get("daily_tasks"), "daily"))
    if scope_type in (None, "weekly"):
        tasks.extend(_normalize_activity_task_rows(cfg.get("weekly_tasks"), "weekly"))
    return tasks


def _week_key() -> str:
    year, week, _ = now_dt().isocalendar()
    return f"{year}-W{week:02d}"


def _task_scope_key(scope_type: str) -> str:
    return today_str() if scope_type == "daily" else _week_key()


def _activity_pass_config(config: dict | None = None) -> dict:
    cfg = config if config is not None else load_config()
    raw = _get_extensions(cfg).get("activity_pass")
    if not isinstance(raw, dict):
        raw = {}
    merged = deepcopy(DEFAULT_ACTIVITY_PASS)
    merged.update(raw)
    merged["enabled"] = _as_bool(merged.get("enabled"), True)
    merged["name"] = _clean_text(merged.get("name"), "节日战令")
    merged["exp_name"] = _clean_text(merged.get("exp_name"), "活跃值")
    merged["level_exp"] = max(1, _as_int(merged.get("level_exp"), 100))
    merged["max_level"] = max(1, _as_int(merged.get("max_level"), 12))
    merged["catchup_enabled"] = _as_bool(merged.get("catchup_enabled"), True)
    merged["catchup_start_day"] = max(1, _as_int(merged.get("catchup_start_day"), 5))
    merged["catchup_level_gap"] = max(1, _as_int(merged.get("catchup_level_gap"), 3))
    merged["catchup_multiplier"] = max(1.0, _as_float(merged.get("catchup_multiplier"), 1.5))
    merged["event_rules"] = _pass_event_rules(merged)
    merged["level_rewards"] = _pass_level_rewards(merged)
    return merged


def _pass_event_rules(pass_cfg: dict) -> list[dict]:
    rows = pass_cfg.get("event_rules")
    if isinstance(rows, dict):
        source = [
            {"event": event, "exp": exp}
            for event, exp in rows.items()
        ]
    elif isinstance(rows, list):
        source = rows
    else:
        source = DEFAULT_PASS_EVENT_RULES

    rules: list[dict] = []
    seen_events: set[str] = set()
    for row in source:
        if isinstance(row, dict):
            event = _clean_text(row.get("event") or row.get("event_key") or row.get("value"))
            exp = _as_int(row.get("exp", row.get("points")), 0)
            daily_limit = _as_int(row.get("daily_limit"), 0)
        else:
            event = _clean_text(row)
            exp = 0
            daily_limit = 0
        if not event or event in seen_events or event not in ACTIVITY_EVENT_LABELS:
            continue
        exp = max(0, exp)
        if exp <= 0:
            continue
        seen_events.add(event)
        rules.append({
            "event": event,
            "exp": exp,
            "daily_limit": max(0, daily_limit),
        })
    return rules


def _pass_level_rewards(pass_cfg: dict) -> list[dict]:
    rows = pass_cfg.get("level_rewards")
    if not isinstance(rows, list):
        rows = DEFAULT_ACTIVITY_PASS["level_rewards"]
    rewards: list[dict] = []
    seen_levels: set[int] = set()
    max_level = max(1, _as_int(pass_cfg.get("max_level"), 12))
    for row in rows:
        if not isinstance(row, dict):
            continue
        level = _as_int(row.get("level"), 0)
        if level <= 0 or level > max_level or level in seen_levels:
            continue
        seen_levels.add(level)
        rewards.append({
            "level": level,
            "name": _clean_text(row.get("name"), f"{level}级奖励"),
            "reward": _clean_text(row.get("reward")),
        })
    rewards.sort(key=lambda item: item["level"])
    return rewards


def _point_shop_items(activity: dict) -> list[dict]:
    rows = activity.get("shop")
    if not isinstance(rows, list):
        return []

    items: list[dict] = []
    seen_keys: set[str] = set()
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            continue
        name = _clean_text(row.get("name") or row.get("title"))
        reward = _clean_text(row.get("reward"))
        cost = _as_int(row.get("cost"), 0)
        if not any((name, reward, cost)):
            continue
        item_key = _normalize_activity_key(
            row.get("item_key") or row.get("key") or name,
            f"item_{index}",
        )
        if item_key in seen_keys:
            item_key = f"{item_key}_{index}"
        seen_keys.add(item_key)
        items.append({
            "item_key": item_key,
            "name": name or f"商品{index}",
            "cost": max(0, cost),
            "reward": reward,
            "limit": max(0, _as_int(row.get("limit", row.get("limit_per_user", 1)), 1)),
            "stock_limit": max(0, _as_int(row.get("stock_limit", row.get("total_limit", 0)), 0)),
        })
    return items


def get_gameplay_activities(config: dict | None = None) -> list[dict]:
    cfg = config or load_config()
    raw_activities = cfg.get("gameplay_activities")
    if not isinstance(raw_activities, list):
        return []

    activities: list[dict] = []
    seen_keys: set[str] = set()
    for index, raw_activity in enumerate(raw_activities, 1):
        if not isinstance(raw_activity, dict):
            continue
        activity = deepcopy(raw_activity)
        activity_type = _clean_text(activity.get("type"), "collect_words")
        if activity_type not in {"collect_words", "event_points", "activity_boss"}:
            continue
        key = _normalize_activity_key(activity.get("key") or activity.get("template_key"), f"{activity_type}_{index}")
        if key in seen_keys:
            key = f"{key}_{index}"
        seen_keys.add(key)
        if activity_type == "activity_boss":
            from .activity_boss import normalize_activity_boss

            activities.append(normalize_activity_boss(activity, index, key))
            continue
        if activity_type == "event_points":
            activity.update({
                "key": key,
                "type": "event_points",
                "template_key": _clean_text(activity.get("template_key"), key),
                "enabled": _as_bool(activity.get("enabled")),
                "name": _clean_text(activity.get("name"), f"积分活动{index}"),
                "description": _clean_text(activity.get("description"), "完成活动事件获得积分，可在活动商店兑换奖励。"),
                "start_time": _clean_text(activity.get("start_time"), "0"),
                "end_time": _clean_text(activity.get("end_time"), "无限"),
                "point_name": _clean_text(activity.get("point_name"), "活动积分"),
                "event_rules": _point_event_rules(activity),
                "shop": _point_shop_items(activity),
            })
            activities.append(activity)
            continue
        phrases = _collect_phrases(activity)
        activity.update({
            "key": key,
            "type": "collect_words",
            "template_key": _clean_text(activity.get("template_key"), key),
            "enabled": _as_bool(activity.get("enabled")),
            "name": _clean_text(activity.get("name"), f"集字活动{index}"),
            "description": _clean_text(activity.get("description"), "完成活动事件有机会获得字牌，集齐词组兑换奖励。"),
            "start_time": _clean_text(activity.get("start_time"), "0"),
            "end_time": _clean_text(activity.get("end_time"), "无限"),
            "drop_events": _normalize_event_list(activity.get("drop_events"), DEFAULT_COLLECT_DROP_EVENTS),
            "drop_rate": _drop_rate(activity.get("drop_rate"), 0.35),
            "daily_drop_limit": max(0, _as_int(activity.get("daily_drop_limit"), 8)),
            "rolls_per_record": max(1, _as_int(activity.get("rolls_per_record"), 1)),
            "pity_threshold": max(0, _as_int(activity.get("pity_threshold"), 0)),
            "letters": _collect_letters(activity, phrases),
            "phrases": phrases,
        })
        activities.append(activity)
    return activities


def _activity_matches_target(activity: dict, target: str) -> bool:
    text = _clean_text(target)
    if not text:
        return False
    return text in {
        _clean_text(activity.get("key")),
        _clean_text(activity.get("name")),
        _clean_text(activity.get("template_key")),
        _clean_text(activity.get("type")),
    }


__all__ = [
    name for name in globals()
    if name in {"get_activity_tasks", "get_gameplay_activities"}
    or (name.startswith("_") and not name.startswith("__"))
]
