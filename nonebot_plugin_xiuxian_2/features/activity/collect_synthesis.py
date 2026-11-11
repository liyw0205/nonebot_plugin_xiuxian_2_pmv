"""Collect-token conversion rules shared by the player view and action."""

from decimal import Decimal

from ...core.numeric import as_int_like
from ...core.player_reasons import reason_text
from ...xiuxian.xiuxian_utils.activity_helpers import as_bool


def synthesis_cost(activity: dict) -> int:
    rule = activity.get("synthesis")
    if not isinstance(rule, dict) or not as_bool(rule.get("enabled"), False):
        return 0
    cost = as_int_like(rule.get("cost"), 0)
    if not 2 <= cost <= 100:
        return 0
    return cost if Decimal(str(rule["cost"])) == cost else 0


def reject(reason: str, message: str = "") -> dict:
    return {"status": "rejected", "reason": reason, "subject": "actor",
            "message": message or reason_text(reason)}


def synthesis_hints(activity: dict, inventory: dict, claims: dict) -> list[str]:
    from collections import Counter
    from ...xiuxian.xiuxian_utils.message_markdown import build_md_command_link

    cost = synthesis_cost(activity)
    if not cost:
        return []
    lines = [f"换字规则：同一种源字 {cost} 张换任一不同目标字 1 张（不增加活动进度）"]
    needed = {}
    missing = []
    for phrase in activity["phrases"]:
        limit = phrase["limit"]
        if limit > 0 and claims.get((activity["key"], phrase["phrase"]), 0) >= limit:
            continue
        for char, count in Counter(phrase["phrase"]).items():
            needed[char] = max(needed.get(char, 0), count)
            if inventory.get(char, 0) < count and char not in missing:
                missing.append(char)
    # Preserve enough of every source for any one remaining phrase.
    sources = [char for char, count in inventory.items() if count - needed.get(char, 0) >= cost]
    sources.sort(key=lambda char: -inventory[char])
    for target in missing[:3]:
        source = next((char for char in sources if char != target), None)
        if source is not None:
            command = f"活动换字 {source} {target} {activity['key']}"
            lines.append(build_md_command_link(command, command))
    if len(lines) == 1:
        lines.append("换字格式：活动换字 源字 目标字 " + activity["key"])
    return lines


def synthesize(repository, operation_id: str, user_id: str, query: str) -> dict:
    # A committed business receipt is authoritative even after configuration changes.
    previous = repository.lookup_receipt(operation_id, user_id, query)
    if previous is not None:
        return previous

    from ...xiuxian.xiuxian_activity.activity_config import (
        _now_dt, activity_runtime_state, activity_state, load_config, parse_time,
    )
    from ...xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities

    parts = query.split(maxsplit=2)
    if len(parts) < 2 or len(parts[0]) != 1 or len(parts[1]) != 1:
        return reject("collect_synthesis_arguments")
    source, target = parts[:2]
    if source == target:
        return reject("collect_synthesis_same_token")
    config = load_config()
    now = _now_dt()

    def window_rejection(activity, message):
        start = parse_time(activity.get("start_time"), is_start=True)
        reason = "activity_disabled" if not activity.get("enabled") else (
            "activity_not_started" if start and now < start else "activity_ended")
        return reject(reason, message)

    runtime = activity_runtime_state(config, at_time=now)
    if not runtime.get("ok"):
        return window_rejection(config, str(runtime["reason"]))
    if "exchange" not in set(runtime.get("features") or ()):
        return reject("activity_feature_closed", f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放集字换字")
    activities = [a for a in get_gameplay_activities(config) if a["type"] == "collect_words"]
    if len(parts) == 3:
        activities = [a for a in activities if parts[2] in {a["key"], a["name"]}]
    elif len(activities) > 1:
        active = [a for a in activities if activity_state(a, at_time=now)[0] and synthesis_cost(a)]
        if len(active) == 1:
            activities = active
    if not activities:
        return reject("collect_synthesis_activity_missing")
    if len(activities) > 1:
        return reject("collect_synthesis_activity_ambiguous", "请在源字和目标字后指定活动名：" + "、".join(a["name"] for a in activities))
    activity = activities[0]
    available, reason = activity_state(activity, at_time=now)
    if not available:
        return window_rejection(activity, reason)
    cost = synthesis_cost(activity)
    if not cost:
        return reject("collect_synthesis_disabled")
    letters = {item["char"] for item in activity["letters"]}
    if source not in letters or target not in letters:
        return reject("collect_synthesis_token_unknown")
    return repository.convert(operation_id, user_id, query, activity["key"], activity["name"], source, target, cost)
