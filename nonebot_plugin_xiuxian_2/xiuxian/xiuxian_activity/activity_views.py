from .activity_utils import _as_int

ACTIVITY_EVENT_CHOICES = (
    {"value": "sign_in", "label": "修仙签到"},
    {"value": "out_closing", "label": "出关"},
    {"value": "xu_out_closing", "label": "虚神界出关"},
    {"value": "cultivation_time", "label": "修炼时长"},
    {"value": "work", "label": "悬赏令"},
    {"value": "boss", "label": "世界BOSS"},
    {"value": "sect_task_complete", "label": "宗门任务"},
    {"value": "pet_travel_claim", "label": "宠物游历"},
    {"value": "dongfu_harvest", "label": "洞府收获"},
    {"value": "map_mission_complete", "label": "地图委托"},
    {"value": "mix_elixir_complete", "label": "炼丹"},
    {"value": "dungeon_clear", "label": "副本通关"},
)
ACTIVITY_EVENT_LABELS = {
    item["value"]: item["label"]
    for item in ACTIVITY_EVENT_CHOICES
}
STAGE_FEATURES = {
    "sign": "签到",
    "task": "任务",
    "pass": "战令",
    "points": "积分",
    "collect": "集字",
    "boss": "首领",
    "shop": "商店",
    "claim": "领奖",
    "exchange": "兑换",
}


def _format_reward_result(title: str, reward: str, reward_msg: list[str]) -> str:
    if not reward:
        return f"{title}：暂无奖励"
    if reward_msg:
        return f"{title}：" + "，".join(reward_msg)
    return f"{title}：{reward}"


def _activity_event_text(events) -> str:
    if isinstance(events, str):
        source = events.replace("\n", ",").split(",")
    elif isinstance(events, list):
        source = events
    else:
        source = []

    labels = []
    seen_events = set()
    for item in source:
        if isinstance(item, dict):
            event = str(item.get("value") or "").strip()
        else:
            event = str(item or "").strip()
        if not event or event in seen_events:
            continue
        seen_events.add(event)
        labels.append(ACTIVITY_EVENT_LABELS.get(event, event))
    return "、".join(labels)


def _format_activity_task(task: dict) -> str:
    name = str(task.get("name") or task.get("key") or "活动任务")
    desc = str(task.get("description") or task.get("desc") or "").strip()
    target = _as_int(task.get("target"), 1)
    reward = str(task.get("reward") or "奖励待补")
    event_text = _activity_event_text(task.get("events"))
    suffix = f"，事件：{event_text}" if event_text else ""
    if desc:
        return f"- {name}：{desc}，目标 {target}{suffix}，奖励：{reward}"
    return f"- {name}：目标 {target}{suffix}，奖励：{reward}"


def _scope_label(scope_type: str) -> str:
    return "每日" if scope_type == "daily" else "周常"


def _task_status_text(progress: int, target: int, claimed: bool) -> str:
    if claimed:
        return "已领取"
    if progress >= target:
        return "可领取"
    return f"{progress}/{target}"


def _stage_feature_text(features: list[str]) -> str:
    return "、".join(STAGE_FEATURES.get(feature, feature) for feature in features) or "暂无开放玩法"


def _stage_time_text(stage: dict | None) -> str:
    if not stage:
        return ""
    return f"{stage.get('start_time', '0')} 至 {stage.get('end_time', '无限')}"
