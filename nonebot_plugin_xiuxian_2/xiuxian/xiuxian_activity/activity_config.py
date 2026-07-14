import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from ...paths import get_paths
from ..xiuxian_utils.json_store import load_json_file, save_json_file
from .config_event_service import (
    ActivityConfigEventService,
    ActivityConfigMutationResult,
    ActivityConfigState,
)

from ..xiuxian_utils.activity_helpers import default_stage_features as _default_stage_features
from .activity_utils import _as_float, _as_int, _clean_text, _normalize_activity_key
from .activity_views import STAGE_FEATURES

BASE_DIR = get_paths().data / "activity"
CONFIG_PATH = BASE_DIR / "activity_config.json"
DEFAULT_CONFIG_PATH = Path(__file__).parent / "activity_config.json"
CONFIG_DB_PATH = BASE_DIR / "activity.db"
activity_config_event_service = ActivityConfigEventService(CONFIG_DB_PATH)

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%Y-%m-%d %H:%M:%S"

DEFAULT_PASS_EVENT_RULES = [
    {"event": "sign_in", "exp": 30, "daily_limit": 30},
    {"event": "work", "exp": 12, "daily_limit": 72},
    {"event": "boss", "exp": 6, "daily_limit": 120},
    {"event": "sect_task_complete", "exp": 14, "daily_limit": 70},
    {"event": "pet_travel_claim", "exp": 12, "daily_limit": 36},
    {"event": "dongfu_harvest", "exp": 12, "daily_limit": 36},
    {"event": "map_mission_complete", "exp": 16, "daily_limit": 80},
    {"event": "mix_elixir_complete", "exp": 10, "daily_limit": 60},
    {"event": "dungeon_clear", "exp": 24, "daily_limit": 96},
]
DEFAULT_ACTIVITY_PASS = {
    "enabled": True,
    "name": "节日战令",
    "exp_name": "活跃值",
    "level_exp": 100,
    "max_level": 12,
    "catchup_enabled": True,
    "catchup_start_day": 5,
    "catchup_level_gap": 3,
    "catchup_multiplier": 1.5,
    "event_rules": DEFAULT_PASS_EVENT_RULES,
    "level_rewards": [
        {"level": 1, "name": "初入庆典", "reward": "灵石x80000"},
        {"level": 2, "name": "勤修补给", "reward": "灵石x120000"},
        {"level": 3, "name": "历练补给", "reward": "灵石x160000"},
        {"level": 4, "name": "伏魔补给", "reward": "灵石x220000"},
        {"level": 5, "name": "小成礼盒", "reward": "灵石x300000,渡厄丹x1"},
        {"level": 6, "name": "进阶补给", "reward": "灵石x360000"},
        {"level": 7, "name": "宗门馈赠", "reward": "灵石x420000"},
        {"level": 8, "name": "秘境馈赠", "reward": "灵石x500000,渡厄丹x1"},
        {"level": 9, "name": "庆典宝匣", "reward": "灵石x650000"},
        {"level": 10, "name": "十阶大礼", "reward": "灵石x800000,渡厄丹x2"},
        {"level": 11, "name": "巅峰补给", "reward": "灵石x1000000"},
        {"level": 12, "name": "圆满庆典", "reward": "灵石x1200000,渡厄丹x3"},
    ],
}
STAGE_TYPE_LABELS = {
    "warmup": "预热期",
    "open": "正式期",
    "boss": "攻坚期",
    "settlement": "结算期",
    "closed": "关闭期",
}
DEFAULT_ACTIVITY_STAGES = [
    {
        "key": "open",
        "name": "正式期",
        "stage_type": "open",
        "start_time": "0",
        "end_time": "无限",
        "features": ["sign", "task", "pass", "points", "collect", "boss", "shop", "claim", "exchange"],
        "multiplier": 1.0,
    },
]


def _ensure_activity_files():
    from .service import ensure_activity_files

    ensure_activity_files()


def _now_dt() -> datetime:
    return datetime.now()


def _load_default_config() -> dict:
    with open(DEFAULT_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _migrate_config(config: dict) -> tuple[dict, bool]:
    if config.get("template_type") == "festival_sign":
        default_config = _load_default_config()
        changed = False
        for key, default_value in (
            ("template_key", default_config.get("template_key", "festival_sign")),
            ("daily_tasks", default_config.get("daily_tasks", [])),
            ("weekly_tasks", default_config.get("weekly_tasks", [])),
            ("gameplay_activities", default_config.get("gameplay_activities", [])),
        ):
            if key not in config:
                config[key] = deepcopy(default_value)
                changed = True
        if not isinstance(config.get("gameplay_activities"), list):
            config["gameplay_activities"] = []
            changed = True
        extensions = config.get("extensions")
        if not isinstance(extensions, dict):
            extensions = {}
            config["extensions"] = extensions
            changed = True
        for key, default in (
            ("repeat_last_daily_reward", True),
            ("activity_info_mode", "brief"),
            ("sign_reply_mode", "minimal"),
            ("activity_pass", deepcopy(DEFAULT_ACTIVITY_PASS)),
            ("stages", deepcopy(DEFAULT_ACTIVITY_STAGES)),
        ):
            if key not in extensions:
                extensions[key] = default
                changed = True
        return config, changed

    default_config = _load_default_config()
    migrated = deepcopy(default_config)
    for key in ("enabled", "start_time", "end_time"):
        if key in config:
            migrated[key] = config[key]

    legacy_keys = {"point_name", "daily_sign", "tasks", "shop"}
    if not legacy_keys.intersection(config.keys()):
        for key in (
            "festival_name",
            "name",
            "description",
            "sign_command",
            "daily_rewards",
            "milestone_rewards",
            "daily_tasks",
            "weekly_tasks",
            "extra_rules",
            "extensions",
            "gameplay_activities",
        ):
            if key in config:
                migrated[key] = config[key]
    if not isinstance(migrated.get("gameplay_activities"), list):
        migrated["gameplay_activities"] = []
    return migrated, True


def _save_config_projection(config: dict):
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    save_json_file(CONFIG_PATH, config, indent=2)


def _load_legacy_config() -> dict:
    _ensure_activity_files()
    config = load_json_file(CONFIG_PATH, _load_default_config(), dict)
    config, changed = _migrate_config(config)
    if changed:
        _save_config_projection(config)
    return config


def load_config_state() -> ActivityConfigState:
    return activity_config_event_service.load_or_import(_load_legacy_config())


def load_config() -> dict:
    return load_config_state().config


def replay_config_event(
    operation_id, request_identity
) -> ActivityConfigMutationResult | None:
    result = activity_config_event_service.replay(operation_id, request_identity)
    if result is not None and result.succeeded and result.config is not None:
        _save_config_projection(result.config)
    return result


def save_config(
    config: dict,
    *,
    operation_id,
    request_identity,
    expected_revision,
    result_text: str = "",
) -> ActivityConfigMutationResult:
    result = activity_config_event_service.replace(
        operation_id,
        request_identity,
        expected_revision,
        config,
        result_text=result_text,
    )
    if result.succeeded and result.config is not None:
        _save_config_projection(result.config)
    return result


def parse_time(value, *, is_start: bool):
    if value in (None, "", "0"):
        return None
    if isinstance(value, str) and value.strip() == "无限":
        return None
    text = str(value).strip()
    for fmt in (TIME_FMT, DATE_FMT):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == DATE_FMT and not is_start:
                return parsed.replace(hour=23, minute=59, second=59)
            return parsed
        except ValueError:
            pass
    return None


def activity_state(config: dict | None = None) -> tuple[bool, str]:
    cfg = config or load_config()
    if not cfg.get("enabled", False):
        return False, "活动未开启"

    now = _now_dt()
    start_time = parse_time(cfg.get("start_time"), is_start=True)
    end_time = parse_time(cfg.get("end_time"), is_start=False)
    if start_time and now < start_time:
        return False, f"活动尚未开始，开始时间：{start_time.strftime(TIME_FMT)}"
    if end_time and now > end_time:
        return False, f"活动已结束，结束时间：{end_time.strftime(TIME_FMT)}"
    return True, ""


def _activity_elapsed_days(config: dict, at_time: datetime | None = None) -> int:
    start_time = parse_time(config.get("start_time"), is_start=True)
    if not start_time:
        return 1
    now = at_time or _now_dt()
    if now < start_time:
        return 0
    return max(1, (now.date() - start_time.date()).days + 1)


def _normalize_stage_features(value, default: list[str] | None = None) -> list[str]:
    if isinstance(value, str):
        source = value.replace("\n", ",").split(",")
    elif isinstance(value, list):
        source = value
    else:
        source = default or []

    features: list[str] = []
    seen: set[str] = set()
    for item in source:
        feature = _clean_text(item.get("value") if isinstance(item, dict) else item)
        if not feature:
            continue
        alias_map = {
            "activity": "task",
            "tasks": "task",
            "activity_pass": "pass",
            "event_points": "points",
            "collect_words": "collect",
            "activity_boss": "boss",
            "reward": "claim",
            "rewards": "claim",
            "buy": "shop",
        }
        feature = alias_map.get(feature, feature)
        if feature not in STAGE_FEATURES or feature in seen:
            continue
        seen.add(feature)
        features.append(feature)
    return features


def _activity_stages_config(config: dict | None = None) -> list[dict]:
    cfg = config if config is not None else load_config()
    raw = _get_extensions(cfg).get("stages")
    if not isinstance(raw, list):
        raw = DEFAULT_ACTIVITY_STAGES

    stages: list[dict] = []
    seen_keys: set[str] = set()
    for index, row in enumerate(raw, 1):
        if not isinstance(row, dict):
            continue
        stage_type = _clean_text(row.get("stage_type") or row.get("type"), "open")
        if stage_type not in STAGE_TYPE_LABELS:
            stage_type = "open"
        key = _normalize_activity_key(row.get("key") or stage_type or f"stage_{index}", f"stage_{index}")
        if key in seen_keys:
            key = f"{key}_{index}"
        seen_keys.add(key)
        features = _normalize_stage_features(row.get("features"), _default_stage_features(stage_type))
        start_text = _clean_text(row.get("start_time"), "0")
        end_text = _clean_text(row.get("end_time"), "无限")
        stages.append({
            "key": key,
            "name": _clean_text(row.get("name"), STAGE_TYPE_LABELS.get(stage_type, "活动阶段")),
            "stage_type": stage_type,
            "start_time": start_text,
            "end_time": end_text,
            "features": features,
            "multiplier": max(0.0, _as_float(row.get("multiplier"), 1.0)),
            "description": _clean_text(row.get("description")),
        })
    return stages


def activity_runtime_state(config: dict | None = None, at_time: datetime | None = None) -> dict:
    cfg = config or load_config()
    ok, reason = activity_state(cfg)
    if not ok:
        return {
            "ok": False,
            "reason": reason,
            "stage": None,
            "stage_key": "",
            "stage_name": reason,
            "stage_type": "closed",
            "features": [],
            "multiplier": 0.0,
            "next_stage": None,
            "can_produce": False,
        }

    now = at_time or _now_dt()
    stages = _activity_stages_config(cfg)
    active_stage = None
    future_stages = []
    for stage in stages:
        start_time = parse_time(stage.get("start_time"), is_start=True)
        end_time = parse_time(stage.get("end_time"), is_start=False)
        if start_time and now < start_time:
            future_stages.append((start_time, stage))
            continue
        if end_time and now > end_time:
            continue
        active_stage = stage
        break

    if not active_stage:
        if stages:
            stage = {
                "key": "closed",
                "name": "关闭期",
                "stage_type": "closed",
                "features": [],
                "multiplier": 0.0,
            }
        else:
            stage = {
                "key": "open",
                "name": "进行中",
                "stage_type": "open",
                "features": _default_stage_features("open"),
                "multiplier": 1.0,
            }
        next_stage = min(future_stages, key=lambda item: item[0])[1] if future_stages else None
        return {
            "ok": bool(stage["features"]),
            "reason": "" if stage["features"] else "当前不在活动开放阶段",
            "stage": stage,
            "stage_key": stage["key"],
            "stage_name": stage["name"],
            "stage_type": stage["stage_type"],
            "features": list(stage["features"]),
            "multiplier": stage["multiplier"],
            "next_stage": next_stage,
            "can_produce": bool({"task", "pass", "points", "collect", "boss"} & set(stage["features"])),
        }

    next_candidates = []
    for stage in stages:
        start_time = parse_time(stage.get("start_time"), is_start=True)
        if start_time and start_time > now:
            next_candidates.append((start_time, stage))
    next_stage = min(next_candidates, key=lambda item: item[0])[1] if next_candidates else None
    features = list(active_stage.get("features") or [])
    return {
        "ok": True,
        "reason": "",
        "stage": active_stage,
        "stage_key": active_stage.get("key", ""),
        "stage_name": active_stage.get("name", "进行中"),
        "stage_type": active_stage.get("stage_type", "open"),
        "features": features,
        "multiplier": max(0.0, _as_float(active_stage.get("multiplier"), 1.0)),
        "next_stage": next_stage,
        "can_produce": bool({"task", "pass", "points", "collect", "boss"} & set(features)),
    }


def _runtime_allows(config: dict, feature: str) -> bool:
    runtime = activity_runtime_state(config)
    return runtime.get("ok", False) and feature in set(runtime.get("features") or [])


def _get_extensions(config: dict) -> dict:
    extensions = config.get("extensions")
    return extensions if isinstance(extensions, dict) else {}


def _activity_config_key(config: dict | None = None) -> str:
    cfg = config if config is not None else load_config()
    return _normalize_activity_key(
        cfg.get("template_key") or cfg.get("name") or "festival_sign",
        "festival_sign",
    )


def _sign_reply_mode(config: dict | None = None) -> str:
    cfg = config if config is not None else load_config()
    mode = _clean_text(_get_extensions(cfg).get("sign_reply_mode"), "minimal")
    return mode if mode in ("minimal", "normal", "verbose") else "minimal"


def _activity_info_mode(config: dict | None = None) -> str:
    cfg = config if config is not None else load_config()
    mode = _clean_text(_get_extensions(cfg).get("activity_info_mode"), "brief")
    return mode if mode in ("brief", "full") else "brief"


def _reward_by_day(config: dict, day_index: int) -> dict:
    rewards = config.get("daily_rewards") or []
    if not rewards:
        return {}

    for reward in rewards:
        if _as_int(reward.get("day")) == day_index:
            return reward

    repeat_last = bool(_get_extensions(config).get("repeat_last_daily_reward", True))
    if repeat_last:
        ordered = sorted(rewards, key=lambda item: _as_int(item.get("day")))
        return ordered[-1] if ordered else {}
    return {}


def _milestone_by_days(config: dict, sign_days: int) -> dict:
    for reward in config.get("milestone_rewards") or []:
        if _as_int(reward.get("days")) == sign_days:
            return reward
    return {}
