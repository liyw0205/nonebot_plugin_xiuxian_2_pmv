import json
import random
import shutil
from collections import Counter
from copy import deepcopy
from datetime import datetime
from pathlib import Path

from nonebot.log import logger

from ..xiuxian_compensation.common import get_item_list, send_reward_to_user
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.activity_helpers import as_bool as _as_bool
from ..xiuxian_utils.activity_helpers import default_stage_features as _default_stage_features
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from .activity_utils import (
    _as_float,
    _as_int,
    _clean_text,
    _drop_rate,
    _normalize_activity_key,
)

_sql_message = XiuxianDateManage()


BASE_DIR = Path() / "data" / "xiuxian" / "activity"
CONFIG_PATH = BASE_DIR / "activity_config.json"
DB_PATH = BASE_DIR / "activity.db"
DEFAULT_CONFIG_PATH = Path(__file__).parent / "activity_config.json"

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%Y-%m-%d %H:%M:%S"

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
DEFAULT_COLLECT_DROP_EVENTS = [
    "sign_in",
    "work",
    "boss",
    "sect_task_complete",
    "pet_travel_claim",
    "dongfu_harvest",
    "map_mission_complete",
    "mix_elixir_complete",
    "dungeon_clear",
]
DEFAULT_POINT_EVENT_RULES = [
    {"event": "sign_in", "points": 20, "daily_limit": 20},
    {"event": "work", "points": 10, "daily_limit": 60},
    {"event": "boss", "points": 5, "daily_limit": 80},
    {"event": "sect_task_complete", "points": 12, "daily_limit": 60},
    {"event": "pet_travel_claim", "points": 10, "daily_limit": 30},
    {"event": "dongfu_harvest", "points": 10, "daily_limit": 30},
    {"event": "map_mission_complete", "points": 12, "daily_limit": 60},
    {"event": "mix_elixir_complete", "points": 8, "daily_limit": 40},
    {"event": "dungeon_clear", "points": 15, "daily_limit": 60},
]
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


def now_dt() -> datetime:
    return datetime.now()


def today_str() -> str:
    return now_dt().strftime(DATE_FMT)


def now_str() -> str:
    return now_dt().strftime(TIME_FMT)


def ensure_activity_files():
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG_PATH.exists():
        shutil.copyfile(DEFAULT_CONFIG_PATH, CONFIG_PATH)
    init_db()


def init_db():
    conn = db_backend.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_user (
                user_id TEXT PRIMARY KEY,
                sign_days INTEGER NOT NULL DEFAULT 0,
                last_sign_date TEXT DEFAULT '',
                total_sign_days INTEGER NOT NULL DEFAULT 0,
                create_time TEXT DEFAULT '',
                update_time TEXT DEFAULT ''
            )
        """)
        columns = set(conn.column_names("activity_user"))
        if "sign_days" not in columns:
            cur.execute("ALTER TABLE activity_user ADD COLUMN sign_days INTEGER NOT NULL DEFAULT 0")
        if "last_sign_date" not in columns:
            cur.execute("ALTER TABLE activity_user ADD COLUMN last_sign_date TEXT DEFAULT ''")
        if "total_sign_days" not in columns:
            cur.execute("ALTER TABLE activity_user ADD COLUMN total_sign_days INTEGER NOT NULL DEFAULT 0")
            cur.execute("UPDATE activity_user SET total_sign_days = sign_days")
        if "create_time" not in columns:
            cur.execute("ALTER TABLE activity_user ADD COLUMN create_time TEXT DEFAULT ''")
        if "update_time" not in columns:
            cur.execute("ALTER TABLE activity_user ADD COLUMN update_time TEXT DEFAULT ''")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_sign_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                sign_date TEXT NOT NULL,
                day_index INTEGER NOT NULL DEFAULT 0,
                reward TEXT DEFAULT '',
                milestone_reward TEXT DEFAULT '',
                reward_status TEXT DEFAULT '',
                reward_message TEXT DEFAULT '',
                create_time TEXT DEFAULT '',
                finish_time TEXT DEFAULT '',
                UNIQUE(user_id, sign_date)
            )
        """)
        log_columns = set(conn.column_names("activity_sign_log"))
        if "reward_status" not in log_columns:
            cur.execute("ALTER TABLE activity_sign_log ADD COLUMN reward_status TEXT DEFAULT ''")
        if "reward_message" not in log_columns:
            cur.execute("ALTER TABLE activity_sign_log ADD COLUMN reward_message TEXT DEFAULT ''")
        if "finish_time" not in log_columns:
            cur.execute("ALTER TABLE activity_sign_log ADD COLUMN finish_time TEXT DEFAULT ''")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_collect_inventory (
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                word_char TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                update_time TEXT DEFAULT '',
                PRIMARY KEY(activity_key, user_id, word_char)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_collect_claim (
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                phrase TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                update_time TEXT DEFAULT '',
                PRIMARY KEY(activity_key, user_id, phrase)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_collect_drop_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                word_char TEXT NOT NULL,
                drop_date TEXT DEFAULT '',
                create_time TEXT DEFAULT ''
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_collect_pity_state (
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                miss_count INTEGER NOT NULL DEFAULT 0,
                update_time TEXT DEFAULT '',
                PRIMARY KEY(activity_key, user_id, event_key)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_point_balance (
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                points INTEGER NOT NULL DEFAULT 0,
                total_points INTEGER NOT NULL DEFAULT 0,
                update_time TEXT DEFAULT '',
                PRIMARY KEY(activity_key, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_point_event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                points INTEGER NOT NULL DEFAULT 0,
                record_date TEXT DEFAULT '',
                create_time TEXT DEFAULT ''
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_point_purchase (
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                item_key TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                update_time TEXT DEFAULT '',
                PRIMARY KEY(activity_key, user_id, item_key)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_task_progress (
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                task_key TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                target INTEGER NOT NULL DEFAULT 1,
                claimed INTEGER NOT NULL DEFAULT 0,
                claim_time TEXT DEFAULT '',
                update_time TEXT DEFAULT '',
                PRIMARY KEY(activity_key, user_id, scope_type, scope_key, task_key)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_task_claim_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                task_key TEXT NOT NULL,
                reward TEXT DEFAULT '',
                create_time TEXT DEFAULT ''
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_pass_balance (
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                exp INTEGER NOT NULL DEFAULT 0,
                total_exp INTEGER NOT NULL DEFAULT 0,
                level INTEGER NOT NULL DEFAULT 0,
                update_time TEXT DEFAULT '',
                PRIMARY KEY(activity_key, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_pass_event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                exp INTEGER NOT NULL DEFAULT 0,
                record_date TEXT DEFAULT '',
                create_time TEXT DEFAULT ''
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_pass_reward_claim (
                activity_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                level INTEGER NOT NULL,
                create_time TEXT DEFAULT '',
                PRIMARY KEY(activity_key, user_id, level)
            )
        """)
        from .activity_boss import init_boss_tables

        init_boss_tables(conn)
        conn.commit()
    finally:
        conn.close()


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


def load_config() -> dict:
    ensure_activity_files()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
    config, changed = _migrate_config(config)
    if changed:
        save_config(config)
    return config


def save_config(config: dict):
    ensure_activity_files()
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


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

    now = now_dt()
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
    now = at_time or now_dt()
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

    now = at_time or now_dt()
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


def resolve_daohao(user_id: str) -> str:
    uid = str(user_id or "").strip()
    if not uid:
        return "无名修士"
    try:
        row = _sql_message.get_user_info_with_id(uid)
        name = _clean_text(row.get("user_name") if row else "")
        if name:
            return name
    except Exception as e:
        logger.debug(f"resolve_daohao failed user_id={uid}: {e}")
    if len(uid) > 6:
        return f"修士·{uid[-4:]}"
    return uid


def resolve_daohao_batch(user_ids: list[str]) -> dict[str, str]:
    ids = []
    seen = set()
    for raw in user_ids:
        uid = str(raw or "").strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)
        ids.append(uid)
    if not ids:
        return {}
    result = {uid: resolve_daohao(uid) for uid in ids}
    try:
        placeholders = ",".join(["%s"] * len(ids))
        rows = _sql_message._read_query(
            f"SELECT user_id, user_name FROM user_xiuxian WHERE user_id IN ({placeholders})",
            tuple(ids),
            dict_row=True,
        )
        for row in rows or []:
            uid = str(row.get("user_id") or "").strip()
            name = _clean_text(row.get("user_name"))
            if uid and name:
                result[uid] = name
    except Exception as e:
        logger.debug(f"resolve_daohao_batch query failed: {e}")
    return result


def _attach_display_names(rows: list[dict], id_key: str = "user_id") -> list[dict]:
    if not rows:
        return rows
    name_map = resolve_daohao_batch([str(row.get(id_key) or "") for row in rows])
    enriched = []
    for row in rows:
        item = dict(row)
        uid = str(item.get(id_key) or "")
        item["user_name"] = name_map.get(uid) or resolve_daohao(uid)
        item["display_name"] = item["user_name"]
        enriched.append(item)
    return enriched


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


def get_user_sign(user_id: str) -> dict:
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM activity_user WHERE user_id=%s", (str(user_id),))
        row = cur.fetchone()
        if row:
            data = dict(row)
            data["sign_days"] = _as_int(data.get("sign_days"))
            data["total_sign_days"] = _as_int(data.get("total_sign_days"), data["sign_days"])
            return data
        return {
            "user_id": str(user_id),
            "sign_days": 0,
            "last_sign_date": "",
            "total_sign_days": 0,
        }
    finally:
        conn.close()


def parse_reward(reward: str) -> list[dict]:
    reward = str(reward or "").strip()
    if not reward:
        return []
    return get_item_list(reward)


def send_reward_items(user_id: str, reward_items: list[dict]) -> list[str]:
    if not reward_items:
        return []
    return send_reward_to_user(str(user_id), reward_items)


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


def _choose_collect_char(activity: dict) -> str:
    letters = _collect_letters(activity, _collect_phrases(activity))
    if not letters:
        return ""
    total_weight = sum(max(1, _as_int(item.get("weight"), 1)) for item in letters)
    needle = random.uniform(0, total_weight)
    current = 0
    for item in letters:
        current += max(1, _as_int(item.get("weight"), 1))
        if needle <= current:
            return _clean_text(item.get("char"))
    return _clean_text(letters[-1].get("char"))


def _get_collect_pity_count(cur, activity_key: str, user_id: str, event_key: str) -> int:
    cur.execute(
        """
        SELECT miss_count
        FROM activity_collect_pity_state
        WHERE activity_key=%s AND user_id=%s AND event_key=%s
        """,
        (str(activity_key), str(user_id), str(event_key)),
    )
    row = cur.fetchone()
    return max(0, _as_int(row["miss_count"] if row else 0))


def _set_collect_pity_count(cur, activity_key: str, user_id: str, event_key: str, miss_count: int) -> None:
    cur.execute(
        """
        INSERT INTO activity_collect_pity_state (
            activity_key, user_id, event_key, miss_count, update_time
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(activity_key, user_id, event_key) DO UPDATE SET
            miss_count = excluded.miss_count,
            update_time = excluded.update_time
        """,
        (str(activity_key), str(user_id), str(event_key), max(0, _as_int(miss_count)), now_str()),
    )


def _collect_pity_progress_map(cur, activity_key: str, user_id: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT event_key, miss_count
        FROM activity_collect_pity_state
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    return {
        str(row["event_key"]): max(0, _as_int(row["miss_count"]))
        for row in cur.fetchall()
    }


def _get_collect_inventory_map(cur, activity_key: str, user_id: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT word_char, count
        FROM activity_collect_inventory
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    return {
        str(row["word_char"]): max(0, _as_int(row["count"]))
        for row in cur.fetchall()
    }


def _get_collect_claim_map(cur, activity_key: str, user_id: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT phrase, count
        FROM activity_collect_claim
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    return {
        str(row["phrase"]): max(0, _as_int(row["count"]))
        for row in cur.fetchall()
    }


def _phrase_need_counter(phrase: str) -> Counter:
    return Counter(word_char for word_char in str(phrase or "") if word_char.strip())


def _get_task_progress_map(cur, activity_key: str, user_id: str) -> dict[tuple[str, str, str], dict]:
    cur.execute(
        """
        SELECT scope_type, scope_key, task_key, progress, target, claimed, claim_time
        FROM activity_task_progress
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    result: dict[tuple[str, str, str], dict] = {}
    for row in cur.fetchall():
        key = (str(row["scope_type"]), str(row["scope_key"]), str(row["task_key"]))
        result[key] = {
            "progress": max(0, _as_int(row["progress"])),
            "target": max(1, _as_int(row["target"], 1)),
            "claimed": bool(_as_int(row["claimed"], 0)),
            "claim_time": _clean_text(row["claim_time"]),
        }
    return result


def _record_activity_task_progress(
    cur,
    config: dict,
    user_id: str,
    event_key: str,
    amount: int,
    messages: list[str],
) -> None:
    runtime = activity_runtime_state(config)
    if not runtime.get("ok") or "task" not in set(runtime.get("features") or []):
        return
    activity_key = _activity_config_key(config)
    for task in get_activity_tasks(config):
        if event_key not in task.get("events", []):
            continue
        scope_type = task["scope_type"]
        scope_key = _task_scope_key(scope_type)
        task_key = task["key"]
        target = max(1, _as_int(task.get("target"), 1))
        cur.execute(
            """
            SELECT progress, claimed FROM activity_task_progress
            WHERE activity_key=%s AND user_id=%s AND scope_type=%s AND scope_key=%s AND task_key=%s
            """,
            (activity_key, user_id, scope_type, scope_key, task_key),
        )
        row = cur.fetchone()
        old_progress = max(0, _as_int(row["progress"] if row else 0))
        claimed = bool(_as_int(row["claimed"] if row else 0))
        if claimed:
            continue
        new_progress = min(target, old_progress + max(1, amount))
        if new_progress <= old_progress and row:
            continue
        ts = now_str()
        cur.execute(
            """
            INSERT INTO activity_task_progress (
                activity_key, user_id, scope_type, scope_key, task_key,
                progress, target, claimed, update_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s)
            ON CONFLICT(activity_key, user_id, scope_type, scope_key, task_key) DO UPDATE SET
                progress = excluded.progress,
                target = excluded.target,
                update_time = excluded.update_time
            """,
            (activity_key, user_id, scope_type, scope_key, task_key, new_progress, target, ts),
        )
        if old_progress < target <= new_progress:
            messages.append(f"活动任务完成：{task['name']}，发送 活动任务领取")


def _calc_pass_level(total_exp: int, level_exp: int, max_level: int) -> int:
    return min(max(0, _as_int(total_exp, 0)) // max(1, level_exp), max(1, max_level))


def _pass_current_exp(total_exp: int, level: int, level_exp: int, max_level: int) -> int:
    if level >= max_level:
        return level_exp
    return max(0, total_exp - level * level_exp)


def _get_pass_balance(cur, activity_key: str, user_id: str, pass_cfg: dict | None = None) -> dict:
    cfg = pass_cfg or _activity_pass_config()
    level_exp = max(1, _as_int(cfg.get("level_exp"), 100))
    max_level = max(1, _as_int(cfg.get("max_level"), 12))
    cur.execute(
        """
        SELECT exp, total_exp, level
        FROM activity_pass_balance
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    row = cur.fetchone()
    total_exp = max(0, _as_int(row["total_exp"] if row else 0))
    level = _calc_pass_level(total_exp, level_exp, max_level)
    current_exp = _pass_current_exp(total_exp, level, level_exp, max_level)
    return {
        "exp": current_exp,
        "total_exp": total_exp,
        "level": level,
        "level_exp": level_exp,
        "max_level": max_level,
    }


def _grant_pass_exp(cur, activity_key: str, user_id: str, pass_cfg: dict, gained_exp: int) -> tuple[dict, dict]:
    before = _get_pass_balance(cur, activity_key, user_id, pass_cfg)
    total_exp = before["total_exp"] + max(0, _as_int(gained_exp, 0))
    level_exp = before["level_exp"]
    max_level = before["max_level"]
    level = _calc_pass_level(total_exp, level_exp, max_level)
    current_exp = _pass_current_exp(total_exp, level, level_exp, max_level)
    ts = now_str()
    cur.execute(
        """
        INSERT INTO activity_pass_balance (
            activity_key, user_id, exp, total_exp, level, update_time
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT(activity_key, user_id) DO UPDATE SET
            exp = excluded.exp,
            total_exp = excluded.total_exp,
            level = excluded.level,
            update_time = excluded.update_time
        """,
        (activity_key, user_id, current_exp, total_exp, level, ts),
    )
    return before, {
        "exp": current_exp,
        "total_exp": total_exp,
        "level": level,
        "level_exp": level_exp,
        "max_level": max_level,
    }


def _pass_catchup_state(cur, config: dict, activity_key: str, user_id: str, pass_cfg: dict) -> dict:
    enabled = bool(pass_cfg.get("catchup_enabled"))
    start_day = max(1, _as_int(pass_cfg.get("catchup_start_day"), 5))
    level_gap = max(1, _as_int(pass_cfg.get("catchup_level_gap"), 3))
    catchup_multiplier = max(1.0, _as_float(pass_cfg.get("catchup_multiplier"), 1.5))
    elapsed_day = _activity_elapsed_days(config)
    balance = _get_pass_balance(cur, activity_key, user_id, pass_cfg)
    cur.execute(
        """
        SELECT COALESCE(MAX(level), 0) AS level
        FROM activity_pass_balance
        WHERE activity_key=%s
        """,
        (activity_key,),
    )
    row = cur.fetchone()
    highest_level = max(0, _as_int(row["level"] if row else 0))
    gap = max(0, highest_level - balance["level"])
    active = enabled and elapsed_day >= start_day and gap >= level_gap and catchup_multiplier > 1.0
    return {
        "enabled": enabled,
        "active": active,
        "elapsed_day": elapsed_day,
        "start_day": start_day,
        "level_gap": level_gap,
        "catchup_multiplier": catchup_multiplier,
        "highest_level": highest_level,
        "user_level": balance["level"],
        "gap": gap,
        "multiplier": catchup_multiplier if active else 1.0,
    }


def _record_activity_pass_progress(
    cur,
    config: dict,
    user_id: str,
    event_key: str,
    amount: int,
    messages: list[str],
) -> None:
    runtime = activity_runtime_state(config)
    if not runtime.get("ok") or "pass" not in set(runtime.get("features") or []):
        return
    pass_cfg = _activity_pass_config(config)
    if not pass_cfg.get("enabled"):
        return
    activity_key = _activity_config_key(config)
    exp_name = pass_cfg.get("exp_name") or "活跃值"
    multiplier = max(0.0, _as_float(runtime.get("multiplier"), 1.0))
    catchup = _pass_catchup_state(cur, config, activity_key, user_id, pass_cfg)
    multiplier *= max(1.0, _as_float(catchup.get("multiplier"), 1.0))
    for rule in pass_cfg.get("event_rules") or []:
        if rule.get("event") != event_key:
            continue
        exp = int(max(0, _as_int(rule.get("exp"), 0)) * max(1, amount) * multiplier)
        if exp <= 0:
            continue
        daily_limit = max(0, _as_int(rule.get("daily_limit"), 0))
        if daily_limit > 0:
            cur.execute(
                """
                SELECT COALESCE(SUM(exp), 0) AS count
                FROM activity_pass_event_log
                WHERE activity_key=%s AND user_id=%s AND event_key=%s AND record_date=%s
                """,
                (activity_key, user_id, event_key, today_str()),
            )
            row = cur.fetchone()
            current_exp = _as_int(row["count"] if row else 0)
            remaining = daily_limit - current_exp
            if remaining <= 0:
                continue
            exp = min(exp, remaining)
        if exp <= 0:
            continue
        ts = now_str()
        cur.execute(
            """
            INSERT INTO activity_pass_event_log (
                activity_key, user_id, event_key, exp, record_date, create_time
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (activity_key, user_id, event_key, exp, today_str(), ts),
        )
        before, after = _grant_pass_exp(cur, activity_key, user_id, pass_cfg, exp)
        catchup_text = "（追赶加成）" if catchup.get("active") else ""
        if after["level"] > before["level"]:
            messages.append(
                f"活动战令：获得{exp}{exp_name}{catchup_text}，提升至{after['level']}级，发送 活动战令领取 领奖"
            )
        else:
            messages.append(
                f"活动战令：获得{exp}{exp_name}{catchup_text}（{after['exp']}/{after['level_exp']}）"
            )


def record_activity_event(user_id: str, event_key: str, amount: int = 1) -> list[str]:
    uid = str(user_id)
    event = str(event_key)
    times = max(0, _as_int(amount, 1))
    if times <= 0:
        return []

    cfg = load_config()
    activities = get_gameplay_activities(cfg)
    if not activities and not get_activity_tasks(cfg) and not _activity_pass_config(cfg).get("enabled"):
        return []
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok") or not runtime.get("can_produce"):
        return []
    features = set(runtime.get("features") or [])
    multiplier = max(0.0, _as_float(runtime.get("multiplier"), 1.0))

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    messages: list[str] = []
    try:
        cur = conn.cursor()
        _record_activity_task_progress(cur, cfg, uid, event, times, messages)
        _record_activity_pass_progress(cur, cfg, uid, event, times, messages)
        for activity in activities:
            ok, _ = activity_state(activity)
            if not ok:
                continue
            if activity.get("type") == "event_points":
                if "points" not in features:
                    continue
                for rule in activity.get("event_rules") or []:
                    if rule.get("event") != event:
                        continue
                    points = int(max(0, _as_int(rule.get("points"), 0)) * times * multiplier)
                    if points <= 0:
                        continue
                    daily_limit = max(0, _as_int(rule.get("daily_limit"), 0))
                    if daily_limit > 0:
                        cur.execute(
                            """
                            SELECT COALESCE(SUM(points), 0) AS count
                            FROM activity_point_event_log
                            WHERE activity_key=%s AND user_id=%s AND event_key=%s AND record_date=%s
                            """,
                            (activity["key"], uid, event, today_str()),
                        )
                        row = cur.fetchone()
                        current_points = _as_int(row["count"] if row else 0)
                        remaining = daily_limit - current_points
                        if remaining <= 0:
                            continue
                        points = min(points, remaining)
                    if points <= 0:
                        continue
                    ts = now_str()
                    cur.execute(
                        """
                        INSERT INTO activity_point_balance (
                            activity_key, user_id, points, total_points, update_time
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT(activity_key, user_id) DO UPDATE SET
                            points = activity_point_balance.points + excluded.points,
                            total_points = activity_point_balance.total_points + excluded.total_points,
                            update_time = excluded.update_time
                        """,
                        (activity["key"], uid, points, points, ts),
                    )
                    cur.execute(
                        """
                        INSERT INTO activity_point_event_log (
                            activity_key, user_id, event_key, points, record_date, create_time
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (activity["key"], uid, event, points, today_str(), ts),
                    )
                    messages.append(
                        f"活动积分：{activity['name']} 获得{points}{activity.get('point_name', '活动积分')}"
                    )
                continue

            if activity.get("type") == "activity_boss":
                if "boss" not in features:
                    continue
                drop_events = activity.get("drop_events") or []
                if event in drop_events:
                    items = activity.get("items") or []
                    if items:
                        pick = random.choice(items)
                        cur.execute(
                            """
                            INSERT INTO activity_item_inventory (activity_key, user_id, item_id, count, update_time)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT(activity_key, user_id, item_id) DO UPDATE SET
                                count = activity_item_inventory.count + excluded.count,
                                update_time = excluded.update_time
                            """,
                            (activity["key"], uid, pick["id"], 1, now_str()),
                        )
                        messages.append(f"活动掉落：{activity['name']} 获得【{pick['name']}】")
                continue

            if activity.get("type") != "collect_words" or event not in activity.get("drop_events", []):
                continue
            if "collect" not in features:
                continue
            daily_limit = max(0, _as_int(activity.get("daily_drop_limit"), 8))
            if daily_limit <= 0:
                continue
            cur.execute(
                """
                SELECT COUNT(*) AS count
                FROM activity_collect_drop_log
                WHERE activity_key=%s AND user_id=%s AND drop_date=%s
                """,
                (activity["key"], uid, today_str()),
            )
            row = cur.fetchone()
            current_count = _as_int(row["count"] if row else 0)
            remaining = daily_limit - current_count
            if remaining <= 0:
                continue

            rolls = min(remaining, max(1, min(times, _as_int(activity.get("rolls_per_record"), 1))))
            drop_rate = min(_drop_rate(activity.get("drop_rate"), 0.35) * multiplier, 1.0)
            pity_threshold = max(0, _as_int(activity.get("pity_threshold"), 0))
            pity_count = (
                _get_collect_pity_count(cur, activity["key"], uid, event)
                if pity_threshold > 0 else 0
            )
            pity_changed = False
            for _ in range(rolls):
                guaranteed = pity_threshold > 0 and pity_count + 1 >= pity_threshold
                if not guaranteed and random.random() > drop_rate:
                    if pity_threshold > 0:
                        pity_count += 1
                        pity_changed = True
                    continue
                word_char = _choose_collect_char(activity)
                if not word_char:
                    continue
                if pity_threshold > 0:
                    pity_count = 0
                    pity_changed = True
                cur.execute(
                    """
                    INSERT INTO activity_collect_inventory (
                        activity_key, user_id, word_char, count, update_time
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(activity_key, user_id, word_char) DO UPDATE SET
                        count = activity_collect_inventory.count + excluded.count,
                        update_time = excluded.update_time
                    """,
                    (activity["key"], uid, word_char, 1, now_str()),
                )
                cur.execute(
                    """
                    INSERT INTO activity_collect_drop_log (
                        activity_key, user_id, event_key, word_char, drop_date, create_time
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (activity["key"], uid, event, word_char, today_str(), now_str()),
                )
                drop_label = "活动保底" if guaranteed else "活动掉落"
                messages.append(f"{drop_label}：{activity['name']} 获得字牌「{word_char}」")
            if pity_changed:
                _set_collect_pity_count(cur, activity["key"], uid, event, pity_count)
        conn.commit()
        return messages
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_collect_inventory(user_id: str, activity_key: str) -> dict[str, int]:
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        return _get_collect_inventory_map(conn.cursor(), activity_key, str(user_id))
    finally:
        conn.close()


def build_collect_bag_text(user_id: str) -> str:
    uid = str(user_id)
    activities = get_gameplay_activities(load_config())
    collect_activities = [activity for activity in activities if activity.get("type") == "collect_words"]
    lines = ["【活动背包】"]
    if not collect_activities:
        lines.append("暂无集字活动")
        return "\n".join(lines)

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        for activity in collect_activities:
            ok, reason = activity_state(activity)
            inventory = _get_collect_inventory_map(cur, activity["key"], uid)
            claims = _get_collect_claim_map(cur, activity["key"], uid)
            letters = _collect_letters(activity, _collect_phrases(activity))
            letter_text = "、".join(
                f"{item['char']}x{inventory.get(item['char'], 0)}"
                for item in letters
            ) or "暂无"
            lines.extend([
                "",
                f"【{activity['name']}】{'进行中' if ok else reason}",
                f"字牌：{letter_text}",
            ])
            pity_threshold = max(0, _as_int(activity.get("pity_threshold"), 0))
            if pity_threshold > 0:
                pity_map = _collect_pity_progress_map(cur, activity["key"], uid)
                pity_parts = []
                for event_key in activity.get("drop_events") or []:
                    label = ACTIVITY_EVENT_LABELS.get(event_key, event_key)
                    pity_parts.append(
                        f"{label} {min(pity_threshold, pity_map.get(event_key, 0))}/{pity_threshold}"
                    )
                if pity_parts:
                    lines.append("保底进度：" + "、".join(pity_parts))
            phrases = _collect_phrases(activity)
            if not phrases:
                lines.append("暂无兑换词组")
                continue
            lines.append("可兑换词组：")
            for phrase in phrases:
                need = _phrase_need_counter(phrase["phrase"])
                owned = sum(min(inventory.get(word_char, 0), count) for word_char, count in need.items())
                total_need = sum(need.values())
                claimed = claims.get(phrase["phrase"], 0)
                limit = _as_int(phrase.get("limit"), 1)
                limit_text = "不限" if limit <= 0 else f"{claimed}/{limit}"
                lines.append(
                    f"- {phrase['name']}：{owned}/{total_need}，已兑换 {limit_text}，"
                    f"兑换：活动兑换 {phrase['name']}"
                )
        return "\n".join(lines).strip()
    finally:
        conn.close()


def _find_collect_phrase(config: dict, query: str) -> tuple[dict, dict] | None:
    text = _clean_text(query)
    if not text:
        return None
    for activity in get_gameplay_activities(config):
        if activity.get("type") != "collect_words":
            continue
        ok, _ = activity_state(activity)
        if not ok:
            continue
        for phrase in _collect_phrases(activity):
            phrase_text = _clean_text(phrase.get("phrase"))
            phrase_name = _clean_text(phrase.get("name"))
            if text in (phrase_text, phrase_name):
                return activity, phrase
            if phrase_text and phrase_text in text:
                return activity, phrase
            if phrase_name and phrase_name in text:
                return activity, phrase
    return None


def claim_collect_phrase(user_id: str, query: str) -> tuple[bool, str]:
    uid = str(user_id)
    target = _clean_text(query)
    if not target:
        return False, "请发送：活动兑换 端午安康"

    cfg = load_config()
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放"
    if "exchange" not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放集字兑换"
    found = _find_collect_phrase(cfg, target)
    if not found:
        return False, "未找到可兑换的活动词组，或活动当前不可兑换"

    activity, phrase = found
    need = _phrase_need_counter(phrase["phrase"])
    if not need:
        return False, "兑换词组配置错误"
    try:
        reward_items = parse_reward(phrase.get("reward") or "")
    except Exception as e:
        return False, f"兑换奖励配置错误：{e}"

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        inventory = _get_collect_inventory_map(cur, activity["key"], uid)
        claims = _get_collect_claim_map(cur, activity["key"], uid)
        missing = [
            f"{word_char}x{count - inventory.get(word_char, 0)}"
            for word_char, count in need.items()
            if inventory.get(word_char, 0) < count
        ]
        if missing:
            return False, "字牌不足，还缺：" + "、".join(missing)

        claimed = claims.get(phrase["phrase"], 0)
        limit = _as_int(phrase.get("limit"), 1)
        if limit > 0 and claimed >= limit:
            return False, "该词组已达到兑换次数上限"

        ts = now_str()
        for word_char, count in need.items():
            cur.execute(
                """
                UPDATE activity_collect_inventory
                SET count=count-%s, update_time=%s
                WHERE activity_key=%s AND user_id=%s AND word_char=%s
                """,
                (count, ts, activity["key"], uid, word_char),
            )
        cur.execute(
            """
            INSERT INTO activity_collect_claim (
                activity_key, user_id, phrase, count, update_time
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(activity_key, user_id, phrase) DO UPDATE SET
                count = activity_collect_claim.count + 1,
                update_time = excluded.update_time
            """,
            (activity["key"], uid, phrase["phrase"], 1, ts),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    try:
        reward_msg = send_reward_items(uid, reward_items)
    except Exception as e:
        logger.warning(f"活动集字兑换发奖失败 user_id={uid}, activity={activity['key']}, phrase={phrase['phrase']}: {e}")
        return False, f"兑换已记录，奖励发放失败：{e}"

    reward_text = "，".join(reward_msg) if reward_msg else ""
    if reward_text:
        return True, f"{activity['name']}兑换成功：{phrase['name']}\n{reward_text}"
    return True, f"{activity['name']}兑换成功：{phrase['name']}"


def _get_point_balance(cur, activity_key: str, user_id: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT points, total_points
        FROM activity_point_balance
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    row = cur.fetchone()
    if not row:
        return {"points": 0, "total_points": 0}
    return {
        "points": max(0, _as_int(row["points"])),
        "total_points": max(0, _as_int(row["total_points"])),
    }


def _get_point_purchase_map(cur, activity_key: str, user_id: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT item_key, count
        FROM activity_point_purchase
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    return {
        str(row["item_key"]): max(0, _as_int(row["count"]))
        for row in cur.fetchall()
    }


def _get_point_shop_total_purchase(cur, activity_key: str, item_key: str) -> int:
    cur.execute(
        """
        SELECT COALESCE(SUM(count), 0) AS count
        FROM activity_point_purchase
        WHERE activity_key=%s AND item_key=%s
        """,
        (str(activity_key), str(item_key)),
    )
    row = cur.fetchone()
    return max(0, _as_int(row["count"] if row else 0))


def build_activity_points_text(user_id: str) -> str:
    uid = str(user_id)
    activities = [
        activity
        for activity in get_gameplay_activities(load_config())
        if activity.get("type") == "event_points"
    ]
    lines = ["【活动积分】"]
    if not activities:
        lines.append("暂无积分活动")
        return "\n".join(lines)

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        for activity in activities:
            ok, reason = activity_state(activity)
            balance = _get_point_balance(cur, activity["key"], uid)
            point_name = activity.get("point_name") or "活动积分"
            lines.extend([
                "",
                f"【{activity['name']}】{'进行中' if ok else reason}",
                f"当前{point_name}：{balance['points']}，累计获得：{balance['total_points']}",
            ])
            rules = activity.get("event_rules") or []
            if rules:
                rule_text = "、".join(
                    f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+{_as_int(rule.get('points'))}"
                    for rule in rules
                )
                lines.append(f"积分来源：{rule_text}")
        return "\n".join(lines).strip()
    finally:
        conn.close()


def build_activity_shop_text(user_id: str) -> str:
    uid = str(user_id)
    activities = [
        activity
        for activity in get_gameplay_activities(load_config())
        if activity.get("type") == "event_points"
    ]
    lines = ["【活动商店】"]
    if not activities:
        lines.append("暂无积分商店")
        return "\n".join(lines)

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        for activity in activities:
            ok, reason = activity_state(activity)
            point_name = activity.get("point_name") or "活动积分"
            balance = _get_point_balance(cur, activity["key"], uid)
            purchases = _get_point_purchase_map(cur, activity["key"], uid)
            lines.extend([
                "",
                f"【{activity['name']}】{'进行中' if ok else reason}",
                f"当前{point_name}：{balance['points']}",
            ])
            shop = activity.get("shop") or []
            if not shop:
                lines.append("暂无商店商品")
                continue
            for item in shop:
                item_key = str(item.get("item_key") or "")
                bought = purchases.get(item_key, 0)
                limit = _as_int(item.get("limit"), 1)
                limit_text = "不限" if limit <= 0 else f"{bought}/{limit}"
                stock_limit = _as_int(item.get("stock_limit"), 0)
                stock_text = ""
                if stock_limit > 0:
                    sold = _get_point_shop_total_purchase(cur, activity["key"], item_key)
                    stock_text = f"，全服库存 {sold}/{stock_limit}"
                lines.append(
                    f"- {item.get('name') or item_key}：{_as_int(item.get('cost'))}{point_name}，"
                    f"已兑换 {limit_text}{stock_text}，奖励：{item.get('reward') or '暂无奖励'}"
                )
        return "\n".join(lines).strip()
    finally:
        conn.close()


def _scope_label(scope_type: str) -> str:
    return "每日" if scope_type == "daily" else "周常"


def _task_status_text(progress: int, target: int, claimed: bool) -> str:
    if claimed:
        return "已领取"
    if progress >= target:
        return "可领取"
    return f"{progress}/{target}"


def build_activity_task_progress_text(user_id: str) -> str:
    cfg = load_config()
    activity_key = _activity_config_key(cfg)
    tasks = get_activity_tasks(cfg)
    lines = [f"【{cfg.get('name', '节日签到活动')} · 活动任务】"]
    if not tasks:
        lines.append("暂无活动任务")
        return "\n".join(lines)

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        progress_map = _get_task_progress_map(cur, activity_key, str(user_id))
        for scope_type in ("daily", "weekly"):
            scope_tasks = [task for task in tasks if task.get("scope_type") == scope_type]
            if not scope_tasks:
                continue
            scope_key = _task_scope_key(scope_type)
            lines.append("")
            lines.append(f"【{_scope_label(scope_type)}目标】")
            for task in scope_tasks:
                key = (scope_type, scope_key, task["key"])
                state = progress_map.get(key, {})
                target = max(1, _as_int(task.get("target"), 1))
                progress = min(target, max(0, _as_int(state.get("progress"), 0)))
                claimed = bool(state.get("claimed"))
                event_text = _activity_event_text(task.get("events"))
                desc = _clean_text(task.get("description"))
                if not desc:
                    desc = f"{event_text} {target} 次" if event_text else f"目标 {target}"
                lines.append(
                    f"- {task['name']}：{_task_status_text(progress, target, claimed)}，"
                    f"{desc}，奖励：{task.get('reward') or '暂无奖励'}"
                )
        lines.append("")
        lines.append("领奖：活动任务领取（自动领取全部可领任务）")
        return "\n".join(lines).strip()
    finally:
        conn.close()


def _select_claimable_tasks(cur, config: dict, user_id: str, query: str = "") -> list[tuple[dict, str, str, int]]:
    activity_key = _activity_config_key(config)
    target_text = _clean_text(query)
    tasks = get_activity_tasks(config)
    selected: list[tuple[dict, str, str, int]] = []
    for task in tasks:
        scope_type = task["scope_type"]
        scope_key = _task_scope_key(scope_type)
        if target_text and target_text not in {
            task["key"],
            task["name"],
            scope_type,
            _scope_label(scope_type),
        } and target_text not in task["name"]:
            continue
        cur.execute(
            """
            SELECT progress, target, claimed
            FROM activity_task_progress
            WHERE activity_key=%s AND user_id=%s AND scope_type=%s AND scope_key=%s AND task_key=%s
            """,
            (activity_key, user_id, scope_type, scope_key, task["key"]),
        )
        row = cur.fetchone()
        progress = max(0, _as_int(row["progress"] if row else 0))
        target = max(1, _as_int(row["target"] if row else task.get("target"), task.get("target", 1)))
        claimed = bool(_as_int(row["claimed"] if row else 0))
        if claimed or progress < target:
            continue
        selected.append((task, scope_type, scope_key, target))
    return selected


def claim_activity_tasks(user_id: str, query: str = "") -> tuple[bool, str]:
    cfg = load_config()
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放"
    if "claim" not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放活动领奖"
    uid = str(user_id)
    activity_key = _activity_config_key(cfg)
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    reward_jobs: list[tuple[dict, list[dict]]] = []
    try:
        cur = conn.cursor()
        claimable = _select_claimable_tasks(cur, cfg, uid, query)
        if not claimable:
            return False, "当前没有可领取的活动任务奖励"
        for task, scope_type, scope_key, target in claimable:
            reward_text = _clean_text(task.get("reward"))
            try:
                reward_items = parse_reward(reward_text)
            except Exception as e:
                conn.rollback()
                return False, f"任务【{task['name']}】奖励配置错误：{e}"
            ts = now_str()
            cur.execute(
                """
                UPDATE activity_task_progress
                SET claimed=1, claim_time=%s, update_time=%s, target=%s
                WHERE activity_key=%s AND user_id=%s AND scope_type=%s AND scope_key=%s AND task_key=%s
                    AND claimed=0 AND progress>=%s
                """,
                (ts, ts, target, activity_key, uid, scope_type, scope_key, task["key"], target),
            )
            if cur.rowcount <= 0:
                continue
            cur.execute(
                """
                INSERT INTO activity_task_claim_log (
                    activity_key, user_id, scope_type, scope_key, task_key, reward, create_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (activity_key, uid, scope_type, scope_key, task["key"], reward_text, ts),
            )
            reward_jobs.append((task, reward_items))
        if not reward_jobs:
            conn.rollback()
            return False, "当前没有可领取的活动任务奖励"
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    lines = ["活动任务奖励领取成功："]
    for task, reward_items in reward_jobs:
        try:
            reward_msg = send_reward_items(uid, reward_items)
            reward_text = "，".join(reward_msg) if reward_msg else _clean_text(task.get("reward"), "暂无奖励")
        except Exception as e:
            logger.warning(f"活动任务发奖失败 user_id={uid}, task={task.get('key')}: {e}")
            reward_text = f"奖励发放失败：{e}"
        lines.append(f"- {task['name']}：{reward_text}")
    return True, "\n".join(lines)


def build_activity_pass_text(user_id: str) -> str:
    cfg = load_config()
    pass_cfg = _activity_pass_config(cfg)
    lines = [f"【{pass_cfg['name']}】"]
    if not pass_cfg.get("enabled"):
        lines.append("活动战令未开启")
        return "\n".join(lines)
    activity_key = _activity_config_key(cfg)
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        balance = _get_pass_balance(cur, activity_key, str(user_id), pass_cfg)
        cur.execute(
            """
            SELECT level FROM activity_pass_reward_claim
            WHERE activity_key=%s AND user_id=%s
            """,
            (activity_key, str(user_id)),
        )
        claimed = {max(0, _as_int(row["level"])) for row in cur.fetchall()}
        exp_name = pass_cfg.get("exp_name") or "活跃值"
        lines.extend([
            f"等级：{balance['level']}/{balance['max_level']}",
            f"{exp_name}：{balance['exp']}/{balance['level_exp']}（累计 {balance['total_exp']}）",
        ])
        catchup = _pass_catchup_state(cur, cfg, activity_key, str(user_id), pass_cfg)
        if catchup.get("enabled"):
            if catchup.get("active"):
                lines.append(
                    f"追赶加成：已触发 {catchup['catchup_multiplier']:.2f}x，"
                    f"当前落后最高等级 {catchup['gap']} 级"
                )
            else:
                lines.append(
                    f"追赶加成：第{catchup['start_day']}天后、落后{catchup['level_gap']}级时触发"
                )
        rules = pass_cfg.get("event_rules") or []
        if rules:
            rule_text = "、".join(
                f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+{_as_int(rule.get('exp'))}"
                for rule in rules
            )
            lines.append(f"获取来源：{rule_text}")
        rewards = pass_cfg.get("level_rewards") or []
        if rewards:
            lines.append("")
            lines.append("【等级奖励】")
            for reward in rewards:
                level = _as_int(reward.get("level"), 0)
                if level <= 0:
                    continue
                if level in claimed:
                    status = "已领取"
                elif balance["level"] >= level:
                    status = "可领取"
                else:
                    status = "未达成"
                lines.append(
                    f"- Lv.{level} {reward.get('name') or '等级奖励'}：{status}，"
                    f"{reward.get('reward') or '暂无奖励'}"
                )
        lines.append("")
        lines.append("领奖：活动战令领取（自动领取全部可领等级奖励）")
        return "\n".join(lines).strip()
    finally:
        conn.close()


def claim_activity_pass_rewards(user_id: str, query: str = "") -> tuple[bool, str]:
    cfg = load_config()
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放"
    if "claim" not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放活动领奖"
    pass_cfg = _activity_pass_config(cfg)
    if not pass_cfg.get("enabled"):
        return False, "活动战令未开启"
    uid = str(user_id)
    activity_key = _activity_config_key(cfg)
    target_text = _clean_text(query)
    target_level = _as_int(target_text, 0) if target_text.isdigit() else 0
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    reward_jobs: list[tuple[dict, list[dict]]] = []
    try:
        cur = conn.cursor()
        balance = _get_pass_balance(cur, activity_key, uid, pass_cfg)
        cur.execute(
            """
            SELECT level FROM activity_pass_reward_claim
            WHERE activity_key=%s AND user_id=%s
            """,
            (activity_key, uid),
        )
        claimed = {max(0, _as_int(row["level"])) for row in cur.fetchall()}
        for reward in pass_cfg.get("level_rewards") or []:
            level = _as_int(reward.get("level"), 0)
            if level <= 0 or level in claimed or balance["level"] < level:
                continue
            if target_level and target_level != level:
                continue
            if target_text and not target_level and target_text not in {
                _clean_text(reward.get("name")),
                f"Lv.{level}",
                f"lv.{level}",
                str(level),
            } and target_text not in _clean_text(reward.get("name")):
                continue
            reward_text = _clean_text(reward.get("reward"))
            try:
                reward_items = parse_reward(reward_text)
            except Exception as e:
                conn.rollback()
                return False, f"战令Lv.{level}奖励配置错误：{e}"
            ts = now_str()
            cur.execute(
                """
                INSERT INTO activity_pass_reward_claim (activity_key, user_id, level, create_time)
                VALUES (%s, %s, %s, %s)
                """,
                (activity_key, uid, level, ts),
            )
            reward_jobs.append((reward, reward_items))
        if not reward_jobs:
            return False, "当前没有可领取的活动战令奖励"
        conn.commit()
    except db_backend.IntegrityError:
        conn.rollback()
        return False, "活动战令奖励已领取"
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    lines = ["活动战令奖励领取成功："]
    for reward, reward_items in reward_jobs:
        level = _as_int(reward.get("level"), 0)
        try:
            reward_msg = send_reward_items(uid, reward_items)
            reward_text = "，".join(reward_msg) if reward_msg else _clean_text(reward.get("reward"), "暂无奖励")
        except Exception as e:
            logger.warning(f"活动战令发奖失败 user_id={uid}, level={level}: {e}")
            reward_text = f"奖励发放失败：{e}"
        lines.append(f"- Lv.{level} {reward.get('name') or '等级奖励'}：{reward_text}")
    return True, "\n".join(lines)


def claim_activity_rewards(user_id: str) -> tuple[bool, str]:
    uid = str(user_id)
    successes: list[str] = []
    misses: list[str] = []

    try:
        ok, text = claim_activity_tasks(uid)
        if ok:
            successes.append(text)
        else:
            misses.append(f"任务：{text}")
    except Exception as e:
        logger.warning(f"活动总领奖任务奖励检查失败 user_id={uid}: {e}")
        misses.append(f"任务：{e}")

    try:
        ok, text = claim_activity_pass_rewards(uid)
        if ok:
            successes.append(text)
        else:
            misses.append(f"战令：{text}")
    except Exception as e:
        logger.warning(f"活动总领奖战令奖励检查失败 user_id={uid}: {e}")
        misses.append(f"战令：{e}")

    try:
        from .activity_boss import claim_boss_rewards

        ok, text = claim_boss_rewards(uid)
        if ok:
            successes.append(text)
        else:
            misses.append(f"首领：{text}")
    except Exception as e:
        logger.warning(f"活动总领奖首领奖励检查失败 user_id={uid}: {e}")
        misses.append(f"首领：{e}")

    if successes:
        return True, "\n\n".join(successes)
    return False, "暂无可领取奖励\n" + "\n".join(misses)


def _parse_shop_query(query: str) -> tuple[str, int]:
    text = _clean_text(query)
    if not text:
        return "", 1
    parts = text.split()
    if len(parts) >= 2:
        try:
            quantity = int(parts[-1])
        except ValueError:
            return text, 1
        return " ".join(parts[:-1]).strip(), max(1, quantity)
    return text, 1


def _find_point_shop_item(config: dict, query: str) -> tuple[dict, dict] | None:
    text = _clean_text(query)
    if not text:
        return None
    fallback: tuple[dict, dict] | None = None
    for activity in get_gameplay_activities(config):
        if activity.get("type") != "event_points":
            continue
        ok, _ = activity_state(activity)
        if not ok:
            continue
        for item in activity.get("shop") or []:
            item_key = _clean_text(item.get("item_key"))
            item_name = _clean_text(item.get("name"))
            if text in (item_key, item_name):
                return activity, item
            if not fallback and ((item_key and text in item_key) or (item_name and text in item_name)):
                fallback = (activity, item)
    return fallback


def _multiply_reward_items(reward_items: list[dict], quantity: int) -> list[dict]:
    multiplier = max(1, _as_int(quantity, 1))
    items = []
    for item in reward_items:
        copied = dict(item)
        copied["quantity"] = max(1, _as_int(copied.get("quantity"), 1)) * multiplier
        items.append(copied)
    return items


def claim_point_shop_item(user_id: str, query: str) -> tuple[bool, str]:
    uid = str(user_id)
    target, quantity = _parse_shop_query(query)
    if not target:
        return False, "请发送：活动购买 灵石补给"

    cfg = load_config()
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放"
    if "shop" not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放活动商店"
    found = _find_point_shop_item(cfg, target)
    if not found:
        return False, "未找到可兑换的活动商品，或活动当前不可兑换"

    activity, item = found
    cost = _as_int(item.get("cost"), 0)
    if cost <= 0:
        return False, "商品积分价格配置错误"
    reward_text = _clean_text(item.get("reward"))
    try:
        reward_items = _multiply_reward_items(parse_reward(reward_text), quantity)
    except Exception as e:
        return False, f"商品奖励配置错误：{e}"

    item_key = _clean_text(item.get("item_key"))
    point_name = activity.get("point_name") or "活动积分"
    total_cost = cost * quantity
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        balance = _get_point_balance(cur, activity["key"], uid)
        if balance["points"] < total_cost:
            return False, f"{point_name}不足，还缺 {total_cost - balance['points']}"
        purchases = _get_point_purchase_map(cur, activity["key"], uid)
        bought = purchases.get(item_key, 0)
        limit = _as_int(item.get("limit"), 1)
        if limit > 0 and bought + quantity > limit:
            return False, f"该商品兑换次数不足，当前已兑换 {bought}/{limit}"
        stock_limit = _as_int(item.get("stock_limit"), 0)
        if stock_limit > 0:
            sold = _get_point_shop_total_purchase(cur, activity["key"], item_key)
            if sold + quantity > stock_limit:
                return False, f"该商品全服库存不足，当前已兑换 {sold}/{stock_limit}"

        ts = now_str()
        cur.execute(
            """
            UPDATE activity_point_balance
            SET points=points-%s, update_time=%s
            WHERE activity_key=%s AND user_id=%s AND points >= %s
            """,
            (total_cost, ts, activity["key"], uid, total_cost),
        )
        if cur.rowcount <= 0:
            conn.rollback()
            return False, f"{point_name}不足"
        cur.execute(
            """
            INSERT INTO activity_point_purchase (
                activity_key, user_id, item_key, count, update_time
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(activity_key, user_id, item_key) DO UPDATE SET
                count = activity_point_purchase.count + excluded.count,
                update_time = excluded.update_time
            """,
            (activity["key"], uid, item_key, quantity, ts),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    try:
        reward_msg = send_reward_items(uid, reward_items)
    except Exception as e:
        logger.warning(f"活动积分兑换发奖失败 user_id={uid}, activity={activity['key']}, item={item_key}: {e}")
        return False, f"兑换已记录，奖励发放失败：{e}"

    reward_result = "，".join(reward_msg) if reward_msg else reward_text
    item_name = item.get("name") or item_key
    quantity_text = f"x{quantity}" if quantity > 1 else ""
    return True, f"{activity['name']}兑换成功：{item_name}{quantity_text}\n消耗：{total_cost}{point_name}\n奖励：{reward_result}"


def _fetch_count(cur, sql: str, params=()) -> int:
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        return 0
    if isinstance(row, dict):
        return _as_int(next(iter(row.values()), 0))
    try:
        return _as_int(row[0])
    except Exception:
        return _as_int(row["count"] if "count" in row.keys() else 0)


def _activity_data_counts(cur, activity_key: str, activity_type: str) -> dict:
    if activity_type == "event_points":
        cur.execute(
            """
            SELECT
                COUNT(*) AS user_count,
                COALESCE(SUM(points), 0) AS current_points,
                COALESCE(SUM(total_points), 0) AS total_points
            FROM activity_point_balance
            WHERE activity_key=%s
            """,
            (activity_key,),
        )
        row = cur.fetchone()
        cur.execute(
            """
            SELECT COALESCE(SUM(count), 0) AS count
            FROM activity_point_purchase
            WHERE activity_key=%s
            """,
            (activity_key,),
        )
        purchase_row = cur.fetchone()
        return {
            "user_count": _as_int(row["user_count"] if row else 0),
            "current_points": _as_int(row["current_points"] if row else 0),
            "total_points": _as_int(row["total_points"] if row else 0),
            "purchase_count": _as_int(purchase_row["count"] if purchase_row else 0),
        }

    if activity_type == "activity_boss":
        cur.execute(
            """
            SELECT hp_left, max_hp
            FROM activity_boss_state
            WHERE activity_key=%s
            """,
            (activity_key,),
        )
        hp_row = cur.fetchone()
        cur.execute(
            """
            SELECT
                COUNT(*) AS user_count,
                COALESCE(SUM(total_damage), 0) AS total_damage
            FROM activity_boss_damage
            WHERE activity_key=%s
            """,
            (activity_key,),
        )
        damage_row = cur.fetchone()
        fight_count = _fetch_count(
            cur,
            "SELECT COUNT(*) AS count FROM activity_boss_fight_log WHERE activity_key=%s",
            (activity_key,),
        )
        milestone_count = _fetch_count(
            cur,
            "SELECT COUNT(*) AS count FROM activity_boss_milestone WHERE activity_key=%s",
            (activity_key,),
        )
        item_count = _fetch_count(
            cur,
            "SELECT COALESCE(SUM(count), 0) AS count FROM activity_item_inventory WHERE activity_key=%s",
            (activity_key,),
        )
        return {
            "user_count": _as_int(damage_row["user_count"] if damage_row else 0),
            "total_damage": _as_int(damage_row["total_damage"] if damage_row else 0),
            "fight_count": fight_count,
            "item_count": item_count,
            "milestone_count": milestone_count,
            "hp_left": _as_int(hp_row["hp_left"] if hp_row else 0),
            "max_hp": _as_int(hp_row["max_hp"] if hp_row else 0),
        }

    cur.execute(
        """
        SELECT
            COUNT(DISTINCT user_id) AS user_count,
            COALESCE(SUM(count), 0) AS inventory_count
        FROM activity_collect_inventory
        WHERE activity_key=%s
        """,
        (activity_key,),
    )
    inventory_row = cur.fetchone()
    drop_count = _fetch_count(
        cur,
        "SELECT COUNT(*) AS count FROM activity_collect_drop_log WHERE activity_key=%s",
        (activity_key,),
    )
    cur.execute(
        """
        SELECT COALESCE(SUM(count), 0) AS count
        FROM activity_collect_claim
        WHERE activity_key=%s
        """,
        (activity_key,),
    )
    claim_row = cur.fetchone()
    return {
        "user_count": _as_int(inventory_row["user_count"] if inventory_row else 0),
        "inventory_count": _as_int(inventory_row["inventory_count"] if inventory_row else 0),
        "drop_count": drop_count,
        "claim_count": _as_int(claim_row["count"] if claim_row else 0),
    }


def _activity_task_data_overview(cur, config: dict, uid: str = "") -> dict:
    activity_key = _activity_config_key(config)
    tasks = get_activity_tasks(config)
    daily_key = _task_scope_key("daily")
    weekly_key = _task_scope_key("weekly")
    cur.execute(
        """
        SELECT
            COUNT(DISTINCT user_id) AS user_count,
            COALESCE(SUM(CASE WHEN progress >= target THEN 1 ELSE 0 END), 0) AS complete_count,
            COALESCE(SUM(claimed), 0) AS claim_count
        FROM activity_task_progress
        WHERE activity_key=%s AND scope_key IN (%s, %s)
        """,
        (activity_key, daily_key, weekly_key),
    )
    row = cur.fetchone()
    overview = {
        "activity_key": activity_key,
        "daily_task_count": len([task for task in tasks if task.get("scope_type") == "daily"]),
        "weekly_task_count": len([task for task in tasks if task.get("scope_type") == "weekly"]),
        "user_count": _as_int(row["user_count"] if row else 0),
        "complete_count": _as_int(row["complete_count"] if row else 0),
        "claim_count": _as_int(row["claim_count"] if row else 0),
    }
    if uid:
        progress_map = _get_task_progress_map(cur, activity_key, uid)
        user_rows = []
        for task in tasks:
            scope_type = task["scope_type"]
            scope_key = _task_scope_key(scope_type)
            state = progress_map.get((scope_type, scope_key, task["key"]), {})
            target = max(1, _as_int(task.get("target"), 1))
            progress = min(target, max(0, _as_int(state.get("progress"), 0)))
            user_rows.append({
                "task_key": task["key"],
                "name": task["name"],
                "scope_type": scope_type,
                "scope_key": scope_key,
                "progress": progress,
                "target": target,
                "claimed": bool(state.get("claimed")),
            })
        overview["user"] = user_rows
    return overview


def _activity_pass_data_overview(cur, config: dict, uid: str = "", limit: int = 10) -> dict:
    activity_key = _activity_config_key(config)
    pass_cfg = _activity_pass_config(config)
    if not pass_cfg.get("enabled"):
        return {"enabled": False, "activity_key": activity_key}
    cur.execute(
        """
        SELECT
            COUNT(*) AS user_count,
            COALESCE(SUM(total_exp), 0) AS total_exp,
            COALESCE(MAX(level), 0) AS max_level
        FROM activity_pass_balance
        WHERE activity_key=%s
        """,
        (activity_key,),
    )
    row = cur.fetchone()
    cur.execute(
        """
        SELECT user_id, total_exp, level, update_time
        FROM activity_pass_balance
        WHERE activity_key=%s
        ORDER BY level DESC, total_exp DESC, update_time ASC
        LIMIT %s
        """,
        (activity_key, max(1, min(_as_int(limit, 10), 50))),
    )
    overview = {
        "enabled": True,
        "activity_key": activity_key,
        "name": pass_cfg.get("name"),
        "exp_name": pass_cfg.get("exp_name"),
        "level_exp": pass_cfg.get("level_exp"),
        "max_level": pass_cfg.get("max_level"),
        "catchup": {
            "enabled": bool(pass_cfg.get("catchup_enabled")),
            "start_day": pass_cfg.get("catchup_start_day"),
            "level_gap": pass_cfg.get("catchup_level_gap"),
            "multiplier": pass_cfg.get("catchup_multiplier"),
        },
        "user_count": _as_int(row["user_count"] if row else 0),
        "total_exp": _as_int(row["total_exp"] if row else 0),
        "highest_level": _as_int(row["max_level"] if row else 0),
        "top_users": _attach_display_names([dict(item) for item in cur.fetchall()]),
    }
    if uid:
        overview["user"] = _get_pass_balance(cur, activity_key, uid, pass_cfg)
        overview["user_catchup"] = _pass_catchup_state(cur, config, activity_key, uid, pass_cfg)
    return overview


def get_activity_data_overview(
    activity_key: str | None = None,
    user_id: str | None = None,
    limit: int = 10,
) -> dict:
    cfg = load_config()
    activities = get_gameplay_activities(cfg)
    key_filter = _clean_text(activity_key)
    uid = _clean_text(user_id)
    row_limit = max(1, min(_as_int(limit, 10), 50))

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        sign_summary = {
            "user_count": _fetch_count(cur, "SELECT COUNT(*) AS count FROM activity_user"),
            "today_count": _fetch_count(
                cur,
                "SELECT COUNT(*) AS count FROM activity_sign_log WHERE sign_date=%s",
                (today_str(),),
            ),
            "log_count": _fetch_count(cur, "SELECT COUNT(*) AS count FROM activity_sign_log"),
        }
        cur.execute(
            """
            SELECT user_id, sign_days, total_sign_days, last_sign_date
            FROM activity_user
            ORDER BY sign_days DESC, total_sign_days DESC, last_sign_date ASC
            LIMIT %s
            """,
            (row_limit,),
        )
        sign_rank = _attach_display_names([dict(row) for row in cur.fetchall()])

        activity_rows = []
        for activity in activities:
            if key_filter and activity["key"] != key_filter:
                continue
            ok, reason = activity_state(activity)
            row = {
                "key": activity["key"],
                "name": activity.get("name", ""),
                "type": activity.get("type", ""),
                "enabled": bool(activity.get("enabled")),
                "state": "进行中" if ok else reason,
                "counts": _activity_data_counts(cur, activity["key"], activity.get("type", "")),
            }
            if activity.get("type") == "event_points":
                cur.execute(
                    """
                    SELECT user_id, points, total_points, update_time
                    FROM activity_point_balance
                    WHERE activity_key=%s
                    ORDER BY total_points DESC, points DESC, update_time ASC
                    LIMIT %s
                    """,
                    (activity["key"], row_limit),
                )
                row["top_users"] = _attach_display_names([dict(item) for item in cur.fetchall()])
                if uid:
                    row["user"] = {
                        "balance": _get_point_balance(cur, activity["key"], uid),
                        "purchases": _get_point_purchase_map(cur, activity["key"], uid),
                    }
            elif activity.get("type") == "activity_boss":
                cur.execute(
                    """
                    SELECT user_id, total_damage, update_time
                    FROM activity_boss_damage
                    WHERE activity_key=%s AND total_damage>0
                    ORDER BY total_damage DESC, update_time ASC
                    LIMIT %s
                    """,
                    (activity["key"], row_limit),
                )
                row["top_users"] = _attach_display_names([dict(item) for item in cur.fetchall()])
                if uid:
                    cur.execute(
                        """
                        SELECT total_damage, update_time
                        FROM activity_boss_damage
                        WHERE activity_key=%s AND user_id=%s
                        """,
                        (activity["key"], uid),
                    )
                    damage_row = cur.fetchone()
                    cur.execute(
                        """
                        SELECT item_id, count
                        FROM activity_item_inventory
                        WHERE activity_key=%s AND user_id=%s
                        """,
                        (activity["key"], uid),
                    )
                    row["user"] = {
                        "damage": dict(damage_row) if damage_row else {"total_damage": 0, "update_time": ""},
                        "items": {
                            str(item["item_id"]): max(0, _as_int(item["count"]))
                            for item in cur.fetchall()
                        },
                        "today_fight_count": _fetch_count(
                            cur,
                            """
                            SELECT COUNT(*) AS count FROM activity_boss_fight_log
                            WHERE activity_key=%s AND user_id=%s AND fight_date=%s
                            """,
                            (activity["key"], uid, today_str()),
                        ),
                    }
            else:
                cur.execute(
                    """
                    SELECT user_id, COUNT(*) AS drop_count, MAX(create_time) AS last_time
                    FROM activity_collect_drop_log
                    WHERE activity_key=%s
                    GROUP BY user_id
                    ORDER BY drop_count DESC, last_time ASC
                    LIMIT %s
                    """,
                    (activity["key"], row_limit),
                )
                row["top_users"] = _attach_display_names([dict(item) for item in cur.fetchall()])
                if uid:
                    row["user"] = {
                        "inventory": _get_collect_inventory_map(cur, activity["key"], uid),
                        "claims": _get_collect_claim_map(cur, activity["key"], uid),
                        "pity": _collect_pity_progress_map(cur, activity["key"], uid),
                    }
            activity_rows.append(row)

        user_sign = None
        if uid:
            cur.execute("SELECT * FROM activity_user WHERE user_id=%s", (uid,))
            row = cur.fetchone()
            if row:
                user_sign = dict(row)
                user_sign["sign_days"] = _as_int(user_sign.get("sign_days"))
                user_sign["total_sign_days"] = _as_int(user_sign.get("total_sign_days"), user_sign["sign_days"])
            else:
                user_sign = {
                    "user_id": uid,
                    "sign_days": 0,
                    "last_sign_date": "",
                    "total_sign_days": 0,
                }
        return {
            "sign": sign_summary,
            "sign_rank": sign_rank,
            "activities": activity_rows,
            "tasks": _activity_task_data_overview(cur, cfg, uid),
            "activity_pass": _activity_pass_data_overview(cur, cfg, uid, row_limit),
            "runtime": activity_runtime_state(cfg),
            "user_id": uid,
            "user_sign": user_sign,
        }
    finally:
        conn.close()


def reset_activity_data(scope: str, activity_key: str | None = None) -> str:
    target_scope = _clean_text(scope, "activity")
    key = _clean_text(activity_key)
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    try:
        cur = conn.cursor()
        deleted = 0
        if target_scope in {"sign", "all"}:
            for table in ("activity_user", "activity_sign_log"):
                cur.execute(f"DELETE FROM {table}")
                deleted += max(0, cur.rowcount)

        if target_scope in {"activity", "gameplay", "all"}:
            if target_scope == "activity" and not key:
                raise ValueError("请选择要清空的玩法活动")
            tables = (
                "activity_collect_inventory",
                "activity_collect_claim",
                "activity_collect_drop_log",
                "activity_collect_pity_state",
                "activity_point_balance",
                "activity_point_event_log",
                "activity_point_purchase",
                "activity_item_inventory",
                "activity_boss_state",
                "activity_boss_damage",
                "activity_boss_fight_log",
                "activity_boss_milestone",
                "activity_boss_milestone_claim",
                "activity_boss_rank_claim",
            )
            for table in tables:
                if key:
                    cur.execute(f"DELETE FROM {table} WHERE activity_key=%s", (key,))
                else:
                    cur.execute(f"DELETE FROM {table}")
                deleted += max(0, cur.rowcount)

        if target_scope in {"task", "tasks", "pass", "activity_pass", "all"}:
            activity_key_for_main = _activity_config_key(load_config())
            task_pass_tables = (
                "activity_task_progress",
                "activity_task_claim_log",
                "activity_pass_balance",
                "activity_pass_event_log",
                "activity_pass_reward_claim",
            )
            for table in task_pass_tables:
                if key:
                    cur.execute(f"DELETE FROM {table} WHERE activity_key=%s", (key,))
                elif target_scope != "all":
                    cur.execute(f"DELETE FROM {table} WHERE activity_key=%s", (activity_key_for_main,))
                else:
                    cur.execute(f"DELETE FROM {table}")
                deleted += max(0, cur.rowcount)

        if target_scope not in {"sign", "activity", "gameplay", "task", "tasks", "pass", "activity_pass", "all"}:
            raise ValueError("清理范围无效")
        conn.commit()
        return f"已清理活动数据，影响记录 {deleted} 条"
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def adjust_activity_points(activity_key: str, user_id: str, amount: int) -> dict:
    key = _clean_text(activity_key)
    uid = _clean_text(user_id)
    delta = _as_int(amount, 0)
    if not key:
        raise ValueError("请选择积分活动")
    activity = next(
        (
            item
            for item in get_gameplay_activities(load_config())
            if item.get("key") == key
        ),
        None,
    )
    if not activity or activity.get("type") != "event_points":
        raise ValueError("请选择积分活动")
    if not uid:
        raise ValueError("请输入用户ID")
    if delta == 0:
        raise ValueError("调整数量不能为 0")

    ts = now_str()
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        balance = _get_point_balance(cur, key, uid)
        next_points = max(0, balance["points"] + delta)
        next_total = balance["total_points"] + max(0, delta)
        cur.execute(
            """
            INSERT INTO activity_point_balance (
                activity_key, user_id, points, total_points, update_time
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(activity_key, user_id) DO UPDATE SET
                points = excluded.points,
                total_points = excluded.total_points,
                update_time = excluded.update_time
            """,
            (key, uid, next_points, next_total, ts),
        )
        conn.commit()
        return {"points": next_points, "total_points": next_total}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def adjust_collect_word(activity_key: str, user_id: str, word_char: str, amount: int) -> dict:
    key = _clean_text(activity_key)
    uid = _clean_text(user_id)
    char = _clean_text(word_char)
    delta = _as_int(amount, 0)
    if not key:
        raise ValueError("请选择集字活动")
    activity = next(
        (
            item
            for item in get_gameplay_activities(load_config())
            if item.get("key") == key
        ),
        None,
    )
    if not activity or activity.get("type") != "collect_words":
        raise ValueError("请选择集字活动")
    if not uid:
        raise ValueError("请输入用户ID")
    if not char:
        raise ValueError("请输入字牌")
    if delta == 0:
        raise ValueError("调整数量不能为 0")
    char = char[0]

    ts = now_str()
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        inventory = _get_collect_inventory_map(cur, key, uid)
        next_count = max(0, inventory.get(char, 0) + delta)
        cur.execute(
            """
            INSERT INTO activity_collect_inventory (
                activity_key, user_id, word_char, count, update_time
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(activity_key, user_id, word_char) DO UPDATE SET
                count = excluded.count,
                update_time = excluded.update_time
            """,
            (key, uid, char, next_count, ts),
        )
        conn.commit()
        return {"word_char": char, "count": next_count}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def adjust_activity_pass_exp(user_id: str, amount: int) -> dict:
    uid = _clean_text(user_id)
    delta = _as_int(amount, 0)
    if not uid:
        raise ValueError("请输入用户ID")
    if delta == 0:
        raise ValueError("调整数量不能为 0")

    cfg = load_config()
    pass_cfg = _activity_pass_config(cfg)
    if not pass_cfg.get("enabled"):
        raise ValueError("活动战令未开启")
    activity_key = _activity_config_key(cfg)
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        before = _get_pass_balance(cur, activity_key, uid, pass_cfg)
        if delta > 0:
            _, after = _grant_pass_exp(cur, activity_key, uid, pass_cfg, delta)
        else:
            next_total = max(0, before["total_exp"] + delta)
            level_exp = max(1, _as_int(pass_cfg.get("level_exp"), 100))
            max_level = max(1, _as_int(pass_cfg.get("max_level"), 12))
            level = _calc_pass_level(next_total, level_exp, max_level)
            current_exp = _pass_current_exp(next_total, level, level_exp, max_level)
            cur.execute(
                """
                INSERT INTO activity_pass_balance (
                    activity_key, user_id, exp, total_exp, level, update_time
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(activity_key, user_id) DO UPDATE SET
                    exp = excluded.exp,
                    total_exp = excluded.total_exp,
                    level = excluded.level,
                    update_time = excluded.update_time
                """,
                (activity_key, uid, current_exp, next_total, level, now_str()),
            )
            after = {
                "exp": current_exp,
                "total_exp": next_total,
                "level": level,
                "level_exp": level_exp,
                "max_level": max_level,
            }
        conn.commit()
        return after
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _finish_sign_log(user_id: str, sign_date: str, status: str, message: str):
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE activity_sign_log
            SET reward_status=%s, reward_message=%s, finish_time=%s
            WHERE user_id=%s AND sign_date=%s
            """,
            (str(status), str(message), now_str(), str(user_id), str(sign_date)),
        )
        conn.commit()
    finally:
        conn.close()


def claim_sign(user_id: str) -> tuple[bool, str]:
    cfg = load_config()
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放"
    if "sign" not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放活动签到"

    uid = str(user_id)
    today = today_str()
    ts = now_str()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM activity_user WHERE user_id=%s", (uid,))
        row = cur.fetchone()
        if row and row["last_sign_date"] == today:
            return False, "今日已经领取过活动签到"

        current_sign_days = _as_int(row["sign_days"] if row else 0)
        if row and "total_sign_days" in row.keys():
            current_total_sign_days = _as_int(row["total_sign_days"], current_sign_days)
        else:
            current_total_sign_days = current_sign_days
        sign_days = current_sign_days + 1
        total_sign_days = current_total_sign_days + 1
        daily_reward = _reward_by_day(cfg, sign_days)
        milestone_reward = _milestone_by_days(cfg, sign_days)
        daily_reward_text = str(daily_reward.get("reward") or "")
        milestone_reward_text = str(milestone_reward.get("reward") or "")
        try:
            daily_reward_items = parse_reward(daily_reward_text)
            milestone_reward_items = parse_reward(milestone_reward_text)
        except Exception as e:
            return False, f"活动奖励配置错误：{e}"

        cur.execute(
            """
            INSERT INTO activity_sign_log (
                user_id, sign_date, day_index, reward, milestone_reward,
                reward_status, create_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (uid, today, sign_days, daily_reward_text, milestone_reward_text, "pending", ts),
        )
        cur.execute(
            """
            INSERT INTO activity_user (
                user_id, sign_days, last_sign_date, total_sign_days, create_time, update_time
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(user_id) DO UPDATE SET
                sign_days = excluded.sign_days,
                last_sign_date = excluded.last_sign_date,
                total_sign_days = excluded.total_sign_days,
                update_time = excluded.update_time
            """,
            (uid, sign_days, today, total_sign_days, ts, ts),
        )
        conn.commit()
    except db_backend.IntegrityError:
        conn.rollback()
        return False, "今日已经领取过活动签到"
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    try:
        daily_msg = send_reward_items(uid, daily_reward_items)
        milestone_msg = send_reward_items(uid, milestone_reward_items)
    except Exception as e:
        logger.warning(f"活动签到发奖失败 user_id={uid}: {e}")
        _finish_sign_log(uid, today, "failed", str(e))
        return False, f"签到已记录，奖励发放失败：{e}"

    reply_mode = _sign_reply_mode(cfg)
    lines = [
        f"{cfg.get('festival_name', '节日')}签到成功",
        f"累计签到：{sign_days} 天",
    ]
    if reply_mode == "minimal":
        milestone_name = _clean_text(milestone_reward.get("name"))
        if milestone_reward_text or milestone_name:
            lines.append(f"达成：{milestone_name or f'累计{sign_days}天'}")
    elif reply_mode == "normal":
        if milestone_reward_text or milestone_reward.get("name"):
            title = str(milestone_reward.get("name") or f"累计{sign_days}天奖励")
            lines.append(_format_reward_result(title, milestone_reward_text, milestone_msg))
    else:
        lines.append(_format_reward_result("今日奖励", daily_reward_text, daily_msg))
        if milestone_reward_text or milestone_reward.get("name"):
            title = str(milestone_reward.get("name") or f"累计{sign_days}天奖励")
            lines.append(_format_reward_result(title, milestone_reward_text, milestone_msg))
    try:
        lines.extend(record_activity_event(uid, "sign_in"))
    except Exception as e:
        logger.warning(f"活动签到记录玩法掉落失败 user_id={uid}: {e}")
    log_lines = list(lines)
    if reply_mode == "minimal":
        log_lines.append(_format_reward_result("今日奖励", daily_reward_text, daily_msg))
        if milestone_reward_text or milestone_reward.get("name"):
            title = str(milestone_reward.get("name") or f"累计{sign_days}天奖励")
            log_lines.append(_format_reward_result(title, milestone_reward_text, milestone_msg))
    _finish_sign_log(uid, today, "success", "\n".join(log_lines))
    return True, "\n".join(lines)


def _append_gameplay_summary(lines: list[str], cfg: dict, *, detail: bool) -> None:
    gameplay_activities = get_gameplay_activities(cfg)
    if not gameplay_activities:
        return
    lines.append("")
    lines.append("【玩法活动】")
    for activity in gameplay_activities:
        ok, reason = activity_state(activity)
        status = "进行中" if ok else reason
        lines.append(f"- {activity.get('name', '集字活动')}：{status}")
        if not detail:
            continue
        if activity.get("description"):
            lines.append(f"  {activity.get('description')}")
        if activity.get("type") == "event_points":
            point_name = activity.get("point_name") or "活动积分"
            rules = activity.get("event_rules") or []
            if rules:
                rule_text = "、".join(
                    f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+{_as_int(rule.get('points'))}"
                    for rule in rules
                )
                lines.append(f"  积分来源：{rule_text}")
            shop = activity.get("shop") or []
            if shop:
                shop_text = "、".join(str(item.get("name") or item.get("item_key")) for item in shop)
                lines.append(f"  {point_name}商店：{shop_text}")
            continue
        if activity.get("type") == "activity_boss":
            mode = activity.get("mode") or "cooperative"
            lines.append(f"  首领：{activity.get('boss_name', '活动首领')}")
            if mode in {"item_raid", "both"}:
                item_names = "、".join(str(it.get("name")) for it in (activity.get("items") or []))
                lines.append(f"  道具讨伐：{item_names or '未配置'}（随机伤害区间）")
                lines.append("  命令：活动讨伐 [首领名] 道具名")
            if mode in {"cooperative", "both"}:
                cap_pct = int(_as_float(activity.get("hit_hp_cap_ratio"), 0.01) * 100)
                lines.append(
                    f"  全服协作：攻力×{int(_as_float(activity.get('atk_ratio'), 0.1) * 100)}%，"
                    f"每日{_as_int(activity.get('daily_fight_limit'), 3)}次，单次伤害上限{cap_pct}%血量"
                )
                lines.append("  命令：活动讨伐 / 讨伐世界BOSS 也会计入（若开启）")
            lines.append("  活动首领 / 活动首领排行 / 活动首领领奖")
            continue

        event_text = _activity_event_text(activity.get("drop_events"))
        lines.append(
            f"  掉落来源：{event_text or '未配置'}，"
            f"每日上限：{_as_int(activity.get('daily_drop_limit'), 0)}，"
            f"掉落概率：{int(_drop_rate(activity.get('drop_rate'), 0) * 100)}%"
        )
        phrases = activity.get("phrases") or []
        if phrases:
            phrase_text = "、".join(str(item.get("name") or item.get("phrase")) for item in phrases)
            lines.append(f"  兑换词组：{phrase_text}")


def _stage_feature_text(features: list[str]) -> str:
    return "、".join(STAGE_FEATURES.get(feature, feature) for feature in features) or "暂无开放玩法"


def _stage_time_text(stage: dict | None) -> str:
    if not stage:
        return ""
    return f"{stage.get('start_time', '0')} 至 {stage.get('end_time', '无限')}"


def _append_stage_summary(lines: list[str], cfg: dict, *, detail: bool = False) -> None:
    runtime = activity_runtime_state(cfg)
    stage = runtime.get("stage") or {}
    lines.append("")
    lines.append("【活动阶段】")
    lines.append(f"当前：{runtime.get('stage_name') or '未开放'}")
    if stage:
        time_text = _stage_time_text(stage)
        if time_text:
            lines.append(f"阶段时间：{time_text}")
    lines.append("开放内容：" + _stage_feature_text(list(runtime.get("features") or [])))
    multiplier = _as_float(runtime.get("multiplier"), 1.0)
    if detail or abs(multiplier - 1.0) > 0.001:
        lines.append(f"阶段倍率：{multiplier:.2f}x")
    next_stage = runtime.get("next_stage")
    if next_stage:
        lines.append(
            f"下一阶段：{next_stage.get('name', '活动阶段')}（{_stage_time_text(next_stage)}）"
        )
    if not runtime.get("can_produce") and "claim" in set(runtime.get("features") or []):
        lines.append("当前阶段主要用于领奖、兑换和结算，不再产出活动积分、字牌或战令经验。")


def _append_pass_summary(lines: list[str], cfg: dict, user_id: str | None = None, *, detail: bool = False) -> None:
    pass_cfg = _activity_pass_config(cfg)
    if not pass_cfg.get("enabled"):
        return
    lines.append("")
    lines.append(f"【{pass_cfg['name']}】")
    if user_id:
        ensure_activity_files()
        conn = db_backend.connect(DB_PATH)
        conn.row_factory = db_backend.Row
        try:
            balance = _get_pass_balance(conn.cursor(), _activity_config_key(cfg), str(user_id), pass_cfg)
        finally:
            conn.close()
        lines.append(
            f"等级 {balance['level']}/{balance['max_level']}，"
            f"{pass_cfg['exp_name']} {balance['exp']}/{balance['level_exp']}"
        )
    else:
        lines.append(f"每 {pass_cfg['level_exp']}{pass_cfg['exp_name']} 提升 1 级")
    if detail:
        rules = pass_cfg.get("event_rules") or []
        if rules:
            rule_text = "、".join(
                f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+{_as_int(rule.get('exp'))}"
                for rule in rules
            )
            lines.append(f"来源：{rule_text}")
    lines.append("命令：活动战令 / 活动战令领取")


def _append_task_summary(lines: list[str], cfg: dict, user_id: str | None = None, *, detail: bool = False) -> None:
    tasks = get_activity_tasks(cfg)
    if not tasks:
        return
    lines.append("")
    lines.append("【活动目标】")
    if not user_id:
        daily_count = len([task for task in tasks if task.get("scope_type") == "daily"])
        weekly_count = len([task for task in tasks if task.get("scope_type") == "weekly"])
        lines.append(f"每日 {daily_count} 项，周常 {weekly_count} 项")
        lines.append("命令：活动任务 / 活动任务领取")
        return

    activity_key = _activity_config_key(cfg)
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        progress_map = _get_task_progress_map(conn.cursor(), activity_key, str(user_id))
    finally:
        conn.close()
    for scope_type in ("daily", "weekly"):
        scope_tasks = [task for task in tasks if task.get("scope_type") == scope_type]
        if not scope_tasks:
            continue
        scope_key = _task_scope_key(scope_type)
        finished = 0
        claimable = 0
        for task in scope_tasks:
            state = progress_map.get((scope_type, scope_key, task["key"]), {})
            target = max(1, _as_int(task.get("target"), 1))
            progress = min(target, max(0, _as_int(state.get("progress"), 0)))
            claimed = bool(state.get("claimed"))
            if claimed:
                finished += 1
            elif progress >= target:
                claimable += 1
        label = _scope_label(scope_type)
        lines.append(f"{label}：已领 {finished}/{len(scope_tasks)}，可领 {claimable}")
    daily_tips = []
    daily_key = _task_scope_key("daily")
    for task in [task for task in tasks if task.get("scope_type") == "daily"]:
        state = progress_map.get(("daily", daily_key, task["key"]), {})
        target = max(1, _as_int(task.get("target"), 1))
        progress = min(target, max(0, _as_int(state.get("progress"), 0)))
        claimed = bool(state.get("claimed"))
        if claimed:
            continue
        if progress >= target:
            daily_tips.append(f"{task['name']}可领")
        else:
            daily_tips.append(f"{task['name']} {progress}/{target}")
        if len(daily_tips) >= 3:
            break
    if daily_tips:
        lines.append("今日建议：" + "、".join(daily_tips))
    if detail:
        lines.append("查看明细：活动任务")
    lines.append("领奖：活动任务领取")


def _append_action_summary(lines: list[str], cfg: dict, user_id: str | None = None) -> None:
    if not user_id:
        return
    uid = str(user_id)
    activity_key = _activity_config_key(cfg)
    tips: list[str] = []
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        claimable_tasks = len(_select_claimable_tasks(cur, cfg, uid))
        if claimable_tasks:
            tips.append(f"{claimable_tasks}个任务奖励可领")

        pass_cfg = _activity_pass_config(cfg)
        if pass_cfg.get("enabled"):
            balance = _get_pass_balance(cur, activity_key, uid, pass_cfg)
            cur.execute(
                """
                SELECT level FROM activity_pass_reward_claim
                WHERE activity_key=%s AND user_id=%s
                """,
                (activity_key, uid),
            )
            claimed = {max(0, _as_int(row["level"])) for row in cur.fetchall()}
            pass_claimable = [
                reward for reward in pass_cfg.get("level_rewards") or []
                if _as_int(reward.get("level"), 0) > 0
                and _as_int(reward.get("level"), 0) <= balance["level"]
                and _as_int(reward.get("level"), 0) not in claimed
            ]
            if pass_claimable:
                tips.append(f"{len(pass_claimable)}档战令奖励可领")
            catchup = _pass_catchup_state(cur, cfg, activity_key, uid, pass_cfg)
            if catchup.get("active"):
                tips.append(f"战令追赶{catchup['catchup_multiplier']:.2f}x生效")

        for activity in get_gameplay_activities(cfg):
            if activity.get("type") != "collect_words":
                continue
            pity_threshold = max(0, _as_int(activity.get("pity_threshold"), 0))
            if pity_threshold <= 0:
                continue
            pity_map = _collect_pity_progress_map(cur, activity["key"], uid)
            near_events = [
                ACTIVITY_EVENT_LABELS.get(event_key, event_key)
                for event_key in activity.get("drop_events") or []
                if pity_map.get(event_key, 0) >= max(1, pity_threshold - 1)
            ]
            if near_events:
                tips.append(f"{activity['name']}接近保底：{near_events[0]}")
                break
    finally:
        conn.close()

    if not tips:
        return
    lines.append("")
    lines.append("【行动建议】")
    lines.append("；".join(tips[:4]))
    lines.append("一键领取：活动领取")


def build_activity_rewards_text() -> str:
    cfg = load_config()
    lines = [f"【{cfg.get('name', '节日签到活动')} · 奖励】"]
    lines.append("")
    lines.append("【每日签到奖励】")
    rewards = cfg.get("daily_rewards") or []
    if rewards:
        for reward in sorted(rewards, key=lambda item: _as_int(item.get("day"))):
            day = _as_int(reward.get("day"))
            name = str(reward.get("name") or f"第{day}天")
            lines.append(f"- 第{day}天 {name}：{reward.get('reward') or '暂无奖励'}")
    else:
        lines.append("暂无每日奖励配置")

    milestones = cfg.get("milestone_rewards") or []
    if milestones:
        lines.append("")
        lines.append("【累计签到奖励】")
        for reward in sorted(milestones, key=lambda item: _as_int(item.get("days"))):
            days = _as_int(reward.get("days"))
            name = str(reward.get("name") or f"累计{days}天")
            lines.append(f"- {name}：{reward.get('reward') or '暂无奖励'}")
    return "\n".join(lines).strip()


def build_activity_tasks_text() -> str:
    cfg = load_config()
    lines = [f"【{cfg.get('name', '节日签到活动')} · 任务】"]
    daily_tasks = cfg.get("daily_tasks") or []
    if daily_tasks:
        lines.append("")
        lines.append("【每日活动任务】")
        for task in daily_tasks:
            if isinstance(task, dict):
                lines.append(_format_activity_task(task))
    else:
        lines.append("")
        lines.append("暂无每日活动任务")

    weekly_tasks = cfg.get("weekly_tasks") or []
    if weekly_tasks:
        lines.append("")
        lines.append("【周常活动任务】")
        for task in weekly_tasks:
            if isinstance(task, dict):
                lines.append(_format_activity_task(task))
    _append_pass_summary(lines, cfg, detail=True)
    return "\n".join(lines).strip()


def build_activity_gameplay_text() -> str:
    cfg = load_config()
    lines = [f"【{cfg.get('name', '节日签到活动')} · 玩法】"]
    _append_stage_summary(lines, cfg, detail=True)
    _append_gameplay_summary(lines, cfg, detail=True)
    if len(lines) <= 1:
        lines.append("")
        lines.append("暂无玩法活动")
    return "\n".join(lines).strip()


def build_activity_info(user_id: str | None = None) -> str:
    cfg = load_config()
    ok, reason = activity_state(cfg)
    lines = [
        f"【{cfg.get('name', '节日签到活动')}】",
        str(cfg.get("description") or ""),
        f"状态：{'进行中' if ok else reason}",
        f"活动时间：{cfg.get('start_time', '0')} 至 {cfg.get('end_time', '无限')}",
        f"签到命令：{cfg.get('sign_command', '活动签到')}",
    ]
    if user_id:
        user = get_user_sign(str(user_id))
        lines.extend([
            "",
            f"我的累计签到：{int(user.get('sign_days', 0) or 0)} 天",
            f"上次签到：{user.get('last_sign_date') or '暂无'}",
        ])

    if _activity_info_mode(cfg) == "full":
        _append_stage_summary(lines, cfg, detail=True)
        lines.append("")
        lines.append("【每日签到奖励】")
        rewards = cfg.get("daily_rewards") or []
        if rewards:
            for reward in sorted(rewards, key=lambda item: _as_int(item.get("day"))):
                day = _as_int(reward.get("day"))
                name = str(reward.get("name") or f"第{day}天")
                lines.append(f"- 第{day}天 {name}：{reward.get('reward') or '暂无奖励'}")
        else:
            lines.append("暂无每日奖励配置")

        milestones = cfg.get("milestone_rewards") or []
        if milestones:
            lines.append("")
            lines.append("【累计签到奖励】")
            for reward in sorted(milestones, key=lambda item: _as_int(item.get("days"))):
                days = _as_int(reward.get("days"))
                name = str(reward.get("name") or f"累计{days}天")
                lines.append(f"- {name}：{reward.get('reward') or '暂无奖励'}")

        daily_tasks = cfg.get("daily_tasks") or []
        if daily_tasks:
            lines.append("")
            lines.append("【每日活动任务】")
            for task in daily_tasks:
                if isinstance(task, dict):
                    lines.append(_format_activity_task(task))

        weekly_tasks = cfg.get("weekly_tasks") or []
        if weekly_tasks:
            lines.append("")
            lines.append("【周常活动任务】")
            for task in weekly_tasks:
                if isinstance(task, dict):
                    lines.append(_format_activity_task(task))

        _append_gameplay_summary(lines, cfg, detail=True)
        _append_task_summary(lines, cfg, user_id, detail=True)
        _append_pass_summary(lines, cfg, user_id, detail=True)
        _append_action_summary(lines, cfg, user_id)
    else:
        lines.extend([
            "",
            "查询奖励：活动奖励",
            "查询任务：活动任务",
            "领取任务：活动任务领取",
            "活动战令：活动战令 / 活动战令领取",
            "玩法说明：活动玩法",
            "个人进度：活动排行 / 活动背包 / 活动积分",
        ])
        _append_action_summary(lines, cfg, user_id)
        _append_stage_summary(lines, cfg, detail=False)
        _append_task_summary(lines, cfg, user_id, detail=False)
        _append_pass_summary(lines, cfg, user_id, detail=False)
        _append_gameplay_summary(lines, cfg, detail=False)

    return "\n".join(lines).strip()


def get_rank(limit: int = 10) -> list[dict]:
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, sign_days, total_sign_days, last_sign_date
            FROM activity_user
            ORDER BY sign_days DESC, total_sign_days DESC, last_sign_date ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = [dict(row) for row in cur.fetchall()]
        return _attach_display_names(rows)
    finally:
        conn.close()


def build_rank_text(limit: int = 10) -> str:
    cfg = load_config()
    rows = get_rank(limit)
    lines = [f"【{cfg.get('name', '节日签到活动')}排行】"]
    if not rows:
        lines.append("暂无排行数据")
        return "\n".join(lines)

    for index, row in enumerate(rows, 1):
        name = row.get("display_name") or row.get("user_name") or resolve_daohao(str(row.get("user_id") or ""))
        lines.append(
            f"{index}. {name} 累计签到 "
            f"{int(row.get('sign_days', 0) or 0)} 天"
        )
    return "\n".join(lines)


def set_enabled(enabled: bool, target: str | None = None) -> str:
    cfg = load_config()
    target_text = _clean_text(target)
    action_text = "开启" if enabled else "关闭"
    if not target_text:
        cfg["enabled"] = bool(enabled)
        save_config(cfg)
        return f"已{action_text}签到活动"

    if target_text in {"全部", "所有", "all", "ALL"}:
        cfg["enabled"] = bool(enabled)
        for activity in cfg.get("gameplay_activities") or []:
            if isinstance(activity, dict):
                activity["enabled"] = bool(enabled)
        extensions = cfg.setdefault("extensions", {})
        if isinstance(extensions, dict):
            activity_pass = extensions.setdefault("activity_pass", deepcopy(DEFAULT_ACTIVITY_PASS))
            if isinstance(activity_pass, dict):
                activity_pass["enabled"] = bool(enabled)
        save_config(cfg)
        return f"已{action_text}全部活动"

    if target_text in {"签到", "节日签到", "签到活动"}:
        cfg["enabled"] = bool(enabled)
        save_config(cfg)
        return f"已{action_text}签到活动"

    if target_text in {"玩法", "玩法活动"}:
        changed_count = 0
        for activity in cfg.get("gameplay_activities") or []:
            if isinstance(activity, dict):
                activity["enabled"] = bool(enabled)
                changed_count += 1
        if changed_count:
            save_config(cfg)
            return f"已{action_text}{changed_count}个玩法活动"
        return "当前没有配置玩法活动"

    if target_text in {"战令", "活动战令", "通行证", "活动通行证", "活跃"}:
        extensions = cfg.setdefault("extensions", {})
        if not isinstance(extensions, dict):
            extensions = {}
            cfg["extensions"] = extensions
        activity_pass = extensions.setdefault("activity_pass", deepcopy(DEFAULT_ACTIVITY_PASS))
        if not isinstance(activity_pass, dict):
            activity_pass = deepcopy(DEFAULT_ACTIVITY_PASS)
            extensions["activity_pass"] = activity_pass
        activity_pass["enabled"] = bool(enabled)
        save_config(cfg)
        return f"已{action_text}活动战令"

    type_targets = {
        "集字": "collect_words",
        "集字活动": "collect_words",
        "积分": "event_points",
        "积分活动": "event_points",
        "活动积分": "event_points",
        "活动商店": "event_points",
        "首领": "activity_boss",
        "活动首领": "activity_boss",
        "BOSS": "activity_boss",
        "boss": "activity_boss",
    }
    if target_text in type_targets:
        target_type = type_targets[target_text]
        changed_count = 0
        for activity in cfg.get("gameplay_activities") or []:
            if isinstance(activity, dict) and _clean_text(activity.get("type"), "collect_words") == target_type:
                activity["enabled"] = bool(enabled)
                changed_count += 1
        if changed_count:
            save_config(cfg)
            return f"已{action_text}{changed_count}个{target_text}"
        return f"当前没有配置{target_text}"

    changed = False
    for activity in cfg.get("gameplay_activities") or []:
        if not isinstance(activity, dict):
            continue
        names = {
            _clean_text(activity.get("key")),
            _clean_text(activity.get("name")),
            _clean_text(activity.get("template_key")),
            _clean_text(activity.get("type")),
        }
        if target_text in names:
            activity["enabled"] = bool(enabled)
            changed = True
            break

    if not changed:
        return f"未找到活动：{target_text}"
    save_config(cfg)
    return f"已{action_text}{target_text}"


ensure_activity_files()
