import shutil
from datetime import datetime
from pathlib import Path

from nonebot.log import logger
from ...paths import get_paths
from ...infrastructure.database import DatabaseUnitOfWork

from ..xiuxian_utils import db_backend
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from .activity_config import DATE_FMT, DEFAULT_CONFIG_PATH, TIME_FMT
from .activity_utils import _clean_text

_sql_message_instance = None


def _sql_message():
    global _sql_message_instance
    if _sql_message_instance is None:
        _sql_message_instance = XiuxianDateManage()
    return _sql_message_instance


BASE_DIR = get_paths().data / "activity"
CONFIG_PATH = BASE_DIR / "activity_config.json"
LEGACY_DB_PATH = BASE_DIR / "activity.db"
DB_PATH = get_paths().game_db

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
    required = {
        table for table in (
            "activity_user", "activity_sign_log", "activity_collect_inventory",
            "activity_collect_claim", "activity_collect_drop_log", "activity_collect_pity_state",
            "activity_point_balance", "activity_point_event_log", "activity_point_purchase",
            "activity_task_progress", "activity_task_claim_log", "activity_pass_balance",
            "activity_pass_event_log", "activity_pass_reward_claim", "activity_item_inventory",
            "activity_boss_state", "activity_boss_damage", "activity_boss_fight_log",
            "activity_boss_milestone", "activity_boss_milestone_claim", "activity_boss_rank_claim",
        )
    }
    with DatabaseUnitOfWork(DB_PATH, read_only=True) as uow:
        existing = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
    missing = sorted(required - existing)
    if missing:
        raise RuntimeError(f"activity_state.001 schema_missing: {', '.join(missing)}")
    if "activity_event_operations" not in existing:
        raise RuntimeError("activity_state.003 schema_missing: activity_event_operations")


def resolve_daohao(user_id: str) -> str:
    uid = str(user_id or "").strip()
    if not uid:
        return "无名修士"
    try:
        row = _sql_message().get_user_info_with_id(uid)
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
        rows = _sql_message()._read_query(
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


__all__ = [
    name for name in globals()
    if name.isupper()
    or name in {"now_dt", "today_str", "now_str", "ensure_activity_files", "init_db", "resolve_daohao", "resolve_daohao_batch"}
    or (name.startswith("_") and not name.startswith("__"))
]
