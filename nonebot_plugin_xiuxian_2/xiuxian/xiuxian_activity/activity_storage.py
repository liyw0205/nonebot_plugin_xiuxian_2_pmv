import shutil
from datetime import datetime
from pathlib import Path

from nonebot.log import logger
from nonebot_plugin_xiuxian_2.paths import get_paths

from ..xiuxian_utils import db_backend
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from .activity_config import DATE_FMT, DEFAULT_CONFIG_PATH, TIME_FMT
from .activity_utils import _clean_text

_sql_message = XiuxianDateManage()


BASE_DIR = get_paths().data / "activity"
CONFIG_PATH = BASE_DIR / "activity_config.json"
DB_PATH = BASE_DIR / "activity.db"

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


__all__ = [
    name for name in globals()
    if name.isupper()
    or name in {"now_dt", "today_str", "now_str", "ensure_activity_files", "init_db", "resolve_daohao", "resolve_daohao_batch"}
    or (name.startswith("_") and not name.startswith("__"))
]
