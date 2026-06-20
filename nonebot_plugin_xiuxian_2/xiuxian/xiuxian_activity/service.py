import json
import shutil
from datetime import datetime
from pathlib import Path

from nonebot.log import logger

from ..xiuxian_compensation.common import get_item_list, send_reward_to_user
from ..xiuxian_utils import db_backend


BASE_DIR = Path() / "data" / "xiuxian" / "activity"
CONFIG_PATH = BASE_DIR / "activity_config.json"
DB_PATH = BASE_DIR / "activity.db"
DEFAULT_CONFIG_PATH = Path(__file__).parent / "activity_config.json"

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%Y-%m-%d %H:%M:%S"


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
        conn.commit()
    finally:
        conn.close()


def _load_default_config() -> dict:
    with open(DEFAULT_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _migrate_config(config: dict) -> tuple[dict, bool]:
    if config.get("template_type") == "festival_sign":
        return config, False

    default_config = _load_default_config()
    migrated = dict(default_config)
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
            "extra_rules",
            "extensions",
        ):
            if key in config:
                migrated[key] = config[key]
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


def _as_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get_extensions(config: dict) -> dict:
    extensions = config.get("extensions")
    return extensions if isinstance(extensions, dict) else {}


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
    ok, reason = activity_state(cfg)
    if not ok:
        return False, reason

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

    lines = [
        f"{cfg.get('festival_name', '节日')}签到成功",
        f"累计签到：{sign_days} 天",
        _format_reward_result("今日奖励", daily_reward_text, daily_msg),
    ]
    if milestone_reward_text or milestone_reward.get("name"):
        title = str(milestone_reward.get("name") or f"累计{sign_days}天奖励")
        lines.append(_format_reward_result(title, milestone_reward_text, milestone_msg))
    _finish_sign_log(uid, today, "success", "\n".join(lines))
    return True, "\n".join(lines)


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
        return [dict(row) for row in cur.fetchall()]
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
        lines.append(
            f"{index}. {row['user_id']} 累计签到 "
            f"{int(row.get('sign_days', 0) or 0)} 天"
        )
    return "\n".join(lines)


def set_enabled(enabled: bool):
    cfg = load_config()
    cfg["enabled"] = bool(enabled)
    save_config(cfg)


ensure_activity_files()
