from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import queue
import threading
from typing import Any

from nonebot.log import logger

from . import db_backend


MESSAGE_DB = Path() / "message.db"
MESSAGE_DB_CONFIG_FILE = Path() / "data" / "xiuxian" / "message_db_config.json"
_last_message_db_cleanup_ts = 0.0
_message_db_initialized = False
_message_db_init_lock = threading.RLock()
_message_db_writer_started = False
_message_db_writer_lock = threading.RLock()
_message_db_config_lock = threading.RLock()


def _env_int(name: str, default: int, minimum: int) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except Exception:
        return default


_message_db_jobs: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue(
    maxsize=_env_int("XIUXIAN_MESSAGE_DB_QUEUE_MAXSIZE", 100000, 1000)
)
_message_db_last_drop_log_ts = 0.0
_message_db_dropped_jobs = 0
_MESSAGE_DB_BATCH_SIZE = _env_int("XIUXIAN_MESSAGE_DB_BATCH_SIZE", 200, 1)

message_db_max_size_mb = _env_int("XIUXIAN_MESSAGE_DB_MAX_SIZE_MB", 1000, 0)
# 消息记录最大大小 MB。达到或超过该值时，按最早日期清理聊天记录。
# 设置为 0 时关闭消息记录写入。

message_group_keep_days = _env_int("XIUXIAN_MESSAGE_GROUP_KEEP_DAYS", 0, 0)
# 群聊消息最大保留天数，0 表示不启用。

message_private_keep_days = _env_int("XIUXIAN_MESSAGE_PRIVATE_KEEP_DAYS", 0, 0)
# 私聊消息最大保留天数，0 表示不启用。


def _int_config(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        return min(maximum, max(minimum, int(value)))
    except Exception:
        return default


def load_message_db_config() -> dict[str, int]:
    global message_db_max_size_mb, message_group_keep_days, message_private_keep_days

    with _message_db_config_lock:
        if MESSAGE_DB_CONFIG_FILE.exists():
            try:
                with MESSAGE_DB_CONFIG_FILE.open("r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                logger.warning(f"[message.db] 读取配置失败: {e}")
                data = {}
        else:
            data = {}

        message_db_max_size_mb = _int_config(
            data.get("message_db_max_size_mb", message_db_max_size_mb),
            message_db_max_size_mb,
            0,
            10000,
        )
        message_group_keep_days = _int_config(
            data.get("message_group_keep_days", message_group_keep_days),
            message_group_keep_days,
            0,
            36500,
        )
        message_private_keep_days = _int_config(
            data.get("message_private_keep_days", message_private_keep_days),
            message_private_keep_days,
            0,
            36500,
        )

        return get_message_db_config(load_from_disk=False)


def get_message_db_config(*, load_from_disk: bool = True) -> dict[str, int]:
    if load_from_disk:
        return load_message_db_config()

    return {
        "message_db_max_size_mb": int(message_db_max_size_mb),
        "message_group_keep_days": int(message_group_keep_days),
        "message_private_keep_days": int(message_private_keep_days),
    }


def save_message_db_config(config: dict[str, int]) -> dict[str, int]:
    MESSAGE_DB_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with MESSAGE_DB_CONFIG_FILE.open("w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return config


def update_message_db_config(values: dict[str, Any]) -> dict[str, int]:
    global message_db_max_size_mb, message_group_keep_days, message_private_keep_days

    with _message_db_config_lock:
        current = get_message_db_config(load_from_disk=True)
        current.update(values)

        message_db_max_size_mb = _int_config(
            current.get("message_db_max_size_mb"),
            message_db_max_size_mb,
            0,
            10000,
        )
        message_group_keep_days = _int_config(
            current.get("message_group_keep_days"),
            message_group_keep_days,
            0,
            36500,
        )
        message_private_keep_days = _int_config(
            current.get("message_private_keep_days"),
            message_private_keep_days,
            0,
            36500,
        )

        config = get_message_db_config(load_from_disk=False)
        return save_message_db_config(config)


def is_message_record_enabled() -> bool:
    return int(message_db_max_size_mb) > 0


load_message_db_config()


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _messages_table_sql() -> str:
    return """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            adapter TEXT,
            bot_id TEXT,

            direction TEXT NOT NULL,
            scene TEXT NOT NULL,

            message_id TEXT,
            source_message_id TEXT,

            group_id TEXT,
            group_name TEXT,

            user_id TEXT,
            username TEXT,
            nickname TEXT,
            avatar TEXT,

            content TEXT,

            reply_used_count NUMERIC DEFAULT 0,

            created_at TEXT NOT NULL
        )
    """


def _ensure_message_db_schema(conn):
    cur = conn.cursor()

    cur.execute(_messages_table_sql())

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_nicknames (
            user_id TEXT PRIMARY KEY,

            username TEXT NOT NULL,

            adapter TEXT,
            bot_id TEXT,

            source_scene TEXT,
            source_group_id TEXT,

            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_nicknames_username ON user_nicknames(username)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_nicknames_source_group_id ON user_nicknames(source_group_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_direction ON messages(direction)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_scene ON messages(scene)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_source_message_id ON messages(source_message_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_group_id ON messages(group_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at)")


def connect_message_db(row_factory: bool = False):
    init_message_db()
    conn = db_backend.connect(MESSAGE_DB)
    if row_factory:
        conn.row_factory = db_backend.Row
    try:
        _ensure_message_db_schema(conn)
        conn.commit()
    except Exception:
        conn.close()
        raise
    return conn


def _start_message_db_writer():
    global _message_db_writer_started

    if _message_db_writer_started:
        return

    with _message_db_writer_lock:
        if _message_db_writer_started:
            return

        worker = threading.Thread(
            target=_message_db_writer_loop,
            name="xiuxian-message-db-writer",
            daemon=True,
        )
        worker.start()
        _message_db_writer_started = True


def init_message_db():
    global _message_db_initialized

    if _message_db_initialized:
        _start_message_db_writer()
        return

    with _message_db_init_lock:
        if _message_db_initialized:
            _start_message_db_writer()
            return

        MESSAGE_DB.parent.mkdir(parents=True, exist_ok=True)

        conn = db_backend.connect(MESSAGE_DB)
        try:
            _ensure_message_db_schema(conn)
            conn.commit()
            _message_db_initialized = True
        finally:
            conn.close()

    _start_message_db_writer()


def get_message_db_path() -> Path:
    return MESSAGE_DB


def _get_message_cleanup_config() -> tuple[int, int, int]:
    max_size_mb = 0 if message_db_max_size_mb <= 0 else max(100, min(10000, message_db_max_size_mb))
    group_keep_days = max(0, message_group_keep_days)
    private_keep_days = max(0, message_private_keep_days)

    return max_size_mb, group_keep_days, private_keep_days


def _message_db_size_mb() -> float:
    try:
        if not MESSAGE_DB.exists():
            return 0.0
        return MESSAGE_DB.stat().st_size / 1024 / 1024
    except Exception:
        return 0.0


def _vacuum_message_db(conn):
    try:
        conn.execute("VACUUM")
    except Exception as e:
        logger.debug(f"[message.db] VACUUM失败: {e}")


def _cleanup_message_db_by_size(conn, max_size_mb: int) -> int:
    return 0


def _cleanup_message_db_by_keep_days(conn, group_keep_days: int, private_keep_days: int) -> int:
    deleted_total = 0
    cur = conn.cursor()

    if group_keep_days > 0:
        cutoff = (datetime.now() - timedelta(days=group_keep_days)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """
            DELETE FROM messages
            WHERE scene IN ('group', 'channel_group')
              AND created_at < %s
            """,
            (cutoff,),
        )
        deleted = cur.rowcount or 0
        deleted_total += deleted
        if deleted > 0:
            logger.info(f"[message.db] 已按群聊保留天数 {group_keep_days} 天清理消息 {deleted} 条")

    if private_keep_days > 0:
        cutoff = (datetime.now() - timedelta(days=private_keep_days)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """
            DELETE FROM messages
            WHERE scene IN ('private', 'channel_private')
              AND created_at < %s
            """,
            (cutoff,),
        )
        deleted = cur.rowcount or 0
        deleted_total += deleted
        if deleted > 0:
            logger.info(f"[message.db] 已按私聊保留天数 {private_keep_days} 天清理消息 {deleted} 条")

    if deleted_total > 0:
        conn.commit()
        _vacuum_message_db(conn)

    return deleted_total


def _cleanup_message_db_with_conn(conn):
    max_size_mb, group_keep_days, private_keep_days = _get_message_cleanup_config()
    _cleanup_message_db_by_size(conn, max_size_mb)
    _cleanup_message_db_by_keep_days(conn, group_keep_days, private_keep_days)


def maybe_cleanup_message_db(conn=None):
    global _last_message_db_cleanup_ts

    now_ts = datetime.now().timestamp()
    if now_ts - _last_message_db_cleanup_ts < 600:
        return

    _last_message_db_cleanup_ts = now_ts

    try:
        init_message_db()

        if conn is not None:
            _cleanup_message_db_with_conn(conn)
            return

        conn = db_backend.connect(MESSAGE_DB)
        try:
            _cleanup_message_db_with_conn(conn)
        finally:
            conn.close()

    except Exception as e:
        logger.warning(f"[message.db] 自动清理失败: {e}")


def _enqueue_message_db_job(kind: str, payload: dict[str, Any]):
    global _message_db_dropped_jobs, _message_db_last_drop_log_ts

    if not is_message_record_enabled():
        return

    try:
        init_message_db()
        _message_db_jobs.put_nowait((kind, payload))
    except queue.Full:
        _message_db_dropped_jobs += 1
        now_ts = datetime.now().timestamp()
        if now_ts - _message_db_last_drop_log_ts >= 10:
            _message_db_last_drop_log_ts = now_ts
            logger.warning(f"[message.db] 写入队列已满，已丢弃 {_message_db_dropped_jobs} 条记录")
    except Exception as e:
        logger.warning(f"[message.db] 写入队列失败: {e}")


def _execute_insert_message_record(cur, payload: dict[str, Any]):
    cur.execute(
        """
        INSERT INTO messages (
            adapter, bot_id,
            direction, scene,
            message_id, source_message_id,
            group_id, group_name,
            user_id, username, nickname, avatar,
            content,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(payload.get("adapter") or ""),
            str(payload.get("bot_id") or ""),
            str(payload.get("direction") or ""),
            str(payload.get("scene") or "unknown"),
            str(payload.get("message_id") or ""),
            str(payload.get("source_message_id") or ""),
            str(payload.get("group_id") or ""),
            str(payload.get("group_name") or ""),
            str(payload.get("user_id") or ""),
            str(payload.get("username") or ""),
            str(payload.get("nickname") or ""),
            str(payload.get("avatar") or ""),
            str(payload.get("content") or ""),
            str(payload.get("created_at") or _now_str()),
        ),
    )


def _execute_increase_recv_reply_used_count(cur, payload: dict[str, Any]):
    source_message_id = str(payload.get("source_message_id") or "")
    if not source_message_id:
        return

    where = [
        "direction = 'recv'",
        "message_id = %s",
    ]
    params: list[Any] = [source_message_id]

    for field in ("adapter", "bot_id", "scene", "group_id", "user_id"):
        value = str(payload.get(field) or "")
        if value:
            where.append(f"{field} = %s")
            params.append(value)

    sql = f"""
        UPDATE messages
        SET reply_used_count = COALESCE(reply_used_count, 0) + 1
        WHERE {' AND '.join(where)}
    """
    cur.execute(sql, params)

    if cur.rowcount == 0:
        fallback_where = [
            "direction = 'recv'",
            "message_id = %s",
        ]
        fallback_params: list[Any] = [source_message_id]

        for field in ("adapter", "bot_id"):
            value = str(payload.get(field) or "")
            if value:
                fallback_where.append(f"{field} = %s")
                fallback_params.append(value)

        fallback_sql = f"""
            UPDATE messages
            SET reply_used_count = COALESCE(reply_used_count, 0) + 1
            WHERE {' AND '.join(fallback_where)}
        """
        cur.execute(fallback_sql, fallback_params)


def _execute_record_group_user_nickname(cur, payload: dict[str, Any]):
    scene = str(payload.get("scene") or "")
    if scene not in ("group", "channel_group"):
        return

    user_id = str(payload.get("user_id") or "").strip()
    username = str(payload.get("username") or "").strip()
    if not user_id or not username:
        return

    now = str(payload.get("now") or _now_str())
    params = (
        user_id,
        username,
        str(payload.get("adapter") or ""),
        str(payload.get("bot_id") or ""),
        scene,
        str(payload.get("group_id") or ""),
        now,
        now,
    )
    cur.execute(
        """
        INSERT INTO user_nicknames (
            user_id,
            username,
            adapter,
            bot_id,
            source_scene,
            source_group_id,
            first_seen_at,
            last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING
        """,
        params,
    )


def _execute_message_db_job(cur, kind: str, payload: dict[str, Any]):
    if kind == "insert":
        _execute_insert_message_record(cur, payload)
    elif kind == "increase_reply":
        _execute_increase_recv_reply_used_count(cur, payload)
    elif kind == "nickname":
        _execute_record_group_user_nickname(cur, payload)


def _message_db_writer_loop():
    conn = None

    while True:
        kind, payload = _message_db_jobs.get()
        jobs = [(kind, payload)]

        while len(jobs) < _MESSAGE_DB_BATCH_SIZE:
            try:
                jobs.append(_message_db_jobs.get_nowait())
            except queue.Empty:
                break

        try:
            if not is_message_record_enabled():
                continue

            if conn is None:
                conn = db_backend.connect(MESSAGE_DB)
                _ensure_message_db_schema(conn)
                conn.commit()

            cur = conn.cursor()
            for job_kind, job_payload in jobs:
                _execute_message_db_job(cur, job_kind, job_payload)
            conn.commit()
            maybe_cleanup_message_db(conn)

        except Exception as e:
            logger.warning(f"[message.db] 后台写入失败: {e}")
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None

        finally:
            for _ in jobs:
                _message_db_jobs.task_done()


def insert_message_record(
    *,
    adapter: str = "",
    bot_id: str = "",
    direction: str,
    scene: str,
    message_id: str = "",
    source_message_id: str = "",
    group_id: str = "",
    group_name: str = "",
    user_id: str = "",
    username: str = "",
    nickname: str = "",
    avatar: str = "",
    content: str = "",
):
    _enqueue_message_db_job(
        "insert",
        {
            "adapter": adapter,
            "bot_id": bot_id,
            "direction": direction,
            "scene": scene,
            "message_id": message_id,
            "source_message_id": source_message_id,
            "group_id": group_id,
            "group_name": group_name,
            "user_id": user_id,
            "username": username,
            "nickname": nickname,
            "avatar": avatar,
            "content": content,
            "created_at": _now_str(),
        },
    )


def increase_recv_reply_used_count(
    *,
    source_message_id: str,
    adapter: str = "",
    bot_id: str = "",
    scene: str = "",
    group_id: str = "",
    user_id: str = "",
):
    if not source_message_id:
        return

    _enqueue_message_db_job(
        "increase_reply",
        {
            "source_message_id": source_message_id,
            "adapter": adapter,
            "bot_id": bot_id,
            "scene": scene,
            "group_id": group_id,
            "user_id": user_id,
        },
    )


def record_group_user_nickname(
    *,
    adapter: str = "",
    bot_id: str = "",
    scene: str = "",
    group_id: str = "",
    user_id: str = "",
    username: str = "",
):
    if scene not in ("group", "channel_group"):
        return

    user_id = str(user_id or "").strip()
    username = str(username or "").strip()
    if not user_id or not username:
        return

    _enqueue_message_db_job(
        "nickname",
        {
            "adapter": adapter,
            "bot_id": bot_id,
            "scene": scene,
            "group_id": group_id,
            "user_id": user_id,
            "username": username,
            "now": _now_str(),
        },
    )
