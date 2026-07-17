from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Callable

from nonebot.log import logger
from ...paths import get_paths

from . import db_backend


CORE_DB = get_paths().game_db
MIGRATION_TABLE = "xiuxian_schema_migrations"
Migration = Callable[[db_backend.SQLiteConnection], None]


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_migration_table(conn):
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db_backend.quote_ident(MIGRATION_TABLE)} (
            version TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )


def _migration_applied(conn, version: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        f"SELECT 1 FROM {db_backend.quote_ident(MIGRATION_TABLE)} WHERE version = %s LIMIT 1",
        (version,),
    )
    return cur.fetchone() is not None


def _mark_migration_applied(conn, version: str):
    conn.execute(
        f"""
        INSERT OR IGNORE INTO {db_backend.quote_ident(MIGRATION_TABLE)} (version, applied_at)
        VALUES (%s, %s)
        """,
        (version, _now_str()),
    )


def _create_index(conn, index_name: str, table_name: str, columns: list[str]):
    if not conn.table_exists(table_name):
        return

    available = {name.lower() for name in conn.column_names(table_name)}
    if not all(column.lower() in available for column in columns):
        return

    column_sql = ", ".join(db_backend.quote_ident(column) for column in columns)
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {db_backend.quote_ident(index_name)}
        ON {db_backend.quote_ident(table_name)} ({column_sql})
        """
    )


def _ensure_core_indexes(conn):
    """热点表索引：幂等执行，不依赖迁移版本。"""
    for index_name, table_name, columns in (
        ("idx_user_xiuxian_user_id", "user_xiuxian", ["user_id"]),
        ("idx_user_xiuxian_user_name", "user_xiuxian", ["user_name"]),
        ("idx_user_xiuxian_sect_id", "user_xiuxian", ["sect_id"]),
        ("idx_user_cd_user_id", "user_cd", ["user_id"]),
        ("idx_user_cd_create_time", "user_cd", ["create_time"]),
        ("idx_back_goods_id", "back", ["goods_id"]),
        ("idx_sects_owner", "sects", ["sect_owner"]),
        ("idx_buffinfo_user_id", "buffinfo", ["user_id"]),
    ):
        _create_index(conn, index_name, table_name, columns)

    # user_id 唯一：防止身外化身/伪装/并发注册写出重复 openid 行
    if conn.table_exists("user_xiuxian"):
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_user_xiuxian_user_id_unique
            ON user_xiuxian (user_id)
            """
        )


def _migration_20260717_0001_dedupe_user_xiuxian(conn):
    """
    拆散 user_xiuxian 中重复的 user_id 行，再依赖唯一索引防复发。

    策略：同一 user_id 保留 id 最小（最早）的一行；
    其余行改写为 `{user_id}__dup{id}`，保留角色数据便于人工合并。
    身外化身本身使用独立 avatar_id，不会产生同 openid 双行；
    重复行通常来自：并发注册、ID 迁移碰撞、或脏写入。
    """
    if not conn.table_exists("user_xiuxian"):
        return

    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, COUNT(*) AS c
        FROM user_xiuxian
        GROUP BY user_id
        HAVING COUNT(*) > 1
        """
    )
    dups = cur.fetchall() or []
    for user_id, _count in dups:
        uid = str(user_id)
        cur.execute(
            "SELECT id FROM user_xiuxian WHERE user_id=%s ORDER BY id ASC",
            (uid,),
        )
        ids = [int(row[0]) for row in (cur.fetchall() or [])]
        if len(ids) <= 1:
            continue
        keep_id = ids[0]
        for rid in ids[1:]:
            new_uid = f"{uid}__dup{rid}"
            # 避免二次冲突
            n = 0
            candidate = new_uid
            while True:
                cur.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1",
                    (candidate,),
                )
                if cur.fetchone() is None:
                    break
                n += 1
                candidate = f"{new_uid}_{n}"
            cur.execute(
                "UPDATE user_xiuxian SET user_id=%s WHERE id=%s",
                (candidate, rid),
            )
            logger.warning(
                f"[xiuxian-db] 拆散重复 user_id：保留 id={keep_id} 的 {uid}，"
                f"将 id={rid} 改写为 {candidate}"
            )


def _migration_20260707_0001_runtime_marker(conn):
    """建立迁移基线；实际热点索引由 _ensure_core_indexes 幂等维护。"""
    _ensure_core_indexes(conn)


CORE_MIGRATIONS: list[tuple[str, Migration]] = [
    ("20260707_0001_runtime_marker", _migration_20260707_0001_runtime_marker),
    ("20260717_0001_dedupe_user_xiuxian", _migration_20260717_0001_dedupe_user_xiuxian),
]


def run_core_migrations(database: str | Path = CORE_DB) -> list[str]:
    applied: list[str] = []

    with db_backend.transaction(database) as conn:
        _ensure_migration_table(conn)

        for version, migration in CORE_MIGRATIONS:
            if _migration_applied(conn, version):
                continue
            migration(conn)
            _mark_migration_applied(conn, version)
            applied.append(version)

        _ensure_core_indexes(conn)

    if applied:
        logger.info(f"[xiuxian-db] 已应用数据库迁移：{', '.join(applied)}")
    return applied
