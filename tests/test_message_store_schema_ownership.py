"""消息库（message_db）schema 归属与 fail-closed 合同。

审计结论：`messages`、`user_nicknames`、`season_rank` 三张表此前只由请求路径创建 ——
`message_db._ensure_message_db_schema` 在每次连接消息库时建 `messages` + `user_nicknames`
+ 15 个索引并对历史库补 `messages.reference_id`；`season_rank_service.ensure_season_rank_table`
在赛季榜每次读/写前建表。两者都没有启动迁移，`message_db` 库此前只路由 `platform.001`。

`message.001` 现在是唯一创建者；请求路径只校验形状：

- `message_db._ensure_message_db_schema`：未就绪 → `RuntimeError`（不建表）
- `season_rank_service.ensure_season_rank_table()`：未就绪 → `False`，
  写路径返回 `None`、读路径返回 `[]`/`None`（不建表）
"""

from __future__ import annotations

import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from nonebot import init as nonebot_init


nonebot_init()

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, message_store
from nonebot_plugin_xiuxian_2.infrastructure.database.message_store import (
    MESSAGES_COLUMNS,
    MESSAGE_TABLES,
    SEASON_RANK_COLUMNS,
    apply_message_store_schema,
    apply_season_rank_schema,
    schema_ready,
)
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend, message_db, season_rank_service

ROOT = Path(__file__).resolve().parents[1]
MESSAGE_DB_SOURCE = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/message_db.py"
SEASON_RANK_SOURCE = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/season_rank_service.py"


class _SqlMessage:
    """Minimal stand-in for the shared message-DB manager used by the service."""

    def __init__(self, database: Path) -> None:
        self.conn = db_backend.connect(database, check_same_thread=False)
        self.lock = threading.RLock()

    def _commit_write(self) -> None:
        self.conn.commit()

    def _read_query(self, sql, params=None, *, one=False, dict_row=False, cache_key=None, cache_ttl=None):
        cur = self.conn.cursor()
        cur.execute(sql, params or ())
        names = [description[0] for description in (cur.description or ())]

        def as_dict(row):
            return dict(zip(names, row))

        if one:
            row = cur.fetchone()
            if row is None:
                return None
            return as_dict(row) if dict_row else row
        rows = cur.fetchall()
        return [as_dict(row) for row in rows] if dict_row else list(rows)

    def close(self) -> None:
        self.conn.close()


def _tables(path: Path) -> set[str]:
    with db_backend.connection(path) as conn:
        return {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }


class MigrationRoutingTests(unittest.TestCase):
    def test_message_schema_routes_only_to_the_message_database(self) -> None:
        migrations = build_migrations()
        message_db_versions = {
            item.version for item in migrations_for_database(migrations, "message_db")
        }
        self.assertIn("message.001", message_db_versions)
        self.assertNotIn("season_rank.001", message_db_versions)
        game_versions = {item.version for item in migrations_for_database(migrations, "game_db")}
        self.assertIn("season_rank.001", game_versions)
        self.assertNotIn("message.001", game_versions)
        for key in ("game_db", "player_db", "trade_db", "impart_db"):
            versions = {item.version for item in migrations_for_database(migrations, key)}
            self.assertNotIn("message.001", versions, key)
        for key in ("player_db", "trade_db", "impart_db", "message_db"):
            versions = {item.version for item in migrations_for_database(migrations, key)}
            self.assertNotIn("season_rank.001", versions, key)


class MigrationOwnershipTests(unittest.TestCase):
    def test_migration_creates_every_message_table_and_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "message.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_message_store_schema(uow)
            with db_backend.connection(database) as conn:
                tables = {
                    str(row[0])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
                indexes = {
                    str(row[0])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
                }
            self.assertTrue({"messages", "user_nicknames"} <= tables)
            self.assertNotIn("season_rank", tables)
            self.assertIn("idx_messages_group_latest", indexes)
            self.assertIn("idx_user_nicknames_username", indexes)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                for table, required in MESSAGE_TABLES.items():
                    self.assertTrue(message_store.table_ready(uow, table, required), table)

    def test_season_rank_migration_belongs_to_the_game_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_season_rank_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                self.assertTrue(message_store.table_ready(uow, "season_rank", SEASON_RANK_COLUMNS))
                self.assertFalse(message_store.table_ready(uow, "messages", MESSAGES_COLUMNS))

    def test_migration_is_idempotent_and_keeps_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "message.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_message_store_schema(uow)
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "INSERT INTO messages (direction, scene, content, created_at) "
                    "VALUES ('recv', 'group', 'hello', '2026-09-22 10:00:00')"
                )
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                before = {table: message_store.table_columns(uow, table) for table in MESSAGE_TABLES}
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_message_store_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                after = {table: message_store.table_columns(uow, table) for table in MESSAGE_TABLES}
                row = uow.query_one("SELECT content FROM messages")
            self.assertEqual(before, after)
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row["content"], "hello")

    def test_migration_backfills_reference_id_on_legacy_messages(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "message.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE messages ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT,adapter TEXT,bot_id TEXT,"
                    "direction TEXT NOT NULL,scene TEXT NOT NULL,message_id TEXT,"
                    "source_message_id TEXT,group_id TEXT,group_name TEXT,user_id TEXT,"
                    "username TEXT,nickname TEXT,avatar TEXT,content TEXT,"
                    "reply_used_count NUMERIC DEFAULT 0,created_at TEXT NOT NULL)"
                )
                conn.execute(
                    "INSERT INTO messages (direction, scene, content, created_at) "
                    "VALUES ('recv', 'group', 'legacy', '2026-09-22 10:00:00')"
                )
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_message_store_schema(uow)
            with DatabaseUnitOfWork(str(database), read_only=True) as uow:
                self.assertTrue(message_store.table_ready(uow, "messages", MESSAGES_COLUMNS))
                row = uow.query_one("SELECT content, reference_id FROM messages")
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row["content"], "legacy")
            self.assertIsNone(row["reference_id"])

    def test_schema_ready_reports_each_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "message.db"
            with db_backend.connection(database) as conn:
                self.assertFalse(schema_ready(conn))
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_message_store_schema(uow)
            with db_backend.connection(database) as conn:
                self.assertTrue(schema_ready(conn))


class NoRequestPathDdlTests(unittest.TestCase):
    def test_message_modules_never_create_or_alter_tables(self) -> None:
        for path in (MESSAGE_DB_SOURCE, SEASON_RANK_SOURCE):
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("CREATE TABLE", source, path.name)
            self.assertNotIn("CREATE INDEX", source, path.name)
            self.assertNotIn("ALTER TABLE", source, path.name)

    def test_message_db_verifier_uses_the_shared_contract(self) -> None:
        source = MESSAGE_DB_SOURCE.read_text(encoding="utf-8")
        self.assertIn("message_store.schema_ready", source)
        self.assertIn("请检查启动迁移", source)


class UnmigratedDegradationTests(unittest.TestCase):
    def test_connect_verifier_rejects_an_unmigrated_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "message.db"
            conn = db_backend.connect(database)
            try:
                with self.assertRaises(RuntimeError):
                    message_db._ensure_message_db_schema(conn)
            finally:
                conn.close()
            self.assertEqual(_tables(database), set(), "the verifier must not create tables")

    def test_season_rank_reads_and_writes_degrade_without_creating_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "message.db"
            with db_backend.connection(database):
                pass
            manager = _SqlMessage(database)
            now = datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc)
            try:
                with (
                    patch.object(season_rank_service, "_sql_message", return_value=manager),
                    patch(
                        "nonebot_plugin_xiuxian_2.paths.get_paths",
                        return_value=SimpleNamespace(game_db=database),
                    ),
                ):
                    self.assertFalse(season_rank_service.ensure_season_rank_table())
                    self.assertIsNone(
                        season_rank_service.add_season_rank_score(
                            rank_type="交易活跃", score=300, mode="monthly",
                            user_id="u1", now=now,
                        )
                    )
                    self.assertEqual(
                        season_rank_service.get_top_season_rank(
                            rank_type="交易活跃", mode="monthly", now=now,
                        ),
                        [],
                    )
                    self.assertIsNone(
                        season_rank_service.get_user_season_rank(
                            user_id="u1", rank_type="交易活跃", mode="monthly", now=now,
                        )
                    )
            finally:
                manager.close()
            self.assertEqual(_tables(database), set(), "reads/writes must not create tables")


class PostMigrationReadWriteTests(unittest.TestCase):
    def test_season_rank_writes_and_reads_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_season_rank_schema(uow)
            with db_backend.transaction(database) as conn:
                # 赛季榜读取会 LEFT JOIN 主库的 user_xiuxian / sects。
                conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_name TEXT)")
                conn.execute("CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, sect_name TEXT)")
                conn.execute("INSERT INTO user_xiuxian (user_id, user_name) VALUES ('u1', '道友甲')")
            manager = _SqlMessage(database)
            now = datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc)
            try:
                with (
                    patch.object(season_rank_service, "_sql_message", return_value=manager),
                    patch(
                        "nonebot_plugin_xiuxian_2.paths.get_paths",
                        return_value=SimpleNamespace(game_db=database),
                    ),
                ):
                    self.assertTrue(season_rank_service.ensure_season_rank_table())
                    stored = season_rank_service.add_season_rank_score(
                        rank_type="交易活跃", score=300, mode="monthly",
                        user_id="u1", now=now,
                    )
                    self.assertIsNotNone(stored)
                    assert stored is not None
                    self.assertEqual(stored["score"], 300)
                    top = season_rank_service.get_top_season_rank(
                        rank_type="交易活跃", mode="monthly", now=now,
                    )
                    self.assertEqual([row["user_id"] for row in top], ["u1"])
                    single = season_rank_service.get_user_season_rank(
                        user_id="u1", rank_type="交易活跃", mode="monthly", now=now,
                    )
                    self.assertIsNotNone(single)
                    assert single is not None
                    self.assertEqual(single["score"], 300)
            finally:
                manager.close()

    def test_messages_accept_writes_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "message.db"
            with DatabaseUnitOfWork(str(database), immediate=True) as uow:
                apply_message_store_schema(uow)
            conn = db_backend.connect(database)
            try:
                message_db._ensure_message_db_schema(conn)
                conn.execute(
                    "INSERT INTO messages (direction, scene, message_id, content, created_at) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    ("recv", "group", "m1", "hi", "2026-09-22 10:00:00"),
                )
                conn.commit()
                row = conn.execute("SELECT content FROM messages WHERE message_id='m1'").fetchone()
            finally:
                conn.close()
            self.assertEqual(row[0], "hi")


if __name__ == "__main__":
    unittest.main()
