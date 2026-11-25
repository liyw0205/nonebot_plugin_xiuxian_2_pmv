from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import season_rank_service
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import (
    process_read_cache,
)
from tests.test_db_backend import db_backend


class SeasonRankSnapshotCacheTests(unittest.TestCase):
    def test_schema_columns_are_cached_until_sqlite_schema_changes(self):
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "message.db"
            connection = db_backend.connect(database)
            self.addCleanup(connection.close)
            with db_backend.transaction(database) as writer:
                writer.execute(
                    "CREATE TABLE season_rank(rank_key TEXT,mode TEXT,period_key TEXT,"
                    "rank_type TEXT,user_id TEXT,sect_id INTEGER,score TEXT,extra TEXT,"
                    "updated_at TEXT)"
                )
            statements = []
            connection._raw.set_trace_callback(statements.append)
            manager = SimpleNamespace(
                database_path=database,
                conn=connection,
                lock=threading.RLock(),
            )

            with patch.object(season_rank_service, "_sql_message", return_value=manager):
                self.assertTrue(season_rank_service.ensure_season_rank_table())
                self.assertTrue(season_rank_service.ensure_season_rank_table())
                self.assertEqual(
                    1,
                    sum('table_info("season_rank")' in sql for sql in statements),
                )

                with db_backend.transaction(database) as writer:
                    writer.execute("DROP TABLE season_rank")
                    writer.execute("CREATE TABLE season_rank(rank_key TEXT)")
                self.assertFalse(season_rank_service.ensure_season_rank_table())

            self.assertEqual(
                2,
                sum('table_info("season_rank")' in sql for sql in statements),
            )

    def test_public_period_snapshot_is_shared_but_personal_rank_stays_live(self):
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE season_rank(rank_key TEXT,mode TEXT,period_key TEXT,"
                    "rank_type TEXT,user_id TEXT,sect_id INTEGER,score TEXT,extra TEXT,updated_at TEXT)"
                )
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)")
                conn.execute("CREATE TABLE sects(sect_id INTEGER,sect_name TEXT)")
                conn.executemany(
                    "INSERT INTO season_rank VALUES('key','weekly','period','战力',?,0,?,'{}',?)",
                    (("u1", "10", "a"), ("u2", "20", "b")),
                )
                conn.executemany(
                    "INSERT INTO user_xiuxian VALUES(?,?)",
                    (("u1", "甲"), ("u2", "乙")),
                )

            connection = db_backend.connect(database)
            self.addCleanup(connection.close)
            calls = []

            def read(sql, params=(), *, one=False, dict_row=False):
                calls.append(sql)
                cursor = connection.execute(sql, params)
                columns = [column[0] for column in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return (rows[0] if rows else None) if one else rows

            manager = SimpleNamespace(database_path=database, _read_query=read)
            with (
                patch.object(season_rank_service, "_sql_message", return_value=manager),
                patch.object(season_rank_service, "ensure_season_rank_table", return_value=True),
                patch.object(
                    season_rank_service,
                    "_build_rank_identity",
                    return_value=("key", "weekly", "period"),
                ),
            ):
                first = season_rank_service.get_top_season_rank("战力", "weekly")
                self.assertEqual(["u2", "u1"], [row["user_id"] for row in first])
                self.assertEqual(1, len(calls))

                with db_backend.transaction(database) as conn:
                    conn.execute(
                        "UPDATE season_rank SET score='30',updated_at='c' WHERE user_id='u1'"
                    )
                shared = season_rank_service.get_top_season_rank("战力", "weekly")
                self.assertEqual(first, shared)
                self.assertEqual(1, len(calls))

                personal = season_rank_service.get_user_season_rank("u1", "战力", "weekly")
                self.assertEqual((1, 30), (personal["rank"], personal["score"]))
                self.assertEqual(3, len(calls))
                self.assertGreaterEqual(process_read_cache.stats()["hits"], 1)


if __name__ == "__main__":
    unittest.main()
