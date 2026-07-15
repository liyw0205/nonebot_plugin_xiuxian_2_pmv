from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.transaction_service import (
    ActivityCollectExchangeService,
)
from tests.test_db_backend import db_backend


class ActivityCollectExchangeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.activity_database = root / "activity.db"
        self.game_database = root / "game.db"
        with db_backend.transaction(self.activity_database) as conn:
            conn.execute(
                "CREATE TABLE activity_collect_inventory(activity_key TEXT,user_id TEXT,word_char TEXT,"
                "count INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id,word_char))"
            )
            conn.execute(
                "CREATE TABLE activity_collect_claim(activity_key TEXT,user_id TEXT,phrase TEXT,"
                "count INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id,phrase))"
            )
            conn.executemany(
                "INSERT INTO activity_collect_inventory VALUES('festival','u',%s,%s,'')",
                (("端", 2), ("午", 2), ("安", 1), ("康", 1)),
            )
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
                "UNIQUE(user_id,goods_id))"
            )
        self.service = ActivityCollectExchangeService(
            self.activity_database, self.game_database
        )
        self.tokens = {"端": 1, "午": 1, "安": 1, "康": 1}
        self.rewards = (
            {"type": "stone", "quantity": 50, "name": "灵石", "desc": "获得 50 灵石"},
            {"id": 101, "type": "道具", "quantity": 2, "name": "福袋", "desc": "获得 福袋x2"},
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def exchange(self, operation_id="exchange", **overrides):
        arguments = {
            "operation_id": operation_id,
            "user_id": "u",
            "activity_key": "festival",
            "phrase": "端午安康",
            "required_tokens": self.tokens,
            "limit": 1,
            "rewards": self.rewards,
            "max_goods_num": 100,
        }
        arguments.update(overrides)
        return self.service.exchange(**arguments)

    def test_atomic_exchange_and_idempotent_replay(self) -> None:
        result = self.exchange()
        duplicate = self.exchange()

        self.assertEqual("applied", result.status)
        self.assertEqual(1, result.claim_count)
        self.assertEqual(("获得 50 灵石", "获得 福袋x2"), result.rewards)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(1, duplicate.claim_count)
        with db_backend.connection(self.activity_database) as conn:
            counts = dict(conn.execute(
                "SELECT word_char,count FROM activity_collect_inventory"
            ).fetchall())
            self.assertEqual({"端": 1, "午": 1, "安": 0, "康": 0}, counts)
            self.assertEqual(1, conn.execute(
                "SELECT count FROM activity_collect_claim"
            ).fetchone()[0])
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back").fetchone()[0])

    def test_operation_conflict_and_exchange_limit(self) -> None:
        self.assertEqual("applied", self.exchange().status)
        self.assertEqual(
            "operation_conflict",
            self.exchange(rewards=({"type": "stone", "quantity": 51, "name": "灵石"},)).status,
        )
        self.assertEqual("limit_reached", self.exchange("second").status)

    def test_reports_missing_tokens_without_mutation(self) -> None:
        result = self.exchange(required_tokens={"端": 3, "午": 1})

        self.assertEqual("tokens_insufficient", result.status)
        self.assertEqual((("端", 1),), result.missing)
        with db_backend.connection(self.activity_database) as conn:
            self.assertEqual(2, conn.execute(
                "SELECT count FROM activity_collect_inventory WHERE word_char='端'"
            ).fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM activity_collect_claim").fetchone()[0])

    def test_inventory_limit_rejects_entire_exchange(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("INSERT INTO back VALUES('u',101,'福袋','道具',99,'','',99)")

        result = self.exchange(max_goods_num=100)

        self.assertEqual("inventory_full", result.status)
        with db_backend.connection(self.activity_database) as conn:
            self.assertEqual(1, conn.execute(
                "SELECT count FROM activity_collect_inventory WHERE word_char='安'"
            ).fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM activity_collect_claim").fetchone()[0])
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(99, conn.execute("SELECT goods_num FROM back").fetchone()[0])

    def test_operation_failure_rolls_back_activity_and_game_databases(self) -> None:
        with db_backend.transaction(self.activity_database) as conn:
            conn.execute(
                "CREATE TABLE activity_collect_exchange_operations(operation_id TEXT PRIMARY KEY,"
                "payload TEXT,result_json TEXT,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_collect_exchange BEFORE INSERT ON activity_collect_exchange_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaises(Exception):
            self.exchange("rollback")

        with db_backend.connection(self.activity_database) as conn:
            self.assertEqual(1, conn.execute(
                "SELECT count FROM activity_collect_inventory WHERE word_char='安'"
            ).fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM activity_collect_claim").fetchone()[0])
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM back").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
