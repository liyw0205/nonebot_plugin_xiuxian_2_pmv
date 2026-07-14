from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.settlement_service import (
    RiftSettlementService,
)
from tests.test_db_backend import db_backend


class RiftSettlementServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_db = root / "game.db"
        self.player_db = root / "player.db"
        self.rift = {"name": "test", "rank": 2, "time": 30}
        with db_backend.transaction(self.game_db) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,"
                "hp INTEGER,mp INTEGER)"
            )
            conn.execute(
                "CREATE TABLE user_cd("
                "user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,"
                "scheduled_time TEXT)"
            )
            conn.execute(
                "CREATE TABLE rift_entries("
                "user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)"
            )
            conn.execute(
                "CREATE TABLE back("
                "user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES('u',100,200,80,60)")
            conn.execute(
                "INSERT INTO user_cd VALUES('u',3,'2000-01-01 00:00:00','30')"
            )
            conn.execute(
                "INSERT INTO rift_entries VALUES('u',%s,'active')",
                (json.dumps(self.rift),),
            )
        with db_backend.transaction(self.player_db) as conn:
            conn.execute(
                'CREATE TABLE rift(user_id TEXT PRIMARY KEY,"explore_count" INTEGER)'
            )
            conn.execute("INSERT INTO rift VALUES('u',7)")
            conn.execute(
                'CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"秘境次数" INTEGER)'
            )
            conn.execute("INSERT INTO statistics VALUES('u',2)")
        self.service = RiftSettlementService(self.game_db, self.player_db)
        self.user = {"stone": 100, "exp": 200, "hp": 80, "mp": 60}
        self.outcome = {
            "delta": {"stone": 15, "exp": 20, "hp": -10},
            "items": [
                {
                    "id": 300,
                    "name": "loot",
                    "type": "weapon",
                    "amount": 1,
                }
            ],
            "statistics": {"秘境次数": 1},
            "message": "fixed event\ncurrent progress: 8/10",
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def settle(self, operation_id="op", **changes):
        values = {
            "user_id": "u",
            "expected_rift": self.rift,
            "expected_user": self.user,
            "expected_explore_count": 7,
            "outcome": self.outcome,
            "max_goods_num": 1000,
        }
        values.update(changes)
        return self.service.settle(operation_id=operation_id, **values)

    def test_full_event_is_cross_database_atomic_and_idempotent(self):
        first = self.settle()
        duplicate = self.settle()
        replay = self.service.replay("op")
        self.assertEqual(
            ("applied", 8, "duplicate", "duplicate"),
            (first.status, first.explore_count, duplicate.status, replay.status),
        )
        self.assertEqual(self.outcome["message"], replay.message)
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(
                (115, 220, 70, 60),
                tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()),
            )
            self.assertEqual(
                "settled",
                conn.execute("SELECT status FROM rift_entries").fetchone()[0],
            )
            self.assertEqual(
                0, conn.execute("SELECT type FROM user_cd").fetchone()[0]
            )
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT goods_num FROM back WHERE goods_id=300"
                ).fetchone()[0],
            )
            self.assertFalse(conn.table_exists("rift_settlement_counts"))
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(
                8,
                conn.execute('SELECT "explore_count" FROM rift').fetchone()[0],
            )
            self.assertEqual(
                3,
                conn.execute('SELECT "秘境次数" FROM statistics').fetchone()[0],
            )

    def test_tenth_completion_resets_progress_and_grants_fixed_reward(self):
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('UPDATE rift SET "explore_count"=9 WHERE user_id=\'u\'')
        outcome = {
            **self.outcome,
            "progress_reward": {
                "id": 20018,
                "name": "token",
                "type": "item",
                "amount": 1,
            },
            "message": "fixed tenth reward",
        }
        result = self.settle(
            expected_explore_count=9,
            outcome=outcome,
        )
        self.assertEqual(("applied", 0), (result.status, result.explore_count))
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(
                0,
                conn.execute('SELECT "explore_count" FROM rift').fetchone()[0],
            )
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT goods_num FROM back WHERE goods_id=20018"
                ).fetchone()[0],
            )

    def test_progress_reward_must_match_tenth_completion(self):
        with self.assertRaisesRegex(ValueError, "progress reward"):
            self.settle(
                "missing-tenth-reward",
                expected_explore_count=9,
                outcome={**self.outcome, "progress_reward": None},
            )
        with self.assertRaisesRegex(ValueError, "progress reward"):
            self.settle(
                "early-progress-reward",
                outcome={
                    **self.outcome,
                    "progress_reward": {
                        "id": 20018,
                        "name": "token",
                        "type": "item",
                        "amount": 1,
                    },
                },
            )
        with self.assertRaisesRegex(ValueError, "positive"):
            self.settle(
                "empty-tenth-reward",
                expected_explore_count=9,
                outcome={
                    **self.outcome,
                    "progress_reward": {
                        "id": 20018,
                        "name": "token",
                        "type": "item",
                        "amount": 0,
                    },
                },
            )
        with self.assertRaisesRegex(ValueError, "between 0 and 9"):
            self.settle(
                "invalid-progress",
                expected_explore_count=10,
                outcome={**self.outcome, "progress_reward": None},
            )

    def test_conflicts_and_inventory_limit_leave_state_unchanged(self):
        changed = self.settle(
            "changed",
            expected_user={**self.user, "hp": 79},
        )
        full = self.settle("full", max_goods_num=0)
        self.assertEqual("state_changed", changed.status)
        self.assertEqual("inventory_full", full.status)
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(
                (100, 200, 80, 60),
                tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()),
            )
            self.assertEqual(
                "active",
                conn.execute("SELECT status FROM rift_entries").fetchone()[0],
            )
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(
                7,
                conn.execute('SELECT "explore_count" FROM rift').fetchone()[0],
            )

    def test_premature_settlement_is_rejected_inside_transaction(self):
        with db_backend.transaction(self.game_db) as conn:
            conn.execute(
                "UPDATE user_cd SET create_time=CURRENT_TIMESTAMP "
                "WHERE user_id='u'"
            )
        result = self.settle("premature")
        self.assertEqual("not_ready", result.status)
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(
                (100, 200, 80, 60, "active", 3),
                tuple(
                    conn.execute(
                        "SELECT u.stone,u.exp,u.hp,u.mp,e.status,c.type "
                        "FROM user_xiuxian u JOIN rift_entries e USING(user_id) "
                        "JOIN user_cd c USING(user_id) WHERE u.user_id='u'"
                    ).fetchone()
                ),
            )
            self.assertFalse(conn.table_exists("rift_settlement_operations"))

    def test_operation_failure_rolls_back_both_databases(self):
        with db_backend.transaction(self.game_db) as conn:
            conn.execute(
                "CREATE TABLE rift_settlement_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,explore_count INTEGER,"
                "message TEXT,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_rift_settlement BEFORE INSERT "
                "ON rift_settlement_operations "
                "BEGIN SELECT RAISE(ABORT,'fail'); END"
            )
        with self.assertRaises(Exception):
            self.settle()
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(
                (100, 200, 80, 60),
                tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()),
            )
            self.assertEqual(
                "active",
                conn.execute("SELECT status FROM rift_entries").fetchone()[0],
            )
            self.assertIsNone(
                conn.execute("SELECT 1 FROM back WHERE goods_id=300").fetchone()
            )
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(
                (7, 2),
                tuple(
                    conn.execute(
                        'SELECT r."explore_count",s."秘境次数" '
                        "FROM rift r JOIN statistics s USING(user_id)"
                    ).fetchone()
                ),
            )


if __name__ == "__main__":
    unittest.main()
