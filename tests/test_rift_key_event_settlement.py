import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.key_event_settlement_service import RiftKeyEventSettlementService
from tests.test_db_backend import db_backend


class RiftKeyEventSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_db, self.player_db = root / "game.db", root / "player.db"
        self.rift = {"name": "test", "rank": 2, "time": 30}
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',100,200,80,60)")
            conn.execute("INSERT INTO user_cd VALUES('u',3,'now','30')")
            conn.execute("INSERT INTO rift_entries VALUES('u',%s,'active')", (json.dumps(self.rift),))
            conn.execute("INSERT INTO back VALUES('u',20001,'key','item',1,'','',1)")
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('CREATE TABLE rift(user_id TEXT PRIMARY KEY,"explore_count" INTEGER)')
            conn.execute("INSERT INTO rift VALUES('u',9)")
        self.service = RiftKeyEventSettlementService(self.game_db, self.player_db)
        self.user = {"stone": 100, "exp": 200, "hp": 80, "mp": 60}
        self.outcome = {"delta": {"stone": 15, "hp": -20}, "items": [{"id": 300, "name": "loot", "type": "weapon", "amount": 1}], "progress_reward": {"id": 20018, "name": "token", "type": "item", "amount": 1}, "statistics": {"rift_combat": 1}, "message": "fixed"}

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_full_event_is_atomic_and_idempotent(self):
        first = self.service.settle("op", "u", 20001, self.rift, self.user, 9, self.outcome, 1000)
        duplicate = self.service.settle("op", "u", 20001, self.rift, self.user, 9, self.outcome, 1000)
        self.assertEqual((first.status, first.explore_count, duplicate.status), ("applied", 0, "duplicate"))
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()), (115, 200, 60, 60))
            self.assertEqual(
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back WHERE goods_id=20001"
                    ).fetchone()
                ),
                (0, 0),
            )
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "settled")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM back WHERE goods_id IN (300,20018)").fetchone()[0], 2)
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(conn.execute('SELECT "explore_count" FROM rift').fetchone()[0], 0)
            self.assertEqual(conn.execute('SELECT "rift_combat" FROM statistics').fetchone()[0], 1)

    def test_conflicts_and_inventory_limit_are_rejected(self):
        self.assertEqual(self.service.settle("bad", "u", 20001, self.rift, {**self.user, "hp": 79}, 9, self.outcome, 1000).status, "state_changed")
        self.assertEqual(self.service.settle("full", "u", 20001, self.rift, self.user, 9, self.outcome, 0).status, "inventory_full")
        self.assertEqual(self.service.settle("op", "u", 20001, self.rift, self.user, 9, self.outcome, 1000).status, "applied")
        self.assertEqual(self.service.settle("op", "u", 20001, self.rift, self.user, 9, {**self.outcome, "message": "changed"}, 1000).status, "state_changed")

    def test_same_consumed_item_reward_is_allowed_at_inventory_limit(self):
        outcome = {
            **self.outcome,
            "items": [],
            "progress_reward": {
                "id": 20001,
                "name": "key",
                "type": "item",
                "amount": 1,
            },
        }
        result = self.service.settle(
            "same-item", "u", 20001, self.rift, self.user, 9, outcome, 1
        )
        self.assertEqual("applied", result.status)
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(
                (1, 1),
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back WHERE goods_id=20001"
                    ).fetchone()
                ),
            )

    def test_progress_reward_must_match_tenth_completion(self):
        with self.assertRaisesRegex(ValueError, "progress reward"):
            self.service.settle(
                "missing-tenth",
                "u",
                20001,
                self.rift,
                self.user,
                9,
                {**self.outcome, "progress_reward": None},
                1000,
            )
        with self.assertRaisesRegex(ValueError, "positive"):
            self.service.settle(
                "empty-tenth",
                "u",
                20001,
                self.rift,
                self.user,
                9,
                {
                    **self.outcome,
                    "progress_reward": {
                        **self.outcome["progress_reward"],
                        "amount": 0,
                    },
                },
                1000,
            )
        with self.assertRaisesRegex(ValueError, "between 0 and 9"):
            self.service.settle(
                "invalid-progress",
                "u",
                20001,
                self.rift,
                self.user,
                10,
                {**self.outcome, "progress_reward": None},
                1000,
            )

    def test_missing_item_is_rejected_without_state_changes(self):
        with db_backend.transaction(self.game_db) as conn:
            conn.execute(
                "UPDATE back SET goods_num=0 WHERE user_id='u' AND goods_id=20001"
            )
        result = self.service.settle(
            "missing", "u", 20001, self.rift, self.user, 9, self.outcome, 1000
        )
        self.assertEqual("item_missing", result.status)
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(
                (100, 200, 80, 60),
                tuple(
                    conn.execute(
                        "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id='u'"
                    ).fetchone()
                ),
            )
            self.assertEqual(
                "active",
                conn.execute(
                    "SELECT status FROM rift_entries WHERE user_id='u'"
                ).fetchone()[0],
            )
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(
                9,
                conn.execute(
                    'SELECT "explore_count" FROM rift WHERE user_id=\'u\''
                ).fetchone()[0],
            )

    def test_operation_failure_rolls_back_both_databases(self):
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("CREATE TABLE rift_key_event_operations(operation_id TEXT PRIMARY KEY,payload TEXT,explore_count INTEGER,message TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_key_event BEFORE INSERT ON rift_key_event_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.service.settle("op", "u", 20001, self.rift, self.user, 9, self.outcome, 1000)
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(
                (1, 1),
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back WHERE goods_id=20001"
                    ).fetchone()
                ),
            )
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(conn.execute('SELECT "explore_count" FROM rift').fetchone()[0], 9)


if __name__ == "__main__":
    unittest.main()
