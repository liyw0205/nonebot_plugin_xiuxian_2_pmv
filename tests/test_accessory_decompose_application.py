from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.accessory_decompose_application import (
    AccessoryDecomposeApplication,
)
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_accessory_affix_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class AccessoryDecomposeApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        self.accessory = {
            "uid": "acc-1",
            "name": "测试戒指",
            "quality": 4,
            "affixes": [{"type": "攻击", "value": 0.1}],
        }
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES('u',20023,'洗练石','特殊道具',5,5)")
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TABLE player_accessory(user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
            )
            conn.execute(
                "INSERT INTO player_accessory VALUES(?,?,?)",
                ("u", "{}", json.dumps([self.accessory], ensure_ascii=False)),
            )
        with DatabaseUnitOfWork(self.game) as uow:
            apply_accessory_affix_operations(uow)
        self.application = AccessoryDecomposeApplication(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def state(self):
        with sqlite3.connect(self.game) as conn:
            stones = conn.execute(
                "SELECT goods_num,bind_num FROM back WHERE user_id='u' AND goods_id=20023"
            ).fetchone()
        with sqlite3.connect(self.player) as conn:
            bag = conn.execute(
                "SELECT bag FROM player_accessory WHERE user_id='u'"
            ).fetchone()[0]
        return tuple(stones), json.loads(bag)

    def test_decompose_replay_and_snapshot_conflict(self) -> None:
        first = self.application.decompose(
            "decompose-1", "u", "acc-1", self.accessory, 20023, "洗练石", 3, 100
        )
        duplicate = self.application.decompose(
            "decompose-1", "u", "acc-1", self.accessory, 20023, "洗练石", 3, 100
        )
        conflict = self.application.decompose(
            "decompose-1", "u", "acc-1", dict(self.accessory, wash_count=1), 20023, "洗练石", 3, 100
        )
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual((8, 8), self.state()[0])
        self.assertEqual(self.state()[1], [])

    def test_inventory_full_and_trigger_failure_do_not_mutate(self) -> None:
        full = self.application.decompose(
            "full", "u", "acc-1", self.accessory, 20023, "洗练石", 3, 7
        )
        self.assertEqual(full.status, "inventory_full")
        self.assertEqual((5, 5), self.state()[0])

        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER fail_decompose BEFORE INSERT ON accessory_transaction_operations "
                "BEGIN SELECT RAISE(ABORT,'reject operation'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.application.decompose(
                "rollback", "u", "acc-1", self.accessory, 20023, "洗练石", 3, 100
            )
        self.assertEqual((5, 5), self.state()[0])
        self.assertEqual(self.state()[1], [self.accessory])

    def test_request_requires_startup_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            with self.assertRaises(sqlite3.OperationalError):
                AccessoryDecomposeApplication(game, self.player).decompose(
                    "no-ddl", "u", "acc-1", self.accessory, 20023, "洗练石", 3, 100
                )
            with sqlite3.connect(game) as conn:
                self.assertIsNone(
                    conn.execute(
                        "SELECT name FROM sqlite_master WHERE name='accessory_transaction_operations'"
                    ).fetchone()
                )

    def test_batch_decompose_uses_whole_bag_snapshot_and_replays(self) -> None:
        second = dict(self.accessory, uid="acc-2", quality=2)
        expected_bag = [self.accessory, second]
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "UPDATE player_accessory SET bag=? WHERE user_id='u'",
                (json.dumps(expected_bag, ensure_ascii=False),),
            )
        first = self.application.batch_decompose(
            "batch-1", "u", expected_bag, ("acc-1", "acc-2"), 20023, "洗练石", 5, 100
        )
        duplicate = self.application.batch_decompose(
            "batch-1", "u", expected_bag, ("acc-1", "acc-2"), 20023, "洗练石", 5, 100
        )
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual((first.affected, duplicate.affected, first.stone_delta), (2, 2, 5))
        self.assertEqual(self.state()[0], (10, 10))
        self.assertEqual(self.state()[1], [])

    def test_batch_decompose_rejects_stale_snapshot_and_inventory_full(self) -> None:
        second = dict(self.accessory, uid="acc-2", quality=2)
        expected_bag = [self.accessory, second]
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "UPDATE player_accessory SET bag=? WHERE user_id='u'",
                (json.dumps(expected_bag, ensure_ascii=False),),
            )
        stale = list(reversed(expected_bag))
        self.assertEqual(
            self.application.batch_decompose(
                "stale-batch", "u", stale, ("acc-1", "acc-2"), 20023, "洗练石", 5, 100
            ).status,
            "state_changed",
        )
        self.assertEqual(
            self.application.batch_decompose(
                "full-batch", "u", expected_bag, ("acc-1", "acc-2"), 20023, "洗练石", 5, 9
            ).status,
            "inventory_full",
        )
        self.assertEqual(self.state()[0], (5, 5))
        self.assertEqual(self.state()[1], expected_bag)


if __name__ == "__main__":
    unittest.main()
