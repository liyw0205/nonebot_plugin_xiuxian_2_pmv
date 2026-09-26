from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.accessory_upgrade_application import (
    AccessoryUpgradeApplication,
)
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_accessory_affix_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class FixedRandom:
    def sample(self, values, count):
        return list(values)[:count]

    def uniform(self, low, high):
        return low


class AccessoryUpgradeApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        self.main = {
            "uid": "main",
            "item_id": 101,
            "name": "烈阳戒指",
            "part": "戒指",
            "set_type": "烈阳",
            "quality": 3,
            "wash_count": 8,
            "affixes": [
                {"type": "攻击", "value": 0.12},
                {"type": "速度", "value": 20},
            ],
            "locked_affixes": [0],
        }
        self.material_one = dict(self.main, uid="material-1", wash_count=0)
        self.material_two = dict(self.main, uid="material-2", wash_count=2)
        self.other = dict(self.main, uid="other", item_id=999)
        equipped = {"戒指": self.main}
        bag = [self.material_one, self.material_two, self.other]
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TABLE player_accessory(user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
            )
            conn.execute(
                "INSERT INTO player_accessory VALUES(?,?,?)",
                ("u", json.dumps(equipped, ensure_ascii=False), json.dumps(bag, ensure_ascii=False)),
            )
        with DatabaseUnitOfWork(self.game) as uow:
            apply_accessory_affix_operations(uow)
        self.application = AccessoryUpgradeApplication(
            self.game, self.player, random_source=FixedRandom()
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def state(self):
        with sqlite3.connect(self.player) as conn:
            row = conn.execute(
                "SELECT equipped,bag FROM player_accessory WHERE user_id='u'"
            ).fetchone()
        return json.loads(row[0]), json.loads(row[1])

    def test_upgrade_replay_fills_affix_and_consumes_materials(self) -> None:
        equipped, bag = self.state()
        first = self.application.upgrade(
            "upgrade-1",
            "u",
            "戒指",
            equipped,
            bag,
            ("material-1", "material-2"),
        )
        duplicate = self.application.upgrade(
            "upgrade-1",
            "u",
            "戒指",
            equipped,
            bag,
            ("material-1", "material-2"),
        )
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(first.affected, 2)
        self.assertEqual(first.accessory["quality"], 4)
        self.assertEqual(first.accessory["wash_count"], 0)
        self.assertEqual(len(first.accessory["affixes"]), 3)
        self.assertEqual(first.accessory["locked_affixes"], [0])
        self.assertEqual([item["uid"] for item in self.state()[1]], ["other"])

    def test_stale_snapshot_and_material_mismatch_do_not_mutate(self) -> None:
        equipped, bag = self.state()
        stale = dict(equipped, 戒指=dict(self.main, wash_count=99))
        self.assertEqual(
            self.application.upgrade(
                "stale", "u", "戒指", stale, bag, ("material-1", "material-2")
            ).status,
            "state_changed",
        )
        mismatch = dict(self.material_one, item_id=999)
        mismatched_bag = [mismatch, self.material_two, self.other]
        self.assertEqual(
            self.application.upgrade(
                "mismatch",
                "u",
                "戒指",
                equipped,
                mismatched_bag,
                ("material-1", "material-2"),
            ).status,
            "material_mismatch",
        )
        self.assertEqual(self.state(), (equipped, bag))

    def test_domain_validation_and_trigger_failure(self) -> None:
        equipped, bag = self.state()
        self.assertEqual(
            self.application.upgrade(
                "missing", "u", "戒指", equipped, bag, ("missing", "material-2")
            ).status,
            "material_missing",
        )
        max_equipped = dict(equipped, 戒指=dict(self.main, quality=5))
        self.assertEqual(
            self.application.upgrade(
                "max", "u", "戒指", max_equipped, bag, ("material-1", "material-2")
            ).status,
            "max_quality",
        )
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER fail_upgrade BEFORE INSERT ON accessory_transaction_operations "
                "BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        before = self.state()
        with self.assertRaises(sqlite3.IntegrityError):
            self.application.upgrade(
                "rollback", "u", "戒指", equipped, bag, ("material-1", "material-2")
            )
        self.assertEqual(self.state(), before)

    def test_request_requires_startup_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            with self.assertRaises(sqlite3.OperationalError):
                AccessoryUpgradeApplication(game, self.player).upgrade(
                    "no-ddl",
                    "u",
                    "戒指",
                    self.state()[0],
                    self.state()[1],
                    ("material-1", "material-2"),
                )
            with sqlite3.connect(game) as conn:
                self.assertIsNone(
                    conn.execute(
                        "SELECT name FROM sqlite_master WHERE name='accessory_transaction_operations'"
                    ).fetchone()
                )


if __name__ == "__main__":
    unittest.main()
