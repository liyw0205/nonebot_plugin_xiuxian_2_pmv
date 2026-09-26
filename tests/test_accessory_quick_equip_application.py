from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.accessory_quick_equip_application import (
    AccessoryQuickEquipApplication,
)
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_accessory_affix_operations
from nonebot_plugin_xiuxian_2.features.accessory_package.attached_migrations import (
    apply_attached_player_accessory,
    apply_attached_player_accessory_operations,
    apply_attached_player_accessory_presets,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


class AccessoryQuickEquipApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        self.old = {"uid": "old", "name": "旧手镯", "part": "手镯"}
        self.target = {"uid": "target", "name": "新手镯", "part": "手镯"}
        self.ring = {"uid": "ring", "name": "戒指", "part": "戒指"}
        self.equipped = {"手镯": self.old, "戒指": self.ring, "手环": None, "项链": None}
        self.bag = [self.target]
        self.preset = {
            "手镯": "target",
            "戒指": "ring",
            "手环": None,
            "项链": "missing",
        }
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TABLE player_accessory("
                "user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT,preset_1 TEXT,preset_2 TEXT,preset_3 TEXT)"
            )
            conn.execute(
                "INSERT INTO player_accessory VALUES(?,?,?,?,?,?)",
                (
                    "u",
                    json.dumps(self.equipped, ensure_ascii=False),
                    json.dumps(self.bag, ensure_ascii=False),
                    json.dumps(self.preset, ensure_ascii=False),
                    json.dumps(self.preset, ensure_ascii=False),
                    json.dumps(self.preset, ensure_ascii=False),
                ),
            )
        with DatabaseUnitOfWork(self.game) as uow:
            apply_accessory_affix_operations(uow)
        with AttachedDatabaseUnitOfWork(
            self.game, attachments={"player_data": self.player}, immediate=True
        ) as uow:
            apply_attached_player_accessory(uow)
            apply_attached_player_accessory_operations(uow)
            apply_attached_player_accessory_presets(uow)
        self.application = AccessoryQuickEquipApplication(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def state(self):
        with sqlite3.connect(self.player) as conn:
            row = conn.execute(
                "SELECT equipped,bag,preset_1 FROM player_accessory WHERE user_id='u'"
            ).fetchone()
        return json.loads(row[0]), json.loads(row[1]), json.loads(row[2])

    def test_equip_replay_swaps_and_cleans_missing_uid(self) -> None:
        first = self.application.equip(
            "equip-1", "u", 1, self.equipped, self.bag, self.preset
        )
        duplicate = self.application.equip(
            "equip-1", "u", 1, self.equipped, self.bag, self.preset
        )
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual((first.affected, duplicate.affected), (1, 1))
        self.assertEqual(first.details, duplicate.details)
        equipped, bag, preset = self.state()
        self.assertEqual(equipped["手镯"]["uid"], "target")
        self.assertEqual([item["uid"] for item in bag], ["old"])
        self.assertIsNone(preset["项链"])
        self.assertEqual(first.details["skipped"], [{"slot": "戒指", "reason": "already_equipped"}])

    def test_part_mismatch_preserves_state(self) -> None:
        wrong = {"uid": "wrong", "name": "错位戒指", "part": "戒指"}
        equipped = {"手镯": None, "戒指": None, "手环": None, "项链": None}
        bag = [wrong]
        preset = {"手镯": "wrong", "戒指": None, "手环": None, "项链": None}
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "UPDATE player_accessory SET equipped=?,bag=?,preset_1=? WHERE user_id='u'",
                (json.dumps(equipped), json.dumps(bag), json.dumps(preset)),
            )
        result = self.application.equip("mismatch", "u", 1, equipped, bag, preset)
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.affected, 0)
        self.assertEqual(result.details["skipped"][0]["reason"], "part_mismatch")
        self.assertEqual(self.state(), (equipped, bag, preset))

    def test_empty_snapshot_and_trigger_failure_do_not_mutate(self) -> None:
        empty = {"手镯": None, "戒指": None, "手环": None, "项链": None}
        self.assertEqual(
            self.application.equip("empty", "u", 1, self.equipped, self.bag, empty).status,
            "preset_empty",
        )
        before = self.state()
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER fail_quick_equip BEFORE INSERT ON accessory_transaction_operations "
                "WHEN NEW.action='quick_equip_preset' BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.application.equip("rollback", "u", 1, self.equipped, self.bag, self.preset)
        self.assertEqual(self.state(), before)

    def test_missing_preset_schema_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with sqlite3.connect(player) as conn:
                conn.execute(
                    "CREATE TABLE player_accessory(user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
                )
                conn.execute(
                    "INSERT INTO player_accessory VALUES(?,?,?)",
                    ("u", json.dumps(self.equipped), json.dumps(self.bag)),
                )
            with DatabaseUnitOfWork(game) as uow:
                apply_accessory_affix_operations(uow)
            result = AccessoryQuickEquipApplication(game, player).equip(
                "schema-missing", "u", 1, self.equipped, self.bag, self.preset
            )
            self.assertEqual(result.status, "schema_missing")
            with sqlite3.connect(player) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(player_accessory)")}
            self.assertNotIn("preset_1", columns)


if __name__ == "__main__":
    unittest.main()
