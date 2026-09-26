from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.accessory_preset_application import (
    AccessoryPresetApplication,
)
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_accessory_affix_operations
from nonebot_plugin_xiuxian_2.features.accessory_package.attached_migrations import (
    apply_attached_player_accessory,
    apply_attached_player_accessory_operations,
    apply_attached_player_accessory_presets,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


class AccessoryPresetApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        self.equipped = {
            "手镯": {"uid": "bracelet", "name": "烈阳手镯"},
            "戒指": {"uid": "ring", "name": "玄渊戒指"},
            "手环": None,
            "项链": None,
        }
        self.empty_preset = {"手镯": None, "戒指": None, "手环": None, "项链": None}
        self.old_preset = {
            "手镯": "old-bracelet",
            "戒指": None,
            "手环": None,
            "项链": None,
        }
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TABLE player_accessory("
                "user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT,"
                "preset_1 TEXT,preset_2 TEXT,preset_3 TEXT)"
            )
            conn.execute(
                "INSERT INTO player_accessory VALUES(?,?,?,?,?,?)",
                (
                    "u",
                    json.dumps(self.equipped, ensure_ascii=False),
                    "[]",
                    json.dumps(self.old_preset, ensure_ascii=False),
                    json.dumps(self.empty_preset, ensure_ascii=False),
                    json.dumps(self.empty_preset, ensure_ascii=False),
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
        self.application = AccessoryPresetApplication(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def state(self, preset_idx: int = 1):
        with sqlite3.connect(self.player) as conn:
            row = conn.execute(
                f"SELECT equipped,preset_{preset_idx} FROM player_accessory WHERE user_id='u'"
            ).fetchone()
        return json.loads(row[0]), json.loads(row[1])

    def test_save_replay_overwrite_and_payload_conflict(self) -> None:
        first = self.application.save(
            "preset-1", "u", 1, self.equipped, self.old_preset
        )
        duplicate = self.application.save(
            "preset-1", "u", 1, self.equipped, self.old_preset
        )
        conflict = self.application.save(
            "preset-1", "u", 1, self.equipped, self.empty_preset
        )
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertTrue(first.details["had_old"])
        self.assertEqual(
            first.details["preset"],
            {"手镯": "bracelet", "戒指": "ring", "手环": None, "项链": None},
        )
        self.assertEqual(self.state()[1], first.details["preset"])

    def test_stale_equipped_and_operation_failure_do_not_mutate(self) -> None:
        stale = dict(self.equipped, 戒指={"uid": "changed"})
        self.assertEqual(
            self.application.save("stale", "u", 1, stale, self.old_preset).status,
            "state_changed",
        )
        before = self.state()
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER fail_preset BEFORE INSERT ON accessory_transaction_operations "
                "WHEN NEW.action='save_preset' BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.application.save("rollback", "u", 1, self.equipped, self.old_preset)
        self.assertEqual(self.state(), before)

    def test_missing_user_can_create_empty_preset_row(self) -> None:
        with sqlite3.connect(self.player) as conn:
            conn.execute("DELETE FROM player_accessory WHERE user_id='u'")
        result = self.application.save(
            "new-user", "u", 2,
            {"手镯": None, "戒指": None, "手环": None, "项链": None},
            self.empty_preset,
        )
        self.assertEqual(result.status, "applied")
        with sqlite3.connect(self.player) as conn:
            row = conn.execute(
                "SELECT equipped,bag,preset_2 FROM player_accessory WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(json.loads(row[0]), {"手镯": None, "戒指": None, "手环": None, "项链": None})
        self.assertEqual(json.loads(row[1]), [])
        self.assertEqual(json.loads(row[2]), self.empty_preset)

    def test_missing_preset_schema_is_rejected_without_ddl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with sqlite3.connect(player) as conn:
                conn.execute(
                    "CREATE TABLE player_accessory(user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
                )
                conn.execute(
                    "INSERT INTO player_accessory VALUES(?,?,?)",
                    ("u", json.dumps(self.equipped), "[]"),
                )
            with DatabaseUnitOfWork(game) as uow:
                apply_accessory_affix_operations(uow)
            result = AccessoryPresetApplication(game, player).save(
                "schema-missing", "u", 1, self.equipped, self.empty_preset
            )
            self.assertEqual(result.status, "schema_missing")
            with sqlite3.connect(player) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(player_accessory)")}
            self.assertNotIn("preset_1", columns)


if __name__ == "__main__":
    unittest.main()
