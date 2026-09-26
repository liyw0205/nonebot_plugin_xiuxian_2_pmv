import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.accessory_package.attached_migrations import (
    ATTACHED_OPERATION_VERSION,
    ATTACHED_PRESET_VERSION,
    ATTACHED_SCHEMA_VERSION,
    apply_attached_player_accessory,
    apply_attached_player_accessory_operations,
    apply_attached_player_accessory_presets,
)
from nonebot_plugin_xiuxian_2.infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


class AttachedAccessoryMigrationTests(unittest.TestCase):
    def test_apply_is_idempotent_and_checksum_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            game.touch()
            player.touch()
            with AttachedDatabaseUnitOfWork(game, attachments={"player_data": player}, immediate=True) as uow:
                self.assertTrue(apply_attached_player_accessory(uow))
                self.assertTrue(apply_attached_player_accessory_operations(uow))
                self.assertTrue(apply_attached_player_accessory_presets(uow))
            with AttachedDatabaseUnitOfWork(game, attachments={"player_data": player}, immediate=True) as uow:
                self.assertFalse(apply_attached_player_accessory(uow))
                self.assertFalse(apply_attached_player_accessory_operations(uow))
                self.assertFalse(apply_attached_player_accessory_presets(uow))
                row = uow.query_one(
                    "SELECT version, name FROM player_data.attached_schema_migrations WHERE version=?",
                    (ATTACHED_SCHEMA_VERSION,),
                )
                operation = uow.query_one(
                    "SELECT version, name FROM player_data.attached_schema_migrations WHERE version=?",
                    (ATTACHED_OPERATION_VERSION,),
                )
                preset = uow.query_one(
                    "SELECT version, name FROM player_data.attached_schema_migrations WHERE version=?",
                    (ATTACHED_PRESET_VERSION,),
                )
                columns = {str(item[1]) for item in uow.execute("PRAGMA player_data.table_info(player_accessory)").fetchall()}
                table = uow.query_one(
                    "SELECT name FROM player_data.sqlite_master WHERE type='table' AND name='accessory_package_operations'"
                )
            self.assertEqual((row["version"], row["name"]), (ATTACHED_SCHEMA_VERSION, "player_accessory"))
            self.assertEqual((operation["version"], operation["name"]), (ATTACHED_OPERATION_VERSION, "accessory_package_operations"))
            self.assertEqual((preset["version"], preset["name"]), (ATTACHED_PRESET_VERSION, "accessory_presets"))
            self.assertTrue({"equipped", "bag", "preset_1", "preset_2", "preset_3"}.issubset(columns))
            self.assertEqual(table["name"], "accessory_package_operations")


if __name__ == "__main__":
    unittest.main()
