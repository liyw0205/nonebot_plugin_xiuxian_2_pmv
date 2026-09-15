import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.accessory_package.attached_migrations import (
    ATTACHED_SCHEMA_VERSION,
    apply_attached_player_accessory,
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
            with AttachedDatabaseUnitOfWork(game, attachments={"player_data": player}, immediate=True) as uow:
                self.assertFalse(apply_attached_player_accessory(uow))
                row = uow.query_one(
                    "SELECT version, name FROM player_data.attached_schema_migrations WHERE version=?",
                    (ATTACHED_SCHEMA_VERSION,),
                )
                columns = {str(item[1]) for item in uow.execute("PRAGMA player_data.table_info(player_accessory)").fetchall()}
            self.assertEqual((row["version"], row["name"]), (ATTACHED_SCHEMA_VERSION, "player_accessory"))
            self.assertTrue({"equipped", "bag"}.issubset(columns))


if __name__ == "__main__":
    unittest.main()
