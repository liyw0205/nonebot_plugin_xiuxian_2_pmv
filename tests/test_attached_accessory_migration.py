from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.accessory_package.attached_migrations import apply_attached_player_accessory
from nonebot_plugin_xiuxian_2.infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


class AttachedAccessoryMigrationTests(unittest.TestCase):
    def test_migration_targets_attached_namespace_and_rolls_back(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game = root / "game.db"
            player = root / "player.db"
            sqlite3.connect(player).close()
            with AttachedDatabaseUnitOfWork(game, attachments={"player_data": player}, immediate=True) as uow:
                apply_attached_player_accessory(uow)
            self.assertEqual(
                sqlite3.connect(player).execute("select count(*) from sqlite_master where type=? and name=?", ("table", "player_accessory")).fetchone()[0],
                1,
            )
            with self.assertRaisesRegex(RuntimeError, "rollback"):
                with AttachedDatabaseUnitOfWork(game, attachments={"player_data": player}, immediate=True) as uow:
                    uow.execute("INSERT INTO player_data.player_accessory VALUES (?, ?, ?)", ("u1", "{}", "[]"))
                    raise RuntimeError("rollback")
            self.assertEqual(sqlite3.connect(player).execute("select count(*) from player_accessory").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
