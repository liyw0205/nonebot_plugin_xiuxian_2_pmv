from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.accessory_package.attached_migrations import apply_attached_player_accessory
from nonebot_plugin_xiuxian_2.infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


class AttachedAccessoryMigrationTests(unittest.TestCase):
    def test_migration_is_durable_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game = root / "game.db"
            player = root / "player.db"
            sqlite3.connect(player).close()
            with AttachedDatabaseUnitOfWork(game, attachments={"player_data": player}, immediate=True) as uow:
                self.assertTrue(apply_attached_player_accessory(uow))
            with AttachedDatabaseUnitOfWork(game, attachments={"player_data": player}, immediate=True) as uow:
                self.assertFalse(apply_attached_player_accessory(uow))
            connection = sqlite3.connect(player)
            self.assertEqual(connection.execute("select count(*) from attached_schema_migrations").fetchone()[0], 1)
            self.assertEqual(connection.execute("select count(*) from player_accessory").fetchone()[0], 0)

    def test_migration_rolls_back_schema_and_ledger_together(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game = root / "game.db"
            player = root / "player.db"
            sqlite3.connect(player).close()
            with self.assertRaisesRegex(RuntimeError, "rollback"):
                with AttachedDatabaseUnitOfWork(game, attachments={"player_data": player}, immediate=True) as uow:
                    apply_attached_player_accessory(uow)
                    raise RuntimeError("rollback")
            connection = sqlite3.connect(player)
            self.assertEqual(connection.execute("select count(*) from sqlite_master where name='attached_schema_migrations'").fetchone()[0], 0)
            self.assertEqual(connection.execute("select count(*) from sqlite_master where name='player_accessory'").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
