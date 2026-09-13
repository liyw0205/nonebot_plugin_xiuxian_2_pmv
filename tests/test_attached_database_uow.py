from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


class AttachedDatabaseUnitOfWorkTests(unittest.TestCase):
    def test_attach_and_rollback_cover_both_databases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game = root / "game.db"
            player = root / "player.db"
            sqlite3.connect(player).execute("CREATE TABLE player_accessory(user_id TEXT PRIMARY KEY, bag TEXT)")
            with self.assertRaisesRegex(RuntimeError, "boom"):
                with AttachedDatabaseUnitOfWork(game, attachments={"player_data": player}, immediate=True) as uow:
                    uow.execute("CREATE TABLE audit(value TEXT)")
                    uow.execute("INSERT INTO player_data.player_accessory VALUES (?, ?)", ("u1", "[]"))
                    raise RuntimeError("boom")
            self.assertFalse(game.exists() and sqlite3.connect(game).execute("select count(*) from sqlite_master where name='audit'").fetchone()[0])
            self.assertEqual(sqlite3.connect(player).execute("select count(*) from player_accessory").fetchone()[0], 0)

    def test_missing_attachment_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                with AttachedDatabaseUnitOfWork(Path(directory) / "game.db", attachments={"player_data": Path(directory) / "missing.db"}):
                    pass


if __name__ == "__main__":
    unittest.main()
