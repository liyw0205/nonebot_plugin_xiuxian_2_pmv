from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..profile_persistence import upsert_tianti_profile


class TiantiProfilePersistenceTests(unittest.TestCase):
    def test_upsert_targets_main_database_and_encodes_json_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                uow.execute(
                    "CREATE TABLE tianti_info(user_id TEXT PRIMARY KEY,tianti_hp INTEGER,opened_qiaoxue TEXT)"
                )
                upsert_tianti_profile(
                    uow,
                    "u",
                    ("tianti_hp", "opened_qiaoxue"),
                    {"tianti_hp": 42, "opened_qiaoxue": ["窍一"]},
                )
            with sqlite3.connect(database) as conn:
                self.assertEqual(
                    conn.execute("SELECT tianti_hp,opened_qiaoxue FROM tianti_info WHERE user_id='u'").fetchone(),
                    (42, '["窍一"]'),
                )

    def test_upsert_targets_attached_player_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game_database = root / "game.db"
            player_database = root / "player.db"
            with sqlite3.connect(player_database) as conn:
                conn.execute(
                    "CREATE TABLE tianti_info(user_id TEXT PRIMARY KEY,tianti_hp INTEGER)"
                )
            with DatabaseUnitOfWork(game_database, immediate=True) as uow:
                uow.attach_database(player_database, "player_data")
                upsert_tianti_profile(
                    uow,
                    "u",
                    ("tianti_hp",),
                    {"tianti_hp": 73},
                    database_alias="player_data",
                )
            with sqlite3.connect(player_database) as conn:
                self.assertEqual(
                    conn.execute("SELECT tianti_hp FROM tianti_info WHERE user_id='u'").fetchone()[0],
                    73,
                )

    def test_upsert_rejects_unknown_database_alias(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with DatabaseUnitOfWork(Path(directory) / "player.db") as uow:
                with self.assertRaisesRegex(ValueError, "database alias"):
                    upsert_tianti_profile(
                        uow, "u", ("tianti_hp",), {"tianti_hp": 1}, database_alias="other"
                    )


if __name__ == "__main__":
    unittest.main()
