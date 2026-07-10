from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import nonebot


nonebot.init()

from nonebot_plugin_xiuxian_2.paths import configure_paths
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import message_db


def _create_legacy_database(path: Path, value: str = "legacy") -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE records (value TEXT NOT NULL)")
        connection.execute("INSERT INTO records (value) VALUES (?)", (value,))
        connection.commit()
    finally:
        connection.close()


class MessageDatabaseMigrationTests(unittest.TestCase):
    def test_path_uses_configured_data_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "runtime-data"
            configure_paths(data_dir)
            self.assertEqual(message_db.get_message_db_path(), data_dir / "message.db")

    def test_legacy_database_is_backed_up_and_copied_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "old-cwd" / "message.db"
            target = root / "data" / "xiuxian" / "message.db"
            legacy.parent.mkdir(parents=True)
            _create_legacy_database(legacy)

            backup = message_db.migrate_legacy_message_db(legacy, target)

            self.assertIsNotNone(backup)
            self.assertFalse(legacy.exists())
            self.assertTrue(target.exists())
            self.assertTrue(backup.exists())
            connection = sqlite3.connect(target)
            try:
                row = connection.execute("SELECT value FROM records").fetchone()
            finally:
                connection.close()
            self.assertEqual(row, ("legacy",))
            self.assertEqual(
                list(target.parent.glob(".message.db.*.migrating")),
                [],
            )

    def test_existing_target_rejects_legacy_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "legacy.db"
            target = root / "message.db"
            _create_legacy_database(legacy, "legacy")
            _create_legacy_database(target, "target")

            with self.assertRaises(message_db.MessageDatabaseMigrationConflict):
                message_db.migrate_legacy_message_db(legacy, target)

            self.assertTrue(legacy.exists())
            connection = sqlite3.connect(target)
            try:
                row = connection.execute("SELECT value FROM records").fetchone()
            finally:
                connection.close()
            self.assertEqual(row, ("target",))

    def test_missing_legacy_database_is_a_noop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = message_db.migrate_legacy_message_db(
                root / "missing.db",
                root / "target.db",
            )
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
