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

    def test_interrupted_migration_file_is_removed_before_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "old" / "message.db"
            target = root / "data" / "xiuxian" / "message.db"
            legacy.parent.mkdir(parents=True)
            target.parent.mkdir(parents=True)
            _create_legacy_database(legacy)
            stale = target.parent / ".message.db.deadbeef.migrating"
            stale.write_bytes(b"interrupted")

            message_db.migrate_legacy_message_db(legacy, target)

            self.assertFalse(stale.exists())
            self.assertTrue(target.exists())

    def test_missing_legacy_database_is_a_noop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = message_db.migrate_legacy_message_db(
                root / "missing.db",
                root / "target.db",
            )
            self.assertIsNone(result)

    def test_size_cleanup_removes_oldest_messages_and_shrinks_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "runtime-data"
            configure_paths(data_dir)
            database = message_db.get_message_db_path()
            conn = message_db.db_backend.connect(database)
            try:
                message_db._ensure_message_db_schema(conn)
                payload = "x" * 32_000
                conn.executemany(
                    """
                    INSERT INTO messages (
                        direction, scene, message_id, content, created_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            "recv",
                            "group",
                            f"message-{index}",
                            payload,
                            f"2026-01-01 00:{index // 60:02d}:{index % 60:02d}",
                        )
                        for index in range(100)
                    ],
                )
                conn.commit()
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                self.assertGreater(message_db._message_db_size_mb(), 1.0)

                deleted = message_db._cleanup_message_db_by_size(conn, 1)

                self.assertGreater(deleted, 0)
                self.assertLess(message_db._message_db_size_mb(), 1.0)
                remaining = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
                self.assertLess(int(remaining), 100)
            finally:
                conn.close()

    def test_size_measurement_includes_wal_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "runtime-data"
            configure_paths(data_dir)
            database = message_db.get_message_db_path()
            database.parent.mkdir(parents=True, exist_ok=True)
            database.write_bytes(b"a" * 1024)
            Path(f"{database}-wal").write_bytes(b"b" * 2048)
            Path(f"{database}-shm").write_bytes(b"c" * 1024)

            self.assertAlmostEqual(message_db._message_db_size_mb(), 4 / 1024, places=6)


if __name__ == "__main__":
    unittest.main()
