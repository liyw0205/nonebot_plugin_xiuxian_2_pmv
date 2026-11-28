from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.base.schema_catalog import (
    sqlite_table_columns,
    sqlite_table_names,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import (
    process_read_cache,
)


class SchemaCatalogTests(unittest.TestCase):
    def setUp(self):
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def _database(self, name, definition):
        database = self.root / name
        with DatabaseUnitOfWork(database) as uow:
            uow.execute(definition)
        return database

    def test_warm_catalog_tracks_renamed_and_dropped_tables(self):
        database = self._database("game.db", "CREATE TABLE scores(user_id TEXT)")
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            self.assertIn("scores", sqlite_table_names(uow, database))
            self.assertEqual(
                {"user_id"}, sqlite_table_columns(uow, database, "scores")
            )

        with DatabaseUnitOfWork(database) as uow:
            uow.execute("ALTER TABLE scores RENAME TO renamed_scores")
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            names = sqlite_table_names(uow, database)
            self.assertNotIn("scores", names)
            self.assertIn("renamed_scores", names)
            self.assertEqual(frozenset(), sqlite_table_columns(uow, database, "scores"))
            self.assertEqual(
                {"user_id"}, sqlite_table_columns(uow, database, "renamed_scores")
            )

        with DatabaseUnitOfWork(database) as uow:
            uow.execute("DROP TABLE renamed_scores")
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            self.assertNotIn("renamed_scores", sqlite_table_names(uow, database))
            self.assertEqual(
                frozenset(), sqlite_table_columns(uow, database, "renamed_scores")
            )

    def test_database_replacement_with_same_schema_version_changes_cache_identity(self):
        database = self._database("game.db", "CREATE TABLE scores(old_value TEXT)")
        replacement = self._database("replacement.db", "CREATE TABLE scores(new_value TEXT)")
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            old_version = uow.execute("PRAGMA schema_version").fetchone()[0]
            self.assertEqual(
                {"old_value"}, sqlite_table_columns(uow, database, "scores")
            )
        with DatabaseUnitOfWork(replacement, read_only=True) as uow:
            self.assertEqual(old_version, uow.execute("PRAGMA schema_version").fetchone()[0])

        replacement.replace(database)

        with DatabaseUnitOfWork(database, read_only=True) as uow:
            self.assertEqual(
                {"new_value"}, sqlite_table_columns(uow, database, "scores")
            )

    def test_attached_metadata_is_separated_by_database_and_live_schema_version(self):
        game = self._database("game.db", "CREATE TABLE training(game_value TEXT)")
        player = self._database("player.db", "CREATE TABLE training(player_value TEXT)")
        other = self._database("other.db", "CREATE TABLE training(other_value TEXT)")

        def attached_columns(database):
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                uow.attach_database(database, "player_data")
                self.assertEqual(
                    {"game_value"}, sqlite_table_columns(uow, game, "training")
                )
                return sqlite_table_columns(
                    uow, database, "training", schema="player_data"
                )

        self.assertEqual({"player_value"}, attached_columns(player))
        self.assertEqual({"other_value"}, attached_columns(other))
        with DatabaseUnitOfWork(player) as uow:
            uow.execute("ALTER TABLE training ADD COLUMN current_value TEXT")
        self.assertEqual({"player_value", "current_value"}, attached_columns(player))
        self.assertEqual({"other_value"}, attached_columns(other))


if __name__ == "__main__":
    unittest.main()
