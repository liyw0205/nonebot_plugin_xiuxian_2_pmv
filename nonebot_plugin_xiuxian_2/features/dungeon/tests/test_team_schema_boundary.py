from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_dungeon_team_schema
from ..team_repository import DungeonTeamRepository
from tests.test_db_backend import db_backend


class DungeonTeamSchemaBoundaryTests(unittest.TestCase):
    def test_startup_schema_preserves_legacy_team_rows_and_prepares_receipts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE teams(user_id TEXT PRIMARY KEY,members TEXT)"
                )
                conn.execute(
                    "INSERT INTO teams(user_id,members) VALUES('team-1','[\"u\"]')"
                )

            with DatabaseUnitOfWork(database) as uow:
                apply_dungeon_team_schema(uow)

            with DatabaseUnitOfWork(database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT user_id,members,version,max_members FROM teams WHERE user_id='team-1'"
                )
                tables = {
                    str(item["name"])
                    for item in uow.query_all(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            self.assertEqual((row["user_id"], row["members"]), ("team-1", '["u"]'))
            self.assertEqual((row["version"], row["max_members"]), (0, 4))
            self.assertTrue(
                {
                    "dungeon_team_operations",
                    "dungeon_team_invites",
                    "team_cd",
                    "dungeon_team_exit_operations",
                } <= tables
            )

    def test_missing_schema_fails_closed_without_creating_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.db"
            repository = DungeonTeamRepository(database)

            self.assertIsNone(repository.team_info("team-1"))
            self.assertIsNone(repository.team_id_for_user("u"))
            self.assertIsNone(repository.pending_invite("u", 100))
            created = repository.create("create-1", "team-1", "试炼队", "u", "100", "now", 100)

            self.assertEqual(created.status, "schema_missing")
            self.assertFalse(database.exists())

    def test_existing_partial_schema_does_not_get_mutated_by_feature_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "partial.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE teams(user_id TEXT PRIMARY KEY)")

            result = DungeonTeamRepository(database).create(
                "create-1", "team-1", "试炼队", "u", "100", "now", 100
            )

            self.assertEqual(result.status, "schema_missing")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertIsNone(
                    uow.query_one(
                        "SELECT name FROM sqlite_master "
                        "WHERE type='table' AND name='dungeon_team_operations'"
                    )
                )

    def test_team_migration_routes_to_player_database(self) -> None:
        from ....plugin import build_migrations, migrations_for_database

        catalog = build_migrations()
        game = {item.version for item in migrations_for_database(catalog, "game_db")}
        player = {item.version for item in migrations_for_database(catalog, "player_db")}
        self.assertIn("dungeon.006", player)
        self.assertNotIn("dungeon.006", game)


if __name__ == "__main__":
    unittest.main()
