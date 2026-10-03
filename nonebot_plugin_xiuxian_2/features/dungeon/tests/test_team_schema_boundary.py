from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_dungeon_team_members_index, apply_dungeon_team_schema
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

    def test_member_projection_backfill_is_bounded_and_tolerates_bad_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE teams(user_id TEXT PRIMARY KEY,members TEXT,version INTEGER)"
                )
                conn.executemany(
                    "INSERT INTO teams(user_id,members,version) VALUES(%s,%s,%s)",
                    (
                        ("team-1", '["u1", "u1", "u2"]', 4),
                        ("team-2", "not-json", 8),
                        ("team-3", "null", 2),
                    ),
                )
            with DatabaseUnitOfWork(database) as uow:
                apply_dungeon_team_members_index(uow)
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                rows = uow.query_all(
                    "SELECT team_id,member_id,version FROM dungeon_team_members "
                    "ORDER BY team_id,member_id"
                )
                indexes = {
                    str(row["name"])
                    for row in uow.query_all(
                        "SELECT name FROM sqlite_master WHERE type='index'"
                    )
                }
            self.assertEqual(
                rows,
                [
                    {"team_id": "team-1", "member_id": "u1", "version": 4},
                    {"team_id": "team-1", "member_id": "u2", "version": 4},
                ],
            )
            self.assertIn("dungeon_team_members_member_idx", indexes)

    def test_mutation_requires_member_projection_and_syncs_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
                conn.execute("INSERT INTO user_xiuxian VALUES('leader')")
                conn.execute(
                    "CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_status TEXT)"
                )
                conn.execute("INSERT INTO player_dungeon_status VALUES('leader','not_started')")
            with DatabaseUnitOfWork(database) as uow:
                from ..migrations import apply_dungeon_team

                apply_dungeon_team(uow)
            repository = DungeonTeamRepository(database)
            self.assertEqual(
                repository.create("missing-index", "team-1", "试炼队", "leader", "g", "now", 1).status,
                "schema_missing",
            )
            with DatabaseUnitOfWork(database) as uow:
                apply_dungeon_team_members_index(uow)
            self.assertEqual(
                repository.create("create", "team-1", "试炼队", "leader", "g", "now", 1).status,
                "applied",
            )
            snapshot = repository.snapshot("team-1")
            self.assertEqual(snapshot.version, 0)
            self.assertEqual(
                repository.transfer("transfer", "leader", "leader", snapshot).status,
                "self_target",
            )
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                projection = uow.query_one(
                    "SELECT team_id,member_id,version FROM dungeon_team_members"
                )
            self.assertEqual(
                projection,
                {"team_id": "team-1", "member_id": "leader", "version": 0},
            )

    def test_complete_team_schema_missing_status_dependency_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
                conn.execute("INSERT INTO user_xiuxian VALUES('leader')")
            with DatabaseUnitOfWork(database) as uow:
                from ..migrations import apply_dungeon_team

                apply_dungeon_team(uow)
                apply_dungeon_team_members_index(uow)
            result = DungeonTeamRepository(database).create(
                "missing-status", "team-1", "试炼队", "leader", "g", "now", 1
            )
            self.assertEqual(result.status, "schema_missing")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS count FROM teams")["count"], 0
                )
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM sqlite_master WHERE name='player_dungeon_status'"
                    )
                )


if __name__ == "__main__":
    unittest.main()
