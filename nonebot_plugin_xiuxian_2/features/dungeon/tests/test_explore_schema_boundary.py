from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_dungeon_explore_player_schema
from ..repository import DungeonSessionSqlRepository
from ..reset_repository import DungeonResetSqlRepository
from tests.test_db_backend import db_backend


class DungeonExploreSchemaBoundaryTests(unittest.TestCase):
    def test_player_startup_schema_preserves_existing_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE player_dungeon_status("
                    "user_id TEXT PRIMARY KEY,dungeon_status TEXT,current_layer INTEGER)"
                )
                conn.execute(
                    "INSERT INTO player_dungeon_status(user_id,dungeon_status,current_layer) "
                    "VALUES('u','exploring',2)"
                )

            with DatabaseUnitOfWork(database) as uow:
                apply_dungeon_explore_player_schema(uow)

            with DatabaseUnitOfWork(database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT user_id,dungeon_status,current_layer,reset_generation "
                    "FROM player_dungeon_status WHERE user_id='u'"
                )
                tables = {
                    str(item["name"])
                    for item in uow.query_all(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            self.assertEqual(
                (row["user_id"], row["dungeon_status"], row["current_layer"]),
                ("u", "exploring", 2),
            )
            self.assertEqual(row["reset_generation"], 0)
            self.assertTrue(
                {"dungeon_global_state", "dungeon_reset_operations"} <= tables
            )

    def test_missing_schema_fails_closed_without_creating_databases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            repository = DungeonSessionSqlRepository(game, player)

            prepared = repository.prepare("op", "u", {"members": [{"user_id": "u"}]})
            settled = repository.settle("op", "u", 99)

            self.assertEqual(prepared["status"], "schema_missing")
            self.assertEqual(settled["status"], "schema_missing")
            self.assertFalse(game.exists())
            self.assertFalse(player.exists())

    def test_prepared_operation_stays_retryable_when_player_schema_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute(
                    "CREATE TABLE dungeon_explore_operations("
                    "operation_id TEXT PRIMARY KEY,request_identity TEXT,phase TEXT,"
                    "prepared_json TEXT,result_status TEXT,result_json TEXT,"
                    "current_layer INTEGER,dungeon_status TEXT)"
                )
                conn.execute(
                    "INSERT INTO dungeon_explore_operations VALUES(?,?,?,?,?,?,?,?)",
                    (
                        "op",
                        json.dumps({"action": "explore", "user_id": "u"}),
                        "prepared",
                        json.dumps({"members": [{"user_id": "u"}]}),
                        "",
                        "{}",
                        0,
                        "exploring",
                    ),
                )
            with db_backend.transaction(player):
                pass

            result = DungeonSessionSqlRepository(game, player).settle("op", "u", 99)

            self.assertEqual((result["status"], result["phase"]), ("schema_missing", "prepared"))
            with db_backend.connection(game) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT phase FROM dungeon_explore_operations WHERE operation_id='op'"
                    ).fetchone()[0],
                    "prepared",
                )

    def test_explore_player_migration_routes_to_player_database(self) -> None:
        from ....plugin import build_migrations, migrations_for_database

        catalog = build_migrations()
        game = {item.version for item in migrations_for_database(catalog, "game_db")}
        player = {item.version for item in migrations_for_database(catalog, "player_db")}
        self.assertIn("dungeon.007", player)
        self.assertNotIn("dungeon.007", game)

    def test_reset_and_status_paths_fail_closed_without_request_schema_creation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            repository = DungeonResetSqlRepository(database)

            reset = repository.reset("reset-1", "2026-10-03", "daily", lambda: {})
            state = repository.global_state()
            with self.assertRaisesRegex(RuntimeError, "schema_missing"):
                repository.ensure_player_status("u")

            self.assertEqual(reset.status, "schema_missing")
            self.assertIsNone(state)
            self.assertFalse(database.exists())


if __name__ == "__main__":
    unittest.main()
