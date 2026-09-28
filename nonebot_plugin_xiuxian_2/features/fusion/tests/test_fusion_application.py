from __future__ import annotations

import tempfile
import unittest

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema, build_migrations, migrations_for_database
from ..application import FusionApplication
from ..migrations import apply_fusion_operations


class FusionApplicationTest(unittest.TestCase):
    def test_operation_schema_migration_is_game_database_only(self) -> None:
        migrations = build_migrations()
        game = {migration.version for migration in migrations_for_database(migrations, "game_db")}
        player = {migration.version for migration in migrations_for_database(migrations, "player_db")}
        trade = {migration.version for migration in migrations_for_database(migrations, "trade_db")}
        self.assertIn("fusion.002", game)
        self.assertNotIn("fusion.002", player)
        self.assertNotIn("fusion.002", trade)

    def test_settlement_schema_is_created_by_startup_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_fusion_operations(uow)
                tables = {
                    row["name"]
                    for row in uow.query_all(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
                        "('fusion_operations', 'fusion_batch_operations')"
                    )
                }
            self.assertEqual(tables, {"fusion_operations", "fusion_batch_operations"})

    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = FusionApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)


if __name__ == "__main__":
    unittest.main()
