from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, MigrationRunner
from ....plugin import build_migrations, migrations_for_database
from ..accelerate_repository import DongfuAccelerateSqlRepository
from ..array_repository import DongfuArrayUpgradeSqlRepository
from ..expansion_repository import DongfuExpansionSqlRepository
from ..fertilize_repository import DongfuFertilizeSqlRepository
from ..harvest_repository import DongfuHarvestSqlRepository
from ..operation_schema import OPERATION_TABLE_COLUMNS
from ..patrol_repository import DongfuPatrolSqlRepository
from ..plant_repository import DongfuPlantSqlRepository
from ..visit_reward_repository import DongfuVisitRewardSqlRepository


class DongfuOperationSchemaTests(unittest.TestCase):
    @staticmethod
    def _calls(game: Path, player: Path):
        return (
            DongfuAccelerateSqlRepository(game, player).accelerate("a", "u", "[]", 1, 1, "now", "later"),
            DongfuArrayUpgradeSqlRepository(game, player).upgrade("b", "u", 1, 2, 10, 1, 1),
            DongfuExpansionSqlRepository(game, player).expand("c", "u", 1, 3, 6, 20),
            DongfuFertilizeSqlRepository(game, player).fertilize("d", "u", "[]", 1, 1, 3),
            DongfuHarvestSqlRepository(game, player).harvest("e", "u", "[]", [], [], 99, "now"),
            DongfuPatrolSqlRepository(game, player).patrol("f", "u", "day", 1, 1, 1),
            DongfuPlantSqlRepository(game, player).plant("g", "u", "[]", 1, 1, "seed", "now", "later"),
            DongfuVisitRewardSqlRepository(game, player).reward("h", "u", "other", 1),
        )

    def test_missing_database_or_operation_schema_fails_without_creating_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            missing_game = Path(directory) / "missing-game.db"
            missing_player = Path(directory) / "missing-player.db"
            self.assertEqual(
                {result.status for result in self._calls(missing_game, missing_player)},
                {"schema_missing"},
            )
            self.assertFalse(missing_game.exists())
            self.assertFalse(missing_player.exists())

            game, player = Path(directory) / "game.db", Path(directory) / "player.db"
            for database in (game, player):
                with DatabaseUnitOfWork(database):
                    pass
            self.assertEqual(
                {result.status for result in self._calls(game, player)},
                {"schema_missing"},
            )
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                tables = {
                    str(row["name"])
                    for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertFalse(tables & set(OPERATION_TABLE_COLUMNS))

    def test_startup_migration_is_game_owned_idempotent_and_preserves_receipts(self):
        migrations = build_migrations()
        game_migrations = migrations_for_database(migrations, "game_db")
        player_migrations = migrations_for_database(migrations, "player_db")
        action_migration = next(item for item in migrations if item.version == "dongfu.004")
        replay_migration = next(item for item in migrations if item.version == "dongfu.005")
        self.assertIn(action_migration, game_migrations)
        self.assertIn(replay_migration, game_migrations)
        self.assertNotIn(action_migration, player_migrations)
        self.assertNotIn(replay_migration, player_migrations)

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            runner = MigrationRunner((action_migration, replay_migration))
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(runner.apply(uow), ["dongfu.004", "dongfu.005"])
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "INSERT INTO dongfu_plant_operations(operation_id,payload) VALUES(?,?)",
                    ("plant-1", "u|1|2"),
                )
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(runner.apply(uow), [])
                self.assertEqual(
                    uow.query_one(
                        "SELECT payload FROM dongfu_plant_operations WHERE operation_id=?",
                        ("plant-1",),
                    )["payload"],
                    "u|1|2",
                )
                for table, expected in OPERATION_TABLE_COLUMNS.items():
                    rows = uow.query_all(f'PRAGMA main.table_info("{table}")')
                    self.assertTrue(expected.issubset({str(row["name"]) for row in rows}), table)
                    self.assertTrue(
                        any(str(row["name"]) == "operation_id" and int(row["pk"]) == 1 for row in rows),
                        table,
                    )


if __name__ == "__main__":
    unittest.main()
