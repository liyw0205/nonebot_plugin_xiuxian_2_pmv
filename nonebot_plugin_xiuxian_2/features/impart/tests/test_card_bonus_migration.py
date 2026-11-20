from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, Migration, MigrationRunner
from ..migrations import apply_impart_base_schema, apply_impart_card_bonus_recalculation


class ImpartCardBonusMigrationTests(unittest.TestCase):
    def test_recalculates_existing_roles_without_changing_inventory_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "impart.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_impart_base_schema(uow)
                uow.execute(
                    "INSERT INTO xiuxian_impart(user_id,impart_atk_per) VALUES(?,?)",
                    ("with-cards", 0.1),
                )
                uow.execute(
                    "INSERT INTO xiuxian_impart(user_id,impart_atk_per) VALUES(?,?)",
                    ("without-cards", 0.9),
                )
                uow.execute(
                    "INSERT INTO impart_cards(user_id,card_name,quantity) VALUES(?,?,?)",
                    ("with-cards", "百慕达奶油", 2),
                )

            runner = MigrationRunner(
                (
                    Migration(
                        "impart.013",
                        "impart_card_bonus_recalculation",
                        apply_impart_card_bonus_recalculation,
                    ),
                )
            )
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                self.assertEqual(runner.apply(uow), ["impart.013"])
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                bonuses = {
                    str(row["user_id"]): float(row["impart_atk_per"])
                    for row in uow.query_all(
                        "SELECT user_id,impart_atk_per FROM xiuxian_impart ORDER BY user_id"
                    )
                }
                inventory = tuple(
                    uow.query_one(
                        "SELECT quantity FROM impart_cards WHERE user_id='with-cards' "
                        "AND card_name='百慕达奶油'"
                    ).values()
                )
            self.assertAlmostEqual(bonuses["with-cards"], 0.15)
            self.assertEqual(bonuses["without-cards"], 0.0)
            self.assertEqual(inventory, (2,))

            with DatabaseUnitOfWork(database, immediate=True) as uow:
                self.assertEqual(runner.apply(uow), [])

    def test_migration_is_routed_only_to_impart_database(self) -> None:
        from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database

        migrations = build_migrations()
        for database_key in ("game_db", "player_db", "trade_db", "impart_db"):
            versions = {
                migration.version
                for migration in migrations_for_database(migrations, database_key)
            }
            self.assertEqual("impart.013" in versions, database_key == "impart_db")


if __name__ == "__main__":
    unittest.main()
