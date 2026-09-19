import asyncio
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database import OperationLedger, OutboxStore
from nonebot_plugin_xiuxian_2.plugin import build_lifecycle
from tests.bootstrap import copy_static_data


class PlatformLedgerMigrationTests(unittest.TestCase):
    def test_ledger_read_does_not_create_schema_before_startup(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.db"
            with DatabaseUnitOfWork(database) as uow:
                with self.assertRaises(Exception):
                    OperationLedger().get(uow, "missing", "test.action")
                table = uow.query_one(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='operation_ledger'"
                )
            self.assertIsNone(table)

    def test_outbox_read_does_not_create_schema_before_startup(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.db"
            with DatabaseUnitOfWork(database) as uow:
                with self.assertRaises(Exception):
                    OutboxStore().pending(uow)
                table = uow.query_one(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='domain_outbox'"
                )
            self.assertIsNone(table)

    def test_startup_creates_shared_schema_for_every_database(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            lifecycle, _, context = build_lifecycle(
                build_runtime_context(data_dir=data_dir, legacy_startup=False)
            )
            state = asyncio.run(lifecycle.start())
            try:
                self.assertEqual(state.phase.value, "ready")
                for spec in context.database.specs():
                    with DatabaseUnitOfWork(spec.path) as uow:
                        rows = uow.query_all(
                            "SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name IN ('operation_ledger', 'operation_audit', 'domain_outbox')"
                        )
                    self.assertEqual({row["name"] for row in rows}, {"operation_ledger", "operation_audit", "domain_outbox"})
            finally:
                asyncio.run(lifecycle.shutdown())

    def test_startup_routes_arena_weekly_rank_schema_to_player_database(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            lifecycle, _, context = build_lifecycle(
                build_runtime_context(data_dir=data_dir, legacy_startup=False)
            )
            state = asyncio.run(lifecycle.start())
            try:
                self.assertEqual(state.phase.value, "ready")
                names = (
                    "arena_weekly_rank_reduction_operations",
                    "arena_weekly_rank_reduction_targets",
                )
                for spec in context.database.specs():
                    with DatabaseUnitOfWork(spec.path) as uow:
                        rows = uow.query_all(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?, ?)",
                            names,
                        )
                    expected = set(names) if spec.key == "player_db" else set()
                    self.assertEqual({row["name"] for row in rows}, expected)
            finally:
                asyncio.run(lifecycle.shutdown())

    def test_startup_routes_arena_daily_reward_schemas_to_owned_databases(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            lifecycle, _, context = build_lifecycle(
                build_runtime_context(data_dir=data_dir, legacy_startup=False)
            )
            state = asyncio.run(lifecycle.start())
            try:
                self.assertEqual(state.phase.value, "ready")
                for spec in context.database.specs():
                    with DatabaseUnitOfWork(spec.path) as uow:
                        operations = uow.query_one(
                            "SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name='arena_season_reward_operations'"
                        )
                        arena_columns = {
                            str(row[1]) for row in uow.execute("PRAGMA table_info(arena)").fetchall()
                        }
                    if spec.key == "game_db":
                        self.assertIsNotNone(operations)
                    else:
                        self.assertIsNone(operations)
                    if spec.key == "player_db":
                        self.assertTrue(
                            {
                                "daily_challenges_used",
                                "daily_extra_challenges",
                                "daily_challenge_buys",
                                "last_reset_date",
                                "last_buy_date",
                            }.issubset(arena_columns)
                        )
            finally:
                asyncio.run(lifecycle.shutdown())

    def test_startup_routes_arena_state_schema_only_to_player_database(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            lifecycle, _, context = build_lifecycle(
                build_runtime_context(data_dir=data_dir, legacy_startup=False)
            )
            state = asyncio.run(lifecycle.start())
            try:
                self.assertEqual(state.phase.value, "ready")
                required = {
                    "score", "total_wins", "total_losses", "daily_challenges_used",
                    "daily_extra_challenges", "daily_challenge_buys", "last_reset_date",
                    "last_buy_date", "last_challenge_time", "win_streak", "max_win_streak",
                    "rank", "honor_points", "total_honor_earned", "weekly_purchases",
                }
                for spec in context.database.specs():
                    with DatabaseUnitOfWork(spec.path) as uow:
                        operation = uow.query_one(
                            "SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name='arena_state_operations'"
                        )
                        columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(arena)").fetchall()}
                    if spec.key == "player_db":
                        self.assertIsNotNone(operation)
                        self.assertTrue(required.issubset(columns))
                    else:
                        self.assertIsNone(operation)
            finally:
                asyncio.run(lifecycle.shutdown())

    def test_startup_routes_tower_state_schema_only_to_player_database(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            lifecycle, _, context = build_lifecycle(
                build_runtime_context(data_dir=data_dir, legacy_startup=False)
            )
            state = asyncio.run(lifecycle.start())
            try:
                self.assertEqual(state.phase.value, "ready")
                required = {"current_floor", "max_floor", "score", "weekly_purchases"}
                for spec in context.database.specs():
                    with DatabaseUnitOfWork(spec.path) as uow:
                        operation = uow.query_one(
                            "SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name='tower_state_operations'"
                        )
                        columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(tower)").fetchall()}
                    if spec.key == "player_db":
                        self.assertIsNotNone(operation)
                        self.assertTrue(required.issubset(columns))
                    else:
                        self.assertIsNone(operation)
            finally:
                asyncio.run(lifecycle.shutdown())

    def test_startup_routes_training_state_schema_only_to_player_database(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            lifecycle, _, context = build_lifecycle(
                build_runtime_context(data_dir=data_dir, legacy_startup=False)
            )
            state = asyncio.run(lifecycle.start())
            try:
                self.assertEqual(state.phase.value, "ready")
                for spec in context.database.specs():
                    with DatabaseUnitOfWork(spec.path) as uow:
                        operation = uow.query_one(
                            "SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name='training_state_operations'"
                        )
                    if spec.key == "player_db":
                        self.assertIsNotNone(operation)
                    else:
                        self.assertIsNone(operation)
            finally:
                asyncio.run(lifecycle.shutdown())

    def test_startup_routes_work_daily_refresh_reset_schema_to_game_database(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            lifecycle, _, context = build_lifecycle(
                build_runtime_context(data_dir=data_dir, legacy_startup=False)
            )
            state = asyncio.run(lifecycle.start())
            try:
                self.assertEqual(state.phase.value, "ready")
                for spec in context.database.specs():
                    with DatabaseUnitOfWork(spec.path) as uow:
                        operations = uow.query_one(
                            "SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name='work_daily_refresh_reset_operations'"
                        )
                        targets = uow.query_one(
                            "SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name='work_daily_refresh_reset_targets'"
                        )
                    if spec.key == "game_db":
                        self.assertIsNotNone(operations)
                        self.assertIsNotNone(targets)
                    else:
                        self.assertIsNone(operations)
                        self.assertIsNone(targets)
            finally:
                asyncio.run(lifecycle.shutdown())

    def test_startup_routes_activity_claim_all_schema_to_game_database(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            lifecycle, _, context = build_lifecycle(
                build_runtime_context(data_dir=data_dir, legacy_startup=False)
            )
            state = asyncio.run(lifecycle.start())
            try:
                self.assertEqual(state.phase.value, "ready")
                for spec in context.database.specs():
                    with DatabaseUnitOfWork(spec.path) as uow:
                        operations = uow.query_one("SELECT name FROM sqlite_master WHERE type='table' AND name='activity_claim_all_operations'")
                        steps = uow.query_one("SELECT name FROM sqlite_master WHERE type='table' AND name='activity_claim_all_steps'")
                    if spec.key == "game_db":
                        self.assertIsNotNone(operations)
                        self.assertIsNotNone(steps)
                    else:
                        self.assertIsNone(operations)
                        self.assertIsNone(steps)
            finally:
                asyncio.run(lifecycle.shutdown())

    def test_startup_routes_dungeon_team_schema_to_player_database(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            lifecycle, _, context = build_lifecycle(
                build_runtime_context(data_dir=data_dir, legacy_startup=False)
            )
            state = asyncio.run(lifecycle.start())
            try:
                self.assertEqual(state.phase.value, "ready")
                for spec in context.database.specs():
                    with DatabaseUnitOfWork(spec.path) as uow:
                        operations = uow.query_one("SELECT name FROM sqlite_master WHERE type='table' AND name='dungeon_team_operations'")
                        invites = uow.query_one("SELECT name FROM sqlite_master WHERE type='table' AND name='dungeon_team_invites'")
                    if spec.key == "player_db":
                        self.assertIsNotNone(operations)
                        self.assertIsNotNone(invites)
                    else:
                        self.assertIsNone(operations)
                        self.assertIsNone(invites)
            finally:
                asyncio.run(lifecycle.shutdown())


if __name__ == "__main__":
    unittest.main()
