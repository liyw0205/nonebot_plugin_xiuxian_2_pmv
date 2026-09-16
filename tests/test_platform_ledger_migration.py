import asyncio
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database import OperationLedger
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


if __name__ == "__main__":
    unittest.main()
