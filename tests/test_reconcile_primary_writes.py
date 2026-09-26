from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
from nonebot_plugin_xiuxian_2.infrastructure.database import (
    DatabaseUnitOfWork,
    OperationLedger,
    OutboxStore,
    ReconcileService,
)


class ReconcilePrimaryWriteTests(unittest.TestCase):
    def test_operation_handler_can_write_primary_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            action = "test.reconcile"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                ledger = OperationLedger()
                outbox = OutboxStore()
                ledger.ensure_schema(uow)
                outbox.ensure_schema(uow)
                ledger.begin(uow, "op", action, {"value": 1})
                outbox.append(
                    uow,
                    event_id=f"op:{action}",
                    aggregate_type="test",
                    aggregate_id="op",
                    event_type=action,
                    payload={"value": 1},
                )

            def handler(record):
                with DatabaseUnitOfWork(database, immediate=True) as callback_uow:
                    callback_uow.execute(
                        "CREATE TABLE IF NOT EXISTS handler_results(value INTEGER NOT NULL)"
                    )
                    callback_uow.execute("INSERT INTO handler_results VALUES(1)")
                return OperationOutcome.applied("op", action, data={"reconciled": True})

            with DatabaseUnitOfWork(database, immediate=True) as uow:
                report = ReconcileService().run(uow, operation_handlers={action: handler})

            self.assertTrue(report.clean, report.to_dict())
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS n FROM handler_results")["n"], 1
                )
                self.assertEqual(
                    uow.query_one(
                        "SELECT status FROM operation_ledger WHERE operation_id='op'"
                    )["status"],
                    "applied",
                )
                self.assertEqual(
                    uow.query_one("SELECT status FROM domain_outbox WHERE event_id='op:test.reconcile'")["status"],
                    "sent",
                )


if __name__ == "__main__":
    unittest.main()
