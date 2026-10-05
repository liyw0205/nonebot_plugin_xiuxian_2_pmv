from __future__ import annotations

import tempfile
import unittest

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..application import DongfuApplication
from ..harvest_repository import DongfuHarvestResult


class DongfuApplicationTest(unittest.TestCase):
    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
            app = DongfuApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)

    def test_harvest_dispatches_outbox_and_reports_pending_effects(self) -> None:
        class Repository:
            def harvest(self, **kwargs):
                return DongfuHarvestResult("harvested", effects_event_id="dongfu.harvest.effects:op")

        class Effects:
            event_id = None

            def dispatch(self, event_id):
                self.event_id = event_id
                return False

        effects = Effects()
        app = DongfuApplication("unused.db", repository=Repository(), game_event_effects=effects)

        result = app.harvest(operation_id="op")

        self.assertEqual(effects.event_id, "dongfu.harvest.effects:op")
        self.assertTrue(result.effects_pending)


if __name__ == "__main__":
    unittest.main()
