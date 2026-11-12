import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class CurrencyReservationRollbackTests(unittest.TestCase):
    def test_release_matches_full_reservation_and_preserves_settled_or_changed_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'game.db'
            ledger = OperationLedger()
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
                ledger.begin(uow, 'new', 'trade.purchase', {'quantity': 1})
                reservation = ledger.get(uow, 'new', 'trade.purchase')
            with DatabaseUnitOfWork(database) as uow:
                ledger.release_unsettled(uow, reservation)
                self.assertIsNone(ledger.get(uow, 'new', 'trade.purchase'))
            for mutation in ["status='applied'", "status='rejected'", "updated_at='later'", "request_hash='other'"]:
                with self.subTest(mutation=mutation):
                    with DatabaseUnitOfWork(database) as uow:
                        uow.execute('DELETE FROM operation_ledger')
                        ledger.begin(uow, 'new', 'trade.purchase', {'quantity': 1})
                        reservation = ledger.get(uow, 'new', 'trade.purchase')
                        uow.execute(f'UPDATE operation_ledger SET {mutation}')
                        before = ledger.get(uow, 'new', 'trade.purchase')
                    with self.assertRaises(OperationConflictError):
                        with DatabaseUnitOfWork(database) as uow:
                            ledger.release_unsettled(uow, reservation)
                    with DatabaseUnitOfWork(database, read_only=True) as uow:
                        self.assertEqual(before, ledger.get(uow, 'new', 'trade.purchase'))
