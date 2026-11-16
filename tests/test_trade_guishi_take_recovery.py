"""Real SQLite rejection, recovery and historical replay contracts for stored items."""
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests import test_trade_guishi_take_repository as fixtures


class GuishiTakeRecoveryTests(unittest.TestCase):
    setUp = fixtures.GuishiStoredItemTakeRepositoryTests.setUp
    tearDown = fixtures.GuishiStoredItemTakeRepositoryTests.tearDown
    take = fixtures.GuishiStoredItemTakeRepositoryTests.take
    state = fixtures.GuishiStoredItemTakeRepositoryTests.state

    def snapshot(self):
        with DatabaseUnitOfWork(self.trade_database, read_only=True) as uow:
            account = uow.query_all('SELECT * FROM guishi_info')
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            return account, uow.query_all('SELECT * FROM back'), uow.query_all('SELECT * FROM guishi_take_item_operations')

    def test_each_rejected_write_rolls_back_all_effects_then_recovers_once(self):
        for target in ('storage', 'inventory', 'receipt'):
            for fault in ('ignore', 'rewrite'):
                with self.subTest(target=target, fault=fault):
                    with DatabaseUnitOfWork(self.game_database) as uow:
                        uow.execute('DROP TRIGGER IF EXISTS reject_take')
                        uow.execute('DELETE FROM back')
                        uow.execute('DELETE FROM guishi_take_item_operations')
                    with DatabaseUnitOfWork(self.trade_database) as uow:
                        uow.execute('DROP TRIGGER IF EXISTS reject_take')
                        uow.execute("UPDATE guishi_info SET stored_stone=15,items='{\"1001\":3}'")
                    database = self.trade_database if target == 'storage' else self.game_database
                    table = {'storage': 'guishi_info', 'inventory': 'back', 'receipt': 'guishi_take_item_operations'}[target]
                    event = 'UPDATE' if target == 'storage' else 'INSERT'
                    timing = 'BEFORE' if fault == 'ignore' else 'AFTER'
                    statement = {
                        'storage': 'UPDATE guishi_info SET stored_stone=0;',
                        'inventory': 'UPDATE back SET bind_num=0;',
                        'receipt': 'UPDATE back SET bind_num=0; UPDATE guishi_take_item_operations SET quantity=1;',
                    }[target]
                    if fault == 'ignore':
                        statement += ' SELECT RAISE(IGNORE);'
                    elif target == 'storage':
                        # The requested result is {}, so add an unrelated item.
                        statement += " UPDATE guishi_info SET items='{\"1002\":1}';"
                    with DatabaseUnitOfWork(database) as uow:
                        uow.execute(f'CREATE TRIGGER reject_take {timing} {event} ON {table} BEGIN {statement} END')
                    before = self.snapshot()
                    result = self.take('fault')
                    self.assertEqual(self.snapshot(), before)
                    self.assertEqual((result.status, result.reason, result.subject),
                                     ('state_changed', f'guishi_take_{target}_write_rejected', 'user'))
                    with DatabaseUnitOfWork(database) as uow:
                        uow.execute('DROP TRIGGER reject_take')
                    self.assertEqual(self.take('fault').status, 'taken')
                    self.assertEqual(self.state(), (15, {}, (3, 3), 1))
                    after = self.snapshot()
                    self.assertEqual(self.take('fault').status, 'duplicate')
                    self.assertEqual(self.snapshot(), after)

    def test_invalid_storage_is_never_silently_discarded(self):
        cases = (
            ('{bad', 'guishi_storage_invalid', 'user'),
            ('[]', 'guishi_storage_invalid', 'user'),
            ('{"1001":3,"1001":4}', 'guishi_storage_duplicate', 'user'),
            ('{"1001":3,"1002":"invalid"}', 'guishi_storage_quantity_invalid', 'item:1002'),
            ('{"1001":3,"1002":true}', 'guishi_storage_quantity_invalid', 'item:1002'),
            ('{"1001":3,"1002":null}', 'guishi_storage_quantity_invalid', 'item:1002'),
            ('{"1001":1.5}', 'guishi_storage_quantity_invalid', 'item:1001'),
            ('{"1001":-3}', 'guishi_storage_quantity_invalid', 'item:1001'),
            ('{"1001":NaN}', 'guishi_storage_quantity_invalid', 'item:1001'),
        )
        for raw, reason, subject in cases:
            with self.subTest(raw=raw):
                with DatabaseUnitOfWork(self.game_database) as uow:
                    uow.execute('DELETE FROM back')
                    uow.execute('DELETE FROM guishi_take_item_operations')
                with DatabaseUnitOfWork(self.trade_database) as uow:
                    uow.execute('UPDATE guishi_info SET items=?', (raw,))
                before = self.snapshot()
                result = self.take('invalid')
                self.assertEqual(self.snapshot(), before)
                self.assertEqual((result.status, result.reason, result.subject), ('state_changed', reason, subject))

    def test_existing_bound_inventory_and_unrelated_stored_values_are_preserved(self):
        with DatabaseUnitOfWork(self.trade_database) as uow:
            uow.execute('UPDATE guishi_info SET items=?', ('{"1001":"3","1002":"4","1003":0}',))
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute("INSERT INTO back VALUES('user',1001,'old','old',96,'created','updated',7)")
        result = self.take()
        self.assertEqual((result.status, result.quantity), ('taken', 3))
        self.assertEqual(self.state(), (15, {'1002': '4', '1003': 0}, (99, 10), 1))
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self.assertEqual(uow.query_one('SELECT create_time FROM back')['create_time'], 'created')

    def test_invalid_inventory_is_rejected_without_overwriting_it(self):
        for total, bound in ((-1, 0), (3, -1), (3, 4), (1.5, 0), (3, 'invalid')):
            with self.subTest(total=total, bound=bound):
                with DatabaseUnitOfWork(self.game_database) as uow:
                    uow.execute('DELETE FROM back')
                    uow.execute('DELETE FROM guishi_take_item_operations')
                    uow.execute("INSERT INTO back VALUES('user',1001,'item','type',?,NULL,NULL,?)", (total, bound))
                with DatabaseUnitOfWork(self.trade_database) as uow:
                    uow.execute("UPDATE guishi_info SET items='{\"1001\":3}'")
                before = self.snapshot()
                result = self.take('inventory')
                self.assertEqual(self.snapshot(), before)
                self.assertEqual((result.status, result.reason, result.subject), ('state_changed', 'inventory_invalid', 'item:1001'))

    def test_message_receipt_replay_precedes_closed_window_and_rejects_changed_item(self):
        operation_id = 'guishi-item-take:message:user'
        self.assertEqual(self.take(operation_id).status, 'taken')
        before = self.snapshot()
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute('UPDATE guishi_take_item_operations SET operation_id=?', (operation_id + ':1001',))
        historical = self.snapshot()
        args = dict(operation_id=operation_id, user_id='user', goods_id=1001,
                    item_name='测试法器', goods_type='装备', max_goods_num=1, take_open=False)
        self.assertEqual(self.repository.take(**args).status, 'duplicate')
        self.assertEqual(self.snapshot(), historical)
        self.assertEqual(self.repository.take(**{**args, 'goods_id': 1002}).status, 'operation_conflict')
        self.assertEqual(self.repository.take(**{**args, 'operation_id': 'new'}).status, 'weekend_closed')
        self.assertEqual(self.snapshot(), historical)
        self.assertEqual(before[0:2], historical[0:2])

    def test_multiple_historical_receipts_are_ambiguous_and_never_guess_a_winner(self):
        with DatabaseUnitOfWork(self.game_database) as uow:
            for goods_id in (1001, 1002):
                uow.execute('INSERT INTO guishi_take_item_operations(operation_id,user_id,goods_id,item_name,goods_type,quantity) '
                            'VALUES(?,?,?,?,?,?)', (f'guishi-item-take:message:user:{goods_id}', 'user', goods_id, '测试法器', '装备', 3))
        before = self.snapshot()
        self.assertEqual(self.take('guishi-item-take:message:user').status, 'operation_conflict')
        self.assertEqual(self.snapshot(), before)

    def test_invalid_historical_receipt_is_not_rewritten_or_reinterpreted_as_success(self):
        for goods_id, quantity in ((1001, -1), (1001, 1.5), ('invalid', 3), (1001, 'invalid')):
            with self.subTest(goods_id=goods_id, quantity=quantity):
                with DatabaseUnitOfWork(self.game_database) as uow:
                    uow.execute('DELETE FROM guishi_take_item_operations')
                    uow.execute('INSERT INTO guishi_take_item_operations(operation_id,user_id,goods_id,item_name,goods_type,quantity) '
                                'VALUES(?,?,?,?,?,?)', ('bad-receipt', 'user', goods_id, '测试法器', '装备', quantity))
                before = self.snapshot()
                result = self.take('bad-receipt')
                self.assertEqual((result.status, result.reason, result.subject),
                                 ('state_changed', 'guishi_take_receipt_invalid', 'user'))
                self.assertEqual(self.snapshot(), before)

    def test_opaque_message_id_reads_only_exact_legacy_suffix(self):
        operation_id = 'guishi-item-take:opaque:message:user'
        with DatabaseUnitOfWork(self.game_database) as uow:
            for suffix in (':1001', ':other:1002'):
                uow.execute('INSERT INTO guishi_take_item_operations(operation_id,user_id,goods_id,item_name,goods_type,quantity) '
                            'VALUES(?,?,?,?,?,?)', (operation_id + suffix, 'user', 1001, '测试法器', '装备', 3))
        before = self.snapshot()
        self.assertEqual(self.take(operation_id).status, 'duplicate')
        self.assertEqual(self.snapshot(), before)

    def test_clock_failure_after_storage_removal_rolls_back_then_retries(self):
        before = self.snapshot()
        with patch.object(self.repository.clock, 'now', side_effect=RuntimeError('clock fault')):
            with self.assertRaisesRegex(RuntimeError, 'clock fault'):
                self.take('clock')
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.take('clock').status, 'taken')

    def test_receipt_trigger_cannot_corrupt_a_previously_written_inventory(self):
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute('CREATE TRIGGER corrupt_inventory AFTER INSERT ON guishi_take_item_operations '
                        'BEGIN UPDATE back SET goods_num=0,bind_num=0; END')
        before = self.snapshot()
        result = self.take('late-trigger')
        self.assertEqual(self.snapshot(), before)
        self.assertEqual((result.status, result.reason), ('state_changed', 'guishi_take_inventory_write_rejected'))

    def test_application_policy_uses_local_weekend_and_replays_before_current_state(self):
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
        clock = fixtures.FixedClock()
        application = TradeApplication(self.game_database, self.trade_database, clock=clock)
        args = dict(operation_id='weekend', user_id='user', goods_id=1001, item_name='测试法器',
                    goods_type='装备', max_goods_num=99)
        before = self.snapshot()
        with patch.object(clock, 'now', return_value=datetime(2026, 11, 20, 15, 59, tzinfo=timezone.utc)):
            self.assertEqual(application.guishi_take_stored_item(**args).status, 'weekend_closed')
        self.assertEqual(self.snapshot(), before)
        with patch.object(clock, 'now', return_value=datetime(2026, 11, 20, 16, tzinfo=timezone.utc)):
            self.assertEqual(application.guishi_take_stored_item(**args).status, 'taken')
        after = self.snapshot()
        with patch.object(clock, 'now', return_value=datetime(2026, 11, 22, 16, tzinfo=timezone.utc)):
            self.assertEqual(application.guishi_take_stored_item(**args).status, 'duplicate')
            self.assertEqual(application.guishi_take_stored_item(**{**args, 'operation_id': 'new'}).status, 'weekend_closed')
        self.assertEqual(self.snapshot(), after)


if __name__ == '__main__':
    unittest.main()
