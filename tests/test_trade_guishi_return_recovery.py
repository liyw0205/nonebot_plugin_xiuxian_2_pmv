"""Real SQLite failures must preserve refunds, orders and replay receipts together."""
import unittest

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests import test_trade_guishi_cancel_repository as cancel_fixtures
from tests import test_trade_guishi_expired_repository as expired_fixtures


class ReturnRecovery:
    def snapshot(self):
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            inventory = uow.query_all('SELECT * FROM back ORDER BY user_id,goods_id')
        with DatabaseUnitOfWork(self.trade_database, read_only=True) as uow:
            tables = uow.query_all("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            return inventory, {row['name']: uow.query_all(f"SELECT * FROM {row['name']}") for row in tables}

    def exercise(self, kind):
        for target in ('asset', 'delete', 'receipt'):
            for fault in ('ignore', 'rewrite'):
                with self.subTest(kind=kind, target=target, fault=fault):
                    # Each trigger runs against a fresh migrated fixture.
                    self.build_fixture()
                    try:
                        table = {'asset': 'guishi_info' if kind == 'qiugou' else 'back',
                                 'delete': 'guishi_item', 'receipt': self.receipt_table}[target]
                        database = self.game_database if table == 'back' else self.trade_database
                        event = 'DELETE' if target == 'delete' else 'INSERT'
                        if target == 'asset' and kind in ('qiugou', 'baitan'):
                            event = 'UPDATE'
                        timing = 'BEFORE' if fault == 'ignore' else 'AFTER'
                        if target == 'asset':
                            effect = ('UPDATE guishi_info SET stored_stone=0;' if kind == 'qiugou'
                                      else 'UPDATE back SET goods_num=0,bind_num=0;')
                        elif target == 'delete':
                            effect = ('INSERT INTO guishi_item SELECT OLD.id,OLD.user_id,OLD.item_id,OLD.item_name,'
                                      'OLD.item_type,OLD.price,OLD.quantity,OLD.filled_quantity;' if fault == 'rewrite'
                                      else 'UPDATE guishi_item SET filled_quantity=0;')
                        else:
                            column = 'refunded_stone' if kind == 'qiugou' else 'refunded_quantity'
                            effect = f'UPDATE {table} SET {column}=0; UPDATE guishi_item SET filled_quantity=0;'
                        if fault == 'ignore':
                            effect += ' SELECT RAISE(IGNORE);'
                        with DatabaseUnitOfWork(database) as uow:
                            uow.execute(f'CREATE TRIGGER reject_return {timing} {event} ON {table} BEGIN {effect} END')
                        before = self.snapshot()
                        result = self.call_return(kind)
                        self.assertEqual(self.snapshot(), before, result)
                        reason = {'asset': 'guishi_refund_balance_write_rejected' if kind == 'qiugou'
                                  else 'guishi_refund_inventory_write_rejected',
                                  'delete': 'guishi_refund_order_delete_rejected',
                                  'receipt': 'guishi_refund_receipt_write_rejected'}[target]
                        self.assertEqual((result.status, result.reason, result.subject),
                                         ('state_changed', reason, 'q1' if kind == 'qiugou' else 'b1'))
                        with DatabaseUnitOfWork(database) as uow:
                            uow.execute('DROP TRIGGER reject_return')
                        result = self.call_return(kind)
                        self.assertTrue(result.applied)
                        self.assertEqual(result.refunded_stone if kind == 'qiugou' else result.refunded_quantity,
                                         30 if kind == 'qiugou' else (6 if kind == 'expired' else 4))
                        after = self.snapshot()
                        self.assertEqual(self.call_return(kind).status, 'duplicate')
                        self.assertEqual(self.snapshot(), after)
                        for path in (self.game_database, self.trade_database):
                            with DatabaseUnitOfWork(path, read_only=True) as uow:
                                self.assertEqual(uow.query_all('PRAGMA integrity_check'), [{'integrity_check': 'ok'}])
                    finally:
                        self.temp_dir.cleanup()


class CancelReturnRecoveryTests(ReturnRecovery, unittest.TestCase):
    receipt_table = 'guishi_order_cancel_operations'
    build_fixture = cancel_fixtures.GuishiCancelRepositoryTests.setUp

    def call_return(self, kind):
        if kind == 'qiugou':
            return self.repository.cancel_qiugou(operation_id='recover-qiugou', user_id='buyer', order_id='q1')
        return self.repository.cancel_baitan(operation_id='recover-baitan', user_id='seller', order_id='b1',
                                             goods_type='药材', max_goods_num=99)

    def test_cancel_qiugou_refund_delete_receipt_rejections(self):
        self.exercise('qiugou')

    def test_cancel_baitan_refund_delete_receipt_rejections(self):
        self.exercise('baitan')

    def test_bad_order_and_inventory_values_are_preserved_without_refund(self):
        cases = (('q1', 'quantity', 1.5, 'guishi_refund_order_invalid'),
                 ('q1', 'filled_quantity', 6, 'guishi_refund_order_invalid'),
                 ('q1', 'price', -1, 'guishi_refund_order_invalid'),
                 ('b1', 'quantity', 'bad', 'guishi_refund_order_invalid'),
                 ('b1', 'bind_num', 5, 'inventory_invalid'))
        for order, column, value, reason in cases:
            with self.subTest(order=order, column=column):
                self.build_fixture()
                try:
                    database = self.game_database if column == 'bind_num' else self.trade_database
                    with DatabaseUnitOfWork(database) as uow:
                        if column == 'bind_num':
                            uow.execute('UPDATE back SET bind_num=?', (value,))
                        else:
                            uow.execute(f'UPDATE guishi_item SET {column}=? WHERE id=?', (value, order))
                    before = self.snapshot()
                    result = self.call_return('qiugou' if order == 'q1' else 'baitan')
                    self.assertEqual((result.reason, result.subject), (reason, order))
                    self.assertEqual(self.snapshot(), before)
                finally:
                    self.temp_dir.cleanup()

    def test_refund_preserves_stored_items_and_existing_bound_inventory(self):
        self.build_fixture()
        try:
            with DatabaseUnitOfWork(self.trade_database) as uow:
                uow.execute("UPDATE guishi_info SET items='raw historical storage'")
            with DatabaseUnitOfWork(self.game_database) as uow:
                uow.execute('UPDATE back SET bind_num=2')
            self.assertTrue(self.call_return('qiugou').applied)
            self.assertTrue(self.call_return('baitan').applied)
            with DatabaseUnitOfWork(self.trade_database, read_only=True) as uow:
                self.assertEqual(uow.query_one('SELECT stored_stone,items FROM guishi_info'),
                                 dict(stored_stone=40, items='raw historical storage'))
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                self.assertEqual(uow.query_one('SELECT goods_num,bind_num FROM back'), dict(goods_num=8, bind_num=2))
        finally:
            self.temp_dir.cleanup()

    def test_corrupt_historical_refund_never_replays_as_success(self):
        self.build_fixture()
        try:
            self.call_return('qiugou')
            with DatabaseUnitOfWork(self.trade_database) as uow:
                uow.execute("UPDATE guishi_order_cancel_operations SET refunded_stone='invalid'")
            before = self.snapshot()
            result = self.call_return('qiugou')
            self.assertEqual((result.reason, result.subject), ('guishi_refund_receipt_invalid', 'q1'))
            self.assertEqual(self.snapshot(), before)
        finally:
            self.temp_dir.cleanup()

    def test_late_receipt_trigger_cannot_change_the_refunded_balance(self):
        self.build_fixture()
        try:
            with DatabaseUnitOfWork(self.trade_database) as uow:
                uow.execute('CREATE TRIGGER corrupt_refund AFTER INSERT ON guishi_order_cancel_operations '
                            'BEGIN UPDATE guishi_info SET stored_stone=0; END')
            before = self.snapshot()
            result = self.call_return('qiugou')
            self.assertEqual(result.reason, 'guishi_refund_balance_write_rejected')
            self.assertEqual(self.snapshot(), before)
        finally:
            self.temp_dir.cleanup()

    def test_missing_account_insert_rejection_preserves_the_buy_order(self):
        self.build_fixture()
        try:
            with DatabaseUnitOfWork(self.trade_database) as uow:
                uow.execute('DELETE FROM guishi_info')
                uow.execute('CREATE TRIGGER reject_account BEFORE INSERT ON guishi_info BEGIN SELECT RAISE(IGNORE); END')
            before = self.snapshot()
            self.assertEqual(self.call_return('qiugou').reason, 'guishi_refund_balance_write_rejected')
            self.assertEqual(self.snapshot(), before)
            with DatabaseUnitOfWork(self.trade_database) as uow:
                uow.execute('DROP TRIGGER reject_account')
            self.assertTrue(self.call_return('qiugou').applied)
            with DatabaseUnitOfWork(self.trade_database, read_only=True) as uow:
                self.assertEqual(uow.query_one('SELECT stored_stone FROM guishi_info')['stored_stone'], 30)
        finally:
            self.temp_dir.cleanup()

    def test_no_remaining_quantity_removes_order_without_changing_assets(self):
        for kind in ('qiugou', 'baitan'):
            with self.subTest(kind=kind):
                self.build_fixture()
                try:
                    with DatabaseUnitOfWork(self.trade_database) as uow:
                        uow.execute('UPDATE guishi_item SET filled_quantity=quantity')
                    before = self.snapshot()
                    result = self.call_return(kind)
                    self.assertTrue(result.applied)
                    self.assertEqual((result.refunded_stone, result.refunded_quantity), (0, 0))
                    after = self.snapshot()
                    self.assertEqual(before[0], after[0])
                    self.assertEqual(before[1]['guishi_info'], after[1]['guishi_info'])
                finally:
                    self.temp_dir.cleanup()


class ExpiredReturnRecoveryTests(ReturnRecovery, unittest.TestCase):
    receipt_table = 'guishi_expired_order_operations'
    build_fixture = expired_fixtures.GuishiExpiredRepositoryTests.setUp

    def call_return(self, kind):
        return self.repository.clear_baitan(operation_id='recover', order_id='b1', goods_type='药材',
                                            max_goods_num=99, expected_user_id='seller')

    def test_expiry_refund_delete_receipt_rejections(self):
        self.exercise('expired')

    def test_bad_expired_quantity_and_receipt_are_preserved(self):
        self.build_fixture()
        try:
            with DatabaseUnitOfWork(self.trade_database) as uow:
                uow.execute('UPDATE guishi_item SET filled_quantity=-1')
            before = self.snapshot()
            self.assertEqual(self.call_return('expired').reason, 'guishi_refund_order_invalid')
            self.assertEqual(self.snapshot(), before)
            with DatabaseUnitOfWork(self.trade_database) as uow:
                uow.execute('UPDATE guishi_item SET filled_quantity=4')
            self.assertTrue(self.call_return('expired').applied)
            with DatabaseUnitOfWork(self.trade_database) as uow:
                uow.execute('UPDATE guishi_expired_order_operations SET refunded_quantity=-1')
            before = self.snapshot()
            self.assertEqual(self.call_return('expired').reason, 'guishi_refund_receipt_invalid')
            self.assertEqual(self.snapshot(), before)
        finally:
            self.temp_dir.cleanup()

    def test_expired_refund_keeps_bound_quantity_at_inventory_limit(self):
        self.build_fixture()
        try:
            with DatabaseUnitOfWork(self.game_database) as uow:
                uow.execute("INSERT INTO back VALUES('seller',1001,'old','old',93,'created','updated',7)")
            self.assertTrue(self.call_return('expired').applied)
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                self.assertEqual(uow.query_one('SELECT goods_num,bind_num,create_time FROM back'),
                                 dict(goods_num=99, bind_num=7, create_time='created'))
        finally:
            self.temp_dir.cleanup()
