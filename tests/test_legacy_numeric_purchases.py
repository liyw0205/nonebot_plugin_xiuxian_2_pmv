from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path
import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_purchase, apply_arena_purchase_receipt
from nonebot_plugin_xiuxian_2.features.arena.repository import ArenaChallengePurchaseSqlRepository
from nonebot_plugin_xiuxian_2.features.boss.migrations import apply_boss_purchase
from nonebot_plugin_xiuxian_2.features.boss.repository import BossPurchaseSqlRepository
from nonebot_plugin_xiuxian_2.features.training.migrations import apply_training_purchase_operations
from nonebot_plugin_xiuxian_2.features.training.purchase_repository import TrainingPurchaseSqlRepository
from nonebot_plugin_xiuxian_2.features.tower.migrations import apply_tower_purchase
from nonebot_plugin_xiuxian_2.features.tower.repository import TowerPurchaseSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class LegacyNumericPurchaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.weekly = {'_last_reset': '2026-11-11'}

    def setup_group(self, kind, value, suffix=''):
        game, player = self.root / (kind + suffix + '-game.db'), self.root / (kind + suffix + '-player.db')
        table, column, migration, cls = {
            'training': ('training', 'points', apply_training_purchase_operations, TrainingPurchaseSqlRepository),
            'arena': ('arena', 'honor_points', apply_arena_purchase, ArenaChallengePurchaseSqlRepository),
            'boss': ('boss_limit', 'integral', apply_boss_purchase, BossPurchaseSqlRepository),
            'tower': ('tower', 'score', apply_tower_purchase, TowerPurchaseSqlRepository),
        }[kind]
        with DatabaseUnitOfWork(game) as uow:
            uow.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)')
            uow.execute("INSERT INTO user_xiuxian VALUES('u')")
            uow.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,create_time TEXT,update_time TEXT,PRIMARY KEY(user_id,goods_id))')
            migration(uow)
            if kind == 'arena':
                apply_arena_purchase_receipt(uow)
        with DatabaseUnitOfWork(player) as uow:
            uow.execute(f'CREATE TABLE {table}(user_id TEXT PRIMARY KEY,{column} TEXT,weekly_purchases TEXT)')
            uow.execute(f'INSERT INTO {table} VALUES(?,?,?)', ('u', value, json.dumps(self.weekly)))
            if kind == 'boss':
                uow.execute('CREATE TABLE boss(user_id TEXT PRIMARY KEY,weekly_purchases TEXT)')
                uow.execute('INSERT INTO boss VALUES(?,?)', ('u', json.dumps(self.weekly)))
        repo = cls(game, player)
        return repo, game, player, table, column

    def purchase(self, group, expected, operation='op'):
        repo, _game, _player, table, _column = group
        if table == 'training':
            return repo.purchase(operation, 'u', item_id=1, item_name='item', item_type='type', quantity=2,
                unit_cost=10, weekly_limit=9, expected_points=expected, expected_weekly_purchases=self.weekly,
                max_goods_num=99, bind_flag=1, today=date(2026, 11, 11))
        args = (operation, 'u', 1, 'item', 'type', 2, 10, 9, expected, self.weekly, 99)
        if table == 'arena':
            return repo.purchase(*args, 1, today=date(2026, 11, 11))
        return repo.purchase(*args, today=date(2026, 11, 11))

    @staticmethod
    def status(result):
        return result['status'] if isinstance(result, dict) else result.status

    @staticmethod
    def snapshot(group):
        result = []
        for path in group[1:3]:
            with sqlite3.connect(path) as conn:
                result.append(list(conn.iterdump()))
        return result

    def test_text_balances_settle_once_and_replay(self):
        for kind in ('training', 'arena', 'boss', 'tower'):
            for index, value in enumerate(('100', '1e2', '100000000000000000001')):
                with self.subTest(kind=kind, value=value):
                    group = self.setup_group(kind, value, str(index))
                    expected = as_int_like(value)
                    self.assertEqual('applied', self.status(self.purchase(group, expected)))
                    before = self.snapshot(group)
                    self.assertEqual('duplicate', self.status(self.purchase(group, expected)))
                    self.assertEqual(before, self.snapshot(group))
                    with sqlite3.connect(group[2]) as conn:
                        self.assertEqual(expected - 20, as_int_like(conn.execute(f'SELECT {group[4]} FROM {group[3]}').fetchone()[0]))

    def test_real_changes_and_late_failures_never_partially_commit(self):
        for kind in ('training', 'arena', 'boss', 'tower'):
            for failure in ('stale', 'receipt', 'write'):
                with self.subTest(kind=kind, failure=failure):
                    group = self.setup_group(kind, '100000000000000000001', failure)
                    expected = 100000000000000000001
                    if failure == 'stale':
                        with sqlite3.connect(group[2]) as conn:
                            conn.execute(f'UPDATE {group[3]} SET {group[4]}=?', (str(expected + 1),))
                    if failure == 'receipt':
                        with DatabaseUnitOfWork(group[1]) as uow:
                            uow.execute(f"CREATE TRIGGER refuse_receipt BEFORE INSERT ON {kind}_purchase_operations BEGIN SELECT RAISE(ABORT,'receipt refused'); END")
                    if failure == 'write':
                        with DatabaseUnitOfWork(group[2]) as uow:
                            uow.execute(f"CREATE TRIGGER refuse_balance BEFORE UPDATE ON {group[3]} BEGIN SELECT RAISE(IGNORE); END")
                    before = self.snapshot(group)
                    if failure == 'receipt':
                        with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt refused'):
                            self.purchase(group, expected)
                    elif failure == 'write':
                        with self.assertRaises(DomainError) as caught:
                            self.purchase(group, expected)
                        self.assertEqual({'training': 'training_points_write_rejected', 'arena': 'arena_honor_write_rejected', 'boss': 'boss_integral_write_rejected', 'tower': 'tower_purchase_write_rejected'}[kind], caught.exception.code)
                    else:
                        self.assertEqual('state_changed', self.status(self.purchase(group, expected)))
                        # Some repositories persist a rejection receipt; assets stay unchanged.
                        with sqlite3.connect(group[1]) as conn:
                            self.assertEqual(0, conn.execute('SELECT COUNT(*) FROM back').fetchone()[0])
                        with sqlite3.connect(group[2]) as conn:
                            self.assertEqual(str(expected + 1), conn.execute(f'SELECT {group[4]} FROM {group[3]}').fetchone()[0])
                        continue
                    self.assertEqual(before, self.snapshot(group))

    def test_legacy_services_use_the_same_numeric_guard_and_rollback_contract(self):
        from nonebot_plugin_xiuxian_2.compatibility.legacy_arena_transactions import ArenaPurchaseService
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_training.transaction_service import TrainingPurchaseService
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.transaction_service import BossPurchaseService
        for kind, cls in (('training', TrainingPurchaseService), ('arena', ArenaPurchaseService), ('boss', BossPurchaseService)):
            for failure in ('success', 'stale', 'write', 'receipt'):
                with self.subTest(kind=kind, failure=failure):
                    group = self.setup_group(kind, '1e2', 'legacy' + failure)
                    group = (cls(group[1], group[2]), *group[1:])
                    # The compatibility receipts retain their original columns.
                    if kind == 'arena':
                        with DatabaseUnitOfWork(group[1]) as uow:
                            uow.execute('DROP TABLE arena_purchase_operations')
                            apply_arena_purchase(uow)
                    if failure == 'stale':
                        with sqlite3.connect(group[2]) as conn:
                            conn.execute(f'UPDATE {group[3]} SET {group[4]}=?', ('101',))
                    if failure == 'write':
                        with DatabaseUnitOfWork(group[2]) as uow:
                            uow.execute(f"CREATE TRIGGER refuse_balance BEFORE UPDATE ON {group[3]} BEGIN SELECT RAISE(IGNORE); END")
                    if failure == 'receipt':
                        with DatabaseUnitOfWork(group[1]) as uow:
                            uow.execute(f"CREATE TRIGGER refuse_receipt BEFORE INSERT ON {kind}_purchase_operations BEGIN SELECT RAISE(ABORT,'receipt refused'); END")
                    before = self.snapshot(group)
                    if failure == 'write':
                        with self.assertRaises(DomainError) as caught:
                            self.purchase(group, 100)
                        self.assertEqual({'training': 'training_points_write_rejected', 'arena': 'arena_honor_write_rejected', 'boss': 'boss_integral_write_rejected'}[kind], caught.exception.code)
                    elif failure == 'receipt':
                        with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt refused'):
                            self.purchase(group, 100)
                    elif failure == 'stale':
                        self.assertEqual('state_changed', self.status(self.purchase(group, 100)))
                    else:
                        self.assertEqual('applied', self.status(self.purchase(group, 100)))
                        before = self.snapshot(group)
                        self.assertEqual('duplicate', self.status(self.purchase(group, 100)))
                    self.assertEqual(before, self.snapshot(group))


if __name__ == '__main__':
    unittest.main()
