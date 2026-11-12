from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from datetime import datetime
from types import SimpleNamespace

from nonebot_plugin_xiuxian_2.features.economy_ledger.repository import EconomyLedgerSqlRepository
from nonebot_plugin_xiuxian_2.features.activity.read_model_repository import ActivityReadModelSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.features.economy_ledger.contribution_flow import ContributionFlowApplication
from nonebot_plugin_xiuxian_2.features.puppet.field_view import PuppetFieldApplication
from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param


class NumericAggregateConsumerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / 'game.db'

    def test_economy_summary_handles_int64_sum_and_minimum_negative(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute('CREATE TABLE economy_log(id INTEGER PRIMARY KEY,user_id TEXT,source TEXT,action TEXT,stone_delta INTEGER,exp_delta INTEGER)')
            uow.executemany('INSERT INTO economy_log VALUES(?,?,?,?,?,?)', [
                (1, 'a', 'trade', 'buy', 2**62, 2**62),
                (2, 'a', 'trade', 'buy', 2**62, 2**62),
                (3, 'b', 'trade', 'buy', -(2**63), 0),
            ])
        result = EconomyLedgerSqlRepository(self.database).query_page({}, 1, 20)
        self.assertIsNone(result.get('error'))
        self.assertEqual((2**63, 2**63, 0, 2**63), tuple(result['summary'][k] for k in ('stone_in', 'stone_out', 'stone_net', 'exp_total')))
        self.assertEqual(2**64, result['source_stats'][0]['gross_stone'])
        self.assertEqual(3, result['large_rows'][0]['id'])

    def test_activity_collect_sum_and_position_agree_above_int64(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute('CREATE TABLE activity_collect_inventory(activity_key TEXT,user_id TEXT,count INTEGER)')
            uow.executemany('INSERT INTO activity_collect_inventory VALUES(?,?,?)', [
                ('event', 'a', 2**62), ('event', 'a', 2**62), ('event', 'b', 9),
            ])
        repository = ActivityReadModelSqlRepository(self.database)
        self.assertEqual([('a', 2**63), ('b', 9)], [(r['user_id'], r['value']) for r in repository.board_rank('collect', 'event')])
        self.assertEqual((1, 2**63), repository.board_position('collect', 'event', 'a'))
        self.assertEqual((2, 9), repository.board_position('collect', 'event', 'b'))

    def test_contribution_flow_preserves_exact_scientific_amounts(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute('CREATE TABLE economy_log(id INTEGER PRIMARY KEY,user_id TEXT,source TEXT,action TEXT,sect_contribution_delta TEXT,created_at TEXT,detail TEXT)')
            uow.executemany("INSERT INTO economy_log VALUES(?,'a','sect','donate',?,'2026-11-11','{}')", [(1, '1.00000000000000000001e20'), (2, '-1e3')])
        result = ContributionFlowApplication(self.database, clock=SimpleNamespace(now=lambda: datetime(2026, 11, 11))).summary('a')
        self.assertEqual((100000000000000000001, 1000, 99999999999999999001), (result['gained'], result['spent'], result['net']))

    def test_puppet_recent_cost_sums_rows_without_integer_overflow(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute('CREATE TABLE puppet_harvest_operations(user_id TEXT,rewards_json TEXT,stone_cost INTEGER,created_at TEXT)')
            uow.executemany("INSERT INTO puppet_harvest_operations VALUES('a','[]',?,'2026-11-11')", [(2**62,), (2**62,)])
        application = PuppetFieldApplication(self.database, self.database)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            result = application._recent(uow, 'a', datetime(2026, 11, 11))
        self.assertEqual((2, 2**63), (result.count, result.stone_cost))

    def test_activity_money_refuses_ambiguous_real_but_pass_keeps_legacy_semantics(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute('CREATE TABLE activity_point_balance(activity_key TEXT,user_id TEXT,points INTEGER,total_points INTEGER)')
            uow.execute('CREATE TABLE activity_pass_balance(activity_key TEXT,user_id TEXT,level INTEGER,total_exp INTEGER)')
            uow.executemany("INSERT INTO activity_point_balance VALUES('event',?,?,?)", [('a', 1e29, 1e29), ('b', 2e29, 2e29), ('c', 900, 900)])
            uow.executemany("INSERT INTO activity_pass_balance VALUES('event',?,10,?)", [('a', 1e29), ('b', 2e29), ('c', 900)])
        repo = ActivityReadModelSqlRepository(self.database)
        for query in (lambda: repo.board_rank('points', 'event'),
                      lambda: repo.board_position('points', 'event', 'a')):
            with self.assertRaises(DomainError) as caught:
                query()
            self.assertEqual('activity_points_precision_lost_real', caught.exception.details['reason'])
        for source in ('pass',):
            with self.subTest(source=source):
                rows = repo.board_rank(source, 'event')
                self.assertEqual(['b', 'a', 'c'], [r['user_id'] for r in rows])
                for rank, row in enumerate(rows, 1):
                    self.assertEqual((rank, row['value']), repo.board_position(source, 'event', row['user_id']))

    def test_activity_money_boards_order_adjacent_blob_integers(self):
        value = 10**45 + 123
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute('CREATE TABLE activity_point_balance(activity_key TEXT,user_id TEXT,points INTEGER,total_points INTEGER)')
            uow.executemany("INSERT INTO activity_point_balance VALUES('event',?,?,?)", [
                (user, bind_sqlite_param(points, exact=True), bind_sqlite_param(points, exact=True))
                for user, points in [('a', value), ('b', value + 1), ('c', value - 1)]])
        repo = ActivityReadModelSqlRepository(self.database)
        rows = repo.board_rank('points', 'event')
        self.assertEqual(['b', 'a', 'c'], [row['user_id'] for row in rows])
        for rank, row in enumerate(rows, 1):
            self.assertEqual((rank, row['value']), repo.board_position('points', 'event', row['user_id']))

    def test_activity_shop_stock_and_compatibility_helpers_agree_above_int64(self):
        from tests.test_db_backend import db_backend
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute('CREATE TABLE activity_point_balance(activity_key TEXT,user_id TEXT,points INTEGER)')
            uow.execute('CREATE TABLE activity_point_purchase(activity_key TEXT,user_id TEXT,item_key TEXT,count INTEGER)')
            uow.executemany("INSERT INTO activity_point_purchase VALUES('event',?,'item',?)", [('a', 2**62), ('b', 2**62)])
        state = ActivityReadModelSqlRepository(self.database).point_shop_state('a', ['event'])
        self.assertEqual(2**63, state['stock'][('event', 'item')])
        with db_backend.connection(self.database) as conn:
            conn.row_factory = db_backend.Row
            self.assertEqual(2**63, service._get_point_shop_total_purchase(conn.cursor(), 'event', 'item'))
            self.assertEqual({'item': 2**63}, service._get_point_shop_total_purchase_map(conn.cursor(), 'event'))
