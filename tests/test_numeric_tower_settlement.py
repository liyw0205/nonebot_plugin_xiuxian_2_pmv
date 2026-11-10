from __future__ import annotations

import sqlite3
import unittest

from tests import test_tower_settlement_service as fixtures
from tests.test_db_backend import db_backend
from nonebot_plugin_xiuxian_2.features.tower.repository import TowerPurchaseSqlRepository


class NumericTowerSettlementTests(unittest.TestCase):
    def test_scientific_settlement_stale_invalid_replay_and_rollback(self):
        for legacy in (False, True):
            for scenario in ('success', 'stale', 'invalid', 'receipt'):
                with self.subTest(legacy=legacy, scenario=scenario):
                    fixture = fixtures.TowerSettlementServiceTests()
                    fixture.setUp()
                    self.addCleanup(fixture.tearDown)
                    with db_backend.transaction(fixture.player_database) as conn:
                        conn.execute('DROP TABLE tower')
                        conn.execute('CREATE TABLE tower(user_id TEXT PRIMARY KEY,current_floor TEXT,max_floor TEXT,score TEXT,weekly_purchases TEXT)')
                        conn.execute("INSERT INTO tower VALUES('user','9e0','9e0','5e1','{}')")
                        if scenario == 'stale':
                            conn.execute("UPDATE tower SET score='51'")
                        if scenario == 'invalid':
                            conn.execute("UPDATE tower SET score='invalid'")
                            fixture.expected = {**fixture.expected, 'score': 0}
                    with db_backend.transaction(fixture.game_database) as conn:
                        fixture.service._ensure_ops_table(conn)
                        if scenario == 'receipt':
                            conn.execute("CREATE TRIGGER reject_numeric_tower BEFORE INSERT ON tower_settlement_operations BEGIN SELECT RAISE(ABORT,'tower receipt rejected'); END")
                    if not legacy:
                        fixture.service = TowerPurchaseSqlRepository(fixture.game_database, fixture.player_database)
                    def snapshot(receipts=True):
                        result = []
                        for path in (fixture.game_database, fixture.player_database):
                            with sqlite3.connect(path) as conn:
                                result.append([line for line in conn.iterdump() if receipts or not line.startswith('INSERT INTO "tower_settlement_operations"')])
                        return result
                    before = snapshot(receipts=False)
                    if scenario == 'receipt':
                        before = snapshot()
                        with self.assertRaisesRegex(sqlite3.IntegrityError, 'tower receipt rejected'):
                            fixture.settle()
                        self.assertEqual(before, snapshot())
                        continue
                    result = fixture.settle()
                    status = result.status if legacy else result['status']
                    if scenario in ('stale', 'invalid'):
                        self.assertEqual('state_changed', status)
                        self.assertEqual(before, snapshot(receipts=False))
                    else:
                        self.assertEqual('applied', status)
                        self.assertEqual(((10, 10, 58), (30, 130, 50, 80, 12), (1, 1)), fixture.state())
                        before = snapshot()
                        replay = fixture.settle()
                        self.assertEqual('duplicate', replay.status if legacy else replay['status'])
                        self.assertEqual(before, snapshot())
