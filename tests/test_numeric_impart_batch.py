from __future__ import annotations

import sqlite3
import unittest
from types import SimpleNamespace

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

from tests import test_admin_impart_stone_batch_adjustment as legacy_fixtures
from nonebot_plugin_xiuxian_2.features.admin_asset.tests import test_impart_stone_batch_repository as feature_fixtures


class NumericImpartBatchTests(unittest.TestCase):
    def fixture(self, legacy):
        fixture = (legacy_fixtures.AdminImpartStoneBatchAdjustmentTests() if legacy
                   else feature_fixtures.AdminImpartStoneBatchRepositoryTests())
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        return fixture

    @staticmethod
    def snapshot(fixture, legacy):
        paths = (fixture.game_database, fixture.impart_database) if legacy else (fixture.game, fixture.impart)
        result = []
        for path in paths:
            with sqlite3.connect(path) as conn:
                result.append(list(conn.iterdump()))
        return result

    def test_legal_child_deltas_can_sum_above_sqlite_integer_limit(self):
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                fixture = self.fixture(legacy)
                def adjust():
                    return (fixture.adjust('large', user_ids=('1', '2', '3'), requested_delta=2**62)
                            if legacy else fixture.adjust('large', delta=2**62))
                result = adjust()
                self.assertEqual(('applied', 3, 3 * 2**62, 3), (result.status, result.completed, result.applied_delta, result.affected_users))
                before = self.snapshot(fixture, legacy)
                duplicate = adjust()
                self.assertEqual(('duplicate', 3 * 2**62), (duplicate.status, duplicate.applied_delta))
                self.assertEqual(before, self.snapshot(fixture, legacy))

    def test_progress_replay_can_compare_minimum_integer_delta(self):
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                fixture = self.fixture(legacy)
                if legacy:
                    fixture.adjust('minimum', user_ids=('1',), requested_delta=-1)
                    repository, user_id, path = fixture.service, '1', fixture.game_database
                else:
                    fixture.adjust('minimum', delta=-1)
                    repository, user_id, path = fixture.repository, 'a', fixture.game
                with DatabaseUnitOfWork(path) as uow:
                    uow.execute('UPDATE admin_impart_stone_batch_progress SET applied_delta=? WHERE operation_id=? AND user_id=?', (-(2**63), 'minimum', user_id))
                before = self.snapshot(fixture, legacy)
                result = SimpleNamespace(status='duplicate', previous_stone=2**63, final_stone=0, applied_delta=-(2**63))
                repository._record('minimum', user_id, result)
                self.assertEqual(before, self.snapshot(fixture, legacy))
