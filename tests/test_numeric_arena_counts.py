from __future__ import annotations

import sqlite3
import unittest

from tests import test_arena_challenge_purchase_service as legacy_fixtures
from nonebot_plugin_xiuxian_2.features.arena.tests import test_arena_challenge_purchase_repository as purchase_fixtures
from nonebot_plugin_xiuxian_2.features.arena.tests import test_arena_challenge_ticket_repository as ticket_fixtures
from nonebot_plugin_xiuxian_2.features.arena.repository import ArenaChallengePurchaseSqlRepository
from nonebot_plugin_xiuxian_2.core.errors import DomainError
from tests.test_db_backend import db_backend


class NumericArenaCountsTests(unittest.TestCase):
    @staticmethod
    def snapshot(fixture, receipt):
        result = []
        for path in (fixture.game, fixture.player):
            with sqlite3.connect(path) as conn:
                result.append([line for line in conn.iterdump() if not line.startswith(f'INSERT INTO "{receipt}"')])
        return result

    def test_ticket_scientific_counts_changes_replay_and_full_rollback(self):
        for scenario in ('success', 'stale', 'arena_ignore', 'item_ignore', 'receipt'):
            with self.subTest(scenario=scenario):
                fixture = ticket_fixtures.ArenaChallengeTicketRepositoryTests()
                fixture.setUp()
                self.addCleanup(fixture.tearDown)
                fixture.repo = ArenaChallengePurchaseSqlRepository(fixture.game, fixture.player)
                with db_backend.transaction(fixture.player) as conn:
                    conn.execute('DROP TABLE arena')
                    conn.execute('CREATE TABLE arena(user_id TEXT PRIMARY KEY,daily_challenges_used TEXT,daily_extra_challenges TEXT)')
                    conn.execute("INSERT INTO arena VALUES('u','2e1','2e1')")
                    if scenario == 'stale':
                        conn.execute("UPDATE arena SET daily_challenges_used='21'")
                    if scenario == 'arena_ignore':
                        conn.execute("CREATE TRIGGER refuse_count BEFORE UPDATE ON arena BEGIN SELECT RAISE(IGNORE); END")
                with db_backend.transaction(fixture.game) as conn:
                    if scenario == 'item_ignore':
                        conn.execute("CREATE TRIGGER refuse_item BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END")
                    if scenario == 'receipt':
                        conn.execute("CREATE TRIGGER refuse_ticket_receipt BEFORE INSERT ON arena_challenge_ticket_operations BEGIN SELECT RAISE(ABORT,'receipt refused'); END")
                receipt = 'arena_challenge_ticket_operations' if scenario == 'stale' else ''
                before = self.snapshot(fixture, receipt)
                def use():
                    return fixture.use(expected_challenges_used=20, expected_extra_challenges=20, challenge_cap=30)
                if scenario.endswith('ignore'):
                    with self.assertRaises(DomainError) as caught:
                        use()
                    expected_reason = ('arena_ticket_item_write_rejected' if scenario == 'item_ignore'
                                       else 'arena_ticket_count_write_rejected')
                    self.assertEqual(expected_reason, caught.exception.code)
                elif scenario == 'receipt':
                    with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt refused'):
                        use()
                elif scenario == 'stale':
                    result = use()
                    self.assertEqual(('state_changed', 'challenge_count_changed'), tuple(result[k] for k in ('status', 'reason')))
                    self.assertEqual(result, use())
                else:
                    result = use()
                    self.assertEqual(('applied', 2, 19), tuple(result[k] for k in ('status', 'item_remaining', 'challenges_used')))
                    with db_backend.connection(fixture.player) as conn:
                        self.assertEqual('19', conn.execute('SELECT daily_challenges_used FROM arena').fetchone()[0])
                    before = self.snapshot(fixture, receipt)
                    self.assertEqual('duplicate', use()['status'])
                self.assertEqual(before, self.snapshot(fixture, receipt))

    def test_purchase_scientific_counts_reject_changes_and_roll_back(self):
        for legacy in (False, True):
            for scenario in ('success', 'stale', 'receipt'):
                with self.subTest(legacy=legacy, scenario=scenario):
                    fixture = (legacy_fixtures.ArenaChallengePurchaseServiceTests() if legacy else purchase_fixtures.ArenaChallengePurchaseRepositoryTests())
                    fixture.setUp()
                    self.addCleanup(fixture.tearDown)
                    uid = 'user' if legacy else 'u'
                    stamp = '2026-07-14' if legacy else '2026-09-15'
                    with db_backend.transaction(fixture.player) as conn:
                        conn.execute('DROP TABLE arena')
                        conn.execute('CREATE TABLE arena(user_id TEXT PRIMARY KEY,daily_challenge_buys TEXT,daily_extra_challenges TEXT,last_buy_date TEXT)')
                        conn.execute('INSERT INTO arena VALUES(%s,%s,%s,%s)', (uid, '1e0', '1e0', stamp))
                        if scenario == 'stale':
                            conn.execute("UPDATE arena SET daily_extra_challenges='2'")
                    with db_backend.transaction(fixture.game) as conn:
                        conn.execute('CREATE TABLE IF NOT EXISTS arena_challenge_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,amount INTEGER,cost INTEGER,stone INTEGER,bought INTEGER,extra INTEGER)')
                        if scenario == 'receipt':
                            conn.execute("CREATE TRIGGER refuse_buy_receipt BEFORE INSERT ON arena_challenge_purchase_operations BEGIN SELECT RAISE(ABORT,'receipt refused'); END")
                    before = self.snapshot(fixture, 'arena_challenge_purchase_operations')
                    buy = fixture.purchase if legacy else fixture.buy
                    status = lambda result: result.status if legacy else result['status']
                    if scenario == 'receipt':
                        with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt refused'):
                            buy()
                    elif scenario == 'stale':
                        result = buy()
                        self.assertEqual('state_changed', status(result))
                        self.assertEqual(result, buy())
                    else:
                        self.assertEqual('applied', status(buy()))
                        before = self.snapshot(fixture, 'arena_challenge_purchase_operations')
                        self.assertEqual('duplicate', status(buy()))
                    self.assertEqual(before, self.snapshot(fixture, 'arena_challenge_purchase_operations'))
