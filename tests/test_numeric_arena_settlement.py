from __future__ import annotations

import sqlite3
import unittest

from tests import test_arena_challenge_settlement as fixtures
from tests.test_db_backend import db_backend


class NumericArenaSettlementTests(unittest.TestCase):
    def fixture(self, legacy):
        fixture = fixtures.ArenaChallengeSettlementTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        with db_backend.transaction(fixture.player) as conn:
            conn.execute('ALTER TABLE arena RENAME TO old_arena')
            conn.execute('CREATE TABLE arena(user_id TEXT PRIMARY KEY,score TEXT,total_wins TEXT,total_losses TEXT,win_streak TEXT,max_win_streak TEXT,rank TEXT,daily_challenges_used TEXT,daily_extra_challenges TEXT,last_challenge_time TEXT)')
            conn.execute("INSERT INTO arena SELECT user_id,score||'e0',total_wins||'e0',total_losses||'e0',win_streak||'e0',max_win_streak||'e0',rank,daily_challenges_used||'e0',daily_extra_challenges||'e0',last_challenge_time FROM old_arena")
            conn.execute('DROP TABLE old_arena')
        settle = fixture.settle if legacy else fixture.feature_settle
        return fixture, lambda *a, **kw: self.result(settle(*a, **kw))

    @staticmethod
    def result(value):
        return value if isinstance(value, dict) else vars(value)

    @staticmethod
    def snapshot(fixture, *, receipts=True):
        result = []
        for path in (fixture.game, fixture.player):
            with sqlite3.connect(path) as conn:
                result.append([line for line in conn.iterdump() if receipts or not line.startswith('INSERT INTO "arena_challenge_settlement_operations"')])
        return result

    def test_scientific_records_settle_once_and_replay(self):
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                fixture, settle = self.fixture(legacy)
                first = settle('science')
                self.assertEqual(('applied', 1510, 1590, 3, 5), tuple(first[k] for k in ('status', 'challenger_score', 'opponent_score', 'used', 'stamina')))
                before = self.snapshot(fixture)
                self.assertEqual('duplicate', settle('science')['status'])
                self.assertEqual(before, self.snapshot(fixture))

    def test_actual_committed_change_is_rejected_for_both_players(self):
        for legacy in (False, True):
            for user_id, subject in (('user', 'challenger'), ('other', 'opponent')):
                with self.subTest(legacy=legacy, user_id=user_id):
                    fixture, settle = self.fixture(legacy)
                    with db_backend.transaction(fixture.player) as conn:
                        conn.execute('UPDATE arena SET score=%s WHERE user_id=%s', ('1491e0' if user_id == 'user' else '1601e0', user_id))
                    before = self.snapshot(fixture, receipts=False)
                    result = settle('stale')
                    self.assertEqual('state_changed', result['status'])
                    if not legacy:
                        self.assertEqual(('score_changed', subject), (result['reason'], result['subject']))
                    self.assertEqual(before, self.snapshot(fixture, receipts=False))

    def test_late_receipt_failure_rolls_back_both_databases(self):
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                fixture, settle = self.fixture(legacy)
                with db_backend.transaction(fixture.game) as conn:
                    conn.execute("CREATE TRIGGER reject_numeric_settlement BEFORE INSERT ON arena_challenge_settlement_operations BEGIN SELECT RAISE(ABORT,'receipt rejected'); END")
                before = self.snapshot(fixture)
                with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt rejected'):
                    settle('rollback')
                self.assertEqual(before, self.snapshot(fixture))
