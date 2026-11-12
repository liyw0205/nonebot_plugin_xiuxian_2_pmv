from __future__ import annotations

import sqlite3
import unittest

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.features.sign_in.tests import test_sign_in_service as sign_fixture
from tests import test_xianshi_plan_listing_feature as plan_fixture
from tests import test_arena_season_reward_service as arena_fixture


class SignRewardPrecisionTests(unittest.TestCase):
    setUp = sign_fixture.SignInApplicationTests.setUp
    tearDown = sign_fixture.SignInApplicationTests.tearDown

    def test_reward_crosses_int64_and_replay_preserves_exact_balance(self):
        value = 2**63 - 1
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=? WHERE user_id='u1'", (value,))
        args = dict(user_id="u1", operation_id="exact-sign", lower_limit=10, upper_limit=30)
        result = self.application.claim(**args)
        self.assertTrue(result.ok, result.to_dict())
        self.assertEqual(value + 25, result.after["stone"])
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(value + 25, as_int_like(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0]))
        self.assertEqual("replayed", self.application.claim(**args).status)

    def test_bad_real_rejection_preserves_balance_and_receipts(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=? WHERE user_id='u1'", (float(2**63),))
            before = list(conn.iterdump())
        try:
            result = self.application.claim(user_id="u1", operation_id="bad-sign", lower_limit=10, upper_limit=30)
        except DomainError as exc:
            self.assertEqual("stone_precision_lost_real", exc.details["reason"])
        else:
            self.assertFalse(result.ok)
            self.assertEqual("stone_precision_lost_real", result.data.get("reason"))
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(before, list(conn.iterdump()))


class PlanListingPrecisionTests(unittest.TestCase):
    setUp = plan_fixture.XianshiPlanListingFeatureTests.setUp
    tearDown = plan_fixture.XianshiPlanListingFeatureTests.tearDown
    list_plan = plan_fixture.XianshiPlanListingFeatureTests.list_plan
    scalar = plan_fixture.XianshiPlanListingFeatureTests.scalar

    def test_plan_fee_and_large_price_round_trip(self):
        value = 10**45 + 123
        for entry in self.plan:
            entry["price"] = 10**30 + entry["goods_id"]
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=?", (bind_sqlite_param(value, exact=True),))
        result = self.list_plan("precise-plan")
        self.assertTrue(result.succeeded, result)
        fee = sum(entry["price"] * entry["quantity"] * 30 // 100 for entry in self.plan)
        self.assertEqual(fee, result.fee_charged)
        self.assertEqual(value - result.fee_charged, as_int_like(self.scalar("SELECT stone FROM user_xiuxian")))
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(sorted([self.plan[0]["price"]] * 2 + [self.plan[1]["price"]]), sorted(as_int_like(row[0]) for row in conn.execute("SELECT price FROM xianshi_item")))
        replay = self.list_plan("precise-plan")
        self.assertEqual("duplicate", replay.status)
        self.assertEqual(value - result.fee_charged, as_int_like(self.scalar("SELECT stone FROM user_xiuxian")))


class ArenaRewardPrecisionTests(unittest.TestCase):
    setUp = arena_fixture.ArenaSeasonRewardServiceTests.setUp
    tearDown = arena_fixture.ArenaSeasonRewardServiceTests.tearDown
    claim = arena_fixture.ArenaSeasonRewardServiceTests.claim
    reset = arena_fixture.ArenaSeasonRewardServiceTests.reset

    def test_season_grant_and_receipt_preserve_big_honor(self):
        value = 10**30 + 123
        with sqlite3.connect(self.player) as conn:
            conn.execute("UPDATE arena SET honor_points=?,total_honor_earned=?", (bind_sqlite_param(value, exact=True), bind_sqlite_param(value + 1, exact=True)))
        result = self.claim("exact-season", honor=value, total=value + 1)
        self.assertEqual("applied", result.status)
        with sqlite3.connect(self.player) as conn:
            row = conn.execute("SELECT honor_points,total_honor_earned FROM arena").fetchone()
            self.assertEqual((value + 800, value + 801), tuple(as_int_like(v) for v in row))
        replay = self.claim("exact-season", honor=value, total=value + 1)
        self.assertEqual(("duplicate", value + 800), (replay.status, replay.honor_points))


class SqlNumericRejectionTests(unittest.TestCase):
    def test_bank_interest_does_not_round_large_principal(self):
        from datetime import datetime
        from nonebot_plugin_xiuxian_2.features.bank.interest_rules import calculate_interest
        for value in (2**53 + 1, 2**63 + 1, 10**45 + 123):
            with self.subTest(value=value):
                result, hours = calculate_interest(saved_stone=value, saved_at="2026-11-12T10:00:00", settled_at=datetime(2026, 11, 12, 11), rate=1.0)
                self.assertEqual(value, result)
                self.assertEqual(1.0, hours)
    def test_callback_preserves_exact_reason_and_rolls_back_prior_writes(self):
        import tempfile
        from pathlib import Path
        from tests.test_db_backend import db_backend
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "game.db"
            with DatabaseUnitOfWork(db) as uow:
                uow.execute("CREATE TABLE account(user_id TEXT,stone INTEGER)")
                uow.execute("INSERT INTO account VALUES('u',?)", (float(2**63),))
            for backend in (lambda: DatabaseUnitOfWork(db), lambda: db_backend.transaction(db)):
                with self.subTest(backend=backend):
                    with self.assertRaises(DomainError) as caught:
                        with backend() as conn:
                            conn.execute("UPDATE account SET user_id='v'")
                            conn.execute("UPDATE account SET stone=xiuxian_int_store(xiuxian_int_sub(stone,1,'stone','actor'))")
                    self.assertEqual(("currency_precision_unavailable", "stone_precision_lost_real", "actor"), (caught.exception.code, caught.exception.details["reason"], caught.exception.details["subject"]))
                    with sqlite3.connect(db) as conn:
                        self.assertEqual(("u", float(2**63)), conn.execute("SELECT * FROM account").fetchone())

    def test_unknown_sql_error_is_not_reclassified(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as directory:
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                with self.assertRaises(sqlite3.OperationalError):
                    uow.execute("SELECT missing_function(1)")


class LegacyBankMigrationPrecisionTests(unittest.TestCase):
    def test_current_account_with_receipt_keeps_authority_over_unrecoverable_old_mirror(self):
        import tempfile
        from pathlib import Path
        from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts, apply_bank_legacy_accounts
        value = 10**45 + 123
        with tempfile.TemporaryDirectory() as directory:
            game, player = Path(directory) / 'game.db', Path(directory) / 'player.db'
            with DatabaseUnitOfWork(game) as uow:
                apply_bank_accounts(uow)
                uow.execute("INSERT INTO bank_accounts VALUES(?,?, '2','new')", ('u', bind_sqlite_param(value, exact=True)))
                uow.execute("INSERT INTO bank_account_operations VALUES('op','u','[]',0,0,0,0,'new')")
            with sqlite3.connect(player) as conn:
                conn.execute('CREATE TABLE bankinfo(user_id TEXT,savestone,savetime TEXT,banklevel TEXT)')
                conn.execute("INSERT INTO bankinfo VALUES('u',?,'old','1')", (float(2**63),))
            with DatabaseUnitOfWork(game) as uow:
                apply_bank_legacy_accounts(uow)
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertEqual(value, as_int_like(uow.query_one('SELECT saved_stone FROM bank_accounts')['saved_stone']))
                self.assertEqual(1, uow.query_one('SELECT retained_rows FROM bank_account_legacy_migration_audit')['retained_rows'])

    def test_scientific_history_imports_exactly_and_replays_without_rewriting_source(self):
        import tempfile
        from pathlib import Path
        from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts, apply_bank_legacy_accounts
        value = 10**45 + 123
        with tempfile.TemporaryDirectory() as directory:
            game, player = Path(directory) / 'game.db', Path(directory) / 'player.db'
            with DatabaseUnitOfWork(game) as uow:
                apply_bank_accounts(uow)
            with sqlite3.connect(player) as conn:
                conn.execute('CREATE TABLE bankinfo(user_id TEXT,savestone,savetime TEXT,banklevel TEXT)')
                conn.execute("INSERT INTO bankinfo VALUES('u',?,'old','1')", (str(value) + 'e0',))
                before = tuple(conn.iterdump())
            for _ in range(2):
                with DatabaseUnitOfWork(game) as uow:
                    apply_bank_legacy_accounts(uow)
                with DatabaseUnitOfWork(game, read_only=True) as uow:
                    self.assertEqual(value, as_int_like(uow.query_one('SELECT saved_stone FROM bank_accounts')['saved_stone']))
                with sqlite3.connect(player) as conn:
                    self.assertEqual(before, tuple(conn.iterdump()))

    def test_bad_history_rejects_whole_backfill_with_specific_reason_and_zero_writes(self):
        import tempfile
        from pathlib import Path
        from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts, apply_bank_legacy_accounts
        for bad, reason in ((float(2**53), 'saved_stone_precision_lost_real'),
                            (float(2**63), 'saved_stone_precision_lost_real'),
                            ('bad', 'saved_stone_invalid_numeric_value')):
            with self.subTest(bad=bad), tempfile.TemporaryDirectory() as directory:
                game, player = Path(directory) / 'game.db', Path(directory) / 'player.db'
                with DatabaseUnitOfWork(game) as uow:
                    apply_bank_accounts(uow)
                with sqlite3.connect(player) as conn:
                    conn.execute('CREATE TABLE bankinfo(user_id TEXT,savestone,savetime TEXT,banklevel TEXT)')
                    conn.executemany("INSERT INTO bankinfo VALUES(?,?,'old','1')", [('first', 100), ('bad-row', bad)])
                def snapshot():
                    with sqlite3.connect(game) as conn, sqlite3.connect(player) as source:
                        return tuple(conn.iterdump()), tuple(source.iterdump())
                before = snapshot()
                with self.assertRaises(DomainError) as caught:
                    with DatabaseUnitOfWork(game) as uow:
                        apply_bank_legacy_accounts(uow)
                self.assertEqual((reason, 'bad-row'), (caught.exception.details['reason'], caught.exception.details['subject']))
                self.assertEqual(before, snapshot())


class SharingRewardPrecisionTests(unittest.TestCase):
    from nonebot_plugin_xiuxian_2.features.dufang.tests.test_share_repository import DufangShareRepositoryTests as Fixture
    setUp = Fixture.setUp
    tearDown = Fixture.tearDown
    migrate = Fixture.migrate
    settle = Fixture.settle
    stones = Fixture.stones
    sharing_stats = Fixture.sharing_stats

    def test_large_sharing_reward_progress_ledger_and_statistic_replay(self):
        value = 10**45 + 123
        result = self.settle(amount=value)
        self.assertEqual(value * 2, result.total_amount)
        self.assertEqual((value + 100, value + 20), (self.stones()['u1'], self.stones()['u2']))
        self.assertEqual(value * 2, self.sharing_stats()['source'][0])
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual([value, value], [as_int_like(row['stone_delta']) for row in uow.query_all('SELECT stone_delta FROM economy_log')])
        replay = self.settle(amount=value)
        self.assertEqual(('duplicate', value * 2), (replay.status, replay.total_amount))
        self.assertEqual(value * 2, self.sharing_stats()['source'][0])


if __name__ == "__main__":
    unittest.main()
