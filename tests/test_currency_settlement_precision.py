from __future__ import annotations

import sqlite3
import unittest

from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
from tests import test_xianshi_fast_purchase_stamina_transaction as trade_fixture
from tests import test_legacy_numeric_purchases as shop_fixture
from nonebot_plugin_xiuxian_2.features.activity.tests import test_point_shop_purchase_repository as activity_fixture
from nonebot_plugin_xiuxian_2.features.boss.integral_repository import BossIntegralSqlRepository
from tests import test_bank_deposit_application as bank_fixture
from tests import test_trade_guishi_deposit_repository as guishi_fixture
from tests import test_trade_guishi_withdraw_repository as withdraw_fixture
from tests import test_xianshi_listing_feature as listing_fixture
from nonebot_plugin_xiuxian_2.features.sect.tests import test_sect_shop_repository as sect_fixture
from nonebot_plugin_xiuxian_2.features.admin_asset.tests import test_stone_repository as admin_fixture
from nonebot_plugin_xiuxian_2.features.stone_gift.tests import test_stone_gift_service as gift_fixture


class XianshiCurrencyPrecisionTests(unittest.TestCase):
    setUp = trade_fixture.XianshiFastPurchaseStaminaTransactionTests.setUp
    tearDown = trade_fixture.XianshiFastPurchaseStaminaTransactionTests.tearDown
    scalar = trade_fixture.XianshiFastPurchaseStaminaTransactionTests.scalar
    purchase = trade_fixture.XianshiFastPurchaseStaminaTransactionTests.purchase

    def test_integer_wallet_debits_and_credits_adjacent_large_values(self):
        buyer, seller = 2**53 + 1, 2**53 + 3
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=? WHERE user_id='buyer'", (buyer,))
            conn.execute("UPDATE user_xiuxian SET stone=? WHERE user_id='seller-1'", (seller,))
            conn.execute("UPDATE xianshi_item SET price=1 WHERE id=?", (self.first_listing,))
        result = self.purchase("exact-small-debit", self.first_listing)
        self.assertEqual("purchased", result.status)
        self.assertEqual(buyer - 1, as_int_like(self.scalar(
            "SELECT stone FROM user_xiuxian WHERE user_id='buyer'")))
        self.assertEqual(seller + 1, as_int_like(self.scalar(
            "SELECT stone FROM user_xiuxian WHERE user_id='seller-1'")))
        before = self.snapshot()
        self.assertEqual("duplicate", self.purchase("exact-small-debit", self.first_listing).status)
        self.assertEqual(before, self.snapshot())

    def snapshot(self):
        with sqlite3.connect(self.database) as conn:
            return list(conn.iterdump())

    def test_feature_large_balances_replay_conflict_and_concurrency(self):
        from concurrent.futures import ThreadPoolExecutor
        from nonebot_plugin_xiuxian_2.features.trade.xianshi_purchase_repository import XianshiPurchaseSqlRepository

        value = 10**45 + 123
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=? WHERE user_id='buyer'", (bind_sqlite_param(value, exact=True),))
            conn.execute("UPDATE xianshi_item SET price=1 WHERE id=?", (self.first_listing,))
        repo = XianshiPurchaseSqlRepository(self.database)

        def purchase():
            return repo.purchase("concurrent-exact", "buyer", self.first_listing, 1, max_goods_num=99)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: purchase(), range(2)))
        self.assertEqual(["duplicate", "purchased"], sorted(result.status for result in results))
        self.assertEqual(value - 1, as_int_like(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='buyer'")))
        self.assertEqual(101, self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='seller-1'"))
        before = self.snapshot()
        conflict = repo.purchase("concurrent-exact", "buyer", self.first_listing, 2, max_goods_num=99)
        self.assertEqual(("state_changed", "operation_conflict"), (conflict.status, conflict.reason))
        self.assertEqual(before, self.snapshot())

    def test_bad_history_and_receipt_fault_have_zero_writes(self):
        from nonebot_plugin_xiuxian_2.core.errors import DomainError
        from nonebot_plugin_xiuxian_2.features.trade.xianshi_purchase_repository import XianshiPurchaseSqlRepository
        repo = XianshiPurchaseSqlRepository(self.database)
        for value, reason in ((float(2**63), "stone_precision_lost_real"), (1.5, "stone_invalid_numeric_value"), ("broken", "stone_invalid_numeric_value")):
            for uid, subject in (("buyer", "buyer"), ("seller-1", "seller")):
                with self.subTest(value=value, subject=subject):
                    with sqlite3.connect(self.database) as conn:
                        conn.execute("UPDATE user_xiuxian SET stone=1000")
                        conn.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=?", (value, uid))
                    before = self.snapshot()
                    with self.assertRaises(DomainError) as caught:
                        repo.purchase("refused", "buyer", self.first_listing, 1, max_goods_num=99, stamina_operation_id="fast", stamina_cost=10)
                    self.assertEqual((reason, subject), (caught.exception.details["reason"], caught.exception.details["subject"]))
                    self.assertEqual(before, self.snapshot())
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=1000")
            conn.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON xianshi_operations BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END")
        before = self.snapshot()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "injected receipt failure"):
            repo.purchase("fault", "buyer", self.first_listing, 1, max_goods_num=99, stamina_operation_id="fast", stamina_cost=10)
        self.assertEqual(before, self.snapshot())


class PointShopCurrencyPrecisionTests(unittest.TestCase):
    setUp = shop_fixture.LegacyNumericPurchaseTests.setUp
    purchase = shop_fixture.LegacyNumericPurchaseTests.purchase
    status = staticmethod(shop_fixture.LegacyNumericPurchaseTests.status)

    def group(self, kind, value, suffix=""):
        group = shop_fixture.LegacyNumericPurchaseTests.setup_group(self, kind, bind_sqlite_param(value, exact=True), suffix)
        _, _, player, table, column = group
        with sqlite3.connect(player) as conn:
            conn.execute(f"ALTER TABLE {table} RENAME TO legacy_fixture")
            conn.execute(f"CREATE TABLE {table}(user_id TEXT PRIMARY KEY,{column} INTEGER,weekly_purchases TEXT)")
            conn.execute(f"INSERT INTO {table} SELECT * FROM legacy_fixture")
            conn.execute("DROP TABLE legacy_fixture")
        return group

    def test_real_integer_affinity_purchase_and_receipt_replay(self):
        for kind in ("training", "arena", "boss", "tower"):
            with self.subTest(asset=kind):
                value = 10**30 + 1
                group = self.group(kind, value)
                first = self.purchase(group, value)
                self.assertEqual("applied", self.status(first))
                with sqlite3.connect(group[2]) as conn:
                    self.assertEqual(value - 20, as_int_like(conn.execute(
                        f"SELECT {group[4]} FROM {group[3]}").fetchone()[0]))
                replay = self.purchase(group, value)
                self.assertEqual("duplicate", self.status(replay))
                field = {"boss": "integral", "tower": "score", "arena": "honor_points", "training": "points"}[kind]
                self.assertEqual(value - 20, replay[field] if isinstance(replay, dict) else getattr(replay, field))

    def test_integral_grant_crosses_int64_without_false_success(self):
        group = self.group("boss", 2**63 - 1)
        result = BossIntegralSqlRepository(group[2]).grant_integral("u", 2)
        self.assertEqual("applied", result.status)
        self.assertEqual(2**63 + 1, result.value)
        self.assertEqual(2**63 + 1, BossIntegralSqlRepository(group[2]).get_integral("u").integral)

    def test_boss_stale_rejection_replay_is_not_purchase_success(self):
        group = self.group("boss", 101)
        first = self.purchase(group, 100)
        replay = self.purchase(group, 100)
        self.assertEqual("state_changed", self.status(first))
        self.assertEqual("state_changed", self.status(replay))
        self.assertEqual("boss_integral_changed", replay["reason"])
        with sqlite3.connect(group[2]) as conn:
            self.assertEqual(101, conn.execute("SELECT integral FROM boss_limit").fetchone()[0])
        with sqlite3.connect(group[1]) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM back").fetchone()[0])

    def test_boss_receipt_safe_payload_format_and_corrupt_envelope(self):
        import json
        from nonebot_plugin_xiuxian_2.core.errors import DomainError
        group = self.group("boss", 101)
        first = self.purchase(group, 100)
        with sqlite3.connect(group[1]) as conn:
            raw = conn.execute('SELECT payload FROM boss_purchase_operations').fetchone()[0]
            saved = json.loads(raw)
            saved['request'][4] = str(saved['request'][4])
            conn.execute('UPDATE boss_purchase_operations SET payload=?', (json.dumps(saved),))
        self.assertEqual(first, self.purchase(group, 100))
        saved['_rejection']['status'] = 'duplicate'
        with sqlite3.connect(group[1]) as conn:
            conn.execute('UPDATE boss_purchase_operations SET payload=?', (json.dumps(saved),))
            before = list(conn.iterdump())
        with self.assertRaises(DomainError) as caught:
            self.purchase(group, 100)
        self.assertEqual('boss_purchase_receipt_invalid', caught.exception.details['reason'])
        with sqlite3.connect(group[1]) as conn:
            self.assertEqual(before, list(conn.iterdump()))

    def test_boss_legacy_zero_receipt_never_replays_success(self):
        group = self.group("boss", 101)
        self.purchase(group, 100)
        import json
        with sqlite3.connect(group[1]) as conn:
            raw = conn.execute('SELECT payload FROM boss_purchase_operations').fetchone()[0]
            conn.execute('UPDATE boss_purchase_operations SET payload=?', (json.dumps(json.loads(raw)['request']),))
        self.assertEqual(('state_changed', 'legacy_rejection'),
                         (self.purchase(group, 100)['status'], self.purchase(group, 100)['reason']))

    def test_point_wallet_bad_real_is_specific_and_zero_write(self):
        from nonebot_plugin_xiuxian_2.core.errors import DomainError
        for kind, field in [('training', 'training_points'), ('arena', 'arena_honor'), ('boss', 'boss_integral'), ('tower', 'tower_score')]:
            with self.subTest(asset=kind):
                group = self.group(kind, 100, kind)
                with sqlite3.connect(group[2]) as conn:
                    conn.execute(f'UPDATE {group[3]} SET {group[4]}=?', (float(2**63),))
                    player_before = list(conn.iterdump())
                with sqlite3.connect(group[1]) as conn:
                    game_before = list(conn.iterdump())
                with self.assertRaises(DomainError) as caught:
                    self.purchase(group, 2**63)
                self.assertEqual(field + '_precision_lost_real', caught.exception.details['reason'])
                for database, before in ((group[1], game_before), (group[2], player_before)):
                    with sqlite3.connect(database) as conn:
                        self.assertEqual(before, list(conn.iterdump()))


class ActivityCurrencyPrecisionTests(unittest.TestCase):
    setUp = activity_fixture.ActivityPointShopPurchaseRepositoryTests.setUp
    tearDown = activity_fixture.ActivityPointShopPurchaseRepositoryTests.tearDown
    purchase = activity_fixture.ActivityPointShopPurchaseRepositoryTests.purchase
    rewards = activity_fixture.ActivityPointShopPurchaseRepositoryTests.rewards

    def test_points_debit_and_wallet_reward_are_exact(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE activity_point_balance SET points=?", (2**53 + 1,))
            conn.execute("UPDATE user_xiuxian SET stone=?", (2**53 + 1,))
        result = self.purchase(quantity=1, unit_cost=1, rewards=({"type": "stone", "quantity": 1},))
        self.assertEqual("applied", result.status)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(2**53, as_int_like(conn.execute("SELECT points FROM activity_point_balance").fetchone()[0]))
            self.assertEqual(2**53 + 2, as_int_like(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0]))


class BankCurrencyPrecisionTests(unittest.TestCase):
    setUp = bank_fixture.BankDepositApplicationTests.setUp
    tearDown = bank_fixture.BankDepositApplicationTests.tearDown

    def test_deposit_saved_balance_and_receipt_cross_int64(self):
        value = 2**63 - 1
        with sqlite3.connect(self.database) as conn:
            conn.execute("INSERT INTO bank_accounts VALUES('u1',?,'1','t')", (value,))
        args = dict(operation_id="exact-bank", user_id="u1", amount=2, interest=0, limit=10**30, bank_level="1", settled_at="t2", expected_saved_stone=value, expected_saved_at="t")
        result = self.application.deposit(**args)
        self.assertEqual("applied", result["status"])
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(value + 2, as_int_like(conn.execute("SELECT saved_stone FROM bank_accounts").fetchone()[0]))
            self.assertEqual(998, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
        replay = self.application.deposit(**args)
        self.assertEqual("duplicate", replay["status"])
        self.assertEqual(value + 2, replay["saved_stone"])


class GuishiCurrencyPrecisionTests(unittest.TestCase):
    setUp = guishi_fixture.GuishiDepositSqlRepositoryTests.setUp
    tearDown = guishi_fixture.GuishiDepositSqlRepositoryTests.tearDown
    deposit = guishi_fixture.GuishiDepositSqlRepositoryTests.deposit

    def test_deposit_charges_large_wallet_exactly(self):
        with sqlite3.connect(self.game_database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=?", (2**53 + 1,))
        result = self.deposit("exact-escrow", 1)
        self.assertEqual("completed", result.status)
        with sqlite3.connect(self.game_database) as conn:
            self.assertEqual(2**53, as_int_like(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0]))
        with sqlite3.connect(self.trade_database) as conn:
            self.assertEqual(1, conn.execute("SELECT stored_stone FROM guishi_info").fetchone()[0])


class SectCurrencyPrecisionTests(unittest.TestCase):
    setUp = sect_fixture.SectShopRepositoryTests.setUp
    tearDown = sect_fixture.SectShopRepositoryTests.tearDown

    def test_shop_debits_contribution_and_materials_exactly(self):
        value = 10**30 + 1
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE user_xiuxian SET sect_contribution=?", (bind_sqlite_param(value, exact=True),))
            conn.execute("UPDATE sects SET sect_materials=?", (bind_sqlite_param(value, exact=True),))
        result = self.r.purchase("exact-sect", "u", 1, 9, "item", "type", 1, 1, 5, 0, 99, week_key="2026-W46")
        self.assertEqual("applied", result["status"])
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(value - 1, as_int_like(conn.execute("SELECT sect_contribution FROM user_xiuxian").fetchone()[0]))
            self.assertEqual(value - 1, as_int_like(conn.execute("SELECT sect_materials FROM sects").fetchone()[0]))


class AdminCurrencyPrecisionTests(unittest.TestCase):
    def test_admin_adjustment_persists_balance_and_ledger_exactly(self):
        import tempfile
        from nonebot_plugin_xiuxian_2.features.admin_asset.stone_repository import AdminStoneSqlRepository

        with tempfile.TemporaryDirectory() as directory:
            db = admin_fixture.AdminStoneRepositoryTests()._database(directory)
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE user_xiuxian SET stone=?", (2**63 - 1,))
            repo = AdminStoneSqlRepository(db)
            result = repo.adjust("exact-admin", "admin", "u", 2**63 - 1, 2)
            self.assertEqual("adjusted", result.status)
            with sqlite3.connect(db) as conn:
                self.assertEqual(2**63 + 1, as_int_like(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0]))
            replay = repo.adjust("exact-admin", "admin", "u", 0, 2)
            self.assertEqual(("duplicate", 2**63 + 1), (replay.status, replay.final_stone))


class GiftCurrencyPrecisionTests(unittest.TestCase):
    setUp = gift_fixture.StoneGiftApplicationTests.setUp
    tearDown = gift_fixture.StoneGiftApplicationTests.tearDown
    balances = gift_fixture.StoneGiftApplicationTests.balances

    def test_transfer_crosses_int64_and_replays_once(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=?", (2**63 - 1,))
        args = dict(operation_id="exact-gift", sender_id="sender", recipient_id="recipient", gross_amount=2, fee_rate=0)
        result = self.application.transfer(**args)
        self.assertTrue(result.ok, result.to_dict())
        self.assertEqual((2**63 - 3, 2**63 + 1), self.balances())
        self.assertEqual("replayed", self.application.transfer(**args).status)
        self.assertEqual((2**63 - 3, 2**63 + 1), self.balances())


class ListingCurrencyPrecisionTests(unittest.TestCase):
    setUp = listing_fixture.XianshiListingFeatureTests.setUp
    tearDown = listing_fixture.XianshiListingFeatureTests.tearDown
    list_items = listing_fixture.XianshiListingFeatureTests.list_items
    scalar = listing_fixture.XianshiListingFeatureTests.scalar

    def test_listing_fee_really_debits_an_adjacent_large_integer(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=?", (2**53 + 1,))
        result = self.list_items(price=10, quantity=1)
        self.assertEqual(("listed", 1), (result.status, result.fee_charged))
        self.assertEqual(2**53, as_int_like(self.scalar("SELECT stone FROM user_xiuxian")))


class GuishiWithdrawalCurrencyPrecisionTests(unittest.TestCase):
    setUp = withdraw_fixture.GuishiWithdrawSqlRepositoryTests.setUp
    tearDown = withdraw_fixture.GuishiWithdrawSqlRepositoryTests.tearDown
    withdraw = withdraw_fixture.GuishiWithdrawSqlRepositoryTests.withdraw

    def test_withdrawal_credits_large_wallet_exactly(self):
        with sqlite3.connect(self.game_database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=?", (2**53 + 1,))
        result = self.withdraw(amount=1)
        self.assertEqual(("completed", 1), (result.status, result.actual_amount))
        with sqlite3.connect(self.game_database) as conn:
            self.assertEqual(2**53 + 2, as_int_like(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0]))
if __name__ == "__main__":
    unittest.main()
