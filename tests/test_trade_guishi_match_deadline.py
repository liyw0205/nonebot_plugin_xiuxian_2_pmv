"""Recovery and per-pair cutoff checks on isolated business clocks."""
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from tests import test_trade_guishi_immediate_match as fixtures


class GuishiMatchDeadlineTests(unittest.TestCase):
    def setUp(self):
        self.db = fixtures.GuishiImmediateMatchTests()
        self.db.setUp()

    def tearDown(self):
        self.db.tearDown()

    def now(self, day=22, hour=0, minute=0):
        self.db.app.clock.now = lambda: datetime(2026, 11, day, hour, minute, tzinfo=timezone.utc)

    def monetary_state(self):
        rows = self.db.state()
        return rows[:4] + rows[5:]

    def test_buy_exact_cutoff_rejects_with_pending_and_no_settlement(self):
        created = self.db.create()
        self.db.seed_counterpart("sell", "baitan", quantity=2)
        self.now()
        before = self.monetary_state()
        result = self.db.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.status, result.failure.reason, result.matches), ("pending", "qiugou_expired", ()))
        self.assertEqual(result.failure.qiugou_order_id, created.order_id)
        self.assertEqual(before, self.monetary_state())

    def test_stall_before_cutoff_still_settles(self):
        created = self.db.create("baitan")
        self.db.seed_counterpart("buy", "qiugou", quantity=2)
        self.now(day=21, hour=23, minute=59)
        result = self.db.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.status, len(result.matches)), ("complete", 1))
        self.assertEqual(self.db.app.guishi_get_account("seller")[0], 40)

    def test_expired_stall_and_empty_buy_do_not_become_complete(self):
        created = self.db.create("baitan")
        self.now(hour=1)
        result = self.db.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual(result.failure.reason, "baitan_expired")
        created = self.db.create()
        self.db.write(self.trade, "DELETE FROM guishi_item WHERE item_type='baitan'")
        self.now(day=23)
        result = self.db.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.failure.reason, result.matches), ("qiugou_expired", ()))

    @property
    def trade(self):
        return self.db.trade

    def test_live_buy_cannot_consume_an_expired_counterpart_inside_pair_transaction(self):
        old = self.db.create("baitan")
        self.now(hour=1)
        buy = self.db.create()
        before = self.monetary_state()
        result = self.db.app.guishi_resume_immediate_match(order_id=buy.order_id)
        self.assertEqual((result.failure.reason, result.failure.baitan_order_id), ("baitan_expired", old.order_id))
        self.assertEqual(before, self.monetary_state())

    def test_live_stall_cannot_consume_an_expired_buy_counterpart(self):
        old = self.db.create()
        self.now(hour=1)
        stall = self.db.create("baitan")
        before = self.monetary_state()
        result = self.db.app.guishi_resume_immediate_match(order_id=stall.order_id)
        self.assertEqual((result.failure.reason, result.failure.qiugou_order_id), ("qiugou_expired", old.order_id))
        self.assertEqual(before, self.monetary_state())

    def test_first_pair_commits_before_clock_boundary_and_second_pair_is_retained(self):
        created = self.db.create()
        self.db.seed_counterpart("first", "baitan", price=10)
        self.db.seed_counterpart("second", "baitan", price=20)
        self.now(day=21, hour=23, minute=59)
        original = self.db.app.guishi_listing_fill_service.match

        def match(**kwargs):
            result = original(**kwargs)
            self.now()
            return result

        with patch.object(self.db.app.guishi_listing_fill_service, "match", side_effect=match):
            result = self.db.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.failure.reason, len(result.matches)), ("qiugou_expired", 1))
        self.assertEqual(self.db.app.guishi_get_account("seller")[0], 10)
        self.assertEqual(self.db.query(self.trade, "SELECT id,filled_quantity FROM guishi_item ORDER BY id"),
                         [(created.order_id, 1), ("second", 0)])

    def test_legacy_missing_anchor_is_not_guessed_as_expired(self):
        created = self.db.create()
        self.db.seed_counterpart("sell", "baitan", quantity=2)
        self.db.write(self.trade, "DELETE FROM guishi_order_create_operations")
        self.now(hour=1)
        result = self.db.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.status, len(result.matches)), ("complete", 1))

    def test_cancellation_after_expiry_closes_pending_without_double_refund(self):
        from nonebot_plugin_xiuxian_2.features.trade.migrations import apply_trade_guishi_order_cancel
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        created = self.db.create()
        self.db.seed_counterpart("sell", "baitan", quantity=2)
        self.now()
        self.assertEqual(self.db.app.guishi_resume_immediate_match(order_id=created.order_id).failure.reason, "qiugou_expired")
        with DatabaseUnitOfWork(self.trade) as uow:
            apply_trade_guishi_order_cancel(uow)
        expired = self.db.app.guishi_expire_qiugou()
        self.assertEqual((len(expired), expired[0].refunded_stone), (1, 40))
        self.assertEqual(self.db.app.guishi_get_account("buyer")[0], 1000)
        result = self.db.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.status, result.matches), ("complete", ()))
        before = self.db.state()
        self.assertEqual(self.db.app.guishi_expire_qiugou(), [])
        self.db.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual(before, self.db.state())

    def test_explicit_utc_uses_a_different_cutoff_for_the_same_instant(self):
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
        created = self.db.create()
        self.db.seed_counterpart("sell", "baitan", quantity=2)
        self.now()
        utc = TradeApplication(self.db.game, self.db.trade, clock=self.db.app.clock,
                               player_database=self.db.player, business_timezone=timezone.utc)
        result = utc.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual((result.status, len(result.matches)), ("complete", 1))

    def test_completed_pair_receipt_replays_after_cutoff_without_rejecting_or_settling(self):
        created = self.db.create()
        self.db.seed_counterpart("sell", "baitan", quantity=2)
        first = self.db.app.guishi_resume_immediate_match(order_id=created.order_id)
        self.assertEqual(len(first.matches), 1)
        self.now()
        before = self.db.state()
        replay = self.db.app.guishi_match(operation_id=f"guishi-match:{created.order_id}:sell",
                                         qiugou_order_id=created.order_id, baitan_order_id="sell")
        self.assertEqual((replay.status, replay.quantity), ("duplicate", 2))
        self.assertEqual(before, self.db.state())


if __name__ == "__main__":
    unittest.main()
