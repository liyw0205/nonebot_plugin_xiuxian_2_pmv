from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
from threading import Barrier
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade_guishi_order_cancel,
    apply_trade_guishi_qiugou,
    apply_trade_guishi_schema,
    apply_trade_guishi_withdraw,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class FixedClock:
    def __init__(self, moment):
        self.moment = moment

    def now(self):
        return self.moment


class GuishiBusinessTimeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.game = Path(self.directory.name) / "game.db"
        self.trade = Path(self.directory.name) / "trade.db"
        self.clock = FixedClock(datetime(2026, 11, 17, 13, tzinfo=timezone.utc))
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('buyer',1000)")
            apply_trade_guishi_withdraw(uow)
        with DatabaseUnitOfWork(self.trade, immediate=True) as uow:
            apply_trade_guishi_schema(uow)
            apply_trade_guishi_qiugou(uow)
            apply_trade_guishi_order_cancel(uow)
            uow.execute(
                "CREATE TABLE guishi_item(id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,"
                "item_name TEXT,item_type TEXT,price INTEGER,quantity INTEGER,filled_quantity INTEGER DEFAULT 0)"
            )
            uow.execute("INSERT INTO guishi_info VALUES('buyer',100,'{}')")
        self.application = TradeApplication(self.game, self.trade, clock=self.clock)

    def state(self):
        with DatabaseUnitOfWork(self.trade, read_only=True) as uow:
            balance = uow.query_one("SELECT stored_stone FROM guishi_info WHERE user_id='buyer'")["stored_stone"]
            orders = uow.query_all("SELECT * FROM guishi_item ORDER BY id")
            creates = uow.query_all("SELECT * FROM guishi_order_create_operations ORDER BY operation_id")
            cancels = uow.query_all("SELECT * FROM guishi_order_cancel_operations ORDER BY operation_id")
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='buyer'")["stone"]
            withdrawals = uow.query_all("SELECT * FROM trade_guishi_withdraw_operations ORDER BY operation_id")
        return balance, orders, creates, cancels, wallet, withdrawals

    def seed_order(self, order_id, anchor, *, kind="qiugou"):
        with DatabaseUnitOfWork(self.trade, immediate=True) as uow:
            uow.execute("INSERT INTO guishi_item VALUES(?,'buyer',1001,'herb',?,20,3,1)", (order_id, kind))
            if anchor is not None:
                uow.execute(
                    "INSERT INTO guishi_order_create_operations VALUES(?, '[]', ?, ?, 60, ?)",
                    (f"create-{order_id}", order_id, kind, anchor),
                )

    def test_historical_utc_anchor_expires_at_local_eight_without_rewriting_it(self):
        self.seed_order("legacy", "2026-11-17 13:00:00")
        self.clock.moment = datetime(2026, 11, 17, 23, 59, tzinfo=timezone.utc)
        before = self.state()
        self.assertEqual([], self.application.guishi_expire_qiugou())
        self.assertEqual(before, self.state())
        self.assertIn("11-18 08:00（剩余 1分）", self.application.guishi_order_status_lines("buyer")["qiugou"][0])
        self.clock.moment = datetime(2026, 11, 18, 0, tzinfo=timezone.utc)
        results = self.application.guishi_expire_qiugou()
        self.assertEqual([("legacy", "cancelled", 40)], [(r.order_id, r.status, r.refunded_stone) for r in results])
        after = self.state()
        self.assertEqual(140, after[0])
        self.assertEqual([], after[1])
        self.assertEqual(before[2], after[2])
        self.assertEqual(1, len(after[3]))
        self.assertEqual([], self.application.guishi_expire_qiugou())
        self.assertEqual(after, self.state())

    def test_local_midnight_does_not_shorten_the_next_day_buy_order_promise(self):
        self.seed_order("after-midnight", "2026-11-17T16:01:00+00:00")
        self.clock.moment = datetime(2026, 11, 18, 0, tzinfo=timezone.utc)
        before = self.state()
        self.assertEqual([], self.application.guishi_expire_qiugou())
        self.assertIn("11-19 08:00（剩余 24小时0分）", self.application.guishi_order_status_lines("buyer")["qiugou"][0])
        self.assertEqual(before, self.state())
        self.clock.moment = datetime(2026, 11, 19, 0, tzinfo=timezone.utc)
        self.assertEqual(["after-midnight"], [r.order_id for r in self.application.guishi_expire_qiugou()])

    def test_overnight_stall_deadline_matches_the_next_cleanup_instead_of_the_next_calendar_day(self):
        self.seed_order("overnight", "2026-11-17T17:00:00+00:00", kind="baitan")
        self.clock.moment = datetime(2026, 11, 17, 23, tzinfo=timezone.utc)
        self.assertIn("截止：11-18 08:00（剩余 1小时0分）", self.application.guishi_order_status_lines("buyer")["baitan"][0])

    def test_weekend_boundaries_and_replay_use_local_business_time(self):
        self.clock.moment = datetime(2026, 11, 20, 15, 59, tzinfo=timezone.utc)
        before = self.state()
        self.assertEqual("weekend_closed", self.application.guishi_withdraw(operation_id="withdraw", user_id="buyer", amount=50).status)
        self.assertEqual(before, self.state())
        self.clock.moment = datetime(2026, 11, 20, 16, tzinfo=timezone.utc)
        result = self.application.guishi_withdraw(operation_id="withdraw", user_id="buyer", amount=50)
        self.assertEqual(("completed", 10, 40), (result.status, result.fee, result.actual_amount))
        after = self.state()
        self.assertEqual((50, 1040, 1), (after[0], after[4], len(after[5])))
        self.clock.moment = datetime(2026, 11, 22, 16, tzinfo=timezone.utc)
        self.assertEqual("duplicate", self.application.guishi_withdraw(operation_id="withdraw", user_id="buyer", amount=50).status)
        self.assertEqual("weekend_closed", self.application.guishi_withdraw(operation_id="monday", user_id="buyer", amount=10).status)
        self.assertEqual(after, self.state())

    def test_creation_anchor_uses_injected_utc_clock_and_survives_replay(self):
        values = dict(operation_id="create", user_id="buyer", item_id=1001, item_name="herb", price=20, quantity=3, max_orders=10)
        result = self.application.guishi_qiugou(**values)
        self.assertEqual("created", result.status)
        before = self.state()
        self.assertEqual("2026-11-17T13:00:00+00:00", before[2][0]["created_at"])
        self.clock.moment = datetime(2026, 11, 18, 0, tzinfo=timezone.utc)
        self.assertEqual("duplicate", self.application.guishi_qiugou(**values).status)
        self.assertEqual(before, self.state())

    def test_missing_or_invalid_anchors_are_retained_without_business_writes(self):
        self.seed_order("missing", None)
        self.seed_order("invalid", "invalid")
        self.clock.moment = datetime(2030, 1, 1, tzinfo=timezone.utc)
        before = self.state()
        self.assertEqual([], self.application.guishi_expire_qiugou())
        lines = self.application.guishi_order_status_lines("buyer")["qiugou"]
        self.assertEqual(2, len(lines))
        self.assertTrue(all("未知（历史订单）" in line for line in lines))
        self.assertEqual(before, self.state())

    def test_explicit_utc_business_zone_is_used_for_both_policy_and_deadline(self):
        application = TradeApplication(self.game, self.trade, clock=self.clock, business_timezone="UTC")
        self.seed_order("utc", "2026-11-17 13:00:00")
        self.clock.moment = datetime(2026, 11, 18, 0, tzinfo=timezone.utc)
        self.assertEqual([], application.guishi_expire_qiugou())
        self.assertIn("剩余 8小时0分", application.guishi_order_status_lines("buyer")["qiugou"][0])
        self.clock.moment = datetime(2026, 11, 20, 16, tzinfo=timezone.utc)
        self.assertEqual("weekend_closed", application.guishi_withdraw(operation_id="utc-friday", user_id="buyer", amount=50).status)

    def test_naive_utc_clock_does_not_discard_the_local_deadline_offset(self):
        self.seed_order("naive-clock", "2026-11-17 13:00:00")
        self.clock.moment = datetime(2026, 11, 18, 0)
        self.assertEqual(["naive-clock"], [r.order_id for r in self.application.guishi_expire_qiugou()])

    def test_local_deadline_crosses_month_and_year_boundaries(self):
        for name, created, boundary in (
            ("month", "2026-02-28 13:00:00", datetime(2026, 3, 1, tzinfo=timezone.utc)),
            ("year", "2026-12-31 13:00:00", datetime(2027, 1, 1, tzinfo=timezone.utc)),
        ):
            with self.subTest(name=name):
                self.seed_order(name, created)
                self.clock.moment = boundary
                self.assertEqual([name], [r.order_id for r in self.application.guishi_expire_qiugou()])

    def test_clock_failure_rolls_back_order_freeze_and_receipt(self):
        before = self.state()
        with patch.object(self.clock, "now", side_effect=RuntimeError("clock unavailable")):
            with self.assertRaisesRegex(RuntimeError, "clock unavailable"):
                self.application.guishi_qiugou(
                    operation_id="clock-failure", user_id="buyer", item_id=1001,
                    item_name="herb", price=20, quantity=3, max_orders=10,
                )
        self.assertEqual(before, self.state())

    def test_web_fallback_uses_the_live_scheduler_timezone_for_the_same_instant(self):
        from nonebot_plugin_xiuxian_2.adapters.web.app import create_app
        from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
        from nonebot_plugin_xiuxian_2.bootstrap.registry import FeatureRegistry
        from nonebot_plugin_xiuxian_2.features.trade.manifest import FEATURE
        from nonebot_plugin_xiuxian_2.infrastructure.database.catalog import DatabaseCatalog
        from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
        from zoneinfo import ZoneInfo

        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_platform_schema(uow)
        context = build_runtime_context(data_dir=self.directory.name, settings={})
        context.clock = self.clock
        context.database = DatabaseCatalog.from_paths({
            key: self.game if key == "game_db" else self.trade if key == "trade_db" else context.database.path(key)
            for key in context.database.keys()
        })
        registry = FeatureRegistry()
        registry.register(FEATURE)
        self.clock.moment = datetime(2026, 11, 20, 16, tzinfo=timezone.utc)
        for zone, expected_http, expected_status in (
            (timezone.utc, 409, "weekend_closed"),
            (ZoneInfo("Asia/Shanghai"), 200, "completed"),
        ):
            with self.subTest(zone=zone), patch(
                "nonebot_plugin_xiuxian_2.compatibility.scheduler.scheduler_bridge",
                return_value=SimpleNamespace(timezone=zone),
            ):
                app = create_app(context=context, registry=registry)
                client = app.test_client()
                with client.session_transaction() as session:
                    session["_csrf_token"] = "csrf"
                response = client.post(
                    "/api/v1/trade/withdraw",
                    headers={"Idempotency-Key": f"web-{zone}", "X-CSRF-Token": "csrf"},
                    json={"user_id": "buyer", "amount": 50},
                )
                self.assertEqual(expected_http, response.status_code, response.get_json())
                self.assertEqual(expected_status, response.get_json()["data"]["data"]["status"])
        after = self.state()
        self.assertEqual((50, 1040, 1), (after[0], after[4], len(after[5])))

    def test_concurrent_expiry_at_the_business_boundary_refunds_once(self):
        self.seed_order("race", "2026-11-17 13:00:00")
        self.clock.moment = datetime(2026, 11, 18, 0, tzinfo=timezone.utc)
        service = self.application.guishi_qiugou_expiry_service
        barrier = Barrier(2)
        cancel = service.cancel

        def synchronized_cancel(**values):
            barrier.wait(timeout=5)
            return cancel(**values)

        with patch.object(service, "cancel", synchronized_cancel), ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(self.application.guishi_expire_qiugou) for _ in range(2)]
            results = [future.result(timeout=10)[0] for future in futures]
        self.assertEqual(["cancelled", "duplicate"], sorted(result.status for result in results))
        after = self.state()
        self.assertEqual(140, after[0])
        self.assertEqual([], after[1])
        self.assertEqual(1, len(after[3]))
        with DatabaseUnitOfWork(self.trade, read_only=True) as uow:
            self.assertEqual([{"integrity_check": "ok"}], uow.query_all("PRAGMA integrity_check"))


if __name__ == "__main__":
    unittest.main()
