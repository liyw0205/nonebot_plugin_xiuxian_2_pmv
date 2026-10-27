from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.guishi_cancel_repository import (
    GuishiOrderCancelResult,
    GuishiOrderCancelSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.guishi_lifecycle import (
    parse_created_at,
    qiugou_deadline,
    qiugou_is_expired,
    qiugou_remaining_seconds,
)
from nonebot_plugin_xiuxian_2.features.trade.guishi_qiugou_expiry import (
    GuishiQiugouExpiryService,
)


class _FixedClock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


class GuishiLifecycleRuleTests(unittest.TestCase):
    def test_deadline_is_next_day_at_end_hour(self) -> None:
        created = datetime(2026, 3, 5, 21, 30, tzinfo=timezone.utc)
        self.assertEqual(
            qiugou_deadline(created), datetime(2026, 3, 6, 8, 0, tzinfo=timezone.utc)
        )

    def test_order_created_after_midnight_still_expires_the_following_day(self) -> None:
        created = datetime(2026, 3, 6, 3, 0, tzinfo=timezone.utc)
        self.assertEqual(
            qiugou_deadline(created), datetime(2026, 3, 7, 8, 0, tzinfo=timezone.utc)
        )

    def test_expiry_boundary_counts_the_deadline_instant(self) -> None:
        created = datetime(2026, 3, 5, 21, 30, tzinfo=timezone.utc)
        self.assertFalse(
            qiugou_is_expired(created, datetime(2026, 3, 6, 7, 59, tzinfo=timezone.utc))
        )
        self.assertTrue(
            qiugou_is_expired(created, datetime(2026, 3, 6, 8, 0, tzinfo=timezone.utc))
        )
        self.assertTrue(
            qiugou_is_expired(created, datetime(2026, 3, 6, 8, 1, tzinfo=timezone.utc))
        )

    def test_deadline_crosses_month_and_year_boundaries(self) -> None:
        self.assertEqual(
            qiugou_deadline(datetime(2026, 12, 31, 23, 0, tzinfo=timezone.utc)),
            datetime(2027, 1, 1, 8, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            qiugou_deadline(datetime(2026, 2, 28, 22, 0, tzinfo=timezone.utc)),
            datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc),
        )

    def test_missing_anchor_never_expires(self) -> None:
        now = datetime(2030, 1, 1, tzinfo=timezone.utc)
        self.assertIsNone(qiugou_deadline(None))
        self.assertFalse(qiugou_is_expired(None, now))
        self.assertEqual(qiugou_remaining_seconds(None, now), 0)

    def test_remaining_seconds_never_go_negative(self) -> None:
        created = datetime(2026, 3, 5, 21, 0, tzinfo=timezone.utc)
        self.assertEqual(
            qiugou_remaining_seconds(
                created, datetime(2026, 3, 5, 23, 0, tzinfo=timezone.utc)
            ),
            9 * 3600,
        )
        self.assertEqual(
            qiugou_remaining_seconds(
                created, datetime(2026, 3, 9, 23, 0, tzinfo=timezone.utc)
            ),
            0,
        )

    def test_naive_and_aware_instants_do_not_raise(self) -> None:
        created = datetime(2026, 3, 5, 21, 0)
        self.assertTrue(qiugou_is_expired(created, datetime(2026, 3, 6, 9, 0)))

    def test_parse_created_at_accepts_sqlite_text_and_iso(self) -> None:
        self.assertEqual(
            parse_created_at("2026-03-05 21:30:00"),
            datetime(2026, 3, 5, 21, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(
            parse_created_at("2026-03-05T21:30:00Z"),
            datetime(2026, 3, 5, 21, 30, tzinfo=timezone.utc),
        )
        self.assertIsNone(parse_created_at(None))
        self.assertIsNone(parse_created_at(""))
        self.assertIsNone(parse_created_at("not-a-time"))


class GuishiQiugouExpiryServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.trade_database = root / "trade.sqlite3"
        sqlite3.connect(self.game_database).close()
        with sqlite3.connect(self.trade_database) as connection:
            connection.execute(
                "CREATE TABLE guishi_item (id TEXT PRIMARY KEY,user_id TEXT,item_id INTEGER,"
                "item_name TEXT,item_type TEXT,price INTEGER,quantity INTEGER,"
                "filled_quantity INTEGER DEFAULT 0)"
            )
            connection.execute(
                "CREATE TABLE guishi_info (user_id TEXT PRIMARY KEY,stored_stone INTEGER,items TEXT)"
            )
            connection.execute(
                "CREATE TABLE guishi_order_create_operations (operation_id TEXT PRIMARY KEY,"
                "payload TEXT NOT NULL,order_id TEXT NOT NULL,order_type TEXT NOT NULL,"
                "amount INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            connection.execute(
                "CREATE TABLE guishi_order_cancel_operations (operation_id TEXT PRIMARY KEY,"
                "payload TEXT NOT NULL,order_id TEXT NOT NULL,order_type TEXT NOT NULL,"
                "user_id TEXT NOT NULL,goods_id INTEGER NOT NULL,item_name TEXT NOT NULL,"
                "goods_type TEXT NOT NULL,refunded_quantity INTEGER NOT NULL,"
                "refunded_stone INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            connection.execute("INSERT INTO guishi_info VALUES('buyer',100,'{}')")
            connection.execute("INSERT INTO guishi_info VALUES('other',100,'{}')")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _add_qiugou(
        self,
        order_id: str,
        *,
        user_id: str = "buyer",
        price: int = 20,
        quantity: int = 5,
        filled: int = 0,
        created_at: str | None = "2026-03-05 21:30:00",
    ) -> None:
        with sqlite3.connect(self.trade_database) as connection:
            connection.execute(
                "INSERT INTO guishi_item(id,user_id,item_id,item_name,item_type,price,"
                "quantity,filled_quantity) VALUES(?,?,?,?,?,?,?,?)",
                (order_id, user_id, 1001, "灵草", "qiugou", price, quantity, filled),
            )
            if created_at is not None:
                connection.execute(
                    "INSERT INTO guishi_order_create_operations("
                    "operation_id,payload,order_id,order_type,amount,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        f"create-{order_id}",
                        "[]",
                        order_id,
                        "qiugou",
                        price * quantity,
                        created_at,
                    ),
                )

    def _service(self, moment: datetime) -> GuishiQiugouExpiryService:
        cancel = GuishiOrderCancelSqlRepository(
            self.game_database, self.trade_database
        ).cancel_qiugou
        return GuishiQiugouExpiryService(
            self.trade_database, cancel=cancel, clock=_FixedClock(moment)
        )

    def _orders(self) -> list[tuple[str, str]]:
        with sqlite3.connect(self.trade_database) as connection:
            return [
                (str(row[0]), str(row[1]))
                for row in connection.execute(
                    "SELECT id,user_id FROM guishi_item ORDER BY id"
                ).fetchall()
            ]

    def _stone(self, user_id: str) -> int:
        with sqlite3.connect(self.trade_database) as connection:
            row = connection.execute(
                "SELECT stored_stone FROM guishi_info WHERE user_id=?", (user_id,)
            ).fetchone()
            return int(row[0]) if row else 0

    def test_expired_order_is_cancelled_and_refunded(self) -> None:
        self._add_qiugou("expired", price=20, quantity=5, filled=2)
        service = self._service(datetime(2026, 3, 6, 9, 0, tzinfo=timezone.utc))

        results = service.expire()

        self.assertEqual(1, len(results))
        self.assertEqual("cancelled", results[0].status)
        self.assertEqual(3 * 20, results[0].refunded_stone)
        self.assertEqual([], self._orders())
        self.assertEqual(100 + 60, self._stone("buyer"))
        with sqlite3.connect(self.trade_database) as connection:
            self.assertEqual(
                1,
                connection.execute(
                    "SELECT COUNT(*) FROM guishi_order_cancel_operations"
                ).fetchone()[0],
            )

    def test_unexpired_order_survives_the_sweep(self) -> None:
        self._add_qiugou("fresh", created_at="2026-03-06 07:30:00")
        service = self._service(datetime(2026, 3, 6, 9, 0, tzinfo=timezone.utc))

        self.assertEqual([], service.expire())
        self.assertEqual([("fresh", "buyer")], self._orders())
        self.assertEqual(100, self._stone("buyer"))

    def test_order_without_creation_anchor_is_never_cancelled(self) -> None:
        self._add_qiugou("legacy", created_at=None)
        service = self._service(datetime(2030, 1, 1, tzinfo=timezone.utc))

        self.assertEqual([], service.expire())
        self.assertEqual([("legacy", "buyer")], self._orders())

    def test_malformed_anchor_is_never_cancelled(self) -> None:
        self._add_qiugou("broken", created_at="not-a-timestamp")
        service = self._service(datetime(2030, 1, 1, tzinfo=timezone.utc))

        self.assertEqual([], service.expire())
        self.assertEqual([("broken", "buyer")], self._orders())

    def test_scoped_sweep_only_touches_the_requested_player(self) -> None:
        self._add_qiugou("mine", user_id="buyer")
        self._add_qiugou("theirs", user_id="other")
        service = self._service(datetime(2026, 3, 6, 9, 0, tzinfo=timezone.utc))

        results = service.expire(user_id="buyer")

        self.assertEqual(["mine"], [result.order_id for result in results])
        self.assertEqual([("theirs", "other")], self._orders())
        self.assertEqual(100, self._stone("other"))

    def test_repeated_sweep_replays_without_refunding_twice(self) -> None:
        self._add_qiugou("expired", price=20, quantity=5, filled=0)
        service = self._service(datetime(2026, 3, 6, 9, 0, tzinfo=timezone.utc))

        first = service.expire()
        second = service.expire()

        self.assertEqual("cancelled", first[0].status)
        self.assertEqual(100 + 100, self._stone("buyer"))
        self.assertEqual([], second)

    def test_missing_anchor_table_returns_empty_without_ddl(self) -> None:
        with sqlite3.connect(self.trade_database) as connection:
            connection.execute("DROP TABLE guishi_order_create_operations")
        self._add_qiugou("expired", created_at=None)
        service = self._service(datetime(2030, 1, 1, tzinfo=timezone.utc))

        self.assertEqual([], service.expire())
        with sqlite3.connect(self.trade_database) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        self.assertNotIn("guishi_order_create_operations", tables)
        self.assertEqual([("expired", "buyer")], self._orders())

    def test_missing_trade_database_is_not_created(self) -> None:
        missing = Path(self.temp_dir.name) / "absent.sqlite3"
        service = GuishiQiugouExpiryService(
            missing,
            cancel=lambda **_: GuishiOrderCancelResult("cancelled", "none"),
            clock=_FixedClock(datetime.now(timezone.utc)),
        )

        self.assertEqual([], service.expire())
        self.assertFalse(missing.exists())

    def test_application_method_uses_injected_clock(self) -> None:
        self._add_qiugou("expired", price=10, quantity=4)
        moment = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
        application = TradeApplication(
            self.game_database, self.trade_database, clock=_FixedClock(moment)
        )

        results = application.guishi_expire_qiugou()

        self.assertEqual(["expired"], [result.order_id for result in results])
        self.assertEqual(100 + 40, self._stone("buyer"))
        self.assertEqual(
            [], application.guishi_get_orders(user_id="buyer", order_type="qiugou") or []
        )

    def test_application_skips_orders_that_are_still_running(self) -> None:
        self._add_qiugou("fresh", created_at="2026-03-06 07:59:59")
        application = TradeApplication(
            self.game_database,
            self.trade_database,
            clock=_FixedClock(datetime(2026, 3, 6, 8, 0, tzinfo=timezone.utc)),
        )

        self.assertEqual([], application.guishi_expire_qiugou())
        self.assertEqual(1, len(application.guishi_get_orders(user_id="buyer") or []))


class GuishiQiugouExpiryWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.command_source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_trade"
            / "__init__.py"
        ).read_text(encoding="utf-8")

    def _slice(self, start: str, end: str) -> str:
        begin = self.command_source.index(start)
        return self.command_source[begin : self.command_source.index(end, begin)]

    def test_info_handler_converges_the_players_own_expired_orders(self) -> None:
        info_handler = self._slice("async def guishi_info_(", "async def clear_all_guishi_")

        self.assertIn("trade_application.guishi_expire_qiugou(user_id=user_id)", info_handler)
        self.assertNotIn("xianshi_repository.clear_expired_guishi_order(", info_handler)

    def test_expiry_job_sweeps_buy_orders_and_still_clears_stalls(self) -> None:
        job = self._slice(
            "async def clear_expired_baitan_orders_job", "@auction_view.handle"
        )

        self.assertIn("trade_application.guishi_expire_qiugou()", job)
        self.assertIn("trade_application.guishi_clear_expired_baitan(", job)
        self.assertNotIn("xianshi_repository.clear_expired_guishi_order(", job)

    def test_qiugou_promise_matches_the_implemented_deadline(self) -> None:
        qiugou_handler = self._slice(
            "async def guishi_qiugou_", "@guishi_cancel_qiugou.handle"
        )

        self.assertIn("次日{GUISHI_BAITAN_END_HOUR}点自动取消订单", qiugou_handler)


if __name__ == "__main__":
    unittest.main()
