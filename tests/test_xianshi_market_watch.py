"""Focused tests for the Xianshi market quotes and watch-list slice.

The fixture builds the real trade migrations (``trade.001``/``trade.013``/
``trade.014``) and inserts rows with the real column sets, so the aggregations
are exercised against the schema production actually has.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.trade.manifest import FEATURE as TRADE_FEATURE
from nonebot_plugin_xiuxian_2.features.trade.migrations import (
    apply_trade,
    apply_trade_xianshi_purchase,
    apply_trade_xianshi_watch,
)
from nonebot_plugin_xiuxian_2.features.trade.xianshi_market_application import (
    XianshiMarketApplication,
)
from nonebot_plugin_xiuxian_2.features.trade.xianshi_market_repository import (
    XianshiMarketQuote,
    XianshiMarketSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.trade.xianshi_market_schemas import (
    WATCH_LIMIT,
    is_valid_goods_name,
    normalize_goods_name,
)
from nonebot_plugin_xiuxian_2.features.trade.xianshi_market_view import (
    render_market_quote,
    render_watch_result,
    render_watchlist,
)
from nonebot_plugin_xiuxian_2.features.trade.xianshi_watch_repository import (
    XianshiWatchResult,
    XianshiWatchSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

ROOT = Path(__file__).resolve().parents[1]
TRADE_SOURCE = (
    ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_trade" / "__init__.py"
).read_text(encoding="utf-8")


class _FixedClock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


NOW = datetime(2026, 11, 2, 12, 0, 0)
CLOCK = _FixedClock(NOW)


def _money(value: int) -> str:
    return f"{int(value):,}"


class _MarketFixture:
    """Real trade schema plus helpers for trade/listing rows."""

    def __init__(self, root: Path, *, with_market_tables: bool = True) -> None:
        self.database = root / "game.sqlite3"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_trade(uow)
            if with_market_tables:
                apply_trade_xianshi_purchase(uow)
            apply_trade_xianshi_watch(uow)

    def add_trade(
        self,
        *,
        operation_id: str,
        name: str,
        quantity: int,
        total_cost: int,
        created_at: str,
        buyer: str = "u1",
        seller: str = "u2",
    ) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO xianshi_operations(operation_id,listing_id,buyer_id,"
                "seller_id,goods_id,name,goods_type,quantity,total_cost,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    operation_id,
                    f"l-{operation_id}",
                    buyer,
                    seller,
                    1001,
                    name,
                    "丹药",
                    quantity,
                    total_cost,
                    created_at,
                ),
            )

    def add_listing(
        self,
        *,
        listing_id: str,
        name: str,
        price: int,
        quantity: int,
        seller: str = "u1",
    ) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO xianshi_item(id,user_id,goods_id,name,type,price,quantity)"
                " VALUES(?,?,?,?,?,?,?)",
                (listing_id, seller, 1001, name, "丹药", price, quantity),
            )

    def watch_count(self) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            return int(
                uow.query_one("SELECT COUNT(*) AS total FROM xianshi_watchlist")["total"]
            )

    def receipt_count(self) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            return int(
                uow.query_one(
                    "SELECT COUNT(*) AS total FROM xianshi_watch_operations"
                )["total"]
            )

    def tables(self) -> set[str]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        return {str(row["name"]) for row in rows}


class XianshiWatchRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.fixture = _MarketFixture(self.root)
        self.repository = XianshiWatchSqlRepository(self.fixture.database, clock=CLOCK)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    # ---------------------------------------------------------------- schema
    def test_migration_creates_tables_and_records_version(self) -> None:
        self.assertIn("xianshi_watchlist", self.fixture.tables())
        self.assertIn("xianshi_watch_operations", self.fixture.tables())
        with DatabaseUnitOfWork(self.fixture.database, read_only=True) as uow:
            versions = {
                str(row["version"])
                for row in uow.query_all(
                    "SELECT version FROM trade_feature_migrations"
                )
            }
        self.assertIn("trade.014", versions)

    def test_reapplying_the_migration_is_idempotent(self) -> None:
        with DatabaseUnitOfWork(self.fixture.database) as uow:
            apply_trade_xianshi_watch(uow)
            apply_trade_xianshi_watch(uow)
        with DatabaseUnitOfWork(self.fixture.database, read_only=True) as uow:
            count = uow.query_one(
                "SELECT COUNT(*) AS total FROM trade_feature_migrations "
                "WHERE version='trade.014'"
            )["total"]
        self.assertEqual(1, int(count))

    def test_missing_schema_fails_closed_without_creating_tables(self) -> None:
        empty = self.root / "empty" / "game.sqlite3"
        repository = XianshiWatchSqlRepository(empty, clock=CLOCK)

        self.assertEqual("schema_missing", repository.add("op-add", "u1", "破境丹").status)
        self.assertEqual("schema_missing", repository.remove("op-rm", "u1", "破境丹").status)
        self.assertEqual([], repository.names("u1"))
        with DatabaseUnitOfWork(empty, read_only=True) as uow:
            rows = uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        self.assertEqual([], [str(row["name"]) for row in rows])

    # ----------------------------------------------------------------- writes
    def test_add_creates_watch_row_and_receipt(self) -> None:
        result = self.repository.add("op-1", "u1", " 破境 丹 ")

        self.assertEqual("added", result.status)
        self.assertEqual("破境 丹", result.goods_name)
        self.assertEqual(1, result.watch_count)
        self.assertTrue(result.changed)
        self.assertEqual(1, self.fixture.watch_count())
        self.assertEqual(1, self.fixture.receipt_count())
        self.assertEqual(["破境 丹"], self.repository.names("u1"))

    def test_replaying_the_same_operation_replays_without_a_second_row(self) -> None:
        self.assertEqual("added", self.repository.add("op-1", "u1", "破境丹").status)
        replay = self.repository.add("op-1", "u1", "破境丹")

        self.assertEqual("duplicate", replay.status)
        self.assertEqual(1, replay.watch_count)
        self.assertEqual(1, self.fixture.watch_count())
        self.assertEqual(1, self.fixture.receipt_count())

    def test_adding_the_same_item_with_another_operation_stays_one_row(self) -> None:
        self.repository.add("op-1", "u1", "破境丹")
        result = self.repository.add("op-2", "u1", "破境丹")

        self.assertEqual("duplicate", result.status)
        self.assertEqual(1, self.fixture.watch_count())
        self.assertEqual(2, self.fixture.receipt_count())

    def test_watch_limit_blocks_and_allows_retry_after_a_cancel(self) -> None:
        for index in range(WATCH_LIMIT):
            self.assertEqual(
                "added", self.repository.add(f"op-{index}", "u1", f"物品{index}").status
            )

        blocked = self.repository.add("op-over", "u1", "第十一件")
        self.assertEqual("limit_reached", blocked.status)
        self.assertEqual(WATCH_LIMIT, blocked.watch_count)
        self.assertEqual(WATCH_LIMIT, self.fixture.watch_count())
        self.assertEqual(WATCH_LIMIT, self.fixture.receipt_count())

        self.assertEqual("removed", self.repository.remove("op-rm", "u1", "物品0").status)
        retried = self.repository.add("op-over", "u1", "第十一件")
        self.assertEqual("added", retried.status)
        self.assertEqual(WATCH_LIMIT, retried.watch_count)

    def test_invalid_names_are_rejected_without_writes(self) -> None:
        cases = {
            "": "empty",
            "   ": "blank",
            "x" * 25: "too long",
            "破\x00境": "control character",
        }
        for name, label in cases.items():
            with self.subTest(label=label):
                result = self.repository.add(f"op-{label}", "u1", name)
                self.assertEqual("invalid_name", result.status)
        self.assertEqual(0, self.fixture.watch_count())
        self.assertEqual(0, self.fixture.receipt_count())

    def test_empty_user_id_is_reported_without_writing(self) -> None:
        result = self.repository.add("op-1", "  ", "破境丹")

        self.assertEqual("invalid_user", result.status)
        self.assertEqual(0, self.fixture.watch_count())

    def test_remove_deletes_the_row_and_writes_a_receipt(self) -> None:
        self.repository.add("op-add", "u1", "破境丹")

        result = self.repository.remove("op-rm", "u1", "破境丹")

        self.assertEqual("removed", result.status)
        self.assertEqual(0, result.watch_count)
        self.assertEqual(0, self.fixture.watch_count())
        self.assertEqual(2, self.fixture.receipt_count())

    def test_removing_an_absent_watch_is_a_noop(self) -> None:
        result = self.repository.remove("op-rm", "u1", "破境丹")

        self.assertEqual("absent", result.status)
        self.assertEqual(0, self.fixture.receipt_count())

    def test_remove_replay_returns_absent_without_a_second_receipt(self) -> None:
        self.repository.add("op-add", "u1", "破境丹")
        self.repository.remove("op-rm", "u1", "破境丹")

        replay = self.repository.remove("op-rm", "u1", "破境丹")

        self.assertEqual("absent", replay.status)
        self.assertEqual(2, self.fixture.receipt_count())

    def test_operation_conflict_is_reported_for_a_different_payload(self) -> None:
        self.repository.add("op-1", "u1", "破境丹")

        conflict = self.repository.add("op-1", "u1", "玄铁")
        cross_action = self.repository.remove("op-1", "u1", "破境丹")

        self.assertEqual("operation_conflict", conflict.status)
        self.assertEqual("operation_conflict", cross_action.status)
        self.assertEqual(1, self.fixture.watch_count())

    def test_names_are_case_insensitive_and_whitespace_normalized(self) -> None:
        self.assertEqual("added", self.repository.add("op-1", "u1", "Sword").status)
        self.assertEqual("duplicate", self.repository.add("op-2", "u1", "sword").status)
        self.assertEqual("added", self.repository.add("op-3", "u1", "破境　丹").status)
        self.assertEqual("duplicate", self.repository.add("op-4", "u1", "破境 丹").status)
        self.assertEqual(2, self.fixture.watch_count())

    def test_names_are_scoped_per_player(self) -> None:
        self.repository.add("op-1", "u1", "破境丹")
        self.repository.add("op-2", "u2", "破境丹")

        self.assertEqual(["破境丹"], self.repository.names("u1"))
        self.assertEqual(["破境丹"], self.repository.names("u2"))
        self.assertEqual(2, self.fixture.watch_count())


class XianshiMarketRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.fixture = _MarketFixture(self.root)
        self.repository = XianshiMarketSqlRepository(self.fixture.database, clock=CLOCK)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_quote_aggregates_trades_with_weighted_average(self) -> None:
        self.fixture.add_trade(
            operation_id="t1", name="破境丹", quantity=2, total_cost=3000,
            created_at="2026-10-28 10:00:00",
        )
        self.fixture.add_trade(
            operation_id="t2", name="破境丹", quantity=3, total_cost=4500,
            created_at="2026-10-30 11:00:00",
        )
        self.fixture.add_trade(
            operation_id="t3", name="破境丹", quantity=1, total_cost=2000,
            created_at="2026-11-01 09:30:00",
        )

        quote = self.repository.snapshot(["破境丹"])["破境丹"]

        self.assertEqual(3, quote.trade_count)
        self.assertEqual(6, quote.traded_quantity)
        self.assertEqual(1500, quote.min_unit_price)
        self.assertEqual(2000, quote.max_unit_price)
        self.assertEqual(1583, quote.avg_unit_price)  # 9500 / 6, half-up
        self.assertEqual("2026-11-01 09:30:00", quote.last_traded_at)
        self.assertEqual(2000, quote.last_unit_price)

    def test_quote_applies_a_lower_bound_only(self) -> None:
        self.fixture.add_trade(
            operation_id="old", name="破境丹", quantity=1, total_cost=999,
            created_at="2026-10-20 08:00:00",
        )
        self.fixture.add_trade(
            operation_id="edge", name="破境丹", quantity=1, total_cost=1000,
            created_at="2026-10-26 12:00:00",
        )
        # A local-time writer can store a stamp ahead of the UTC clock; an upper
        # bound would wrongly drop it.
        self.fixture.add_trade(
            operation_id="future", name="破境丹", quantity=1, total_cost=1100,
            created_at="2026-11-02 20:00:00",
        )

        quote = self.repository.snapshot(["破境丹"])["破境丹"]

        self.assertEqual(2, quote.trade_count)
        self.assertEqual(1050, quote.avg_unit_price)

    def test_quote_ignores_dirty_trade_rows(self) -> None:
        self.fixture.add_trade(
            operation_id="ok", name="破境丹", quantity=2, total_cost=1000,
            created_at="2026-10-30 10:00:00",
        )
        self.fixture.add_trade(
            operation_id="zero-qty", name="破境丹", quantity=0, total_cost=500,
            created_at="2026-10-30 11:00:00",
        )
        self.fixture.add_trade(
            operation_id="negative", name="破境丹", quantity=1, total_cost=-5,
            created_at="2026-10-30 12:00:00",
        )
        self.fixture.add_trade(
            operation_id="blank-time", name="破境丹", quantity=1, total_cost=700,
            created_at="",
        )

        quote = self.repository.snapshot(["破境丹"])["破境丹"]

        self.assertEqual(1, quote.trade_count)
        self.assertEqual(2, quote.traded_quantity)
        self.assertEqual(500, quote.avg_unit_price)

    def test_quote_aggregates_listings_across_sellers(self) -> None:
        self.fixture.add_listing(
            listing_id="l1", name="破境丹", price=1200, quantity=2, seller="u1"
        )
        self.fixture.add_listing(
            listing_id="l2", name="破境丹", price=1500, quantity=3, seller="u2"
        )
        self.fixture.add_listing(
            listing_id="l3", name="破境丹", price=1800, quantity=1, seller="0"
        )
        self.fixture.add_listing(
            listing_id="l4", name="破境丹", price=0, quantity=9, seller="u3"
        )
        self.fixture.add_listing(
            listing_id="l5", name="破境丹", price=900, quantity=0, seller="u4"
        )

        quote = self.repository.snapshot(["破境丹"])["破境丹"]

        self.assertEqual(3, quote.listing_count)
        self.assertEqual(3, quote.seller_count)
        self.assertEqual(6, quote.listing_quantity)
        self.assertEqual(1200, quote.min_listing_price)

    def test_quote_matches_names_case_insensitively(self) -> None:
        self.fixture.add_trade(
            operation_id="t1", name="Sword", quantity=1, total_cost=500,
            created_at="2026-10-30 10:00:00",
        )

        quote = self.repository.snapshot(["sWORD"])["sword"]

        self.assertEqual(1, quote.trade_count)
        self.assertEqual("sWORD", quote.name)

    def test_unknown_name_returns_zeros_but_stays_available(self) -> None:
        quote = self.repository.snapshot(["没人卖的"])["没人卖的"]

        self.assertTrue(quote.available)
        self.assertEqual(0, quote.trade_count)
        self.assertEqual(0, quote.listing_count)

    def test_missing_market_tables_degrade_per_source(self) -> None:
        lean = self.root / "lean"
        lean.mkdir()
        fixture = _MarketFixture(lean, with_market_tables=False)
        repository = XianshiMarketSqlRepository(fixture.database, clock=CLOCK)

        quote = repository.snapshot(["破境丹"])["破境丹"]

        self.assertFalse(quote.trades_ready)
        self.assertFalse(quote.listings_ready)
        self.assertFalse(quote.available)

    def test_missing_database_file_degrades_without_creating_it(self) -> None:
        missing = self.root / "absent" / "game.sqlite3"
        repository = XianshiMarketSqlRepository(missing, clock=CLOCK)

        quote = repository.snapshot(["破境丹"])["破境丹"]

        self.assertFalse(quote.available)
        self.assertFalse(missing.exists())

    def test_snapshot_handles_empty_requests(self) -> None:
        self.assertEqual({}, self.repository.snapshot([]))
        self.assertEqual({}, self.repository.snapshot(["   "]))

    def test_snapshot_returns_one_entry_per_name(self) -> None:
        quotes = self.repository.snapshot(["甲", "乙", "甲"])

        self.assertEqual({"甲", "乙"}, set(quotes))


class XianshiMarketApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.fixture = _MarketFixture(self.root)
        self.application = XianshiMarketApplication(
            self.fixture.database, clock=CLOCK
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_overview_returns_a_quote_per_watched_name(self) -> None:
        self.application.watch(operation_id="op-1", user_id="u1", goods_name="破境丹")
        self.application.watch(operation_id="op-2", user_id="u1", goods_name="玄铁")
        self.fixture.add_trade(
            operation_id="t1", name="破境丹", quantity=1, total_cost=1000,
            created_at="2026-10-30 10:00:00",
        )

        overview = self.application.overview("u1")

        self.assertEqual(("玄铁", "破境丹"), overview.names)
        self.assertEqual(1, overview.quote_for("破境丹").trade_count)
        self.assertEqual(0, overview.quote_for("玄铁").trade_count)

    def test_overview_survives_a_broken_market_repository(self) -> None:
        class _Broken:
            def snapshot(self, names, *, days=7):
                raise sqlite3.Error("boom")

        application = XianshiMarketApplication(
            self.fixture.database,
            watch_repository=XianshiWatchSqlRepository(self.fixture.database, clock=CLOCK),
            market_repository=_Broken(),
            clock=CLOCK,
        )
        application.watch(operation_id="op-1", user_id="u1", goods_name="破境丹")

        overview = application.overview("u1")

        self.assertEqual(("破境丹",), overview.names)
        self.assertFalse(overview.quote_for("破境丹").available)

    def test_quote_for_a_missing_watch_is_still_usable(self) -> None:
        quote = self.application.quote("破境丹")

        self.assertTrue(quote.available)
        self.assertEqual(0, quote.trade_count)


class XianshiMarketViewTests(unittest.TestCase):
    def test_render_quote_full(self) -> None:
        quote = XianshiMarketQuote(
            "破境丹",
            trade_count=12,
            traded_quantity=30,
            min_unit_price=1000,
            max_unit_price=2500,
            avg_unit_price=1600,
            last_traded_at="2026-11-01 20:15:00",
            last_unit_price=1500,
            listing_count=3,
            listing_quantity=8,
            seller_count=2,
            min_listing_price=1200,
        )

        text = render_market_quote("破境丹", quote, watched=True, number=_money)

        self.assertIn("【仙肆行情】破境丹", text)
        self.assertIn("近7天成交：12 笔 / 30 件", text)
        self.assertIn("成交单价：最低 1,000灵石｜最高 2,500灵石｜加权均价 1,600灵石", text)
        self.assertIn("最近成交：11-01 20:15（单价 1,500灵石）", text)
        self.assertIn("当前在售：3 个摊位 / 8 件", text)
        self.assertIn("最低在售单价：1,200灵石", text)
        self.assertIn("已关注：发送【仙肆关注 取消 破境丹】可取消", text)

    def test_render_quote_empty_states(self) -> None:
        text = render_market_quote(
            "破境丹", XianshiMarketQuote("破境丹"), watched=False, number=_money
        )

        self.assertIn("近7天没有成交记录。", text)
        self.assertIn("当前没有在售。", text)
        self.assertIn("未关注：发送【仙肆关注 破境丹】加入关注", text)

    def test_render_quote_unavailable_sources(self) -> None:
        text = render_market_quote(
            "破境丹",
            XianshiMarketQuote("破境丹", trades_ready=False, listings_ready=False),
            watched=False,
            number=_money,
        )

        self.assertIn("行情数据暂不可用", text)

        partial = render_market_quote(
            "破境丹",
            XianshiMarketQuote("破境丹", trades_ready=True, listings_ready=False),
            watched=False,
            number=_money,
        )
        self.assertIn("近7天没有成交记录。", partial)
        self.assertIn("在售数据暂不可用。", partial)

    def test_render_watchlist_rows_and_footer(self) -> None:
        overview = _overview_for_test()

        text = render_watchlist(overview, number=_money)

        self.assertIn("【我的仙肆关注】2/10", text)
        self.assertIn("1. 破境丹｜近7天 12 笔，均价 1,600灵石｜在售 3 摊 8 件，最低 1,200灵石", text)
        self.assertIn("2. 玄铁｜近7天无成交｜当前无在售", text)
        self.assertIn("提示：仙肆关注 物品名 添加", text)

    def test_render_watchlist_empty(self) -> None:
        from nonebot_plugin_xiuxian_2.features.trade.xianshi_market_application import (
            XianshiWatchOverview,
        )

        text = render_watchlist(XianshiWatchOverview(names=()))

        self.assertIn("你还没有关注任何仙肆物品。", text)
        self.assertIn(f"最多 {WATCH_LIMIT} 个", text)

    def test_render_watch_result_covers_every_status(self) -> None:
        expectations = {
            "added": "已关注",
            "duplicate": "已在关注列表中",
            "removed": "已取消关注",
            "absent": "不在关注列表中",
            "limit_reached": "关注列表已满",
            "invalid_name": "物品名无效",
            "invalid_user": "缺少玩家身份",
            "operation_conflict": "该操作号已用于其他关注操作",
            "schema_missing": "trade.014",
        }
        for status, needle in expectations.items():
            with self.subTest(status=status):
                text = render_watch_result(
                    XianshiWatchResult(status, "破境丹", "u1", 2)
                )
                self.assertIn(needle, text)

    def test_name_helpers(self) -> None:
        self.assertEqual("破境 丹", normalize_goods_name("  破境　丹  "))
        self.assertEqual("破 境", normalize_goods_name("破\t境"))
        self.assertTrue(is_valid_goods_name("破境丹"))
        self.assertTrue(is_valid_goods_name("x" * 24))
        self.assertFalse(is_valid_goods_name("x" * 25))
        self.assertFalse(is_valid_goods_name(""))
        self.assertFalse(is_valid_goods_name("破\x00境"))


def _overview_for_test():
    from nonebot_plugin_xiuxian_2.features.trade.xianshi_market_application import (
        XianshiWatchOverview,
    )

    quotes = {
        "破境丹": XianshiMarketQuote(
            "破境丹",
            trade_count=12,
            traded_quantity=30,
            min_unit_price=1000,
            max_unit_price=2500,
            avg_unit_price=1600,
            last_traded_at="2026-11-01 20:15:00",
            last_unit_price=1500,
            listing_count=3,
            listing_quantity=8,
            seller_count=2,
            min_listing_price=1200,
        ),
        "玄铁": XianshiMarketQuote("玄铁"),
    }
    return XianshiWatchOverview(names=("破境丹", "玄铁"), quotes=quotes)


class XianshiMarketWiringTests(unittest.TestCase):
    def test_commands_and_aliases_are_registered(self) -> None:
        self.assertIn('on_command("仙肆关注", aliases={"关注仙肆物品"}', TRADE_SOURCE)
        self.assertIn('on_command("仙肆行情", aliases={"仙肆价格"}', TRADE_SOURCE)

    def test_manifest_declares_both_commands(self) -> None:
        names = {command.name for command in TRADE_FEATURE.commands}
        self.assertIn("仙肆关注", names)
        self.assertIn("仙肆行情", names)

    def test_handlers_use_session_identity_and_application(self) -> None:
        self.assertIn("xianshi_market_application.watch(", TRADE_SOURCE)
        self.assertIn("xianshi_market_application.unwatch(", TRADE_SOURCE)
        self.assertIn("xianshi_market_application.quote(", TRADE_SOURCE)
        self.assertIn("xianshi_market_application.overview(", TRADE_SOURCE)
        self.assertIn("user_id = str(user_info['user_id'])", TRADE_SOURCE)
        self.assertIn('"xianshi-watch:{action}:{event_id}:{user_id}:{name}"', TRADE_SOURCE)

    def test_watch_command_supports_cancel_prefix(self) -> None:
        self.assertIn('for prefix in ("取消", "取关")', TRADE_SOURCE)
        self.assertIn('action = "remove"', TRADE_SOURCE)


if __name__ == "__main__":
    unittest.main()
