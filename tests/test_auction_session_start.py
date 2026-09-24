from __future__ import annotations

import tempfile
import unittest
import importlib
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.transaction_service import (
    AuctionSessionService,
)
from nonebot_plugin_xiuxian_2.features.auction.session_start_application import (
    AuctionSessionStartApplication,
)
from nonebot_plugin_xiuxian_2.features.auction.migrations import (
    apply_auction_player_queue,
    apply_auction_queue_operations,
    apply_auction_settlement,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


def test_trade_facade_defers_auction_session_service_construction():
    trade = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade"
    )
    assert trade._auction_session_service_instance is None
    assert trade._auction_session_start_application_instance is None


def test_auction_session_uses_lazy_game_trade_resolver():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_trade/__init__.py"
    ).read_text(encoding="utf-8")
    assert "_auction_session_service_instance = None" in source
    assert "def _auction_session_service(" in source
    assert "get_paths().game_db" in source
    assert "get_paths().trade_db" in source
    assert "bind_auction_repository(_auction_bid_repository, _auction_session_service)" in source
    assert "return _auction_session_start_application()" in source
    assert "auction_session_service=_auction_session_service" in source
    assert "auction_session_start_application=_auction_session_start_application" in source
    assert "def _auction_session_start_application(" in source
    assert "auction_session_service = AuctionSessionService(" not in source


class FixedClock:
    def now(self):
        from datetime import datetime, timezone

        return datetime(2026, 9, 24, 12, 34, 56, tzinfo=timezone.utc)


class FixedRandom:
    def __init__(self):
        self.calls = 0

    def sample(self, values, count):
        self.calls += 1
        return list(values)[:count]


class AuctionSessionStartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.trade = root / "trade.sqlite3"
        with DatabaseUnitOfWork(self.game) as uow:
            apply_auction_settlement(uow)
            apply_auction_queue_operations(uow)
        with DatabaseUnitOfWork(self.trade) as uow:
            apply_auction_player_queue(uow)
        with db_backend.transaction(self.trade) as conn:
            conn.execute(
                "INSERT INTO auction_player_upload VALUES (%s,%s,%s,%s,%s)",
                ("seller", 1001, "玩家法器", 600000, "卖家"),
            )
        self.service = AuctionSessionService(self.game, self.trade, 99)
        self.system = [{"item_id": 2001, "name": "系统丹药", "start_price": 800000}]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_start_moves_queue_and_records_session_atomically(self) -> None:
        result = self.service.start(
            "start-1", "session-1", start_time=100.0, end_time=200.0,
            system_items=self.system,
        )
        self.assertEqual(result.status, "started")
        self.assertEqual(result.items_count, 2)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_current").fetchone()[0], 2)
            row = conn.execute("SELECT status,items_count FROM auction_sessions").fetchone()
            self.assertEqual(tuple(row), ("active", 2))
        with db_backend.connection(self.trade) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_player_upload").fetchone()[0], 0)

    def test_start_is_idempotent_and_rejects_conflicting_payload(self) -> None:
        first = self.service.start("start-1", "session-1", start_time=100, end_time=200, system_items=self.system)
        replay = self.service.start("start-1", "session-1", start_time=100, end_time=200, system_items=self.system)
        conflict = self.service.start("start-1", "session-2", start_time=100, end_time=200, system_items=self.system)
        self.assertEqual(first.status, "started")
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(conflict.status, "state_changed")

    def test_parallel_session_is_rejected(self) -> None:
        self.service.start("start-1", "session-1", start_time=100, end_time=200, system_items=self.system)
        result = self.service.start("start-2", "session-2", start_time=100, end_time=200, system_items=self.system)
        self.assertEqual(result.status, "already_active")

    def test_operation_failure_rolls_back_items_queue_and_session(self) -> None:
        self.service.get_active_session()
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER reject_start_operation BEFORE INSERT ON auction_session_operations "
                "BEGIN SELECT RAISE(ABORT, 'reject'); END"
            )
        with self.assertRaises(Exception):
            self.service.start("start-1", "session-1", start_time=100, end_time=200, system_items=self.system)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_current").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_sessions").fetchone()[0], 0)
        with db_backend.connection(self.trade) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM auction_player_upload").fetchone()[0], 1)

    def test_application_replays_before_drawing_system_items(self) -> None:
        random_source = FixedRandom()
        application = AuctionSessionStartApplication(
            self.game,
            self.trade,
            clock=FixedClock(),
            random_source=random_source,
        )
        system_config = {
            "丹药": {"id": 2001, "start_price": 800000},
            "法器": {"id": 2002, "start_price": 900000},
        }
        first = application.start(
            "application-start",
            system_items_config=system_config,
            duration_hours=2,
            system_item_count=1,
        )
        replay = application.start(
            "application-start",
            system_items_config=system_config,
            duration_hours=2,
            system_item_count=1,
        )
        self.assertEqual((first.status, replay.status), ("started", "duplicate"))
        self.assertEqual(random_source.calls, 1)
        self.assertEqual(first.items_count, 2)

    def test_reading_missing_session_schema_does_not_create_tables(self) -> None:
        other = self.temp_dir.name + "/unmigrated.sqlite3"
        service = AuctionSessionService(other, self.trade, 99)
        self.assertIsNone(service.get_active_session())
        with db_backend.connection(other) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='auction_sessions'"
                ).fetchone()
            )


if __name__ == "__main__":
    unittest.main()
