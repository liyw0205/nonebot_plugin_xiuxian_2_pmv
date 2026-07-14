from __future__ import annotations

from datetime import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import auction_config
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import auction_service
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import auction_utils
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.auction_session_service import (
    AuctionSessionService,
)
from tests.test_db_backend import db_backend


class AuctionDatabaseStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.trade = root / "trade.sqlite3"
        with db_backend.transaction(self.game):
            pass
        with db_backend.transaction(self.trade) as conn:
            conn.execute(
                "CREATE TABLE auction_player_upload ("
                "user_id TEXT NOT NULL, item_id INTEGER NOT NULL, "
                "item_name TEXT NOT NULL, start_price INTEGER NOT NULL, "
                "user_name TEXT NOT NULL, PRIMARY KEY(user_id,item_id))"
            )
        self.service = AuctionSessionService(self.game, self.trade, 99)
        self.service_patch = patch.object(
            auction_utils, "auction_session_service", self.service
        )
        self.service_patch.start()
        self.original_config = auction_config.get_auction_config()
        self.system_items = [
            {
                "item_id": 1001,
                "name": "System item",
                "start_price": 200,
            }
        ]

    def tearDown(self) -> None:
        auction_config.save_config(self.original_config)
        self.service_patch.stop()
        self.temp_dir.cleanup()

    def start(self, operation_id: str = "start"):
        return self.service.start(
            operation_id,
            "session",
            start_time=100.0,
            end_time=200.0,
            system_items=self.system_items,
        )

    def test_status_tracks_active_database_session(self) -> None:
        self.assertFalse(auction_utils.get_auction_status()["active"])

        self.start()
        status = auction_utils.get_auction_status()

        self.assertTrue(status["active"])
        self.assertEqual(status["start_time"], datetime.fromtimestamp(100.0))
        self.assertEqual(status["end_time"], datetime.fromtimestamp(200.0))
        self.assertEqual(status["items_count"], 1)

    def test_in_memory_status_cannot_override_database(self) -> None:
        auction_config.set_auction_config_value(
            "auction_status",
            {
                "active": True,
                "start_time": "20260714170000",
                "end_time": "20260714220000",
                "items_count": 99,
            },
        )
        self.assertFalse(auction_utils.get_auction_status()["active"])

        self.start()
        auction_config.set_auction_config_value(
            "auction_status", {"active": False}
        )

        status = auction_utils.get_auction_status()
        self.assertTrue(status["active"])
        self.assertEqual(status["items_count"], 1)

    def test_failed_start_transaction_stays_inactive(self) -> None:
        self.service.get_active_session()
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER fail_start_status "
                "BEFORE INSERT ON auction_session_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.start()

        self.assertFalse(auction_utils.get_auction_status()["active"])

    def test_finished_session_immediately_becomes_inactive(self) -> None:
        self.start()

        result = self.service.finish(
            "finish",
            "session",
            end_time=300.0,
            fee_rate=0.1,
            item_types={1001: "item"},
        )

        self.assertEqual(result.status, "settled")
        self.assertFalse(auction_utils.get_auction_status()["active"])

    def test_settled_start_operation_is_not_reported_as_active(self) -> None:
        self.start()
        with patch.object(auction_service, "_auction_session_service", self.service):
            self.assertTrue(auction_service.start_auction_process(None, "start"))

        self.service.finish(
            "finish",
            "session",
            end_time=300.0,
            fee_rate=0.1,
            item_types={1001: "item"},
        )

        with patch.object(auction_service, "_auction_session_service", self.service):
            self.assertFalse(auction_service.start_auction_process(None, "start"))

    def test_runtime_source_has_no_session_json_projection(self) -> None:
        trade_root = Path(auction_utils.__file__).parent
        utils_source = (trade_root / "auction_utils.py").read_text(encoding="utf-8")
        service_source = (trade_root / "auction_service.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("auction_session_service.get_active_session()", utils_source)
        self.assertNotIn("persist_auction_status", utils_source)
        self.assertNotIn("_restore_auction_status_from_disk", utils_source)
        self.assertNotIn("set_auction_status(", service_source)
        self.assertNotIn("clear_persisted_auction_status", service_source)


if __name__ == "__main__":
    unittest.main()
