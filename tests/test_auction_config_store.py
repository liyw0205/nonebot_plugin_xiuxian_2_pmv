from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import auction_config


class AuctionConfigStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)
        self.session_file = self.data_dir / "auction_session.json"
        self.path_patches = (
            patch.object(auction_config, "XIUXIAN_DATABASE", self.data_dir),
            patch.object(auction_config, "AUCTION_SESSION_FILE", self.session_file),
        )
        for path_patch in self.path_patches:
            path_patch.start()

    def tearDown(self) -> None:
        for path_patch in reversed(self.path_patches):
            path_patch.stop()
        self.temp_dir.cleanup()

    @staticmethod
    def active_status():
        return {
            "active": True,
            "start_time": "20260710170000",
            "end_time": "20260710220000",
            "last_display_refresh_time": "",
            "items_count": 3,
        }

    def test_active_session_round_trips_through_central_store(self) -> None:
        auction_config.persist_auction_status(self.active_status())

        self.assertEqual(
            auction_config.load_persisted_auction_status(),
            self.active_status(),
        )
        self.assertEqual(list(self.data_dir.glob(".*.tmp")), [])

    def test_invalid_session_is_backed_up_and_resets_inactive(self) -> None:
        self.session_file.write_text("{broken", encoding="utf-8")

        self.assertIsNone(auction_config.load_persisted_auction_status())
        self.assertTrue(list(self.data_dir.glob("auction_session.json.invalid.*.bak")))
        reset = json.loads(self.session_file.read_text(encoding="utf-8"))
        self.assertFalse(reset["active"])

    def test_inactive_status_and_explicit_clear_remove_session(self) -> None:
        auction_config.persist_auction_status(self.active_status())
        auction_config.persist_auction_status({"active": False})
        self.assertFalse(self.session_file.exists())

        auction_config.persist_auction_status(self.active_status())
        auction_config.clear_persisted_auction_status()
        self.assertFalse(self.session_file.exists())

    def test_filesystem_error_is_not_silently_swallowed(self) -> None:
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.auction_config.save_json_file",
            side_effect=OSError("disk full"),
        ):
            with self.assertRaisesRegex(OSError, "disk full"):
                auction_config.persist_auction_status(self.active_status())


if __name__ == "__main__":
    unittest.main()
