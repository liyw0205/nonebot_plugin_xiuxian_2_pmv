from __future__ import annotations

from copy import deepcopy
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import auction_config


class AuctionConfigStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original = deepcopy(auction_config._AUCTION_CONFIG)

    def tearDown(self) -> None:
        auction_config._AUCTION_CONFIG = self.original

    def test_config_is_normalized_in_memory(self) -> None:
        auction_config.save_config(
            {
                "schedule": {"start_hour": 99, "duration_hours": 0},
                "rules": {"fee_rate": 2},
            }
        )

        config = auction_config.get_auction_config()

        self.assertEqual(config["schedule"]["start_hour"], 23)
        self.assertEqual(config["schedule"]["duration_hours"], 1)
        self.assertEqual(config["rules"]["fee_rate"], 1.0)
        self.assertIn("activity", config)

    def test_callers_receive_a_copy(self) -> None:
        config = auction_config.get_auction_config()
        config["schedule"]["enabled"] = False

        self.assertTrue(auction_config.get_auction_schedule()["enabled"])

    def test_schedule_updates_stay_in_memory(self) -> None:
        auction_config.update_schedule({"enabled": False, "start_minute": 45})

        self.assertEqual(
            auction_config.get_auction_schedule()["enabled"], False
        )
        self.assertEqual(auction_config.get_auction_schedule()["start_minute"], 45)

    def test_config_module_has_no_session_json_store(self) -> None:
        source = Path(auction_config.__file__).read_text(encoding="utf-8")

        self.assertNotIn("auction_session.json", source)
        self.assertNotIn("load_persisted_auction_status", source)
        self.assertNotIn("persist_auction_status", source)
        self.assertNotIn("save_json_file", source)
        self.assertNotIn("get_auction_status_config", source)


if __name__ == "__main__":
    unittest.main()
