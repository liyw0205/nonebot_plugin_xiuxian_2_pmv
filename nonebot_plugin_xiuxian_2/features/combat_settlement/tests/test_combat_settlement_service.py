from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import CombatSettlementApplication


class CombatSettlementServiceTests(unittest.TestCase):
    def test_application_requires_an_operation_id(self):
        with tempfile.TemporaryDirectory() as directory:
            app = CombatSettlementApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=object())
            with self.assertRaises(Exception):
                app.settle(
                    operation_id="",
                    user_id="u",
                    expected_daily={"date": "2026-09-12"},
                    snapshot="snapshot",
                    daily_limit=4,
                    stone=1,
                    items=(),
                    max_goods_num=10,
                )


if __name__ == "__main__":
    unittest.main()
