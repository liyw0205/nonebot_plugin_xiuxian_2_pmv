import unittest
from datetime import datetime, timezone

from nonebot_plugin_xiuxian_2.features.sign_in.commands import format_lottery_snapshot
from nonebot_plugin_xiuxian_2.features.sign_in.lottery_snapshot import LotterySnapshot, LotteryWinner


class LotteryCommandFormattingTests(unittest.TestCase):
    def test_empty_snapshot(self):
        text = format_lottery_snapshot(LotterySnapshot("2026-09-14", 0, 0), str)
        self.assertIn("暂无历史中奖记录", text)
        self.assertIn("当前奖池累计", text)

    def test_winner_snapshot(self):
        winner = LotteryWinner("u1", "道友", "2026-09-14 00:00:00", 100000, 1666, "first")
        text = format_lottery_snapshot(LotterySnapshot("2026-09-14", 42, 1, winner), str)
        self.assertIn("道友", text)
        self.assertIn("100000", text)


if __name__ == "__main__":
    unittest.main()
