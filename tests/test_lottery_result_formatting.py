import unittest
from types import SimpleNamespace

from nonebot_plugin_xiuxian_2.features.sign_in.commands import format_lottery_result


class LotteryResultFormattingTests(unittest.TestCase):
    def test_terminal_statuses(self):
        self.assertIn("冲突", format_lottery_result(SimpleNamespace(status="operation_conflict"), str))
        self.assertIn("未找到", format_lottery_result(SimpleNamespace(status="user_missing"), str))
        self.assertIn("已经参与", format_lottery_result(SimpleNamespace(status="already_participated", lottery_number=0), str))

    def test_prize_and_miss(self):
        prize = SimpleNamespace(status="settled", prize_tier="first", lottery_number=7, prize=100)
        self.assertIn("一等奖", format_lottery_result(prize, str))
        miss = SimpleNamespace(status="settled", prize_tier="none", lottery_number=8, prize=0)
        self.assertIn("未中奖", format_lottery_result(miss, str))


if __name__ == "__main__":
    unittest.main()
