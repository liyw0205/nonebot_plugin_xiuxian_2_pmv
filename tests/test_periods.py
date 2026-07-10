from __future__ import annotations

import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.periods import (
    format_duration_compact,
    format_duration_full,
    format_remaining_time,
)


class PeriodFormattingTests(unittest.TestCase):
    def test_full_duration_keeps_all_units(self) -> None:
        self.assertEqual(format_duration_full(90061), "1天1小时1分1秒")
        self.assertEqual(format_duration_full(0, zero="未知"), "未知")

    def test_compact_duration_omits_leading_zero_units(self) -> None:
        self.assertEqual(format_duration_compact(61), "1分1秒")
        self.assertEqual(format_duration_compact(3600), "1小时0分0秒")

    def test_remaining_time_has_explicit_ready_state(self) -> None:
        self.assertEqual(format_remaining_time(0), "已可用")
        self.assertEqual(format_remaining_time(5), "5秒")


if __name__ == "__main__":
    unittest.main()
