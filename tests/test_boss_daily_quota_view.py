"""世界BOSS 每日额度单一来源 + 讨伐回执剩余次数。

`12000`/`300000000`/`30` 曾在结算与积分信息两处各写一遍，视图与结算可能各自漂移；
战斗成功回执也从不显示今天还剩几次。本套件钉住共享常量、纯函数回执行与调用点。
"""

from __future__ import annotations

import unittest
from pathlib import Path

import nonebot

try:
    nonebot.get_driver()
except ValueError:  # pragma: no cover - import-order dependent
    nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.boss_limit import (
    DAILY_BATTLE_COUNT,
    DAILY_INTEGRAL_LIMIT,
    DAILY_STONE_LIMIT,
    daily_quota_line,
)

ROOT = Path(__file__).resolve().parents[1]
BOSS_MODULE = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_boss" / "__init__.py"


class DailyQuotaLineTests(unittest.TestCase):
    def test_values_match_the_established_limits(self) -> None:
        self.assertEqual(DAILY_BATTLE_COUNT, 30)
        self.assertEqual(DAILY_INTEGRAL_LIMIT, 12_000)
        self.assertEqual(DAILY_STONE_LIMIT, 300_000_000)

    def test_line_reports_used_total_and_remaining(self) -> None:
        self.assertEqual(
            daily_quota_line(1), f"今日讨伐：1/{DAILY_BATTLE_COUNT}（剩余 29 次）"
        )
        self.assertEqual(
            daily_quota_line(29), f"今日讨伐：29/{DAILY_BATTLE_COUNT}（剩余 1 次）"
        )

    def test_last_attempt_is_rendered_as_exhausted(self) -> None:
        self.assertEqual(
            daily_quota_line(30), f"今日讨伐：30/{DAILY_BATTLE_COUNT}（已用尽）"
        )

    def test_remaining_never_negative_and_bad_input_is_zero(self) -> None:
        self.assertIn("已用尽", daily_quota_line(999))
        self.assertEqual(
            daily_quota_line(None), f"今日讨伐：0/{DAILY_BATTLE_COUNT}（剩余 30 次）"
        )
        self.assertEqual(
            daily_quota_line("bad"), f"今日讨伐：0/{DAILY_BATTLE_COUNT}（剩余 30 次）"
        )
        self.assertIn("已用尽", daily_quota_line(-5, limit=0))

    def test_custom_limit_is_honoured(self) -> None:
        self.assertEqual(daily_quota_line(2, limit=5), "今日讨伐：2/5（剩余 3 次）")


class BossQuotaWiringTests(unittest.TestCase):
    def test_command_module_uses_shared_constants_and_no_literals(self) -> None:
        source = BOSS_MODULE.read_text(encoding="utf-8")
        self.assertNotIn("integral_limit = 12000", source)
        self.assertNotIn("stone_limit = 300000000", source)
        self.assertGreaterEqual(source.count("DAILY_INTEGRAL_LIMIT"), 2)
        self.assertGreaterEqual(source.count("DAILY_STONE_LIMIT"), 2)
        self.assertIn("daily_quota_line(settlement.battle_count)", source)


if __name__ == "__main__":
    unittest.main()
