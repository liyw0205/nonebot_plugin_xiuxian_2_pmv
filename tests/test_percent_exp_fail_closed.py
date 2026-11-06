"""gap/next_power 锚定的未知境界必须 fail-closed，而不是回退 current 锚定。

`percent_exp_reward` 曾在 `stage_power_gap(level) <= 0` 时静默回退 `current` 锚定，
于是境界名漂移/写错时，单次奖励重新变成「累计修为 × 百分比」——正是文档
`docs/features/xiuxian_progression.md` 指出的后期崩塌源。本套件把根因锁死：
未知**字符串**境界名返回 0；int/float 境界序号与 None 保持 current 锚定的历史语义。
"""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from unittest import mock

import nonebot

ROOT = Path(__file__).resolve().parents[1]
NUMERIC_PATH = (
    ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_utils" / "numeric_bind.py"
)


def _numeric():
    try:
        nonebot.get_driver()
    except ValueError:
        nonebot.init()
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    name = "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.numeric_bind"
    if name in sys.modules:
        return importlib.reload(sys.modules[name])
    return importlib.import_module(name)


LEVELS = {
    "祭道境初期": {"power": 10_000_000_000_000, "spend": 1},
    "祭道境中期": {"power": 17_000_000_000_000, "spend": 1},
}


class PercentExpFailClosedTests(unittest.TestCase):
    def test_unknown_string_realm_returns_zero_not_current_anchor(self) -> None:
        nb = _numeric()
        with mock.patch.object(nb, "_level_power_table", lambda: LEVELS):
            small = nb.percent_exp_reward(100_000, 0.01, "不存在的境界初期", anchor="gap")
            huge = nb.percent_exp_reward(10**20, 0.01, "不存在的境界初期", anchor="gap")
            next_small = nb.percent_exp_reward(
                100_000, 0.01, "不存在的境界初期", anchor="next_power"
            )
        self.assertEqual(small, 0)
        self.assertEqual(huge, 0)
        self.assertEqual(next_small, 0)
        # 旧行为会在 exp=1e20 时给出 1e18 级别的奖励；这里同时钉住「不随 exp 膨胀」。
        self.assertNotEqual(huge, int(10**20 * 0.01))

    def test_empty_string_realm_also_fails_closed(self) -> None:
        nb = _numeric()
        with mock.patch.object(nb, "_level_power_table", lambda: LEVELS):
            self.assertEqual(
                nb.percent_exp_reward(10**15, 0.01, "   ", anchor="gap"),
                0,
            )

    def test_known_realm_gap_reward_unchanged(self) -> None:
        nb = _numeric()
        with mock.patch.object(nb, "_level_power_table", lambda: LEVELS):
            got = nb.percent_exp_reward(
                10**30,
                0.001,
                "祭道境初期",
                anchor="gap",
                apply_rank_suppress=False,
            )
        gap = 17_000_000_000_000 - 10_000_000_000_000
        self.assertEqual(got, int(gap * 0.001))

    def test_integer_rank_and_none_keep_current_anchor(self) -> None:
        nb = _numeric()
        # int 境界序号：历史语义是 current 锚定（rank 20 → 压制 1.0）。
        self.assertEqual(nb.percent_exp_reward(1_000_000, 0.01, 20), 10_000)
        # None：同样 current 锚定。
        self.assertEqual(
            nb.percent_exp_reward(1_000_000, 0.01, None, apply_rank_suppress=False),
            10_000,
        )

    def test_source_contract_no_current_fallback_for_unknown_names(self) -> None:
        src = NUMERIC_PATH.read_text(encoding="utf-8")
        self.assertNotIn("gap 未知时回退 current", src)
        self.assertIn("未知境界名 fail-closed", src)

    def test_boss_reward_guard_still_present(self) -> None:
        boss_src = (
            ROOT / "nonebot_plugin_xiuxian_2" / "features" / "boss" / "exp_reward.py"
        ).read_text(encoding="utf-8")
        self.assertIn("stage_power_gap(level) <= 0", boss_src)


if __name__ == "__main__":
    unittest.main()
