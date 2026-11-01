"""世界BOSS 修为奖励的 gap 锚定回归 + 历练惩罚的压制一致性。

背景（`docs/features/xiuxian_progression.md` 口径）：高频可重复玩法的修为奖励必须
锚定「本境 → 下境」gap，并叠加境界压制 `min(0.1×(rank//3), 1)`：单次奖励占升级需求的
比值恒定，升级节奏由玩法次数/体力上限决定。

此前 `xiuxian_boss/__init__.py` 手写
`user_info['exp'] * exp_buff/10000 * min(0.1*max(rank//3,1),1)`，是 **current 锚定**
（自身累计修为 × 百分比），会让单次首领奖励随累计修为膨胀——正是文档点名的
"后期难度崩塌源"。本套件钉住修复后的口径：

1. 奖励与 `stage_power_gap(level)` 成正比；
2. **改变玩家当前累计 exp 不改变奖励**（证明不再是 current 锚定）；
3. 压制分母为 3；
4. 境界名未知时 fail-closed 返回 0，而不是回退到 current 锚定；
5. 三个零奖励分支不变；
6. 历练惩罚与奖励两侧都带 `divide_by_three=True`（源合同）。
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.boss.exp_reward import (
    EXP_BUFF_SCALE,
    boss_exp_reward,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.numeric_bind import (
    percent_exp_reward,
    rank_suppress_factor,
    stage_power_gap,
)

ROOT = "天灵根"
WIN = "群友赢了"
# 取两个真实境界（境界表共 77 境，命名是 初期/中期/圆满 三段）。
LOW = "金丹境中期"
HIGH = "化神境初期"


def expected(level: str, exp_buff: int, user_exp: int) -> int:
    """按口径独立算一遍：gap × (buff/10000) × min(0.1×(rank//3),1)。"""
    return percent_exp_reward(
        user_exp,
        exp_buff / EXP_BUFF_SCALE,
        level,
        divide_by_three=True,
        anchor="gap",
    )


class BossExpRewardAnchorTests(unittest.TestCase):
    def call(self, **kwargs) -> int:
        args = dict(user_exp=1_000_000, exp_buff=10000, level=LOW, root=ROOT, victor=WIN)
        args.update(kwargs)
        return boss_exp_reward(**args)

    # -------------------------------------------------- 1. gap 成正比
    def test_reward_matches_gap_formula(self) -> None:
        for level in (LOW, HIGH):
            with self.subTest(level=level):
                self.assertEqual(
                    self.call(level=level), expected(level, 10000, 1_000_000)
                )

    def test_reward_scales_with_gap(self) -> None:
        low_gap, high_gap = stage_power_gap(LOW), stage_power_gap(HIGH)
        self.assertGreater(low_gap, 0)
        self.assertGreater(high_gap, low_gap)
        low_reward = self.call(level=LOW)
        # 同一 percent 下把 gap 放大到 HIGH，奖励按 gap 比例放大（压制饱和时严格成正比）
        if rank_suppress_factor(LOW, divide_by_three=True) == rank_suppress_factor(
            HIGH, divide_by_three=True
        ):
            ratio = high_gap / low_gap
            self.assertAlmostEqual(
                self.call(level=HIGH) / low_reward, ratio, places=3
            )

    # -------------------------------------------------- 2. 与累计修为无关
    def test_reward_is_independent_of_accumulated_exp(self) -> None:
        rewards = {
            self.call(user_exp=value)
            for value in (1, 10**6, 10**9, 10**15, 10**19)
        }
        self.assertEqual(len(rewards), 1, f"奖励仍随累计修为变化：{sorted(rewards)}")

    # -------------------------------------------------- 3. 压制分母为 3
    def test_suppression_divides_rank_by_three(self) -> None:
        gap = stage_power_gap(LOW)
        by_three = rank_suppress_factor(LOW, divide_by_three=True)
        no_three = rank_suppress_factor(LOW, divide_by_three=False)
        self.assertAlmostEqual(self.call(), gap * 1.0 * by_three, delta=1)
        # 若误用未 ÷3 的压制，奖励会明显更大（rank 足够时）
        if no_three > by_three:
            self.assertLess(self.call(), gap * 1.0 * no_three)

    # -------------------------------------------------- 4. 未知境界 fail-closed
    def test_unknown_level_fails_closed(self) -> None:
        for bogus in ("元婴境圆满", "", "不存在境初期", None):
            with self.subTest(level=bogus):
                self.assertEqual(
                    self.call(level=bogus, user_exp=10**15), 0,
                    "未知境界必须返回 0，而不是回退到 current 锚定",
                )

    # -------------------------------------------------- 5. 零奖励分支
    def test_zero_branches(self) -> None:
        self.assertEqual(self.call(exp_buff=0), 0)
        self.assertEqual(self.call(exp_buff=-5), 0)
        self.assertEqual(self.call(exp_buff="not-a-number"), 0)
        self.assertEqual(self.call(root="凡人"), 0)
        self.assertEqual(self.call(victor="群友输了"), 0)
        self.assertEqual(self.call(victor=None), 0)

    def test_normal_case_is_positive(self) -> None:
        self.assertGreater(self.call(), 0)

    # -------------------------------------------------- 6. 历练压制一致性（源合同）
    def test_training_exp_sites_both_divide_by_three(self) -> None:
        src = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "features"
            / "training"
            / "event_resolver.py"
        ).read_text(encoding="utf-8")
        blocks = []
        for match in re.finditer(r"percent_exp_reward\s*\(", src):
            start = match.end()
            depth, i, buf = 1, start, []
            while i < len(src) and depth:
                if src[i] == "(":
                    depth += 1
                elif src[i] == ")":
                    depth -= 1
                if depth:
                    buf.append(src[i])
                i += 1
            blocks.append(" ".join("".join(buf).split()))
        self.assertEqual(len(blocks), 2, f"历练应有 2 处修为计算，实际 {len(blocks)}")
        for body in blocks:
            self.assertIn("divide_by_three=True", body.replace(" ", ""),
                          f"历练修为计算缺少 ÷3 压制：{body}")
            self.assertIn('anchor="gap"', body)

    def test_boss_no_longer_hand_rolls_current_anchor(self) -> None:
        src = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_boss"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("boss_exp_reward(", src)
        # 旧手写公式必须消失（current 锚定的膨胀源）
        self.assertNotIn("* exp_buff\n            / 10000", src)


if __name__ == "__main__":
    unittest.main()
