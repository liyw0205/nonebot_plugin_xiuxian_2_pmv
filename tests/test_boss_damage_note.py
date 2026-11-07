"""P4 世界BOSS 伤害上限说明。

`actual_damage = min(total_damage, boss_all_hp*0.2, boss_old_hp)` 一直生效，
但回执只写「受天道限制，实际削减…」，玩家会把上限读成 bug。本套件钉住纯函数
`boss_damage_note` 的原因命名、未被截断时零输出，以及三个回执分支的接线。
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.boss.schemas import boss_damage_note

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE = REPO_ROOT / "nonebot_plugin_xiuxian_2"
BOSS_MODULE = "xiuxian/xiuxian_boss/__init__.py"


def _read(relative: str) -> str:
    return (PACKAGE / relative).read_text(encoding="utf-8")


class BossDamageNoteTests(unittest.TestCase):
    def test_uncapped_hit_adds_nothing(self):
        self.assertEqual(boss_damage_note(1000, 1000, 5000, 9000), "")
        self.assertEqual(boss_damage_note(1000, 1200, 5000, 9000), "", "actual 不可能高于 total，但高了也不报截断")

    def test_missing_actual_operand_still_judges_from_the_caps(self):
        note = boss_damage_note(9000, None, 2000, 50000)

        self.assertIn("本次上限 2000", note, "actual 缺失时不伪装成未截断，交由调用方决定")

    def test_single_hit_cap_is_named_when_it_is_the_binding_limit(self):
        note = boss_damage_note(9000, 2000, 2000, 50000)

        self.assertIn("单次讨伐最多削减世界BOSS最大血量的20%", note)
        self.assertIn("本次上限 2000", note)
        self.assertIn("超出的伤害不结算", note)

    def test_remaining_hp_is_named_when_it_is_the_binding_limit(self):
        note = boss_damage_note(9000, 300, 2000, 300)

        self.assertIn("剩余血量只剩 300", note)
        self.assertIn("无处结算", note)

    def test_cap_wins_ties_because_it_is_the_recurring_rule(self):
        note = boss_damage_note(9000, 300, 300, 300)

        self.assertIn("单次讨伐最多削减", note)
        self.assertNotIn("剩余血量只剩", note)

    def test_missing_operands_never_invent_a_reason(self):
        self.assertEqual(boss_damage_note(9000, 10, None, None), "")
        self.assertEqual(boss_damage_note(9000, 10), "")

    def test_dirty_and_negative_values_are_clamped(self):
        self.assertEqual(boss_damage_note("bad", "bad", None, None), "")
        self.assertEqual(boss_damage_note("bad", "bad", "bad", "bad"), "")
        note = boss_damage_note("9000", "2000", "2000", "50000")
        self.assertIn("本次上限 2000", note)


class BossDamageNoteWiringTests(unittest.TestCase):
    def setUp(self):
        self.source = _read(BOSS_MODULE)
        start = self.source.index("async def battle_")
        self.handler = self.source[start : self.source.index("class BossDrops", start)]

    def test_note_is_computed_from_the_same_operands_as_actual_damage(self):
        self.assertIn("from ...features.boss.schemas import boss_damage_note", self.source)
        self.assertIn(
            "damage_note = boss_damage_note(\n"
            "        total_damage, actual_damage, max_single_damage, boss_old_hp\n"
            "    )",
            self.source,
        )

    def test_all_three_damage_reply_branches_render_the_note(self):
        branches = self.handler.split('f"本次战斗造成伤害：')

        for index, branch in enumerate(branches[1:], start=1):
            with self.subTest(branch=index):
                self.assertIn("damage_note_line", branch)

    def test_note_line_is_empty_when_there_is_no_note(self):
        self.assertIn(
            'damage_note_line = f"{damage_note}\\n" if damage_note else ""', self.source
        )

    def test_damage_and_reward_math_is_unchanged(self):
        self.assertIn(
            "actual_damage = min(total_damage, max_single_damage, boss_old_hp)", self.source
        )
        self.assertIn("max_single_damage = int(boss_all_hp * 0.2)", self.source)
        self.assertIn("damage_ratio = actual_damage / boss_all_hp if boss_all_hp > 0 else 0", self.source)
        self.assertIn("boss_integral = max(int(damage_ratio * 3000), 1)", self.source)

    def test_note_renders_before_the_quota_line(self):
        note_at = self.handler.index("damage_note_line")
        quota_at = self.handler.index("daily_quota_line(settlement.battle_count)")

        self.assertLess(note_at, quota_at)

    def test_note_function_has_no_io_or_nonebot_imports(self):
        source = _read("features/boss/schemas.py")

        self.assertNotIn("nonebot", source)
        self.assertNotIn("sqlite3", source)
        self.assertNotIn("DatabaseUnitOfWork", source)


if __name__ == "__main__":
    unittest.main()
