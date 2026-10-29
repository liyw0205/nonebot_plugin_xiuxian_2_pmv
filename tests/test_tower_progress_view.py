from __future__ import annotations

import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.tower.progress_view import (
    next_milestone,
    render_tower_progress,
    weekly_purchase_summary,
)


class TowerProgressViewTests(unittest.TestCase):
    shop = {
        "1999": {"name": "渡厄丹", "cost": 1000, "weekly_limit": 10},
        "20012": {"name": "秘境加速券", "cost": 10000, "weekly_limit": 5},
        "zero": {"name": "不限期", "cost": 1, "weekly_limit": 0},
    }
    config = {
        "积分奖励": {"每层基础": 100, "每10层额外": 500},
        "灵石奖励": {"每层基础": 1000000, "每10层额外": 5000000},
        "修为奖励": {"每10层": 0.001},
    }

    def test_next_milestone_boundaries(self) -> None:
        self.assertEqual(10, next_milestone(0))
        self.assertEqual(10, next_milestone(9))
        self.assertEqual(20, next_milestone(10))
        self.assertEqual(100, next_milestone(99))
        self.assertEqual(110, next_milestone(100))
        self.assertEqual(10, next_milestone("bad"))

    def test_weekly_summary_counts_limits_and_exhausted_items(self) -> None:
        summary = weekly_purchase_summary(
            {"1999": 3, "20012": "5", "zero": 9, "_last_reset": "2026-10-29"},
            self.shop,
        )

        self.assertEqual((summary.used, summary.limit, summary.exhausted), (8, 15, 1))

    def test_weekly_summary_ignores_bad_values_and_empty_config(self) -> None:
        summary = weekly_purchase_summary(
            {"1999": -4, "20012": "bad", "zero": 3},
            self.shop,
        )
        empty = weekly_purchase_summary(None, None)

        self.assertEqual((summary.used, summary.limit, summary.exhausted), (0, 15, 0))
        self.assertEqual((empty.used, empty.limit, empty.exhausted), (0, 0, 0))

    def test_weekly_summary_accepts_integer_item_keys(self) -> None:
        summary = weekly_purchase_summary({1999: 4, 20012: 1}, self.shop)

        self.assertEqual((summary.used, summary.limit, summary.exhausted), (5, 15, 0))

    def test_renderer_shows_milestone_rewards_and_weekly_progress(self) -> None:
        text = render_tower_progress(
            current_floor=9,
            max_floor=9,
            score=1234,
            weekly_purchases={"1999": 3, "20012": 5},
            shop_items=self.shop,
            config=self.config,
        )

        self.assertIn("【进度与里程碑】", text)
        self.assertIn("下一里程碑：第10层（还差1层）", text)
        self.assertIn("积分+500", text)
        self.assertIn("灵石+5,000,000", text)
        self.assertIn("修为+0.10%", text)
        self.assertIn("本周兑换：8/15（已兑完 1 项）", text)
        self.assertIn("每周一 0 点重置", text)

    def test_renderer_without_shop_items_still_shows_milestone(self) -> None:
        text = render_tower_progress(
            current_floor=0,
            max_floor=0,
            score=0,
            weekly_purchases=None,
            shop_items={},
            config={},
        )

        self.assertIn("下一里程碑：第10层（还差10层）", text)
        self.assertIn("本周兑换：暂无有效限购商品", text)
        self.assertIn("积分+0", text)

    def test_renderer_tolerates_bad_weekly_purchases(self) -> None:
        text = render_tower_progress(
            current_floor="bad",
            max_floor="bad",
            score="bad",
            weekly_purchases="not-a-mapping",
            shop_items=self.shop,
            config=self.config,
        )

        self.assertIn("第10层", text)
        self.assertIn("本周兑换：0/15", text)

    def test_renderer_is_repeatable_and_does_not_mutate_inputs(self) -> None:
        weekly = {"1999": 3, "20012": 5}
        weekly_before = dict(weekly)
        shop_before = {key: dict(value) for key, value in self.shop.items()}

        first = render_tower_progress(
            current_floor=9,
            max_floor=9,
            score=1234,
            weekly_purchases=weekly,
            shop_items=self.shop,
            config=self.config,
        )
        second = render_tower_progress(
            current_floor=9,
            max_floor=9,
            score=1234,
            weekly_purchases=weekly,
            shop_items=self.shop,
            config=self.config,
        )

        self.assertEqual(first, second)
        self.assertEqual(weekly, weekly_before)
        self.assertEqual(self.shop, shop_before)

    def test_tower_info_handler_renders_progress_and_keeps_existing_lines(self) -> None:
        source_path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_tower"
            / "__init__.py"
        )
        source = source_path.read_text(encoding="utf-8")
        start = source.index("@tower_info.handle")
        handler = source[start : source.index("@tower_shop.handle", start)]

        self.assertIn("render_tower_progress(", handler)
        self.assertIn("weekly_purchases=tower_info_data.get", handler)
        for text in ("当前层数", "历史最高", "累计积分", "挑战通天塔", "速通通天塔"):
            self.assertIn(text, handler)


if __name__ == "__main__":
    unittest.main()
