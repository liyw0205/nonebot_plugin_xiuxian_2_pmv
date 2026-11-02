"""魔修入侵领奖的 feature 域规则与结算组装（定向合同）。

这些规则原先内联在兼容命令模块里，无法单独回归；本套件覆盖纯规则边界、
随机池筛选、无上 1% 降级、结算明细组装、历史 JSON 脏数据容错，以及
「命令侧不再内联计算」的源合同。
"""

from __future__ import annotations

import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.world_events import reward_rules as rules
from nonebot_plugin_xiuxian_2.features.world_events import reward_settlement as settlement

ROOT = Path(__file__).resolve().parents[1]
FACADE = (
    ROOT
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_world_events"
    / "__init__.py"
)


class StubRng:
    """Deterministic RNG shaped like SystemRandom for the reward path.

    ``choice`` always prefers the 无上 row so the 1% keep/reroll branch is
    actually exercised; ``randint`` then decides keep (100) or reroll (other).
    """

    def __init__(self, *, gate: float = 0.0, pick_keep_wushang: bool = False) -> None:
        self.gate = gate
        self.pick_keep_wushang = pick_keep_wushang

    def random(self) -> float:
        return self.gate

    def choice(self, seq):
        rows = list(seq)
        for row in rows:
            if rules.is_wushang_reward_item(row[1]):
                return row
        return rows[0]

    def randint(self, a, b) -> int:
        return 100 if self.pick_keep_wushang else 1


ATTACK_SKILL = {"name": "风雷剑诀", "item_type": "神通", "rank": "3", "type": "功法"}
PLAIN_SKILL = {"name": "凡功", "item_type": "功法", "rank": "0", "type": "功法"}
WUSHANG_SKILL = {"name": "自在天功", "item_type": "功法", "rank": "1", "level": "无上", "type": "功法"}
SPECIAL_ITEM = {"name": "化道石", "item_type": "特殊物品", "rank": "0", "type": "特殊道具"}
FUSION_GEAR = {"name": "合成法剑", "item_type": "法器", "rank": "1", "fusion": {"need_item": {}}, "type": "法器"}
BAIT_GEAR = {"name": "低阶法剑", "item_type": "法器", "rank": "1", "type": "法器"}
MOBILITY = {"name": "踏云步", "item_type": "身法", "rank": "0", "type": "功法"}

CATALOG = {
    1: PLAIN_SKILL,
    2: ATTACK_SKILL,
    3: WUSHANG_SKILL,
    4: SPECIAL_ITEM,
    5: FUSION_GEAR,
    6: BAIT_GEAR,
    7: MOBILITY,
    "bad": PLAIN_SKILL,
}


class ContributionRuleTests(unittest.TestCase):
    def test_contribution_clamps_into_reward_band(self) -> None:
        self.assertEqual(rules.reward_contribution(0.0), rules.DEMON_MIN_REWARD_CONTRIBUTION)
        self.assertEqual(rules.reward_contribution(-1.0), rules.DEMON_MIN_REWARD_CONTRIBUTION)
        self.assertEqual(rules.reward_contribution(0.42), 0.42)
        self.assertEqual(rules.reward_contribution(1.4), 1.0)
        self.assertEqual(rules.reward_contribution("0.2"), 0.2)
        self.assertEqual(rules.reward_contribution(None), rules.DEMON_MIN_REWARD_CONTRIBUTION)
        self.assertEqual(rules.reward_contribution("bad"), rules.DEMON_MIN_REWARD_CONTRIBUTION)

    def test_stone_and_exp_rate_follow_contribution(self) -> None:
        self.assertEqual(rules.stone_reward(0.5), 25_000_000)
        self.assertEqual(rules.stone_reward(1.0), rules.DEMON_STONE_REWARD_CAP)
        self.assertEqual(rules.stone_reward(3.0), rules.DEMON_STONE_REWARD_CAP)
        self.assertEqual(rules.stone_reward(None), 0)
        self.assertAlmostEqual(rules.exp_reward_rate(1.0), 0.05, places=12)
        self.assertAlmostEqual(rules.exp_reward_rate(0.2), 0.01, places=12)

    def test_talisman_tiers_and_cap(self) -> None:
        self.assertEqual(rules.talisman_reward_count(0.05), 0)
        self.assertEqual(rules.talisman_reward_count(0.10), 1)
        self.assertEqual(rules.talisman_reward_count(0.29), 2)
        self.assertEqual(rules.talisman_reward_count(1.0), rules.DEMON_TALISMAN_REWARD_CAP)
        self.assertEqual(rules.talisman_reward_count(-5), 0)
        self.assertEqual(rules.talisman_reward_count("bad"), 0)

    def test_contribution_percent_text(self) -> None:
        self.assertEqual(rules.format_contribution_percent(0.5), "50.00%")
        self.assertEqual(rules.format_contribution_percent(0.5, 1.5), "50.00%，结算75.00%")
        self.assertEqual(rules.format_contribution_percent(2.0, 1.0), "100.00%")

    def test_boss_and_record_multiplier_floor(self) -> None:
        self.assertEqual(rules.boss_reward_multiplier({"reward_multiplier": 1.5}), 1.5)
        self.assertEqual(rules.boss_reward_multiplier({"reward_multiplier": 0.2}), 1.0)
        self.assertEqual(rules.boss_reward_multiplier(None), 1.0)
        self.assertEqual(rules.record_reward_multiplier({"reward_multiplier": "2"}), 2.0)
        self.assertEqual(rules.record_reward_multiplier(None), 1.0)


class RandomPoolTests(unittest.TestCase):
    def test_wushang_detection(self) -> None:
        self.assertTrue(rules.is_wushang_reward_item(WUSHANG_SKILL))
        self.assertTrue(rules.is_wushang_reward_item({"item_type": "法器", "rank": "5"}))
        self.assertTrue(rules.is_wushang_reward_item({"rank": "无上"}))
        self.assertFalse(rules.is_wushang_reward_item(PLAIN_SKILL))
        self.assertFalse(rules.is_wushang_reward_item(None))

    def test_pool_filter_rules(self) -> None:
        low = rules.random_reward_pool(CATALOG, 0.1)
        low_ids = [item_id for item_id, _ in low]
        self.assertEqual(low_ids, [2, 3, 4, 6])
        high = rules.random_reward_pool(CATALOG, 0.6)
        high_ids = [item_id for item_id, _ in high]
        self.assertEqual(high_ids, [2, 3, 4, 6, 7])
        self.assertNotIn(5, low_ids)  # fusion gear never enters the pool
        self.assertNotIn(1, low_ids)  # rank 0 skill is filtered out
        self.assertNotIn("bad", low_ids)  # non-numeric id is skipped

    def test_mobility_needs_contribution_or_rank(self) -> None:
        self.assertFalse(rules.is_random_reward_item(MOBILITY, 0.2))
        self.assertTrue(rules.is_random_reward_item(MOBILITY, 0.5))
        self.assertTrue(rules.is_random_reward_item({**MOBILITY, "rank": "2"}, 0.2))

    def test_pick_keeps_wushang_only_on_100(self) -> None:
        pool = [(1, PLAIN_SKILL), (3, WUSHANG_SKILL)]
        self.assertEqual(rules.pick_random_reward(pool, StubRng(pick_keep_wushang=True))[0], 3)
        self.assertEqual(rules.pick_random_reward(pool, StubRng(pick_keep_wushang=False))[0], 1)
        self.assertIsNone(rules.pick_random_reward([], StubRng()))
        only_wushang = [(3, WUSHANG_SKILL)]
        self.assertIsNone(rules.pick_random_reward(only_wushang, StubRng(pick_keep_wushang=False)))

    def test_describe_random_reward(self) -> None:
        self.assertEqual(rules.describe_random_reward(2, ATTACK_SKILL), "风雷剑诀")
        self.assertEqual(
            rules.describe_random_reward(3, WUSHANG_SKILL), "无上:自在天功"
        )
        self.assertIsNone(rules.describe_random_reward(9, None))


class SettlementPlanTests(unittest.TestCase):
    def test_plan_without_random_reward(self) -> None:
        plan = settlement.build_demon_reward_plan(
            raw_contribution=0.25,
            talisman_item={"name": "灵签宝箓", "type": "特殊道具"},
            item_catalog=CATALOG,
            rng=StubRng(gate=1.0),  # gate above contribution → no random pick
        )
        self.assertEqual((plan.raw_contribution, plan.contribution), (0.25, 0.25))
        self.assertEqual(plan.stone, 12_500_000)
        self.assertAlmostEqual(plan.exp_rate, 0.0125, places=12)
        self.assertEqual(plan.talisman_count, 2)
        self.assertIsNone(plan.random_reward_id)
        self.assertEqual(plan.random_reward_text, settlement.NO_RANDOM_REWARD_TEXT)
        self.assertEqual(len(plan.items), 1)
        self.assertEqual(
            dict(plan.items[0]),
            {"id": rules.DEMON_TALISMAN_ITEM_ID, "name": "灵签宝箓", "type": "特殊道具", "amount": 2},
        )

    def test_plan_with_random_reward_appends_item(self) -> None:
        plan = settlement.build_demon_reward_plan(
            raw_contribution=0.6,
            talisman_item={"name": "灵签宝箓", "type": "特殊道具"},
            item_catalog={4: SPECIAL_ITEM},
            rng=StubRng(gate=0.0),
        )
        self.assertEqual(plan.random_reward_id, 4)
        self.assertEqual(plan.random_reward_text, "化道石")
        self.assertEqual(len(plan.items), 2)
        self.assertEqual(
            dict(plan.items[1]),
            {"id": 4, "name": "化道石", "type": "特殊道具", "amount": 1},
        )

    def test_plan_degrades_without_talisman_info(self) -> None:
        plan = settlement.build_demon_reward_plan(
            raw_contribution=0.5,
            talisman_item=None,
            item_catalog=(),
            rng=StubRng(gate=1.0),
        )
        self.assertEqual(plan.items, ())
        self.assertIsNone(plan.random_reward_id)

    def test_wave_and_contribution_text_helpers(self) -> None:
        self.assertEqual(settlement.wave_text(0, 0), "未知")
        self.assertEqual(settlement.wave_text("2", "2"), "第2波")
        self.assertEqual(settlement.wave_text(1, 3), "第1-3波")
        self.assertTrue(settlement.contribution_matches_raw(0.25, 0.25))
        self.assertFalse(settlement.contribution_matches_raw(0.25, 0.05))


class RecordContributionTests(unittest.TestCase):
    def test_wave_defaults_and_text_fields(self) -> None:
        self.assertEqual(rules.record_wave({"wave": "3"}), 3)
        self.assertEqual(rules.record_wave({}), 1)
        self.assertEqual(rules.record_wave(None), 1)

    def test_base_hp_prefers_largest_candidate(self) -> None:
        state = {"bosses": {"化神境": {"wave": 2, "boss_max_hp": 800}}}
        record = {"wave": 2, "damage": 900, "reward_base_hp": 100}
        self.assertEqual(rules.record_reward_base_hp(state, record, "化神境", 2), 900)
        self.assertEqual(rules.record_reward_base_hp(state, None, "化神境", 2), 1)
        other_wave = {"wave": 1, "damage": 10, "reward_base_hp": 50}
        self.assertEqual(rules.record_reward_base_hp(state, other_wave, "化神境", 1), 50)

    def test_record_contribution_prefers_frozen_value(self) -> None:
        frozen = {"reward_contribution": "0.4", "damage": 10}
        self.assertEqual(rules.record_reward_contribution({}, frozen, "化神境", 1), 0.4)
        computed = {"damage": 50, "reward_multiplier": 1.5, "reward_base_hp": 100}
        self.assertAlmostEqual(
            rules.record_reward_contribution({}, computed, "化神境", 1), 0.75, places=12
        )
        capped = {"damage": 100, "reward_multiplier": 4.0, "reward_base_hp": 100}
        self.assertEqual(rules.record_reward_contribution({}, capped, "化神境", 1), 1.0)
        self.assertEqual(rules.record_reward_contribution({}, None, "化神境", 1), 0.0)

    def test_sum_aggregates_damage_contribution_and_waves(self) -> None:
        state = {"bosses": {}}
        records = [
            ("a", {"wave": 2, "damage": 100, "realm": "化神境", "reward_contribution": 0.3}),
            ("b", {"wave": 1, "damage": 50, "realm": "化神境", "reward_contribution": 0.25}),
        ]
        damage, contribution, first, last = rules.sum_reward_contribution(state, records)
        self.assertEqual((damage, first, last), (150, 1, 2))
        self.assertAlmostEqual(contribution, 0.55, places=12)

    def test_sum_clamps_and_tolerates_missing_fields(self) -> None:
        records = [("a", {}), ("b", {"damage": "bad"})]
        damage, contribution, first, last = rules.sum_reward_contribution({}, records)
        self.assertEqual(contribution, 0.0)
        self.assertEqual((damage, first, last), (0, 1, 1))


class FacadeOwnershipContractTests(unittest.TestCase):
    def test_command_module_delegates_reward_computation(self) -> None:
        source = FACADE.read_text(encoding="utf-8")
        claim = source[source.index("plan = build_demon_reward_plan(") :]
        self.assertIn("rng=runtime_random", claim)
        self.assertIn("reward_items = [dict(item) for item in (plan.items", claim)
        for token in (
            "runtime_random.choice(reward_pool)",
            "min(int(DEMON_STONE_REWARD_CAP * contribution)",
            "int((contribution + 1e-12) / DEMON_TALISMAN_CONTRIBUTION_STEP)",
        ):
            self.assertNotIn(token, source)

    def test_rule_module_is_pure(self) -> None:
        module = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "world_events" / "reward_rules.py"
        text = module.read_text(encoding="utf-8")
        for banned in ("import nonebot", "import flask", "sqlite3", "DatabaseUnitOfWork"):
            self.assertNotIn(banned, text)


if __name__ == "__main__":
    unittest.main()
