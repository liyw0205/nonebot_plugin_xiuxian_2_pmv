from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

import nonebot

from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.plugin import build_lifecycle
from tests.bootstrap import copy_static_data


class BackAlchemyWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    def test_lifecycle_binds_feature_back_application_to_legacy_handler(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_rift

        previous_rift_entry_service = xiuxian_rift._rift_entry_service_instance
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            context = build_runtime_context(data_dir=data_dir, legacy_startup=True)
            lifecycle, _, context = build_lifecycle(context)
            asyncio.run(lifecycle.start())
            try:
                from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_back

                self.assertIs(xiuxian_back.back_application, context.services["back"])
                self.assertEqual(context.services["back"].alchemy_application.__class__.__name__, "AlchemyApplication")
                self.assertEqual(context.services["back"].cultivation_item_application.__class__.__name__, "CultivationItemApplication")
                self.assertEqual(context.services["back"].skill_learning_application.__class__.__name__, "SkillLearningApplication")
                self.assertEqual(context.services["back"].lottery_talisman_application.__class__.__name__, "LotteryTalismanApplication")
                self.assertEqual(context.services["back"].stone_reward_application.__class__.__name__, "StoneRewardApplication")
                self.assertEqual(context.services["back"].three_cultivation_pill_application.__class__.__name__, "ThreeCultivationPillApplication")
                self.assertEqual(context.services["back"].breakthrough_rate_item_application.__class__.__name__, "BreakthroughRateItemApplication")
                self.assertEqual(context.services["back"].recovery_item_application.__class__.__name__, "RecoveryItemApplication")
                self.assertEqual(context.services["back"].permanent_atk_item_application.__class__.__name__, "PermanentAtkItemApplication")
                self.assertEqual(context.services["back"].unbind_application.__class__.__name__, "UnbindApplication")
                self.assertEqual(context.services["back"].blessed_flag_replace_application.__class__.__name__, "BlessedFlagReplaceApplication")
                self.assertEqual(context.services["back"].equipment_application.__class__.__name__, "EquipmentApplication")
                self.assertEqual(context.services["back"].repair_application.__class__.__name__, "BackpackRepairApplication")
                self.assertEqual(context.services["back"].pet_egg_application.__class__.__name__, "PetEggApplication")
            finally:
                asyncio.run(lifecycle.shutdown())
                xiuxian_rift._rift_entry_service_instance = previous_rift_entry_service


if __name__ == "__main__":
    unittest.main()
