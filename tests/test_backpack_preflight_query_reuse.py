from __future__ import annotations

import asyncio
from contextlib import ExitStack
from copy import deepcopy
import importlib
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import nonebot
from nonebot.exception import FinishedException


class MixelixirPreflightQueryReuseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        cls.module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_mixelixir"
        )

    def setUp(self) -> None:
        self.rows = [
            {"goods_id": 1, "goods_name": "草", "goods_type": "药材", "goods_num": 2},
            {"goods_id": 2, "goods_name": "引", "goods_type": "药材", "goods_num": 2},
            {"goods_id": 3, "goods_name": "辅", "goods_type": "药材", "goods_num": 2},
            {"goods_id": 4, "goods_name": "炉", "goods_type": "炼丹炉", "goods_num": 1},
        ]
        self.items = SimpleNamespace(get_data_by_item_id=lambda item_id: {
            1: {"name": "草"}, 2: {"name": "引"}, 3: {"name": "辅"}, 4: {"buff": 0},
        }[int(item_id)])

    def test_projection_helpers_use_supplied_rows_and_preserve_first_match(self) -> None:
        rows = self.rows + [dict(self.rows[0], goods_id=5, goods_num=9)]
        catalog = SimpleNamespace(get_data_by_item_id=lambda item_id: (
            {"name": "草"} if int(item_id) == 5 else self.items.get_data_by_item_id(item_id)
        ))
        original = deepcopy(rows)
        with (
            patch.object(self.module, "Items", return_value=catalog),
            patch.object(self.module, "_sql_message", side_effect=AssertionError("unexpected live read")),
        ):
            self.assertEqual(asyncio.run(self.module.check_yaocai_name_in_back(
                "u", "草", 2, back_rows=rows
            )), (True, 1))
            self.assertEqual(asyncio.run(self.module.check_yaocai_name_in_back(
                "u", "草", 3, back_rows=rows
            )), (True, 5))
            self.assertEqual(asyncio.run(self.module.check_yaocai_name_in_back(
                "u", "草", 0, back_rows=rows
            )), (False, 0))
            self.assertEqual(asyncio.run(self.module.check_ldl_name_in_back(
                "u", "炉", back_rows=rows
            )), (True, {"buff": 0}))
            self.assertEqual(asyncio.run(self.module.check_yaocai_name_in_back(
                "u", "草", 1, back_rows=[]
            )), (False, 0))
            self.assertEqual(asyncio.run(self.module.check_ldl_name_in_back(
                "u", "炉", back_rows=[]
            )), (False, {}))
        self.assertEqual(rows, original)

    def test_standalone_helpers_read_live_each_time(self) -> None:
        manager = SimpleNamespace(get_back_msg=Mock(side_effect=[self.rows, [], self.rows, None]))
        with (
            patch.object(self.module, "_sql_message", return_value=manager),
            patch.object(self.module, "Items", return_value=self.items),
        ):
            self.assertEqual(asyncio.run(self.module.check_yaocai_name_in_back("u", "草", 1)), (True, 1))
            self.assertEqual(asyncio.run(self.module.check_yaocai_name_in_back("u", "草", 1)), (False, 0))
            self.assertEqual(asyncio.run(self.module.check_ldl_name_in_back("u", "炉")), (True, {"buff": 0}))
            self.assertEqual(asyncio.run(self.module.check_ldl_name_in_back("u", "炉")), (False, {}))
        self.assertEqual(manager.get_back_msg.call_count, 4)
        self.assertEqual([call.kwargs for call in manager.get_back_msg.call_args_list],
                         [{"goods_types": ("药材",)}, {"goods_types": ("药材",)},
                          {"goods_types": ("炼丹炉",)}, {"goods_types": ("炼丹炉",)}])

    def _reject_recipe(self, rows, recipe="主药草1药引引1辅药辅1丹炉炉"):
        application = SimpleNamespace(
            refine_cost_replay=Mock(return_value=None),
            latest_ready_refine_task=Mock(return_value=None),
            refine_cost=Mock(side_effect=AssertionError("unexpected settlement")),
        )
        manager = SimpleNamespace(get_back_msg=Mock(return_value=rows))
        send = AsyncMock()
        bot, event = SimpleNamespace(self_id="bot"), SimpleNamespace(message_id="preflight")
        with (
            patch.object(self.module, "mixelixir_application", application),
            patch.object(self.module, "_sql_message", return_value=manager),
            patch.object(self.module, "Items", return_value=self.items),
            patch.object(self.module, "assign_bot", new=AsyncMock(return_value=(bot, None))),
            patch.object(self.module, "check_user", return_value=(True, {"user_id": "u", "mixelixir_num": 0}, "")),
            patch.object(self.module, "handle_send", new=send),
        ):
            with self.assertRaises(FinishedException):
                asyncio.run(self.module.mix_make_(bot, event, mode=recipe))
        manager.get_back_msg.assert_called_once_with("u", goods_types=("药材", "炼丹炉"))
        application.refine_cost.assert_not_called()
        send.assert_awaited_once()
        return send.await_args.args[2]

    def test_handler_reuses_one_read_and_preserves_rejection_order(self) -> None:
        for length, subject in ((0, "主药：草"), (1, "药引：引"), (2, "辅药：辅"), (3, "炼丹炉：炉")):
            with self.subTest(subject=subject):
                self.assertIn(subject, self._reject_recipe(self.rows[:length]))

    def test_repeated_main_or_catalyst_and_auxiliary_require_combined_count(self) -> None:
        for recipe, subject in (
            ("主药草2药引引1辅药草1丹炉炉", "主药：草"),
            ("主药草1药引引2辅药引1丹炉炉", "药引：引"),
        ):
            with self.subTest(recipe=recipe):
                self.assertIn(subject, self._reject_recipe(self.rows, recipe))


class FusionPreflightQueryReuseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        cls.module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_fusion"
        )
        cls.back_util = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.back_util"
        )

    def setUp(self) -> None:
        self.equipment = {
            "name": "成品", "type": "特殊道具",
            "fusion": {"need_item": {1: 2}, "limit": 1, "need_stone": 10},
        }
        self.application = SimpleNamespace(
            apply_result=Mock(return_value=None),
            repository=SimpleNamespace(batch_result=Mock(return_value=None)),
            apply=Mock(return_value=SimpleNamespace(
                ok=True, replayed=False, code="", status="applied",
                data={"status": "applied", "successful": True, "protected": False},
            )),
            apply_batch=Mock(return_value=SimpleNamespace(
                ok=True, replayed=False, code="", status="applied",
                data={"status": "applied", "successful_count": 2, "failed_count": 0, "protected_count": 0},
            )),
        )
        self.manager = SimpleNamespace(
            get_user_info_with_id=Mock(return_value={"level": "江湖好手", "exp": 100, "stone": 100}),
            get_back_msg=Mock(),
        )
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for target, name, value in (
            (self.module, "fusion_application", self.application),
            (self.module, "_sql_message", Mock(return_value=self.manager)),
            (self.module, "convert_rank", Mock(return_value=(1,))),
            (self.module, "XiuConfig", Mock(return_value=SimpleNamespace(max_goods_num=99))),
            (self.module, "_items", Mock(return_value=SimpleNamespace(
                get_data_by_item_id=lambda item_id: {"name": f"材料{item_id}"}
            ))),
            (self.back_util, "_sql_message", Mock(side_effect=AssertionError("unexpected item point read"))),
        ):
            self.stack.enter_context(patch.object(target, name, value))

    def _run(self, rows, quantity=1):
        self.manager.get_back_msg.return_value = rows
        return asyncio.run(self.module.general_fusion("u", 100, self.equipment, "operation", quantity))

    def test_duplicate_material_rows_are_counted_with_first_row_limit_and_reservation(self) -> None:
        # Production migration makes goods IDs unique; these rows test indexing only.
        rows = [
            {"goods_id": 1, "goods_type": "装备", "goods_num": 2, "state": 1, "bind_num": 2},
            {"goods_id": 1, "goods_type": "装备", "goods_num": 2, "state": 1, "bind_num": 1},
            {"goods_id": 100, "goods_type": "道具", "goods_num": 0},
            {"goods_id": 100, "goods_type": "道具", "goods_num": 99},
        ]
        original = deepcopy(rows)
        successful, message = self._run(rows)
        self.assertTrue(successful, message)
        self.manager.get_back_msg.assert_called_once_with("u")
        self.manager.get_user_info_with_id.assert_called_once_with("u")
        self.assertEqual(self.application.apply.call_args.kwargs["reserved_items"], {1: 1})
        self.assertEqual(self.application.apply.call_args.kwargs["needed_items"], {1: 2})
        self.assertEqual(rows, original)

    def test_unique_material_row_preserves_equipped_reservation(self) -> None:
        for state in (0, 1):
            with self.subTest(state=state):
                self.assertTrue(self._run([
                    {"goods_id": 1, "goods_type": "装备", "goods_num": 3, "state": state},
                ])[0])
                self.assertEqual(self.application.apply.call_args.kwargs["reserved_items"],
                                 {1: 1} if state else {})

    def test_missing_material_message_uses_existing_equipment_row_in_recipe_order(self) -> None:
        self.equipment["fusion"]["need_item"] = {1: 3, 2: 1}
        result = self._run([{"goods_id": 1, "goods_type": "装备", "goods_num": 1, "state": 1}])
        self.assertEqual(result, (False, "道友还缺少：\n3 个 材料1\n1 个 材料2"))
        self.application.apply.assert_not_called()

    def test_string_inventory_ids_keep_original_nonmatching_behavior(self) -> None:
        result = self._run([{"goods_id": "1", "goods_type": "道具", "goods_num": 99}])
        self.assertEqual(result, (False, "道友还缺少：\n2 个 材料1"))
        self.application.apply.assert_not_called()

    def test_single_and_batch_replay_skip_live_user_and_inventory_reads(self) -> None:
        self.application.apply_result.return_value = SimpleNamespace(succeeded=True, successful=True)
        self.application.repository.batch_result.return_value = SimpleNamespace(
            succeeded=True, successful_count=2, failed_count=0, protected_count=0
        )
        for quantity in (1, 2):
            with self.subTest(quantity=quantity):
                successful, message = self._run([], quantity)
                self.assertTrue(successful)
                self.assertIn("已经处理", message)
        self.manager.get_back_msg.assert_not_called()
        self.manager.get_user_info_with_id.assert_not_called()
        self.application.apply.assert_not_called()
        self.application.apply_batch.assert_not_called()

    def test_separate_requests_read_live_inventory_again(self) -> None:
        self.assertTrue(self._run([{"goods_id": 1, "goods_type": "道具", "goods_num": 2}])[0])
        self.assertEqual(self._run([]), (False, "道友还缺少：\n2 个 材料1"))
        self.assertEqual(self.manager.get_back_msg.call_count, 2)
        self.application.apply.assert_called_once()

    def test_live_transactional_material_rejection_still_reaches_reply(self) -> None:
        self.application.apply.return_value = SimpleNamespace(
            ok=False, replayed=False, code="item_insufficient", status="rejected",
            data={"status": "item_insufficient"},
        )
        result = self._run([{"goods_id": 1, "goods_type": "道具", "goods_num": 2}])
        self.assertEqual(result, (False, "合成：材料不足，本次合成未结算。"))
        self.application.apply.assert_called_once()

    def test_batch_keeps_reservation_and_quantity_for_transactional_application(self) -> None:
        self.equipment["fusion"]["limit"] = 3
        rows = [{"goods_id": 1, "goods_type": "装备", "goods_num": 5, "state": 1}]
        self.assertTrue(self._run(rows, quantity=2)[0])
        self.application.apply.assert_not_called()
        kwargs = self.application.apply_batch.call_args.kwargs
        self.assertEqual(kwargs["reserved_items"], {1: 1})
        self.assertEqual(kwargs["outcomes"], (True, True))
        self.assertEqual(kwargs["needed_items"], {1: 2})
        self.assertEqual(kwargs["target_limit"], 3)


if __name__ == "__main__":
    unittest.main()
