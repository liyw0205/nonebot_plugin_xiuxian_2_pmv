import asyncio
from contextlib import ExitStack
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import nonebot
from nonebot.exception import FinishedException


class CatalogLiveReadReuseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_back
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import back_util
        cls.module, cls.util = xiuxian_back, back_util

    def setUp(self):
        self.profile = {"user_id": "u", "level": "realm", "root": "凡人", "root_type": "root",
                        "blessed_spot_flag": 0, "exp": 10**30 + 3, "stone": 10**31 + 9}
        self.manager = SimpleNamespace(
            get_user_info_with_id=Mock(return_value=self.profile),
            goods_max_num=Mock(return_value=9), goods_max_nums=Mock(side_effect=lambda ids: dict.fromkeys(ids, 9)),
            get_item_by_good_id_and_user_id=Mock(return_value={"all_num": 2, "goods_num": 3}),
            goods_num=Mock(return_value=3),
        )
        self.item = {"name": "weapon", "type": "装备", "item_type": "法器", "rank": 1, "desc": "desc"}
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for obj, name, value in (
            (self.util, "_sql_message", Mock(return_value=self.manager)),
            (self.module, "_sql_message", Mock(return_value=self.manager)),
            (self.util.items, "get_data_by_item_id", Mock(side_effect=lambda _: self.item)),
            (self.util, "get_required_rank_name", Mock(return_value=("realm", 10))),
            (self.util, "get_weapon_info_msg", Mock(return_value="weapon description")),
        ):
            self.stack.enter_context(patch.object(obj, name, value))

    def test_explicit_stage_inputs_skip_reads_but_default_helpers_read_each_time(self):
        text = self.util.get_item_msg(1, "u", user_info=self.profile, goods_total=0)
        self.assertIn("全服持有：0 个", text)
        self.manager.get_user_info_with_id.assert_not_called()
        self.manager.goods_max_num.assert_not_called()
        self.manager.goods_max_num.side_effect = [9, 12]
        self.assertIn("全服持有：9 个", self.util.get_item_msg(1, "u"))
        self.assertIn("全服持有：12 个", self.util.get_item_msg(1, "u"))
        self.assertEqual(self.manager.get_user_info_with_id.call_count, 2)
        self.util.get_item_msg(1, "u", user_info=None, goods_total=9)
        self.assertEqual(self.manager.get_user_info_with_id.call_count, 2)

    def test_pill_without_global_holding_keeps_personal_usage_live(self):
        self.item = {"name": "pill", "type": "丹药", "item_type": "丹药", "buff_type": "level_up_big",
                     "desc": "desc", "all_num": 10}
        self.assertIn("耐药性：2/10", self.util.get_item_msg(2, "u", user_info=self.profile))
        self.manager.get_item_by_good_id_and_user_id.assert_called_once_with("u", 2)
        self.manager.goods_max_num.assert_not_called()

    def test_item_effect_handler_passes_check_user_profile(self):
        bot, event = SimpleNamespace(self_id="bot"), SimpleNamespace()
        send = AsyncMock()
        with (
            patch.object(self.module, "assign_bot", AsyncMock(return_value=(bot, None))),
            patch.object(self.module, "check_user", return_value=(True, self.profile, "")),
            patch.object(self.module.items, "get_data_by_item_name", return_value=(1, self.item)),
            patch.object(self.module, "handle_send", send),
        ):
            with self.assertRaises(FinishedException):
                asyncio.run(self.module.check_item_effect_(bot, event, SimpleNamespace(extract_plain_text=lambda: "weapon")))
        self.manager.get_user_info_with_id.assert_not_called()
        self.assertIn("全服持有：9 个", send.await_args.args[2])

    def test_use_helpers_default_live_and_explicit_stage_preserve_eligibility(self):
        self.item = {"name": "pill", "type": "丹药", "item_type": "丹药", "rank": 1,
                     "all_num": 5, "buff_type": "unsupported"}
        original = deepcopy(self.profile)
        with patch.object(self.util, "convert_rank", return_value=(1,)):
            self.assertIn("不支持", self.util.check_use_elixir("u", 2, 1, user_info=self.profile))
            self.assertIn("不支持", self.util.check_use_elixir("u", 2, 1))
        self.assertIn("未拥有", self.util.get_use_jlq_msg("u", 3, user_info=self.profile))
        self.assertIn("未拥有", self.util.get_use_jlq_msg("u", 3))
        self.assertEqual(self.manager.get_user_info_with_id.call_count, 2)
        self.assertEqual(self.profile, original)

    def test_stamina_branch_retains_its_explicit_refresh(self):
        self.item = {"name": "pill", "rank": 1, "all_num": 5, "buff_type": "stamina"}
        refreshed = dict(self.profile, user_stamina=100)
        self.manager.get_user_info_with_id.return_value = refreshed
        with (
            patch.object(self.util, "convert_rank", return_value=(1,)),
            patch.object(self.util, "XiuConfig", return_value=SimpleNamespace(max_stamina=100)),
        ):
            self.assertIn("体力是满的", self.util.check_use_elixir("u", 2, 1, user_info=self.profile))
        self.manager.get_user_info_with_id.assert_called_once_with("u")

    def test_use_handler_passes_stage_profile_for_pill_flag_and_cultivation_item(self):
        bot = SimpleNamespace(self_id="bot")
        event = SimpleNamespace(message_id="use-event", get_plaintext=lambda: "使用 item")
        application = SimpleNamespace(cultivation_item=Mock(return_value=SimpleNamespace(succeeded=True)))
        self.manager.get_root_rate = Mock(return_value=2)
        for kind in ("丹药", "聚灵旗", "神物"):
            with self.subTest(kind=kind):
                self.item = {"name": "item", "type": kind, "item_type": kind, "rank": 1,
                             "all_num": 5, "buff_type": "unsupported", "buff": 10**30 + 1}
                self.manager.get_user_info_with_id.reset_mock()
                with (
                    patch.object(self.module, "assign_bot", AsyncMock(return_value=(bot, None))),
                    patch.object(self.module, "check_user", return_value=(True, self.profile, "")),
                    patch.object(self.module.items, "get_data_by_item_name", return_value=(1, self.item)),
                    patch.object(self.module, "convert_rank", return_value=(1,)),
                    patch.object(self.util, "convert_rank", return_value=(1,)),
                    patch.object(self.module, "get_required_rank_name", return_value=("realm", 10)),
                    patch.object(self.module, "added_ranks", 100),
                    patch.object(self.module, "XiuConfig", return_value=SimpleNamespace(max_goods_num=99)),
                    patch.object(self.module, "jsondata", SimpleNamespace(level_data=lambda: {"realm": {"spend": 3}})),
                    patch.object(self.module, "back_application", application),
                    patch.object(self.module, "handle_send", AsyncMock()),
                ):
                    with self.assertRaises(FinishedException):
                        asyncio.run(self.module.use_(bot, event, SimpleNamespace(extract_plain_text=lambda: "item")))
                self.manager.get_user_info_with_id.assert_not_called()
        self.assertEqual(application.cultivation_item.call_args.kwargs["exp_gain"], 10**30 + 1)
        self.assertEqual(application.cultivation_item.call_args.kwargs["operation_id"], "cultivation-item:use-event:u:1")

    def test_catalog_batches_all_matching_ids_before_original_format_and_page_order(self):
        rows = {str(i): {"name": f"skill{i}", "type": "技能", "item_type": "神通"} for i in range(1, 18)}
        formats, replies = [], []
        bot, event = SimpleNamespace(self_id="bot"), SimpleNamespace()

        def formatter(goods_id, *, goods_total):
            formats.append((goods_id, goods_total))
            return f"skill{goods_id}: {goods_total}"

        async def send(*args, **kwargs):
            replies.extend(args[4])

        with (
            patch.object(self.module, "assign_bot", AsyncMock(return_value=(bot, None))),
            patch.object(self.module.items, "get_data_by_item_type", return_value=rows),
            patch.object(self.module, "get_item_msg", formatter),
            patch.object(self.module, "send_msg_handler", send),
        ):
            with self.assertRaises(FinishedException):
                asyncio.run(self.module.chakan_wupin_(bot, event, SimpleNamespace(extract_plain_text=lambda: "神通2")))
        self.manager.goods_max_nums.assert_called_once_with(rows)
        self.assertEqual(formats, [(key, 9) for key in rows])
        self.assertTrue(replies[0].startswith("ID：16\n"))
        self.assertTrue(replies[1].startswith("ID：17\n"))
        self.assertIn("第 2/2 页", "\n".join(replies))
        self.manager.goods_max_num.assert_not_called()
