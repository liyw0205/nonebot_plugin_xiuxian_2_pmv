"""Typed inventory reads keep legacy rows, ordering, errors and live values."""
from __future__ import annotations

import asyncio
from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

import nonebot
from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XiuxianDateManage


FIELDS = ("goods_id", "goods_name", "goods_type", "goods_num", "bind_num", "state")


class InventoryReadProjectionTests(unittest.TestCase):
    def reader(self, *, indexed=False):
        raw = sqlite3.connect(":memory:")
        self.addCleanup(raw.close)
        raw.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                    "goods_type TEXT COLLATE NOCASE,goods_num INTEGER,bind_num,state,unused TEXT)")
        if indexed:
            raw.execute("CREATE INDEX user_goods ON back(user_id,goods_id)")
        raw.executemany("INSERT INTO back VALUES(?,?,?,?,?,?,?,?)", [
            ("u", 8, "First herb", "药材", 2, None, 0, "unused"),
            ("u", 2, "Equipment", "装备", 3, 1, 1, "unused"),
            ("u", 8, "Second herb", "药材", 9, 2, None, "unused"),
            ("u", 4, "Pill", "丹药", 1, 0, 0, "unused"),
            ("u", 5, "Furnace", "炼丹炉", 1, 0, 0, "unused"),
            ("u", 6, "Zero", "药材", 0, 0, 0, "unused"),
            ("u", 7, "Negative", "装备", -1, 0, 0, "unused"),
            ("u", 9, "Null type", None, 1, 0, 0, "unused"),
            ("u", 10, "Null quantity", "药材", None, 0, 0, "unused"),
            ("u", 11, "Blob type", sqlite3.Binary("药材".encode()), 1, 0, 0, "unused"),
            ("u", 12, "Unrelated", "技能", 4, 0, 0, "unused"),
            ("other", 8, "Other user", "药材", 100, 0, 0, "unused"),
        ])
        raw.commit()
        manager = object.__new__(XiuxianDateManage)
        manager._read_cache_ttl = 0
        connection = db_backend.SQLiteConnection(raw, Path(":memory:"))
        manager._borrow_fast_conn = Mock(return_value=connection)
        manager._return_fast_conn = Mock()
        self.addCleanup(lambda: setattr(manager, "conn", None))
        return manager, raw

    def test_fixed_projection_matches_legacy_filter_including_duplicates_and_nulls(self):
        for indexed in (False, True):
            with self.subTest(indexed=indexed):
                manager, raw = self.reader(indexed=indexed)
                statements = []
                raw.set_trace_callback(statements.append)
                original = manager.get_back_msg("u")
                self.assertIn("unused", original[0])
                before = raw.total_changes
                for types in (("药材",), ("装备",), ("丹药",), ("药材", "炼丹炉")):
                    statements.clear()
                    actual = manager.get_back_msg("u", goods_types=types)
                    expected = [{field: row[field] for field in FIELDS}
                                for row in original if row["goods_type"] in types]
                    self.assertEqual(expected, actual)
                    self.assertEqual(1, len(statements))
                    self.assertNotIn("SELECT *", statements[0])
                self.assertEqual([{field: row[field] for field in FIELDS} for row in original],
                                 manager.get_back_msg("u", compact=True))
                self.assertEqual(before, raw.total_changes)

    def test_external_writes_and_local_result_mutation_never_reuse_personal_values(self):
        manager, raw = self.reader(indexed=True)
        rows = manager.get_back_msg("u", goods_types=("装备",))
        rows[0]["state"] = 99
        raw.execute("UPDATE back SET goods_num=5,bind_num=NULL,state=0 WHERE user_id='u' AND goods_id=2")
        raw.commit()
        self.assertEqual({"goods_id": 2, "goods_name": "Equipment", "goods_type": "装备",
                          "goods_num": 5, "bind_num": None, "state": 0},
                         manager.get_back_msg("u", goods_types=("装备",))[0])
        raw.execute("UPDATE back SET goods_num=0 WHERE user_id='u' AND goods_id=2")
        raw.commit()
        self.assertEqual([], manager.get_back_msg("u", goods_types=("装备",)))
        self.assertEqual([], manager.get_back_msg("absent", compact=True))

    def test_invalid_types_do_no_io_and_real_read_errors_propagate(self):
        manager, raw = self.reader()
        self.assertEqual([], manager.get_back_msg("u", goods_types=()))
        for invalid in ("药材", ("unknown",), ("药材'); DROP TABLE back;--",)):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                manager.get_back_msg("u", goods_types=invalid)
        manager._borrow_fast_conn.assert_not_called()
        raw.set_authorizer(lambda action, *_: sqlite3.SQLITE_DENY
                           if action == sqlite3.SQLITE_READ else sqlite3.SQLITE_OK)
        with self.assertRaises(sqlite3.DatabaseError):
            manager.get_back_msg("u", goods_types=("药材",))
        self.assertFalse(manager._return_fast_conn.call_args.kwargs["reusable"])
        raw.set_authorizer(None)
        self.assertEqual(2, len(manager.get_back_msg("u", goods_types=("药材",))))


class InventoryProjectionCallerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_back, xiuxian_mixelixir
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import back_util
        cls.back, cls.mix, cls.util = xiuxian_back, xiuxian_mixelixir, back_util

    def test_plain_and_markdown_views_match_old_full_rows_without_extra_reads(self):
        rows = [
            {"goods_id": 1, "goods_name": "stored herb", "goods_type": "药材", "goods_num": 2, "bind_num": 1, "state": 0},
            {"goods_id": 2, "goods_name": "stored pill", "goods_type": "丹药", "goods_num": 3, "bind_num": 1, "state": 0},
            {"goods_id": 3, "goods_name": "stored sword", "goods_type": "装备", "goods_num": 1, "bind_num": 0, "state": 1},
            {"goods_id": 4, "goods_name": "ignored", "goods_type": "技能", "goods_num": 9, "bind_num": 0, "state": 0},
        ]
        catalog = {
            1: {"name": "catalog herb", "level": "一品药材"},
            2: {"name": "catalog pill", "buff_type": "hp", "level": "一品"},
            3: {"name": "catalog sword", "item_type": "法器", "level": "人阶下品"},
        }
        calls = [(self.util, "get_user_equipment_msg", "装备"),
                 (self.util, "get_user_danyao_back_msg", "丹药"),
                 (self.util, "get_user_yaocai_back_msg", "药材"),
                 (self.util, "get_user_yaocai_detail_back_msg", "药材"),
                 (self.back, "_build_danyao_sections_for_md", "丹药"),
                 (self.back, "_build_yaocai_sections_for_md", "药材")]
        with ExitStack() as stack:
            for module in (self.util, self.back):
                stack.enter_context(patch.object(module.items, "get_data_by_item_id", side_effect=catalog.get))
            stack.enter_context(patch.object(self.util, "get_weapon_info_msg", return_value="weapon"))
            stack.enter_context(patch.object(self.util, "get_yaocai_info_msg", return_value="herb detail"))
            for module, name, kind in calls:
                with self.subTest(view=name):
                    full = SimpleNamespace(get_back_msg=Mock(return_value=deepcopy(rows)))
                    typed = SimpleNamespace(get_back_msg=Mock(return_value=[deepcopy(row) for row in rows
                                                                           if row["goods_type"] == kind]))
                    with patch.object(module, "_sql_message", return_value=full):
                        expected = getattr(module, name)("u")
                    with patch.object(module, "_sql_message", return_value=typed):
                        actual = getattr(module, name)("u")
                    self.assertTrue(actual)
                    self.assertEqual(expected, actual)
                    typed.get_back_msg.assert_called_once_with("u", goods_types=(kind,))

    def test_generation_preserves_whole_bag_empty_vs_no_herbs_without_point_reads(self):
        for rows, message in (([], "背包空空如也"),
                              ([{"goods_type": "技能"}], "背包内没有药材"),
                              ([{"goods_id": 1, "goods_type": "药材", "goods_num": 2}], "背包内没有炼丹炉")):
            with self.subTest(message=message):
                manager = SimpleNamespace(get_back_msg=Mock(return_value=deepcopy(rows)))
                send = AsyncMock()
                bot, event = SimpleNamespace(self_id="bot"), SimpleNamespace()
                with (
                    patch.object(self.mix, "_sql_message", return_value=manager),
                    patch.object(self.mix, "assign_bot", new=AsyncMock(return_value=(bot, None))),
                    patch.object(self.mix, "check_user", return_value=(True, {"user_id": "projection-generation"}, "")),
                    patch.object(self.mix.items, "get_data_by_item_id", return_value={"name": "herb"}),
                    patch.object(self.mix, "handle_send", new=send),
                ):
                    with self.assertRaises(FinishedException):
                        asyncio.run(self.mix.mix_elixir_(bot, event))
                manager.get_back_msg.assert_called_once_with("projection-generation", compact=True)
                self.assertIn(message, send.await_args.args[2])

    def test_main_backpack_full_and_compact_rows_preserve_output_and_duplicate_order(self):
        rows = [
            {"goods_id": 3, "goods_name": "same sword", "goods_type": "装备", "goods_num": 2, "bind_num": 1, "state": None},
            {"goods_id": 3, "goods_name": "same sword", "goods_type": "装备", "goods_num": 3, "bind_num": 2, "state": 1},
            {"goods_id": 5, "goods_name": "stored skill", "goods_type": "技能", "goods_num": 4, "bind_num": 0, "state": 0},
            {"goods_id": 8, "goods_name": "stored herb", "goods_type": "药材", "goods_num": 9, "bind_num": 0, "state": 0},
            {"goods_id": 9, "goods_name": "ignored", "goods_type": None, "goods_num": 1, "bind_num": 0, "state": None},
        ]
        catalog = {3: {"name": "catalog sword", "item_type": "法器", "level": "下品符器"},
                   5: {"name": "catalog skill", "item_type": "功法", "level": "人阶下品"},
                   8: {"name": "catalog herb"}}
        for module, name in ((self.util, "get_user_main_back_msg"),
                             (self.back, "_build_main_backpack_sections_for_md")):
            with self.subTest(view=name):
                outputs = []
                for compact in (False, True):
                    returned = [dict(row, user_id="u", unused="legacy extra") for row in rows]
                    if compact:
                        returned = [{field: row[field] for field in FIELDS} for row in returned]
                    manager = SimpleNamespace(get_back_msg=Mock(return_value=returned),
                                              get_item_by_good_id_and_user_id=Mock(
                                                  side_effect=AssertionError("loaded state must be reused")))
                    with (
                        patch.object(module, "_sql_message", return_value=manager),
                        patch.object(self.util, "_sql_message", return_value=manager),
                        patch.object(module.items, "get_data_by_item_id", side_effect=catalog.get),
                    ):
                        outputs.append(getattr(module, name)("u"))
                    manager.get_back_msg.assert_called_once_with("u", compact=True)
                    manager.get_item_by_good_id_and_user_id.assert_not_called()
                self.assertEqual(outputs[0], outputs[1])
                if module is self.util:
                    swords = [text for text in outputs[1] if "catalog sword" in text]
                    self.assertEqual(len(swords), 2)
                    self.assertIn("拥有数量：3，绑定数量：2", swords[0])
                    self.assertIn("※已装备※", swords[0])
                    self.assertIn("拥有数量：2，绑定数量：1", swords[1])
                else:
                    self.assertEqual([section[0] for section in outputs[1]], ["法器", "功法"])
                    sword = outputs[1][0][1][0]
                    self.assertEqual((sword["name"], sword["count"], sword["bind"], sword["is_equipped"]),
                                     ("same sword", 5, 3, False))

    def test_main_backpack_compact_rows_keep_empty_and_unknown_catalog_behavior(self):
        for kind in (None, "药材", "装备"):
            rows = [] if kind is None else [
                {"goods_id": 9, "goods_name": "unknown", "goods_type": kind,
                 "goods_num": 1, "bind_num": 0, "state": 0},
            ]
            for module, name in ((self.util, "get_user_main_back_msg"),
                                 (self.back, "_build_main_backpack_sections_for_md")):
                for compact in (False, True):
                    with self.subTest(kind=kind, view=name, compact=compact):
                        returned = [dict(row, user_id="u", unused="legacy extra") for row in rows]
                        if compact:
                            returned = [{field: row[field] for field in FIELDS} for row in returned]
                        manager = SimpleNamespace(get_back_msg=Mock(return_value=returned),
                                                  get_item_by_good_id_and_user_id=Mock(
                                                      side_effect=AssertionError("no point read expected")))
                        with (
                            patch.object(module, "_sql_message", return_value=manager),
                            patch.object(self.util, "_sql_message", return_value=manager),
                            patch.object(module.items, "get_data_by_item_id", return_value=None),
                        ):
                            if module is self.util and kind == "装备":
                                with self.assertRaises(TypeError):
                                    getattr(module, name)("u")
                            else:
                                self.assertEqual(getattr(module, name)("u"), [])
                        manager.get_back_msg.assert_called_once_with("u", compact=True)
                        manager.get_item_by_good_id_and_user_id.assert_not_called()

    def _run_inventory_action(self, name, rows, catalog, argument, *, succeeded=True):
        manager = SimpleNamespace(get_back_msg=Mock(return_value=deepcopy(rows)),
                                  get_item_by_good_id_and_user_id=Mock(return_value={"state": 1}))
        application = SimpleNamespace(
            alchemy=Mock(return_value=SimpleNamespace(succeeded=succeeded)),
            change_equipment=Mock(return_value=SimpleNamespace(succeeded=succeeded)),
        )
        bot, event = SimpleNamespace(self_id="bot"), SimpleNamespace(message_id="projection-action")
        send, summary = AsyncMock(), AsyncMock()
        with (
            patch.object(self.back, "assign_bot", AsyncMock(return_value=(bot, None))),
            patch.object(self.back, "check_user", return_value=(True, {"user_id": "u"}, "")),
            patch.object(self.back, "_sql_message", return_value=manager),
            patch.object(self.util, "_sql_message", return_value=manager),
            patch.object(self.back.items, "get_data_by_item_id", side_effect=catalog.get),
            patch.object(self.back, "back_application", application),
            patch.object(self.back, "get_recover", side_effect=lambda goods_id, num: goods_id * num),
            patch.object(self.back, "BANNED_ITEM_IDS_ALCHEMY", set()),
            patch.object(self.back, "type_mapping", {"全部": ["法器", "防具", "药材"]}),
            patch.object(self.back, "rank_map", {"全部": ["rank"]}),
            patch.object(self.back, "_back_op_fail_msg", return_value="authoritative rejection"),
            patch.object(self.back, "handle_send", send),
            patch.object(self.back, "send_msg_handler", summary),
        ):
            with self.assertRaises(FinishedException):
                asyncio.run(getattr(self.back, name)(bot, event, SimpleNamespace(
                    extract_plain_text=lambda: argument)))
        manager.get_back_msg.assert_called_once_with("u", compact=True)
        return manager, application, send, summary

    def test_fast_alchemy_compact_plan_preserves_reservations_duplicates_and_authoritative_rejection(self):
        rows = [
            {"goods_id": 3, "goods_name": "equipped", "goods_type": "装备", "goods_num": 4, "bind_num": 3, "state": 1},
            {"goods_id": 8, "goods_name": "herb first", "goods_type": "药材", "goods_num": 2, "bind_num": None, "state": None},
            {"goods_id": 8, "goods_name": "herb second", "goods_type": "药材", "goods_num": 3, "bind_num": 0, "state": 1},
            {"goods_id": 4, "goods_name": "pill first", "goods_type": "丹药", "goods_num": 5, "bind_num": None, "state": 2},
            {"goods_id": 4, "goods_name": "pill second", "goods_type": "丹药", "goods_num": 2, "bind_num": 1, "state": None},
            {"goods_id": 9, "goods_name": "unknown", "goods_type": None, "goods_num": 1, "bind_num": 0, "state": None},
        ]
        catalog = {3: {"type": "装备", "item_type": "法器", "level": "rank"},
                   8: {"type": "药材", "item_type": "药材", "level": "rank"},
                   4: {"type": "丹药", "item_type": "丹药", "buff_type": "hp", "level": "rank"}}
        for argument, consumed, reward, mode in (
            ("全部 全部", [(3, 3), (8, 2), (8, 2)], 41, "fast-全部-全部"),
            ("回血丹", [(4, 3), (4, 2)], 20, "fast-hp"),
        ):
            for succeeded in (True, False):
                with self.subTest(argument=argument, succeeded=succeeded):
                    results = []
                    for compact in (False, True):
                        returned = [dict(row, unused="extra", user_id="u") for row in rows]
                        if compact:
                            returned = [{field: row[field] for field in FIELDS} for row in returned]
                        manager, application, send, summary = self._run_inventory_action(
                            "fast_alchemy_", returned, catalog, argument, succeeded=succeeded)
                        application.alchemy.assert_called_once_with(
                            operation_id=f"alchemy:projection-action:u:{mode}", user_id="u",
                            reward_stone=reward, consume_items=consumed)
                        manager.get_item_by_good_id_and_user_id.assert_not_called()
                        results.append((send.await_args_list, summary.await_args_list))
                        if succeeded:
                            summary.assert_awaited_once()
                            send.assert_not_awaited()
                            text = "\n".join(summary.await_args.args[4])
                            self.assertIn("总计获得：", text)
                            if mode != "fast-hp":
                                self.assertIn("已装备或使用中，保留1个", text)
                        else:
                            summary.assert_not_awaited()
                            self.assertIn("背包数量发生变化", send.await_args.args[2])
                    self.assertEqual(results[0], results[1])

    def test_compact_actions_keep_empty_vs_nonempty_unmatched_bag_replies(self):
        unknown = {"goods_id": 9, "goods_name": "unknown", "goods_type": None,
                   "goods_num": 1, "bind_num": None, "state": None}
        for name, argument, absent in (("fast_alchemy_", "回血丹", "背包中没有回血丹"),
                                       ("fast_alchemy_", "全部 全部", "背包中没有符合条件"),
                                       ("no_use_zb_", "target", "请检查道具 target 是否在背包内")):
            for rows, expected in (([], "背包空空如也"), ([unknown], absent)):
                with self.subTest(handler=name, argument=argument, empty=not rows):
                    manager, application, send, summary = self._run_inventory_action(name, rows, {}, argument)
                    self.assertIn(expected, send.await_args.args[2])
                    application.alchemy.assert_not_called()
                    application.change_equipment.assert_not_called()
                    manager.get_item_by_good_id_and_user_id.assert_not_called()
                    summary.assert_not_awaited()

    def test_unequip_compact_selection_retains_first_name_and_live_point_read(self):
        rows = [{"goods_id": goods_id, "goods_name": "same name", "goods_type": "装备",
                 "goods_num": 1, "bind_num": None, "state": None} for goods_id in (3, 5)]
        for compact in (False, True):
            with self.subTest(compact=compact):
                returned = [dict(row, unused="extra", user_id="u") for row in rows]
                if compact:
                    returned = [{field: row[field] for field in FIELDS} for row in returned]
                manager, application, send, summary = self._run_inventory_action(
                    "no_use_zb_", returned, {3: {"item_type": "法器"}}, "same name", succeeded=False)
                manager.get_item_by_good_id_and_user_id.assert_called_once_with("u", 3)
                application.change_equipment.assert_called_once_with(
                    operation_id="equipment:projection-action:unequip:3", user_id="u",
                    goods_id=3, item_type="法器", equip=False)
                self.assertEqual(send.await_args.args[2], "authoritative rejection")
                summary.assert_not_awaited()

    def test_replay_safe_special_items_dispatch_requested_quantity_without_unused_inventory_read(self):
        bot, event = SimpleNamespace(), SimpleNamespace()
        for goods_id, handler_name in ((20001, "use_rift_key"), (20007, "use_rift_explore"),
                                       (20012, "use_rift_speedup"), (20013, "use_rift_big_speedup"),
                                       (20018, "use_rift_boss")):
            for have in (0, 2):
                with self.subTest(goods_id=goods_id, inventory=have):
                    manager = SimpleNamespace(goods_num=Mock(return_value=have))
                    handler = AsyncMock()
                    with (
                        patch.object(self.back, "assign_bot", AsyncMock(return_value=(bot, None))),
                        patch.object(self.back, "check_user", return_value=(True, {"user_id": "u"}, "")),
                        patch.object(self.back.items, "get_data_by_item_name", return_value=(
                            goods_id, {"type": "特殊道具", "name": "item"})),
                        patch.object(self.back, "_sql_message", return_value=manager),
                        patch.object(self.back, handler_name, handler),
                    ):
                        with self.assertRaises(FinishedException):
                            asyncio.run(self.back.use_item_(bot, event, "item 7"))
                    manager.goods_num.assert_not_called()
                    handler.assert_awaited_once_with(bot, event, goods_id, 7)

    def test_other_special_items_still_read_live_inventory_and_clamp_each_request(self):
        bot, event = SimpleNamespace(), SimpleNamespace()
        manager = SimpleNamespace(goods_num=Mock(side_effect=[2, 5, 0]))
        handler, send = AsyncMock(), AsyncMock()
        with (
            patch.object(self.back, "assign_bot", AsyncMock(return_value=(bot, None))),
            patch.object(self.back, "check_user", return_value=(True, {"user_id": "u"}, "")),
            patch.object(self.back.items, "get_data_by_item_name", return_value=(
                20020, {"type": "特殊道具", "name": "item"})),
            patch.object(self.back, "_sql_message", return_value=manager),
            patch.object(self.back, "use_spirit_stone_bag", handler),
            patch.object(self.back, "handle_send", send),
        ):
            for _ in range(2):
                with self.assertRaises(FinishedException):
                    asyncio.run(self.back.use_item_(bot, event, "item 7"))
            asyncio.run(self.back.use_item_(bot, event, "item 7"))
        self.assertEqual(manager.goods_num.call_args_list, [call("u", 20020)] * 3)
        self.assertEqual([item.args[3] for item in handler.await_args_list], [2, 5])
        self.assertIn("背包中没有足够", send.await_args.args[2])

    def test_local_special_dispatch_reuses_only_this_checked_profile(self):
        bot, event = SimpleNamespace(), SimpleNamespace()
        entries = [(20010, "use_lottery_talisman"), (20019, "use_unbind_charm"),
                   (20020, "use_spirit_stone_bag"), (20021, "use_tianji_stone_trigger"),
                   (20022, "use_three_cultivation_pill")]
        entries.extend((item_id, "use_pet_egg_item") for item_id in self.back.PET_EGG_IDS)
        for item_id, handler_name in entries:
            with self.subTest(item_id=item_id):
                profiles = [{"user_id": "u", "stone": 10**80 + delta} for delta in (1, 2)]
                handler = AsyncMock()
                with (
                    patch.object(self.back, "assign_bot", AsyncMock(return_value=(bot, None))),
                    patch.object(self.back, "check_user", side_effect=[
                        (True, profile, "") for profile in profiles]) as check,
                    patch.object(self.back.items, "get_data_by_item_name", return_value=(
                        item_id, {"type": "特殊道具", "name": "item"})),
                    patch.object(self.back, "_sql_message", return_value=SimpleNamespace(
                        goods_num=Mock(return_value=5))),
                    patch.object(self.back, handler_name, handler),
                ):
                    for _ in profiles:
                        with self.assertRaises(FinishedException):
                            asyncio.run(self.back.use_item_(bot, event, "item 2 target" if item_id == 20019 else "item 2"))
                self.assertEqual(2, check.call_count)
                self.assertEqual(2, handler.await_count)
                for call_args, profile in zip(handler.await_args_list, profiles):
                    self.assertEqual((bot, event, item_id, 2), call_args.args)
                    self.assertIs(profile, call_args.kwargs["user_info"])
                    if item_id == 20019:
                        self.assertEqual("target", call_args.kwargs["target_item_name"])

    def test_local_special_helpers_still_validate_independent_calls(self):
        for handler_name in ("use_pet_egg_item", "use_lottery_talisman", "use_unbind_charm",
                             "use_spirit_stone_bag", "use_tianji_stone_trigger", "use_three_cultivation_pill"):
            with (
                self.subTest(handler=handler_name),
                patch.object(self.back, "assign_bot", AsyncMock(return_value=("bot", None))),
                patch.object(self.back, "check_user", return_value=(False, None, "refused")) as check,
                patch.object(self.back, "handle_send", AsyncMock()) as send,
                patch.object(self.back, "back_application") as application,
            ):
                asyncio.run(getattr(self.back, handler_name)("bot", "event", 20022, 1))
                check.assert_called_once_with("event")
                send.assert_awaited_once_with("bot", "event", "refused", md_type="我要修仙")
                self.assertEqual([], application.mock_calls)

    def test_three_pill_uses_checked_fate_level_and_preserves_exact_arguments(self):
        profile = {"user_id": "u", "level": "realm", "root_type": "命运道果",
                   "root_level": 40, "exp": 10**80 + 1}
        manager = SimpleNamespace(get_root_rate=Mock(return_value=49.5),
                                  get_user_info_with_id=Mock(side_effect=AssertionError("duplicate profile")))
        result = SimpleNamespace(succeeded=False)
        with (
            patch.object(self.back, "assign_bot", AsyncMock(side_effect=AssertionError("duplicate assignment"))),
            patch.object(self.back, "check_user", side_effect=AssertionError("duplicate identity")),
            patch.object(self.back, "_sql_message", return_value=manager),
            patch.object(self.back.jsondata, "level_data", return_value={"realm": {"spend": 2}}),
            patch.object(self.back, "UserBuffDate", return_value=SimpleNamespace(
                BuffInfo={}, get_user_main_buff_data=lambda: {})),
            patch.object(self.back, "XiuConfig", return_value=SimpleNamespace(
                closing_exp=100, closing_exp_upper_limit=2)),
            patch.object(self.back, "OtherSet", return_value=SimpleNamespace(
                set_closing_type=lambda level: 10**80 + 1)),
            patch.object(self.back.random, "uniform", return_value=1),
            patch.object(self.back, "_cultivation_item_operation_id", return_value="operation"),
            patch.object(self.back.back_application, "three_cultivation_pill", return_value=result) as apply,
            patch.object(self.back, "_back_op_fail_msg", return_value="live refusal"),
            patch.object(self.back, "handle_send", AsyncMock()) as send,
        ):
            asyncio.run(self.back.use_three_cultivation_pill("bot", "event", 20022, 2, user_info=profile))
        manager.get_root_rate.assert_called_once_with("命运道果", "u", root_level=40)
        apply.assert_called_once_with(operation_id="operation", user_id="u", item_id=20022,
                                      quantity=2, requested_exp=9900 * 2 * 333,
                                      max_exp=(10**80 + 1) * 2, power_multiplier=99.0)
        send.assert_awaited_once_with("bot", "event", "live refusal")


if __name__ == "__main__":
    unittest.main()
