"""Typed inventory reads keep legacy rows, ordering, errors and live values."""
from __future__ import annotations

import asyncio
from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

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


if __name__ == "__main__":
    unittest.main()
