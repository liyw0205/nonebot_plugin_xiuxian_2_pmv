"""Catalog projections share a read stage; real settlements still recheck SQL state."""
from __future__ import annotations

import asyncio
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_secbuff
from nonebot_plugin_xiuxian_2.features.sect.tests.fixtures import migrate_sect
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from tests.test_sect_read_stage import Finished, load


ROOT = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect"


class SectCatalogReadStageTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.database = Path(temp.name) / "sect.db"
        migrate_sect(self.database)
        with DatabaseUnitOfWork(self.database) as uow:
            apply_sect_secbuff(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,"
                        "sect_position INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',1,0)")
            uow.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,"
                        "sect_scale INTEGER,sect_used_stone INTEGER,sect_materials INTEGER,"
                        "mainbuff TEXT,secbuff TEXT)")
            uow.execute("INSERT INTO sects VALUES(1,'u',10000000,10000,20000,'[101]','[201]')")
            uow.execute("CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY,sec_buff INTEGER)")
            uow.execute("INSERT INTO BuffInfo VALUES('u',0)")
        self.app, self.number = SectApplication(self.database), 0
        self.helpers = load(ROOT / "sect_member_utils.py", {
            "sect_catalog_ids", "get_sect_mainbuff_id_list", "get_sect_secbuff_id_list",
            "set_sect_list"}, {"sect_application": self.app})
        self.items = {str(key): {"name": f"Item{key}", "level": "tier"}
                      for key in (101, 102, 201, 202)}

    def run_handler(self, name):
        async def finish():
            raise Finished

        sender, pages = AsyncMock(), AsyncMock()
        namespace = {
            "CommandArg": lambda: None,
            "assign_bot": AsyncMock(return_value=(SimpleNamespace(self_id="bot"), None)),
            "check_user": lambda event: (True, self.app.get_user_profile("u"), ""),
            "handle_send": sender, "send_msg_handler": pages, "sect_application": self.app,
            "sect_catalog_ids": self.helpers["sect_catalog_ids"],
            "set_sect_list": self.helpers["set_sect_list"],
            "items": SimpleNamespace(get_data_by_item_id=lambda key: self.items[str(key)]),
            "get_main_info_msg": lambda key: (self.items[str(key)], "description"),
            "get_sec_msg": lambda row: "description", "buffrankkey": {"tier": 1},
            "get_secname_list": lambda keys: [self.items[key]["name"] for key in keys if key],
            "get_secnameid": lambda name, keys: next(key for key in keys if self.items[key]["name"] == name),
            "UserBuffDate": lambda uid: SimpleNamespace(BuffInfo=self.buff()),
            "get_sectbufftxt": lambda scale, config: (1, ["tier"]),
            "BuffJsonDate": lambda: SimpleNamespace(get_gfpeizhi=lambda: {
                "tier": {"gf_list": ["102"], "st_list": ["202"]}}),
            "runtime_random": SimpleNamespace(randint=lambda low, high: 0, choice=lambda values: values[0]),
            "config": {key: {"获取消耗的灵石": 100, "获取消耗的资材": 200,
                             "获取到功法的概率": 100, "获取到神通的概率": 100,
                             "学习资材消耗": 150}
                       for key in ("宗门主功法参数", "宗门神通参数")},
            "jsondata": SimpleNamespace(sect_config_data=lambda: {"0": {"title": "宗主"}}),
            "_sect_operation_id": lambda event, *args: f"catalog:{event}",
            "number_to": str, "result_failure": lambda action, result: result.status,
        }
        for matcher in ("sect_buff_info", "sect_buff_info2", "sect_mainbuff_get",
                        "sect_secbuff_get", "sect_secbuff_learn"):
            namespace[matcher] = SimpleNamespace(finish=finish)
        load(ROOT / "__init__.py", {name}, namespace)
        self.number += 1
        args = (None, self.number)
        if name == "sect_secbuff_learn_":
            args += (SimpleNamespace(extract_plain_text=lambda: "Item201"),)
        with patch.object(self.app, "get_sect_info", wraps=self.app.get_sect_info) as reads:
            with self.assertRaises(Finished):
                asyncio.run(namespace[name](*args))
            self.assertEqual(1, reads.call_count, name)
        return sender.call_args.args[2] if sender.called else pages.call_args.args[4]

    def buff(self):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_one("SELECT sec_buff FROM BuffInfo WHERE user_id='u'")

    def reset(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE sects SET sect_used_stone=10000,sect_materials=20000,"
                        "mainbuff='[101]',secbuff='[201]'")
            uow.execute("UPDATE BuffInfo SET sec_buff=0")

    def test_parser_preserves_legacy_string_rules_and_wrappers_read_live(self):
        parser = self.helpers["sect_catalog_ids"]
        for value in ("[]", "[1, 2]", "[1,,2]", "[001]", "broken", "", 0, None, [1, 2]):
            with self.subTest(value=value):
                self.assertEqual(str(value)[1:-1].split(','), parser(value))
        self.assertEqual(["101"], self.helpers["get_sect_mainbuff_id_list"](1))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE sects SET mainbuff='[102]',secbuff='[202]'")
        self.assertEqual(["102"], self.helpers["get_sect_mainbuff_id_list"](1))
        self.assertEqual(["202"], self.helpers["get_sect_secbuff_id_list"](1))

    def test_both_views_read_one_row_are_zero_write_and_next_request_is_live(self):
        before = self.app.get_sect_info(1)
        self.assertEqual(["tierItem101"], self.run_handler("sect_buff_info_"))
        self.assertEqual(["tier:Item201"], self.run_handler("sect_buff_info2_"))
        self.assertEqual(before, self.app.get_sect_info(1))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE sects SET mainbuff='[102]',secbuff='[202]'")
        self.assertEqual(["tierItem102"], self.run_handler("sect_buff_info_"))
        self.assertEqual(["tier:Item202"], self.run_handler("sect_buff_info2_"))

    def test_all_three_action_callers_use_single_read_and_real_atomic_settlement(self):
        for name, column, catalog in (("sect_mainbuff_get_", "mainbuff", "[101,102]"),
                                      ("sect_secbuff_get_", "secbuff", "[201,202]")):
            with self.subTest(name=name):
                self.reset()
                self.assertIn("共消耗100宗门灵石，200宗门资材", self.run_handler(name))
                row = self.app.get_sect_info(1)
                self.assertEqual((9900, 19800, catalog),
                                 (row["sect_used_stone"], row["sect_materials"], row[column]))
        self.reset()
        self.assertIn("成功学习", self.run_handler("sect_secbuff_learn_"))
        self.assertEqual(19850, self.app.get_sect_info(1)["sect_materials"])
        self.assertEqual(201, self.buff()["sec_buff"])
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(2, uow.query_one("SELECT COUNT(*) AS n FROM sect_buff_search_operations")["n"])
            self.assertEqual(1, uow.query_one("SELECT COUNT(*) AS n FROM sect_secbuff_learn_operations")["n"])

    def test_search_both_catalogs_and_assets_are_rechecked_after_read_stage(self):
        original = self.app.apply_buff_search
        for name, column in (("sect_mainbuff_get_", "mainbuff"), ("sect_secbuff_get_", "secbuff")):
            for change, status in (("catalog", "catalog_changed"), ("stone", "stone_insufficient"),
                                   ("materials", "materials_insufficient")):
                with self.subTest(name=name, change=change):
                    self.reset()
                    def mutate(*args, **kwargs):
                        with DatabaseUnitOfWork(self.database) as uow:
                            sql = {"catalog": f"UPDATE sects SET {column}='[999]'",
                                   "stone": "UPDATE sects SET sect_used_stone=0",
                                   "materials": "UPDATE sects SET sect_materials=0"}[change]
                            uow.execute(sql)
                        return original(*args, **kwargs)
                    with patch.object(self.app, "apply_buff_search", side_effect=mutate):
                        self.assertEqual(status, self.run_handler(name))
                    row = self.app.get_sect_info(1)
                    self.assertEqual(0 if change == "stone" else 10000, row["sect_used_stone"])
                    self.assertEqual(0 if change == "materials" else 20000, row["sect_materials"])
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM sect_buff_search_operations")["n"])

    def test_secondary_learning_rechecks_catalog_and_materials_before_charge(self):
        original = self.app.learn_secondary
        for change in ("catalog", "materials"):
            with self.subTest(change=change):
                self.reset()
                def mutate(**kwargs):
                    with DatabaseUnitOfWork(self.database) as uow:
                        uow.execute("UPDATE sects SET secbuff='[999]'" if change == "catalog"
                                    else "UPDATE sects SET sect_materials=0")
                    return original(**kwargs)
                with patch.object(self.app, "learn_secondary", side_effect=mutate):
                    reply = self.run_handler("sect_secbuff_learn_")
                self.assertNotIn("成功学习", reply)
                self.assertIn("catalog_changed" if change == "catalog" else "不满足条件", reply)
                self.assertEqual(0, self.buff()["sec_buff"])
                self.assertEqual(20000 if change == "catalog" else 0,
                                 self.app.get_sect_info(1)["sect_materials"])
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM sect_secbuff_learn_operations")["n"])
