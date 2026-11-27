"""Sect displays and upgrade preflight reuse only one explicit read stage."""
from __future__ import annotations

import __future__
import ast
import asyncio
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param
from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.features.sect.member_repository import SectMemberSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


ROOT = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect"


def load(path, names, namespace):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    if {node.name for node in nodes} != set(names):
        raise AssertionError("missing sect implementation")
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


class Finished(Exception):
    pass


class SectReadFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "sect.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT,sect_id INTEGER,"
                        "stone INTEGER,sect_position INTEGER,sect_contribution INTEGER,"
                        "atkpractice INTEGER,hppractice INTEGER,mppractice INTEGER)")
            uow.execute("CREATE INDEX member_sect ON user_xiuxian(sect_id)")
            uow.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,"
                        "sect_scale INTEGER,sect_materials INTEGER,sect_owner TEXT,"
                        "sect_used_stone INTEGER,elixir_room_level INTEGER,join_open INTEGER,closed INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u','Viewer',1,1000000000,1,"
                        "1000000000,0,0,0)")
            uow.execute("INSERT INTO sects VALUES(1,'Sect',50000000,8000000000,'u',100,0,1,0)")
            uow.execute("CREATE TABLE sect_practice_operations(operation_id TEXT PRIMARY KEY,"
                        "user_id TEXT,sect_id INTEGER,practice_type TEXT,from_level INTEGER,"
                        "to_level INTEGER,stone_cost INTEGER,materials_cost INTEGER)")
        self.app = SectApplication(self.database)
        self.helpers = load(ROOT / "sect_member_utils.py",
                            {"get_sect_level", "sect_level_from_scale"}, {
                                "sect_application": self.app, "config": {"等级建设度": 5000000}})
        self.message = 1

    def run_handler(self, name, count="1"):
        async def finish():
            raise Finished

        sender = AsyncMock()
        rules = {"等级建设度": 5000000,
                 "宗门丹房参数": {"elixir_room_level": {"1": {"name": "Room"}}}}
        namespace = {
            "CommandArg": lambda: None,
            "assign_bot": AsyncMock(return_value=(None, None)),
            "check_user": lambda event: (True, self.app.get_user_profile("u"), ""),
            "handle_send": sender, "sect_application": self.app,
            "sect_level_from_scale": self.helpers["sect_level_from_scale"],
            "get_sect_level": Mock(side_effect=AssertionError("duplicate sect read")),
            "get_sect_contribution_level": lambda value: divmod(value, 5000000),
            "get_sect_member_limit": lambda value: min(20 + value // 50000000, 100),
            "get_sect_fairyland_bonus": lambda level: 0,
            "_get_sect_fairyland_level": lambda row: 0,
            "_get_sect_fairyland_config": lambda level: {"name": "Room", "minutes": 10},
            "config": rules, "number_to": str,
            "LEVLECOST": {str(level): 5000000 for level in range(101)},
            "_parse_practice_count": lambda args, cap: int(args or 1),
            "_sect_operation_id": lambda event, action, sect: f"sect:{event}:{action}:{sect}",
            "result_failure": lambda action, result: result.reason or result.status,
            "jsondata": SimpleNamespace(sect_config_data=lambda: {
                "0": {"title": "宗主"}, "1": {"title": "Member"}}),
        }
        for matcher in ("upatkpractice", "uphppractice", "upmppractice", "my_sect",
                        "sect_buildings", "sect_position_update"):
            namespace[matcher] = SimpleNamespace(finish=finish)
        load(ROOT / "__init__.py", {name}, namespace)
        args = (None, self.message)
        if "practice" in name:
            args += (SimpleNamespace(extract_plain_text=lambda: count),)
        self.message += 1
        with self.assertRaises(Finished):
            asyncio.run(namespace[name](*args))
        return sender.call_args.args[2]


class SectReadStageTests(SectReadFixture):
    def test_all_three_upgrade_callers_read_one_sect_row_then_use_live_transaction(self):
        for name, field in (("upatkpractice_", "atkpractice"), ("uphppractice_", "hppractice"),
                            ("upmppractice_", "mppractice")):
            with self.subTest(name=name), patch.object(
                    self.app, "get_sect_info", wraps=self.app.get_sect_info) as reads:
                text = self.run_handler(name)
                self.assertEqual(1, reads.call_count)
                self.assertIn("升级成功", text)
                self.assertEqual(1, self.app.get_user_profile("u")[field])
        row = self.app.get_user_profile("u")
        self.assertEqual(985000000, row["stone"])
        self.assertEqual(7850000000, self.app.get_sect_info(1)["sect_materials"])

    def test_all_upgrade_callers_keep_the_hundred_level_cap(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE sects SET sect_scale=505000000 WHERE sect_id=1")
            uow.execute("UPDATE user_xiuxian SET atkpractice=99,hppractice=99,mppractice=99")
        for name, field in (("upatkpractice_", "atkpractice"), ("uphppractice_", "hppractice"),
                            ("upmppractice_", "mppractice")):
            with self.subTest(name=name):
                self.assertIn("升级成功", self.run_handler(name, "10"))
                self.assertEqual(100, self.app.get_user_profile("u")[field])
        self.assertEqual(985000000, self.app.get_user_profile("u")["stone"])

    def test_balance_and_material_changes_after_preflight_are_rechecked(self):
        upgrade = self.app.upgrade_practice
        for target, sql in (("player", "UPDATE user_xiuxian SET stone=0 WHERE user_id='u'"),
                            ("sect", "UPDATE sects SET sect_materials=0 WHERE sect_id=1")):
            with self.subTest(target=target):
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("UPDATE user_xiuxian SET stone=1000000000 WHERE user_id='u'")
                    uow.execute("UPDATE sects SET sect_materials=8000000000 WHERE sect_id=1")
                def change_then_upgrade(*args):
                    with DatabaseUnitOfWork(self.database) as uow:
                        uow.execute(sql)
                    return upgrade(*args)
                with patch.object(self.app, "upgrade_practice", side_effect=change_then_upgrade):
                    self.assertNotIn("升级成功", self.run_handler("upatkpractice_"))
                self.assertEqual(0, self.app.get_user_profile("u")["atkpractice"])
                with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                    self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM sect_practice_operations")["n"])

    def test_displays_use_count_and_building_row_once_and_next_request_is_fresh(self):
        with patch.object(self.app, "get_active_task", return_value=None), patch.object(
                self.app, "list_sect_members", side_effect=AssertionError("full member list")), \
                patch.object(self.app, "count_sect_members", wraps=self.app.count_sect_members) as counts:
            for name in ("my_sect_", "sect_buildings_"):
                with self.subTest(name=name), patch.object(
                        self.app, "get_sect_info", wraps=self.app.get_sect_info) as reads:
                    text = self.run_handler(name)
                    self.assertEqual(1, reads.call_count)
                    self.assertIn("1/21", text)
            self.assertEqual(2, counts.call_count)
            with DatabaseUnitOfWork(self.database) as uow:
                uow.execute("INSERT INTO user_xiuxian(user_id,sect_id) VALUES('other',1)")
                uow.execute("UPDATE sects SET sect_scale=100000000,sect_materials=777 WHERE sect_id=1")
            text = self.run_handler("sect_buildings_")
            self.assertIn("2/22", text)
            self.assertIn("修炼上限：20级", text)
            self.assertIn("宗门资材：777", text)

    def test_legacy_level_wrapper_reads_live_and_pure_scale_preserves_large_arithmetic(self):
        pure = self.helpers["sect_level_from_scale"]
        for value in (0, -1, 4999999, 5000000, 10**100 + 7):
            with self.subTest(value=value):
                self.assertEqual(divmod(value, 5000000), pure(value))
        self.assertEqual((10, 0), self.helpers["get_sect_level"](1))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE sects SET sect_scale=? WHERE sect_id=1",
                        (bind_sqlite_param(10**100, exact=True),))
        self.assertEqual(divmod(10**100, 5000000), self.helpers["get_sect_level"](1))


class SectMemberCountTests(SectReadFixture):
    def test_count_preserves_all_matching_rows_and_does_not_normalize_unrelated_values(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany("INSERT INTO user_xiuxian(user_id,sect_id,stone) VALUES(?,?,?)",
                            [("duplicate", 1, "invalid"), ("duplicate", 1, 10), (None, 1, 20),
                             ("other", 2, 10), ("none", None, 10)])
        repo = SectMemberSqlRepository(self.database)
        with patch("nonebot_plugin_xiuxian_2.features.sect.member_repository.normalize_user_row",
                   side_effect=AssertionError("unused full row normalization")):
            for sect, expected in ((1, 4), ("1", 4), (2, 1), (None, 0), (999, 0)):
                with self.subTest(sect=sect):
                    self.assertEqual(expected, repo.count_by_sect_id(sect))

    def test_count_is_readonly_uses_covering_index_and_read_denial_recovers(self):
        original = DatabaseUnitOfWork.__enter__
        def readonly(uow):
            self.assertTrue(uow.read_only)
            return original(uow)
        with patch.object(DatabaseUnitOfWork, "__enter__", readonly):
            self.assertEqual(1, self.app.count_sect_members(1))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            plans = uow.query_all("EXPLAIN QUERY PLAN SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=?", (1,))
        self.assertTrue(any("COVERING INDEX member_sect" in row["detail"] for row in plans))
        def deny(uow):
            result = original(uow)
            uow.connection.set_authorizer(lambda action, table, *_:
                sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and table == "user_xiuxian"
                else sqlite3.SQLITE_OK)
            return result
        with patch.object(DatabaseUnitOfWork, "__enter__", deny), self.assertRaises(sqlite3.DatabaseError):
            self.app.count_sect_members(1)
        self.assertEqual(1, self.app.count_sect_members(1))
        missing = self.database.parent / "missing.db"
        with self.assertRaises(sqlite3.OperationalError):
            SectMemberSqlRepository(missing).count_by_sect_id(1)
        self.assertFalse(missing.exists())
