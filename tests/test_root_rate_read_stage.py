from __future__ import annotations

import ast
import importlib.util
import sqlite3
import sys
import unittest
from pathlib import Path
from types import MethodType, ModuleType, SimpleNamespace
from unittest.mock import patch


UTILS = (
    Path(__file__).resolve().parents[1]
    / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils"
)
FATE = "命运道果"
ETERNAL = "永恒道果"
ORDINARY = "天灵根"


def load_root_rate():
    source_path = UTILS / "xiuxian2_handle.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    sentinel = next(
        node for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "_ROOT_LEVEL_UNSET"
                for target in node.targets)
    )
    manager_class = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "XiuxianDateManage"
    )
    method = next(
        node for node in manager_class.body
        if isinstance(node, ast.FunctionDef) and node.name == "get_root_rate"
    )
    package_name = "_root_rate_read_stage_source"
    namespace = {
        "__package__": package_name,
        "jsondata": SimpleNamespace(root_data=lambda: {
            FATE: {"type_speeds": 2.0},
            ETERNAL: {"type_speeds": 7.0},
            ORDINARY: {"type_speeds": 3.5},
        }),
    }
    exec(compile(ast.Module(body=[sentinel, method], type_ignores=[]),
                 str(source_path), "exec"), namespace)
    spec = importlib.util.spec_from_file_location(
        "_root_rate_read_stage_numeric", UTILS / "numeric_bind.py"
    )
    assert spec and spec.loader
    numeric = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(numeric)
    package = ModuleType(package_name)
    package.__path__ = []
    return namespace["get_root_rate"], {
        package_name: package,
        f"{package_name}.numeric_bind": numeric,
    }


class RootRateReadStageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rate_method, cls.modules = load_root_rate()

    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.connection.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,root_level)"
        )
        self.connection.execute("INSERT INTO user_xiuxian VALUES('u',40)")
        self.selects = []
        self.connection.set_trace_callback(
            lambda statement: self.selects.append(statement)
            if statement.lstrip().upper().startswith("SELECT") else None
        )

        def read_profile(user_id):
            row = self.connection.execute(
                "SELECT * FROM user_xiuxian WHERE user_id=?", (user_id,)
            ).fetchone()
            return dict(row) if row is not None else None

        self.manager = SimpleNamespace(get_user_info_with_id=read_profile)
        self.rate = MethodType(type(self).rate_method, self.manager)
        self.module_patch = patch.dict(sys.modules, self.modules)
        self.module_patch.start()
        self.addCleanup(self.module_patch.stop)
        self.addCleanup(self.connection.close)

    def test_explicit_level_including_none_and_zero_never_reads_profile(self):
        for value, expected in ((None, 7.0), (0, 7.0), ("0", 7.0), (40, 49.5)):
            with self.subTest(root_level=value):
                self.assertEqual(self.rate(FATE, "u", root_level=value), expected)
        self.assertEqual(self.selects, [])

    def test_legacy_two_argument_call_reads_live_requested_user_each_time(self):
        self.assertEqual(self.rate(FATE, "u"), 49.5)
        self.connection.execute("UPDATE user_xiuxian SET root_level=1 WHERE user_id='u'")
        self.assertEqual(self.rate(FATE, "u"), 9.0)
        self.assertEqual(self.rate(FATE, "missing"), 7.0)
        self.assertEqual(len(self.selects), 3)
        self.assertIn("'missing'", self.selects[-1])

    def test_snapshot_is_local_to_the_supplied_read_stage(self):
        first = self.manager.get_user_info_with_id("u")
        self.assertEqual(self.rate(FATE, "u", root_level=first["root_level"]), 49.5)
        self.connection.execute("UPDATE user_xiuxian SET root_level=2 WHERE user_id='u'")
        second = self.manager.get_user_info_with_id("u")
        self.assertEqual(self.rate(FATE, "u", root_level=second["root_level"]), 11.0)
        self.assertEqual(len(self.selects), 2)

    def test_profile_read_failure_propagates_for_legacy_call_only(self):
        self.connection.set_authorizer(
            lambda operation, *_: sqlite3.SQLITE_DENY
            if operation == sqlite3.SQLITE_READ else sqlite3.SQLITE_OK
        )
        self.assertEqual(self.rate(FATE, "u", root_level=1), 9.0)
        with self.assertRaises(sqlite3.DatabaseError):
            self.rate(FATE, "u")

    def test_ordinary_root_ignores_level_without_query(self):
        self.assertEqual(self.rate(ORDINARY, "u"), 3.5)
        self.assertEqual(self.rate(ORDINARY, "u", root_level=object()), 3.5)
        self.assertEqual(self.selects, [])

    def test_invalid_fate_level_keeps_previous_conversion_failure(self):
        self.connection.execute("UPDATE user_xiuxian SET root_level='invalid'")
        with self.assertRaises(ValueError):
            self.rate(FATE, "u")
        self.selects.clear()
        with self.assertRaises(ValueError):
            self.rate(FATE, "u", root_level="invalid")
        self.assertEqual(self.selects, [])


if __name__ == "__main__":
    unittest.main()
