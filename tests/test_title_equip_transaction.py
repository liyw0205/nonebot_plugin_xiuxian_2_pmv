import json
import importlib
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title.title_transaction_service import TitleTransactionService
from nonebot_plugin_xiuxian_2.features.title.migrations import apply_title_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend


def test_title_facade_defers_transaction_service_construction():
    title = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title"
    )
    assert title._title_transaction_service_instance is None


def test_title_facade_defers_sql_manager_construction():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_title/__init__.py"
    ).read_text(encoding="utf-8")
    assert "_sql_message_instance = None" in source
    assert "def _sql_message(" in source
    assert "_sql_message().get_all_user_id(" in source
    assert "_sql_message().get_user_info_with_id(" in source
    assert "_sql_message().get_user_info_with_name(" in source
    assert "sql_message = XiuxianDateManage()" not in source


def test_title_equip_replay_uses_feature_application():
    source = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_title/__init__.py"
    handler = source.read_text(encoding="utf-8").split("@title_equip_cmd.handle", 1)[1].split("@title_unequip_cmd.handle", 1)[0]
    assert "title_application.get_result(" in handler
    assert "_title_transaction_service().get_result(" not in handler


def test_title_entries_use_lazy_replay_and_application_write_paths():
    title_source = open(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_title/__init__.py",
        encoding="utf-8",
    ).read()
    data_source = open(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_title/title_data.py",
        encoding="utf-8",
    ).read()
    assert "_title_transaction_service_instance = None" in title_source
    assert "def _title_transaction_service(" in title_source
    assert "title_application.get_result(" in title_source
    assert "_title_transaction_service().get_result(" not in title_source
    assert "title_transaction_service.get_result(" not in title_source
    assert "from . import title_application" in data_source
    assert "title_application.execute(" in data_source


class TitleEquipTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "player.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE title(user_id TEXT PRIMARY KEY,unlocked TEXT,equipped TEXT)")
            conn.execute("INSERT INTO title VALUES(%s,%s,%s)", ("u", json.dumps(["1", "2"]), "1"))
        with DatabaseUnitOfWork(self.db) as uow:
            apply_platform_schema(uow)
            apply_title_schema(uow)
        self.service = TitleTransactionService(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def test_equip_and_replay(self):
        self.assertEqual(self.service.equip("op", "u", ["1", "2"], "1", "2").status, "applied")
        self.assertEqual(self.service.equip("op", "u", ["1", "2"], "1", "2").status, "duplicate")
        with db_backend.connection(self.db) as conn:
            self.assertEqual(conn.execute("SELECT equipped FROM title").fetchone()[0], "2")

    def test_locked_stale_and_conflict(self):
        self.assertEqual(self.service.equip("locked", "u", ["1", "2"], "1", "3").status, "title_locked")
        self.assertEqual(self.service.equip("stale", "u", ["1"], "1", "1").status, "state_changed")
        self.service.equip("same", "u", ["1", "2"], "1", "2")
        # Request identity is equip+user+title_id; mutable expected_* no longer cause conflict.
        self.assertEqual(self.service.equip("same", "u", ["1", "2"], "2", "2").status, "duplicate")
        self.assertEqual(self.service.equip("same", "u", ["1", "2"], "2", "1").status, "operation_conflict")

    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TRIGGER fail_title_equip BEFORE INSERT ON title_transaction_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):
            self.service.equip("fail", "u", ["1", "2"], "1", "2")
        with db_backend.connection(self.db) as conn:
            self.assertEqual(conn.execute("SELECT equipped FROM title").fetchone()[0], "1")
