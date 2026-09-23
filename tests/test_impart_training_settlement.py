import tempfile
import unittest
import importlib
from pathlib import Path
import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk.transaction_service import ImpartTrainingSettlementService
from tests.test_db_backend import db_backend


def test_impart_facade_defers_training_settlement_service_construction():
    impart = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk"
    )
    assert impart._impart_training_settlement_service_instance is None


def test_impart_facade_defers_sql_manager_construction():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_impart_pk/__init__.py"
    ).read_text(encoding="utf-8")
    assert "_sql_message_instance = None" in source
    assert "def _sql_message(" in source
    assert "_sql_message().get_user_cd(" in source
    assert "_sql_message().get_user_info_with_id(" in source
    assert "_sql_message().update_last_check_info_time(" in source
    assert "sql_message = XiuxianDateManage()" not in source


def test_impart_training_handler_uses_lazy_three_database_service():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_impart_pk/__init__.py"
    ).read_text(encoding="utf-8")
    handler = source[source.index("async def impart_pk_exp_"):source.index("async def impart_pk_go_")]
    assert "impart_pk_application.training_settle(" in handler
    assert "_run_impart_pk_action(" not in handler
    assert "_impart_training_settlement_service().settle(" not in handler
    assert "_impart_training_settlement_service_instance = None" in source
    assert "def _impart_training_settlement_service(" in source
    assert "get_paths().game_db, get_paths().impart_db, get_paths().player_db" in source
    assert "impart_training_settlement_service.settle(" not in handler


class ImpartTrainingSettlementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); root = Path(self.tmp.name)
        self.game, self.impart, self.player = root / "game.db", root / "impart.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,power INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',1000,10)")
        with db_backend.transaction(self.impart) as conn:
            conn.execute("CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER,impart_lv INTEGER)")
            conn.execute("INSERT INTO xiuxian_impart VALUES('u',500,3)")
        self.service = ImpartTrainingSettlementService(self.game, self.impart, self.player)
        self.daily = {"pk_num": 7, "impart_num": 10, "exp_used": 20, "exp_count": 1, "exp_load": 10, "exp_gain": 30}

    def tearDown(self): self.tmp.cleanup()

    def call(self, operation="train", expected_exp=1000):
        return self.service.settle(operation, "u", expected_exp=expected_exp, expected_exp_day=500,
            expected_daily={k: self.daily[k] for k in ("exp_used", "exp_count", "exp_load", "exp_gain")},
            exp_cost=100, exp_gain=250, exp_load_gain=5, power=2500, legacy_state=self.daily)

    def test_atomic_settlement_and_idempotency(self):
        self.assertEqual("applied", self.call().status)
        # mutable expected_exp snapshot must not break same-op replay
        self.assertEqual("duplicate", self.call(expected_exp=999).status)
        self.assertIsNotNone(self.service.get_result("train"))
        with db_backend.connection(self.game) as conn: self.assertEqual((1250, 2500), tuple(conn.execute("SELECT exp,power FROM user_xiuxian").fetchone()))
        with db_backend.connection(self.impart) as conn: self.assertEqual(400, conn.execute("SELECT exp_day FROM xiuxian_impart").fetchone()[0])
        with db_backend.connection(self.player) as conn:
            self.assertEqual((120, 2, 15, 280), tuple(conn.execute("SELECT exp_used,exp_count,exp_load,exp_gain FROM impart_pk_daily").fetchone()))
            self.assertEqual((100, 1, 250, 5), tuple(conn.execute('SELECT "虚神界修炼","虚神界修炼次数","虚神界修炼修为","虚神界修炼承载" FROM statistics').fetchone()))

    def test_snapshot_and_rollback(self):
        self.assertEqual("state_changed", self.call(expected_exp=999).status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE impart_training_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT)")
            conn.execute("CREATE TRIGGER fail_train BEFORE INSERT ON impart_training_operations BEGIN SELECT RAISE(ABORT,'x'); END")
        with self.assertRaises(Exception): self.call("rollback")
        with db_backend.connection(self.impart) as conn: self.assertEqual(500, conn.execute("SELECT exp_day FROM xiuxian_impart").fetchone()[0])
        with db_backend.connection(self.game) as conn: self.assertEqual(1000, conn.execute("SELECT exp FROM user_xiuxian").fetchone()[0])
