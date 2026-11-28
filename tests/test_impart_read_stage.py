"""Reuse repaired rows within a stage, keeping repair and settlement boundaries live."""
from __future__ import annotations

import __future__
import ast
import asyncio
from copy import deepcopy
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock

import nonebot

try:
    nonebot.get_driver()
except ValueError:
    nonebot.init()

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.impart.repository import ImpartRepository
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart_pk as pk
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart import impart_uitls
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk import impart_pk_uitls as pk_utils
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import xiuxian2_handle as legacy


def load(module, name, **overrides):
    namespace = {**vars(module), **overrides}
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    node = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
                and node.name == name)
    node.decorator_list = []
    exec(compile(ast.Module(body=[node], type_ignores=[]), module.__file__, "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace[name]


class Finished(Exception):
    pass


async def finish():
    raise Finished


class ImpartReadStageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.game, self.impart, self.player = (root / name for name in
                                              ("game.db", "impart.db", "player.db"))
        self.manager = object.__new__(legacy.XIUXIAN_IMPART_BUFF)
        self.manager.conn = legacy.db_backend.connect(self.impart)
        self.manager._conn_lock = threading.RLock()
        self.addCleanup(self.manager.conn.close)
        fields = ",".join(f'"{field}" DEFAULT 0' for field in self.manager._default_fields)
        self.manager.conn.execute(f"CREATE TABLE xiuxian_impart(user_id TEXT,{fields},"
                                  "stone_num_exact TEXT,stone_num_precision_state TEXT DEFAULT 'exact')")
        self.manager.conn.commit()
        self.manager._create_user("u")
        self.reads = []
        self.manager.conn._raw.set_trace_callback(lambda sql: self.reads.append(sql)
            if sql.lower().startswith("select") else None)
        self.card_init = Mock()
        self.simple_check = load(impart_uitls, "impart_check", _impart_manager=lambda: self.manager,
                                 _impart_data_manager=lambda: self.card_init)
        self.repository = ImpartRepository(self.impart)
        self.ensure = Mock(wraps=self.repository.state)
        self.check = load(pk_utils, "impart_pk_check", xiuxian_impart=self.manager,
                          get_paths=lambda: SimpleNamespace(impart_db=self.impart),
                          ImpartRepository=lambda _: SimpleNamespace(state=self.ensure))

    def write(self, sql, params=()):
        self.manager.conn.execute(sql, params)
        self.manager.conn.commit()

    def test_normal_helpers_read_one_legacy_row_and_next_call_is_live(self):
        for check in (self.simple_check, self.check):
            with self.subTest(helper=check.__name__):
                self.write("UPDATE xiuxian_impart SET exp_day=11")
                self.reads.clear()
                self.assertEqual(11, asyncio.run(check("u"))["exp_day"])
                self.assertEqual(1, len(self.reads))
                self.write("UPDATE xiuxian_impart SET exp_day=12")
                self.assertEqual(12, asyncio.run(check("u"))["exp_day"])
                self.assertEqual(2, len(self.reads))
        self.ensure.assert_called_with("u", ensure=True)
        self.card_init.find_user_impart.assert_called_with("u")

    def test_repair_write_is_followed_by_actual_readback_in_both_helpers(self):
        self.write("CREATE TRIGGER repaired_day AFTER UPDATE OF exp_day ON xiuxian_impart "
                   "WHEN NEW.exp_day=0 BEGIN UPDATE xiuxian_impart SET exp_day=777 "
                   "WHERE rowid=NEW.rowid; END")
        for check in (self.simple_check, self.check):
            with self.subTest(helper=check.__name__):
                self.write("UPDATE xiuxian_impart SET exp_day=NULL")
                self.reads.clear()
                self.assertEqual(777, asyncio.run(check("u"))["exp_day"])
                self.assertEqual(2, len(self.reads))
                self.assertEqual(777, self.manager.conn.execute(
                    "SELECT exp_day FROM xiuxian_impart").fetchone()[0])

    def test_missing_creation_and_explicit_none_fallback_remain_available(self):
        for check in (self.simple_check, self.check):
            self.write("DELETE FROM xiuxian_impart")
            self.assertEqual(0, asyncio.run(check("new"))["exp_day"])
        fallback = Mock()
        fallback.get_user_impart_info_with_id.side_effect = [None, {"exp_day": 9}]
        check = load(impart_uitls, "impart_check", _impart_manager=lambda: fallback,
                     _impart_data_manager=lambda: self.card_init)
        self.assertEqual({"exp_day": 9}, asyncio.run(check("u")))
        fallback._create_user.assert_called_once_with("u")
        self.assertEqual(2, fallback.get_user_impart_info_with_id.call_count)

    def test_default_getter_retains_legacy_repair_return_contract(self):
        self.write("CREATE TRIGGER repaired_day AFTER UPDATE OF exp_day ON xiuxian_impart "
                   "WHEN NEW.exp_day=0 BEGIN UPDATE xiuxian_impart SET exp_day=777 "
                   "WHERE rowid=NEW.rowid; END")
        self.write("UPDATE xiuxian_impart SET exp_day=NULL")
        self.reads.clear()
        self.assertEqual(0, self.manager.get_user_impart_info_with_id("u")["exp_day"])
        self.assertEqual(1, len(self.reads))
        self.assertEqual(777, asyncio.run(self.simple_check("u"))["exp_day"])

    def test_first_duplicate_exact_balance_and_precision_failure_are_preserved(self):
        huge = 10**80 + 1
        self.write("UPDATE xiuxian_impart SET stone_num_exact=?", (str(huge),))
        self.manager._create_user("other")
        self.write("UPDATE xiuxian_impart SET user_id='u' WHERE user_id='other'")
        for check in (self.simple_check, self.check):
            self.assertEqual(huge, asyncio.run(check("u"))["stone_num"])
        self.write("UPDATE xiuxian_impart SET stone_num_precision_state='precision_lost_real' "
                   "WHERE rowid=(SELECT MIN(rowid) FROM xiuxian_impart)")
        for check in (self.simple_check, self.check):
            with self.assertRaises(DomainError) as caught:
                asyncio.run(check("u"))
            self.assertEqual("impart_crystal_precision_unavailable", caught.exception.code)

    def test_read_and_repair_failures_propagate_and_recover(self):
        self.manager.conn._raw.set_authorizer(lambda action, table, *_:
            sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and table == "xiuxian_impart"
            else sqlite3.SQLITE_OK)
        with self.assertRaises(sqlite3.DatabaseError):
            asyncio.run(self.simple_check("u"))
        self.manager.conn._raw.set_authorizer(None)
        self.write("UPDATE xiuxian_impart SET exp_day=NULL")
        self.write("CREATE TRIGGER reject_repair BEFORE UPDATE ON xiuxian_impart "
                   "BEGIN SELECT RAISE(ABORT,'reject repair'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            asyncio.run(self.simple_check("u"))
        self.manager.conn.rollback()
        self.write("DROP TRIGGER reject_repair")
        self.assertEqual(0, asyncio.run(self.simple_check("u"))["exp_day"])


class ImpartHandlerReadStageTests(unittest.TestCase):
    def stage(self, *, training=True, fallback=False, status="applied"):
        profile = {"user_id": "u", "root_type": "test", "level": "test", "exp": 1000}
        impart = {"exp_day": 100, "impart_lv": 2, "impart_exp_up": .25}
        check = AsyncMock(return_value=impart)
        manager = Mock(get_root_rate=Mock(return_value=2),
                       get_user_info_with_id=Mock(return_value=profile),
                       get_user_cd=Mock(side_effect=([None, {"type": 4, "create_time": "bad"}]
                           if fallback else [{"type": 4, "create_time": "bad"}])))
        buff = Mock(BuffInfo={"blessed_spot": 3}, get_user_main_buff_data=Mock(return_value=None))
        result = SimpleNamespace(status=status, succeeded=status == "applied", exp_load=5,
                                 exp_gain=250, exp_count=1, blessing_cost=0, exp_day_remaining=100)
        application = Mock(training_settle=Mock(return_value=result), closing_settle=Mock(return_value=result))
        namespace = dict(assign_bot=AsyncMock(return_value=(None, None)),
            check_user=lambda event: (True, profile, ""), handle_send=AsyncMock(),
            impart_pk_check=check, _sql_message=lambda: manager,
            xiuxian_impart=Mock(get_user_impart_info_with_id=Mock(side_effect=AssertionError("nested read"))),
            UserBuffDate=Mock(return_value=buff),
            jsondata=SimpleNamespace(level_data=lambda: {"test": {"spend": 3}}),
            XiuConfig=lambda: SimpleNamespace(closing_exp=10, closing_exp_upper_limit=1),
            OtherSet=lambda: SimpleNamespace(set_closing_type=lambda _: 10**9,
                                            send_hp_mp=lambda *args: (("", ""), (1, 2, 30))),
            _daily_impart_state=lambda _: dict(exp_used=0, exp_count=0, exp_load=0, exp_gain=0),
            _resolve_impart_closing_user_id=lambda *args: "u", check_user_type=Mock(return_value=(True, "")),
            impart_pk_application=application, _apply_spirit_vein_exp_bonus=lambda exp, cap: (exp, ""),
            log_message=Mock(), record_task_progress=Mock(), result_failure=lambda *args: "live rejection",
            impart_pk_exp=SimpleNamespace(finish=finish), impart_pk_out_closing=SimpleNamespace(finish=finish))
        handler = load(pk, "impart_pk_exp_" if training else "impart_pk_out_closing_", **namespace)
        return handler, namespace, application, manager, check, impart

    def run_stage(self, training=True, **kwargs):
        handler, namespace, application, manager, check, impart = self.stage(training=training, **kwargs)
        before = deepcopy(impart)
        args = (None, SimpleNamespace(message_id=42))
        if training:
            args += (SimpleNamespace(extract_plain_text=lambda: "1"),)
        with self.assertRaises(Finished):
            asyncio.run(handler(*args))
        self.assertEqual(before, impart)
        check.assert_awaited_once_with("u")
        self.assertEqual(1, namespace["UserBuffDate"].call_count)
        namespace["xiuxian_impart"].get_user_impart_info_with_id.assert_not_called()
        return namespace, application, manager

    def test_training_single_repaired_row_keeps_full_expected_tokens_and_gain(self):
        namespace, application, _ = self.run_stage()
        values = application.training_settle.call_args.kwargs
        expected = int(10 * 2 * 3 * (1 + pk.effective_xu_impart_exp_up(.25)) * (1 + pk.xu_impart_lv_bonus(2)))
        self.assertEqual((1000, 100, 1, expected), (values["expected_exp"], values["expected_exp_day"],
                                                  values["exp_cost"], values["exp_gain"]))
        self.assertEqual({"exp_used": 0, "exp_count": 0, "exp_load": 0, "exp_gain": 0},
                         values["expected_daily"])
        self.assertIn("本次增加修为", namespace["handle_send"].await_args.args[2])

    def test_closing_reuses_normal_cd_and_buff_but_preserves_fallback_reread(self):
        for fallback, count in ((False, 1), (True, 2)):
            with self.subTest(fallback=fallback):
                namespace, application, manager = self.run_stage(training=False, fallback=fallback)
                self.assertEqual(count, manager.get_user_cd.call_count)
                values = application.closing_settle.call_args.kwargs
                self.assertEqual((1000, 100), (values["expected_exp"], values["expected_exp_day"]))
                self.assertEqual("bad", values["expected_create_time"])
                self.assertIn("虚神界闭关结束", namespace["handle_send"].await_args.args[2])

    def test_rejection_and_replay_use_settlement_result_without_fresh_reward(self):
        for training in (False, True):
            for status in ("state_changed", "duplicate"):
                with self.subTest(training=training, status=status):
                    namespace, _, _ = self.run_stage(training=training, status=status)
                    text = namespace["handle_send"].await_args.args[2]
                    self.assertIn("无需重复提交" if status == "duplicate" else "live rejection", text)
                    if not training:
                        namespace["record_task_progress"].assert_not_called()


if __name__ == "__main__":
    unittest.main()
