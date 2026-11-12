import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

import nonebot
nonebot.init()

from nonebot.exception import FinishedException
from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.impart.application import ImpartApplication
from nonebot_plugin_xiuxian_2.features.impart.card_bonus import BONUS_FIELDS
from nonebot_plugin_xiuxian_2.features.impart.migrations import (
    apply_impart_draw_operations, apply_impart_crystal_and_card_operations,
    apply_impart_draw_player_statistics,
    apply_impart_exact_crystal_balances,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.application import ImpartPkApplication
from nonebot_plugin_xiuxian_2.features.impart_pk.migrations import apply_impart_pk_operations, apply_impart_pk_closing_statistics
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import failure_reply
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner


class ColdStartTests(unittest.TestCase):
    def test_real_plugin_load_does_not_discover_guard_as_plugin(self):
        code = """
import tests
import nonebot
nonebot.init()
plugin = nonebot.load_plugin('nonebot_plugin_xiuxian_2')
assert plugin is not None
from nonebot_plugin_xiuxian_2.adapters.nonebot.commands import report_exception
assert nonebot.get_plugin('command_guard') is None
"""
        completed = subprocess.run([sys.executable, "-c", code], env=os.environ.copy(), capture_output=True, text=True, timeout=80)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertNotIn("Failed to import", completed.stdout + completed.stderr)
        self.assertNotIn("is not loaded as a plugin", completed.stdout + completed.stderr)


class HistoricalImpartTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.game, self.impart, self.player = [Path(self.temp.name) / n for n in ("game", "impart", "player")]
        with sqlite3.connect(self.game) as c:
            c.executescript("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER); INSERT INTO user_xiuxian VALUES('u',100,100,1,2,3,4); CREATE TABLE user_cd(user_id TEXT,type INTEGER,create_time TEXT,scheduled_time TEXT); INSERT INTO user_cd VALUES('u',4,'start',NULL);")
        with sqlite3.connect(self.impart) as c:
            columns = ','.join(f'"{f}" REAL DEFAULT 0' for f in BONUS_FIELDS)
            c.execute(f"CREATE TABLE xiuxian_impart(user_id TEXT,wish INTEGER,impart_num INTEGER,stone_num INTEGER,exp_day INTEGER,{columns})")
            c.executemany("INSERT INTO xiuxian_impart(user_id,wish,impart_num,stone_num,exp_day) VALUES('u',0,0,10,30)", [(), ()])
            c.executescript("CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,PRIMARY KEY(user_id,card_name)); INSERT INTO impart_cards VALUES('u','a',4);")
        for path, migrations in ((self.game, (apply_impart_draw_operations, apply_impart_pk_operations)), (self.impart, (apply_impart_crystal_and_card_operations, apply_impart_exact_crystal_balances)), (self.player, (apply_impart_draw_player_statistics, apply_impart_pk_closing_statistics))):
            with DatabaseUnitOfWork(path) as uow:
                if path == self.player:
                    uow.execute("CREATE TABLE statistics(user_id TEXT PRIMARY KEY)")
                for migration in migrations:
                    migration(uow)
        self.app = ImpartApplication(self.game, impart_database=self.impart, player_database=self.player)
        self.pk = ImpartPkApplication(self.game, self.impart, self.player)
        self.defs = {"a": dict(type="impart_atk_per", vale=0.1)}

    def act(self, name):
        if name == "draw":
            return self.app.draw(operation_id=name,user_id="u",expected_stone=100,expected_wish=0,expected_count=0,cost=20,new_wish=1,pulls=1,cards=["a"],card_definitions=self.defs)
        if name == "crystal":
            return self.app.crystal_draw(operation_id=name,user_id="u",expected_stone=10,expected_wish=0,cost=2,new_wish=1,exp_minutes=5,cards=["a"],card_definitions=self.defs)
        if name == "split":
            return self.app.disassemble(operation_id=name,user_id="u",card_name="a",quantity=1,expected_card_quantity=4,expected_stone_quantity=10,reward_per_card=2,card_definitions=self.defs)
        return self.pk.closing_settle(operation_id=name,user_id="u",expected_create_time="start",expected_exp=100,expected_exp_day=30,exp_gain=20,blessing_cost=10,closing_minutes=60,hp=40,mp=50,atk=6,power=99)

    def snapshot(self):
        result = []
        for path in (self.game, self.impart, self.player):
            with sqlite3.connect(path) as c:
                result.append(tuple(c.iterdump()))
        return result

    def test_duplicate_rows_settle_once_and_replay_without_double_rewards(self):
        for name, expected in (("draw", (1,1,10,30)), ("crystal", (1,0,8,35)), ("split", (0,0,12,30)), ("close", (0,0,10,20))):
            with self.subTest(action=name):
                if name != "draw":
                    self.tearDownFixture()
                    self.setUp()
                self.assertEqual(self.act(name).status, "applied")
                before = self.snapshot()
                self.assertEqual(self.act(name).status, "duplicate")
                self.assertEqual(self.snapshot(), before)
                with sqlite3.connect(self.impart) as c:
                    self.assertEqual(c.execute("SELECT wish,impart_num,stone_num,exp_day FROM xiuxian_impart").fetchall(), [expected, expected])
                    self.assertEqual(c.execute("SELECT stone_num_exact,stone_num_precision_state FROM xiuxian_impart").fetchall(), [(str(expected[2]), "exact")] * 2)
                    self.assertEqual(c.execute("SELECT quantity FROM impart_cards").fetchone()[0], {"draw":5,"crystal":5,"split":3,"close":4}[name])
                with sqlite3.connect(self.game) as c:
                    self.assertEqual(c.execute("SELECT stone,exp FROM user_xiuxian").fetchone(), (80,100) if name=="draw" else (100,120) if name=="close" else (100,100))

    def tearDownFixture(self):
        self.temp.cleanup()

    def test_inconsistent_history_names_field_and_rolls_back_every_database(self):
        for name, field in (("draw","wish"),("crystal","stone_num"),("split","stone_num"),("close","exp_day")):
            with self.subTest(action=name):
                self.tearDownFixture()
                self.setUp()
                with sqlite3.connect(self.impart) as c:
                    if field == "stone_num":
                        c.execute("UPDATE xiuxian_impart SET stone_num=stone_num+1,stone_num_exact='11' WHERE rowid=2")
                    else:
                        c.execute(f"UPDATE xiuxian_impart SET {field}={field}+1 WHERE rowid=2")
                before = self.snapshot()
                with self.assertRaises(DomainError) as caught:
                    self.act(name)
                reply = failure_reply(name, caught.exception)
                self.assertIn("历史", reply)
                self.assertNotIn("定位号", reply)
                self.assertEqual(before, self.snapshot())

    def test_partial_historical_write_rolls_back_assets_and_cards(self):
        for name in ("draw", "crystal", "split", "close"):
            with self.subTest(action=name):
                self.tearDownFixture()
                self.setUp()
                with sqlite3.connect(self.impart) as c:
                    c.execute("CREATE TRIGGER skip_second BEFORE UPDATE ON xiuxian_impart WHEN OLD.rowid=2 BEGIN SELECT RAISE(IGNORE); END")
                before = self.snapshot()
                with self.assertRaises(DomainError) as caught:
                    self.act(name)
                self.assertEqual(caught.exception.code, "impart_write_rejected")
                self.assertEqual(before, self.snapshot())


class MentorHandlerFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_graduation_failure_reaches_real_handler_reply(self):
        sender = AsyncMock()
        user = dict(user_id="b",user_name="徒弟",level="无界境",stone=10)
        failed = Mock(succeeded=False,status="state_changed",reason="stone_changed",subject="a")
        with patch.object(partner,"assign_bot",AsyncMock(return_value=(None,None))), patch.object(partner,"check_user",return_value=(True,user,"")), patch.object(partner,"_resolve_user_id_from_args",return_value=None), patch.object(partner,"load_mentor",return_value=dict(mentor_id="a")), patch.object(partner,"get_user_profile",return_value=dict(user_name="师父",stone=10)), patch.object(partner,"is_wujie_or_above",return_value=True), patch.object(partner,"_player_data_manager",return_value=Mock(get_fields=Mock(return_value={}))), patch.object(partner,"_mentor_graduation_service",return_value=Mock(apply=Mock(return_value=failed))), patch.object(partner,"handle_send",sender):
            with self.assertRaises(FinishedException):
                await partner.unbind_mentor_(None,Mock(user_id="b"),Mock())
        self.assertIn("灵石余额",sender.call_args.args[2])

    async def test_transmission_failure_reaches_real_handler_reply(self):
        sender = AsyncMock()
        mentor = dict(user_id="a",user_name="师父",level="无界境",exp=100)
        apprentice = dict(user_id="b",user_name="徒弟",level="练气境",exp=100)
        failed = Mock(succeeded=False,status="state_changed",reason="exp_changed",subject="b")
        with patch.object(partner,"assign_bot",AsyncMock(return_value=(None,None))), patch.object(partner,"check_user",return_value=(True,mentor,"")), patch.object(partner,"get_valid_apprentices",return_value=["b"]), patch.object(partner,"_resolve_user_id_from_args",return_value="b"), patch.object(partner,"check_is_mentor_pair",return_value=True), patch.object(partner,"_sql_message",return_value=Mock(get_user_info_with_id=Mock(return_value=apprentice))), patch.object(partner,"get_realm_gap",return_value=2), patch.object(partner,"_get_bind_wait_remaining",return_value=0), patch.object(partner,"_get_remaining_mentor_transmission",return_value=3), patch.object(partner,"OtherSet",return_value=Mock(set_closing_type=Mock(return_value=10000))), patch.object(partner,"_recovered_attributes",return_value=(1,2,3)), patch.object(partner,"_relation_power",return_value=4), patch.object(partner,"_mentor_transmission_service",return_value=Mock(apply=Mock(return_value=failed))), patch.object(partner,"handle_send",sender):
            with self.assertRaises(FinishedException):
                await partner.mentor_transmission_(None,Mock(user_id="a"),Mock())
        self.assertIn("修为",sender.call_args.args[2])
        self.assertNotIn("邀请",sender.call_args.args[2])
