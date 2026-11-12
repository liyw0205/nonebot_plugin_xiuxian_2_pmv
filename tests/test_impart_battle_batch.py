from __future__ import annotations
import tempfile
import unittest
import importlib
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk.transaction_service import ImpartBattleBatchService
from nonebot_plugin_xiuxian_2.features.impart.migrations import apply_impart_exact_crystal_balances
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


def test_impart_facade_defers_battle_batch_service_construction():
    impart = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk"
    )
    assert impart._impart_battle_batch_service_instance is None


def test_impart_battle_handler_uses_lazy_dual_database_service():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_impart_pk/__init__.py"
    ).read_text(encoding="utf-8")
    handler = source[source.index("async def impart_pk_now_"):source.index("async def impart_pk_exp_")]
    assert "_impart_battle_batch_service().get_pk_num(" in handler
    assert "impart_pk_application.battle_settle(" in handler
    assert "_run_impart_pk_action(" not in handler
    assert "_impart_battle_batch_service().settle(" not in handler
    assert "xu_world.del_xu_world(" not in handler
    assert "_impart_battle_batch_service_instance = None" in source
    assert "def _impart_battle_batch_service(" in source
    assert "get_paths().impart_db, get_paths().player_db" in source
    assert "impart_battle_batch_service.settle(" not in handler

class ImpartBattleBatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); root=Path(self.tmp.name); self.impart,self.player=root/"impart.db",root/"player.db"
        with db_backend.transaction(self.impart) as c:
            c.execute("CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER)"); c.execute("INSERT INTO xiuxian_impart VALUES(%s,%s),(%s,%s)",( "a",0,"b",5))
        with DatabaseUnitOfWork(self.impart) as uow:
            apply_impart_exact_crystal_balances(uow)
        self.service=ImpartBattleBatchService(self.impart,self.player); self.assertEqual(7,self.service.get_pk_num("a",7)); self.assertEqual(6,self.service.get_pk_num("b",6))
    def tearDown(self): self.tmp.cleanup()
    def state(self):
        with db_backend.connection(self.impart) as c: stones=tuple(r[0] for r in c.execute("SELECT stone_num FROM xiuxian_impart ORDER BY user_id").fetchall())
        with db_backend.connection(self.player) as c: pk=tuple(tuple(r) for r in c.execute("SELECT user_id,pk_num,win_num FROM impart_pk_state ORDER BY user_id").fetchall())
        return stones,pk
    def test_two_player_batch_is_atomic_and_idempotent(self):
        args=("op","a",7,2,1,50,"b",6,1,2,40)
        self.assertEqual("applied",self.service.settle(*args).status)
        # mutable win/loss/stone must not break same-op replay
        alt=("op","a",7,9,0,1,"b",6,0,0,0)
        self.assertEqual("duplicate",self.service.settle(*alt).status)
        self.assertIsNotNone(self.service.get_result("op"))
        self.assertEqual(((50,45),(("a",6,2),("b",4,1))),self.state())
        with db_backend.connection(self.player) as c: stats=tuple(c.execute('SELECT "虚神界对决次数","虚神界对决胜利","虚神界对决失败","思恋结晶获取" FROM statistics WHERE user_id=%s',( "a",)).fetchone())
        self.assertEqual((3,2,1,50),stats)
    def test_bot_batch_and_failure_roll_back(self):
        self.assertEqual("applied",self.service.settle("bot","a",7,3,2,80).status); self.assertEqual(((80,5),(("a",5,3),("b",6,0))),self.state())
        before=self.state()
        with db_backend.transaction(self.impart) as c: c.execute("CREATE TRIGGER reject_batch BEFORE INSERT ON impart_battle_batch_operations BEGIN SELECT RAISE(ABORT,'reject'); END")
        with self.assertRaises(db_backend.IntegrityError): self.service.settle("fail","a",5,1,1,30)
        self.assertEqual(before,self.state())

    def test_unrecoverable_real_balance_refuses_without_battle_settlement(self):
        with db_backend.transaction(self.impart) as conn:
            conn.execute("DROP TRIGGER impart_crystal_exact_write_guard")
            conn.execute(
                "UPDATE xiuxian_impart SET stone_num=CAST(%s AS REAL),"
                "stone_num_exact=NULL,stone_num_precision_state='exact' WHERE user_id='a'",
                (float(2**63),),
            )
        with DatabaseUnitOfWork(self.impart) as uow:
            apply_impart_exact_crystal_balances(uow)
        before = self.state()

        result = self.service.settle("precision", "a", 7, 2, 0, 50)

        self.assertEqual(
            (result.status, result.reason, result.subject),
            ("precision_unavailable", "crystal_precision_lost_real", "a"),
        )
        self.assertEqual(self.state(), before)
        with db_backend.connection(self.impart) as conn:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                    "AND name='impart_battle_batch_operations'"
                ).fetchone()[0],
            )
if __name__ == "__main__": unittest.main()
