"""悬赏令玩家反馈：查看显示剩余刷新次数；结算被本境上限截断时如实说明。

`悬赏令查看`（状态 3）此前只显示剩余时间与任务表，剩余刷新次数只在刷新回执里出现；
结算仓储把修为截到 `max_exp - 当前修为` 后，回执只报实际值而不解释差异。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.work import migrations as work_migrations
from nonebot_plugin_xiuxian_2.features.work.presentation import (
    refresh_count_line,
    settlement_exp_note,
)
from nonebot_plugin_xiuxian_2.features.work.settlement_repository import (
    WorkSettlementSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
WORK_MODULE = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_work" / "__init__.py"


class RefreshCountLineTests(unittest.TestCase):
    def test_reports_value_within_total(self) -> None:
        self.assertEqual(refresh_count_line(3, 5), "今日剩余刷新次数：3次")
        self.assertEqual(refresh_count_line(0, 5), "今日剩余刷新次数：0次")

    def test_missing_or_bad_value_falls_back_to_total(self) -> None:
        self.assertEqual(refresh_count_line(None, 5), "今日剩余刷新次数：5次")
        self.assertEqual(refresh_count_line("bad", 5), "今日剩余刷新次数：5次")
        self.assertEqual(refresh_count_line("", 5), "今日剩余刷新次数：5次")

    def test_values_clamp_to_zero_and_total(self) -> None:
        self.assertEqual(refresh_count_line(-2, 5), "今日剩余刷新次数：0次")
        self.assertEqual(refresh_count_line(99, 5), "今日剩余刷新次数：5次")
        self.assertEqual(refresh_count_line(3, 0), "今日剩余刷新次数：0次")


class SettlementExpNoteTests(unittest.TestCase):
    def test_no_note_when_fully_granted(self) -> None:
        self.assertEqual(settlement_exp_note(500, 500), "")
        self.assertEqual(settlement_exp_note(500, None), "")

    def test_note_only_when_clipped(self) -> None:
        note = settlement_exp_note(0, 500)
        self.assertIn("本境修为上限", note)
        self.assertIn("未发放", note)
        self.assertIn("未发放", settlement_exp_note(120, 500))

    def test_bad_values_do_not_crash(self) -> None:
        self.assertEqual(settlement_exp_note("bad", "worse"), "")
        self.assertEqual(settlement_exp_note(None, None), "")


class SettlementReceiptTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.game = Path(self._tmp.name) / "game.db"
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            work_migrations.apply_work_settlement_operations(uow)
            work_migrations.apply_work_abort_cleanup(uow)
            work_migrations.apply_work_claim_operations(uow)
            work_migrations.apply_work_offer_snapshots(uow)
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, exp INTEGER)"
            )
            conn.execute("CREATE TABLE user_cd (user_id TEXT PRIMARY KEY, type INTEGER, "
                         "create_time TEXT, scheduled_time TEXT)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('u', 900)")
            conn.execute("INSERT INTO user_cd VALUES ('u', 2, 'start', '挖矿')")
        self.repo = WorkSettlementSqlRepository(self.game)

    def settle(self, op: str = "op"):
        return self.repo.settle(
            op,
            "u",
            {"create_time": "start", "scheduled_time": "挖矿"},
            500,
            None,
            1_000,
            10,
            success_kind="ok",
        )

    def receipt(self, op: str = "op") -> dict:
        with db_backend.connection(self.game) as conn:
            row = conn.execute(
                "SELECT result_json FROM work_settlement_operations WHERE operation_id=?",
                (op,),
            ).fetchone()
        return json.loads(row[0])

    def test_receipt_records_requested_and_applied_exp(self) -> None:
        result = self.settle()
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.exp, 100)
        stored = self.receipt()
        self.assertEqual(stored["exp"], 100)
        self.assertEqual(stored["exp_requested"], 500)
        self.assertNotEqual(settlement_exp_note(stored["exp"], stored["exp_requested"]), "")

    def test_replay_preserves_requested_value_for_the_note(self) -> None:
        self.settle()
        replay = self.settle()
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(replay.exp, 100)
        stored = self.receipt()
        self.assertEqual(stored["exp_requested"], 500)


class WorkModuleWiringTests(unittest.TestCase):
    def test_status_view_and_settlement_render_the_new_lines(self) -> None:
        source = WORK_MODULE.read_text(encoding="utf-8")
        self.assertIn("refresh_count_line(", source)
        self.assertIn("user_info.get(\"work_num\")", source)
        self.assertEqual(source.count("settlement_exp_note("), 2)  # 两条回执路径


if __name__ == "__main__":
    unittest.main()
