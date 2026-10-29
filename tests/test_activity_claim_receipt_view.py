from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.claim_receipt_view import (
    ActivityClaimReceiptView,
    render_claim_receipts,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_repository import (
    ActivityReadModelSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


def _receipt(ok: bool, steps: list[tuple[str, bool]], text: str = "") -> str:
    return json.dumps(
        {
            "ok": ok,
            "text": text,
            "steps": [{"name": name, "ok": passed, "text": ""} for name, passed in steps],
        },
        ensure_ascii=True,
    )


class ActivityClaimReceiptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE activity_claim_all_operations("
                "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,status TEXT NOT NULL,"
                "result_json TEXT NOT NULL DEFAULT '',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
        self.repository = ActivityReadModelSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def insert(
        self,
        operation_id: str,
        *,
        user_id: str = "u1",
        status: str = "completed",
        result_json: str = "",
        created_at: str = "2026-10-29 10:00:00",
        updated_at: str = "2026-10-29 10:00:00",
    ) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_claim_all_operations("
                "operation_id,user_id,status,result_json,created_at,updated_at"
                ") VALUES(?,?,?,?,?,?)",
                (operation_id, user_id, status, result_json, created_at, updated_at),
            )

    def test_recent_receipts_order_limit_and_user_scope(self) -> None:
        self.insert("old", updated_at="2026-10-28 10:00:00")
        self.insert("new", updated_at="2026-10-29 12:00:00")
        self.insert("other", user_id="u2", updated_at="2026-10-30 10:00:00")

        rows = self.repository.recent_claim_receipts("u1", limit=2)

        self.assertEqual([row.created_at for row in rows], ["2026-10-29 12:00:00", "2026-10-28 10:00:00"])
        self.assertEqual((), self.repository.recent_claim_receipts("u1", limit=0))

    def test_receipt_parses_success_partial_failure_pending_and_bad_json(self) -> None:
        self.insert(
            "ok",
            result_json=_receipt(True, [("tasks", True), ("pass", True)]),
            updated_at="2026-10-29 13:00:00",
        )
        self.insert(
            "partial",
            result_json=_receipt(True, [("tasks", True), ("stage", False)]),
            updated_at="2026-10-29 12:00:00",
        )
        self.insert(
            "failed",
            result_json=_receipt(False, [("tasks", False), ("rank", False)]),
            updated_at="2026-10-29 11:00:00",
        )
        self.insert("pending", status="pending", updated_at="2026-10-29 10:00:00")
        self.insert("bad", result_json="not-json", updated_at="2026-10-29 09:00:00")

        rows = self.repository.recent_claim_receipts("u1")

        self.assertEqual(
            [(row.status, row.ok, row.settled_labels, row.failed_labels) for row in rows],
            [
                ("已领取", True, ("任务", "战令"), ()),
                ("已领取", True, ("任务",), ("阶段",)),
                ("未领取", False, (), ("任务", "排行")),
                ("处理中", False, (), ()),
                ("未领取", False, (), ()),
            ],
        )

    def test_missing_table_and_database_return_empty_without_ddl(self) -> None:
        empty = Path(self.temp.name) / "empty.db"
        with DatabaseUnitOfWork(empty, immediate=True) as uow:
            uow.execute("CREATE TABLE filler (id INTEGER)")
        missing = Path(self.temp.name) / "missing.db"

        self.assertEqual((), ActivityReadModelSqlRepository(empty).recent_claim_receipts("u1"))
        self.assertEqual((), ActivityReadModelSqlRepository(missing).recent_claim_receipts("u1"))
        self.assertFalse(missing.exists())
        with DatabaseUnitOfWork(empty) as uow:
            self.assertIsNone(
                uow.query_one(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='activity_claim_all_operations'"
                )
            )

    def test_renderer_shows_receipts_and_empty_is_blank(self) -> None:
        rows = (
            ActivityClaimReceiptView("t1", "已领取", True, ("任务", "战令")),
            ActivityClaimReceiptView("t2", "已领取", True, ("任务",), ("阶段",)),
            ActivityClaimReceiptView("t3", "处理中", False),
            ActivityClaimReceiptView("t4", "未领取", False, (), ("任务",)),
        )

        text = render_claim_receipts(rows)
        self.assertIn("【最近一键领取】", text)
        self.assertIn("t1 已领取：任务、战令", text)
        self.assertIn("t2 已领取：任务；未完成：阶段", text)
        self.assertIn("t3 处理中（可重试）", text)
        self.assertIn("t4 未领取：任务", text)
        self.assertEqual("", render_claim_receipts(()))

    def test_center_appends_receipts_only_for_user_with_rows(self) -> None:
        config = {
            "name": "活动",
            "start_time": "2026-10-01",
            "end_time": "2026-10-31",
            "daily_tasks": [],
            "weekly_tasks": [],
            "extensions": {"activity_pass": {"enabled": False}},
        }
        app = ActivityReadModelApplication(
            "/tmp/activity-receipt-missing.db",
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: datetime(2026, 10, 15, 12)),
        )
        app.repository.recent_claim_receipts = lambda user_id, limit=5: (
            ActivityClaimReceiptView("t", "已领取", True, ("任务",)),
        )

        self.assertIn("【最近一键领取】", app.activity_center_text("u1"))
        self.assertNotIn("【最近一键领取】", app.activity_center_text())

    def test_center_source_contract_reads_receipts(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "features"
            / "activity"
            / "read_model_application.py"
        ).read_text(encoding="utf-8")
        start = source.index("def activity_center_text(")
        block = source[start : source.index("def activity_stage_text", start)]

        self.assertIn("recent_claim_receipts", block)
        self.assertIn("render_claim_receipts", block)


if __name__ == "__main__":
    unittest.main()
