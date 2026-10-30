"""玩家「我的灵石流水」视图（个人经济流水）。

The gap this feature closes: ``economy_log`` is the server-wide economy ledger and
is written by real gameplay (task claims, dufang shares, past-life finals, admin
asset changes, ``safe_log_economy_change``), but the only reader was the
administrator Web blueprint — ``features/economy_ledger/manifest.py`` had no
``commands`` and ``features/economy_ledger/commands.py`` declared that the ledger
"exposes administrative Web reads, not a matcher" (``COMMANDS = ()``).
Players had no way to see where their stone went.

This suite pins the player view against a REAL ``economy_log`` table (the same
15-column shape the plugin's own migrations produce) and the real
``EconomyLedgerApplication``, plus a pure-rendering contract for
``format_stone_flow``.
"""

from __future__ import annotations

import ast
import sqlite3
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.economy_ledger.application import (
    EconomyLedgerApplication,
    format_stone_flow,
)
from nonebot_plugin_xiuxian_2.features.economy_ledger.schemas import (
    STONE_FLOW_DAYS,
    STONE_FLOW_RECENT,
    STONE_FLOW_TOP_N,
)
from nonebot_plugin_xiuxian_2.infrastructure.clock import SystemClock
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

# The authoritative economy_log column set produced by the plugin's migrations.
ECONOMY_LOG_DDL = (
    "CREATE TABLE economy_log("
    "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
    "source TEXT NOT NULL,action TEXT NOT NULL,"
    "stone_delta INTEGER NOT NULL DEFAULT 0,exp_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_contribution_delta INTEGER NOT NULL DEFAULT 0,sect_scale_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_materials_delta INTEGER NOT NULL DEFAULT 0,item_delta TEXT NOT NULL DEFAULT '[]',"
    "detail TEXT NOT NULL DEFAULT '{}',trace_id TEXT,created_at TEXT NOT NULL,event_id TEXT)"
)


def _fmt(value) -> str:
    return f"{int(value):,}"


class StoneFlowViewTests(unittest.TestCase):
    """Drive the player view the way the ``我的灵石流水`` handler drives it."""

    def build(self, temp: str, rows=(), *, with_table: bool = True) -> Path:
        database = Path(temp) / "game.db"
        if not with_table:
            database.write_bytes(b"")
            return database
        now = SystemClock().now()
        with DatabaseUnitOfWork(str(database)) as uow:
            uow.execute(ECONOMY_LOG_DDL)
            for user_id, source, action, delta, days_ago in rows:
                stamp = (now - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
                uow.execute(
                    "INSERT INTO economy_log(user_id,source,action,stone_delta,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (user_id, source, action, delta, stamp),
                )
        return database

    # ------------------------------------------------------------ 汇总口径
    def test_summary_counts_income_expense_and_net(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(
                temp,
                [
                    ("u1", "task", "daily_claim", 5000, 0),
                    ("u1", "sign_in", "sign", 1200, 1),
                    ("u1", "work", "settle", -2000, 2),
                    ("u1", "trade", "buy", -8000, 3),
                ],
            )
            payload = EconomyLedgerApplication(database).stone_flow_summary("u1")
            self.assertTrue(payload["ok"])
            summary = payload["summary"]
            self.assertEqual(int(summary["records"]), 4)
            self.assertEqual(int(summary["stone_in"]), 6200)
            self.assertEqual(int(summary["stone_out"]), 10000)
            self.assertEqual(int(summary["stone_net"]), -3800)

    def test_other_players_are_never_counted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(
                temp,
                [
                    ("u1", "task", "daily_claim", 5000, 0),
                    ("u2", "task", "daily_claim", 9999, 0),
                    ("u2", "trade", "buy", -7777, 1),
                ],
            )
            payload = EconomyLedgerApplication(database).stone_flow_summary("u1")
            self.assertEqual(int(payload["summary"]["records"]), 1)
            self.assertEqual(int(payload["summary"]["stone_in"]), 5000)
            self.assertEqual(int(payload["summary"]["stone_out"]), 0)
            for row in payload["income"] + payload["expense"] + payload["recent"]:
                self.assertNotIn("9999", str(row))
                self.assertNotIn("7777", str(row))

    def test_rows_outside_the_window_are_excluded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(
                temp,
                [
                    ("u1", "task", "recent", 1000, 1),
                    ("u1", "task", "stale", 7777, STONE_FLOW_DAYS + 2),
                ],
            )
            payload = EconomyLedgerApplication(database).stone_flow_summary("u1")
            self.assertEqual(int(payload["summary"]["records"]), 1)
            self.assertEqual(int(payload["summary"]["stone_in"]), 1000)

    def test_income_and_expense_are_capped_and_ranked(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            rows = [
                ("u1", "src_a", "act", 100, 0),
                ("u1", "src_b", "act", 900, 0),
                ("u1", "src_c", "act", 500, 0),
                ("u1", "src_d", "act", 700, 0),
                ("u1", "sink_a", "act", -100, 0),
                ("u1", "sink_b", "act", -900, 0),
                ("u1", "sink_c", "act", -500, 0),
                ("u1", "sink_d", "act", -700, 0),
            ]
            database = self.build(temp, rows)
            payload = EconomyLedgerApplication(database).stone_flow_summary("u1")
            self.assertEqual(len(payload["income"]), STONE_FLOW_TOP_N)
            self.assertEqual(len(payload["expense"]), STONE_FLOW_TOP_N)
            self.assertEqual(
                [int(row["stone_in"]) for row in payload["income"]], [900, 700, 500]
            )
            self.assertEqual(
                [int(row["stone_out"]) for row in payload["expense"]], [900, 700, 500]
            )

    def test_recent_rows_are_capped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            rows = [("u1", "src", "act", 100, 0) for _ in range(STONE_FLOW_RECENT + 4)]
            database = self.build(temp, rows)
            payload = EconomyLedgerApplication(database).stone_flow_summary("u1")
            self.assertEqual(len(payload["recent"]), STONE_FLOW_RECENT)

    def test_missing_table_reports_notice_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, with_table=False)
            payload = EconomyLedgerApplication(database).stone_flow_summary("u1")
            self.assertFalse(payload["ok"])
            self.assertIn("economy_log", str(payload["notice"]))
            with sqlite3.connect(database) as conn:
                present = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='economy_log'"
                ).fetchone()
            self.assertIsNone(present)

    def test_blank_user_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp)
            payload = EconomyLedgerApplication(database).stone_flow_summary("   ")
            self.assertFalse(payload["ok"])
            self.assertIn("玩家身份", str(payload["notice"]))

    # ------------------------------------------------------------ 渲染合同
    def test_render_full_view(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(
                temp,
                [
                    ("u1", "task", "daily_claim", 5000, 0),
                    ("u1", "trade", "buy", -8000, 1),
                ],
            )
            payload = EconomyLedgerApplication(database).stone_flow_summary("u1")
            text = format_stone_flow(payload, format_number=_fmt)
            self.assertIn("【我的灵石流水】近7天", text)
            self.assertIn("灵石收入：+5,000", text)
            self.assertIn("灵石支出：-8,000", text)
            self.assertIn("净收支：-3,000", text)
            self.assertIn("记录：2 笔", text)
            self.assertIn("主要收入来源：", text)
            self.assertIn("task·daily_claim +5,000", text)
            self.assertIn("主要支出方向：", text)
            self.assertIn("trade·buy -8,000", text)
            self.assertIn("最近 2 笔：", text)

    def test_render_omits_income_section_when_no_income(self) -> None:
        payload = {
            "ok": True,
            "notice": None,
            "summary": {"records": 1, "stone_in": 0, "stone_out": 500, "stone_net": -500},
            "income": [],
            "expense": [{"source": "trade", "action": "buy", "stone_out": 500, "records": 1}],
            "recent": [{"source": "trade", "action": "buy", "stone_delta": -500,
                        "created_at": "2026-10-30 10:00:00"}],
        }
        text = format_stone_flow(payload, format_number=_fmt)
        self.assertNotIn("主要收入来源", text)
        self.assertIn("主要支出方向", text)

    def test_render_omits_expense_section_when_no_expense(self) -> None:
        payload = {
            "ok": True,
            "notice": None,
            "summary": {"records": 1, "stone_in": 500, "stone_out": 0, "stone_net": 500},
            "income": [{"source": "task", "action": "claim", "stone_in": 500, "records": 1}],
            "expense": [],
            "recent": [{"source": "task", "action": "claim", "stone_delta": 500,
                        "created_at": "2026-10-30 10:00:00"}],
        }
        text = format_stone_flow(payload, format_number=_fmt)
        self.assertIn("主要收入来源", text)
        self.assertNotIn("主要支出方向", text)

    def test_render_empty_window(self) -> None:
        payload = {
            "ok": True,
            "notice": None,
            "summary": {"records": 0, "stone_in": 0, "stone_out": 0, "stone_net": 0},
            "income": [],
            "expense": [],
            "recent": [],
        }
        self.assertEqual(format_stone_flow(payload), "近7天没有灵石收支记录。")

    def test_render_passes_through_notice(self) -> None:
        payload = {"ok": False, "notice": "economy_log 表尚未创建，暂无经济流水。",
                   "summary": {}, "income": [], "expense": [], "recent": []}
        self.assertEqual(format_stone_flow(payload), "economy_log 表尚未创建，暂无经济流水。")

    # ------------------------------------------------------- handler 源合同
    def test_handler_source_contract(self) -> None:
        path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_base"
            / "__init__.py"
        )
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)

        matchers = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                fn = node.value.func
                if getattr(fn, "id", None) == "on_command" and node.value.args:
                    name = node.value.args[0]
                    if isinstance(name, ast.Constant):
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                matchers[name.value] = target.id
        self.assertEqual(matchers.get("我的灵石流水"), "stone_flow")

        # 命令必须由 @stone_flow.handle 绑定，且 handler 用会话 user_id 查询。
        handler = None
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    seg = ast.get_source_segment(source, dec) or ""
                    if "stone_flow.handle" in seg:
                        handler = node
        self.assertIsNotNone(handler, "stone_flow handler must be registered")
        body = ast.get_source_segment(source, handler) or ""
        self.assertIn('stone_flow_summary(str(user_info["user_id"]))', body)
        self.assertIn("format_stone_flow(", body)
        # 安全：不得把命令参数里的 user_id 传给应用。
        self.assertNotIn("extract_plain_text", body)
        self.assertNotIn("args", body)


if __name__ == "__main__":
    unittest.main()
