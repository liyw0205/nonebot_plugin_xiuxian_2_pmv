"""玩家「我的宗门贡献流水」视图（只读个人贡献收支）。

宗门贡献是玩家用灵石换来的个人额度：`宗门捐献` 加、`宗门商店兑换` 减、宗门设施升级
也走同一列。`economy_log.sect_contribution_delta` 由这些真实写路径持续记录
（`xiuxian/xiuxian_sect/__init__.py` 的 donate / shop_exchange / fairyland_upgrade /
elixir_room_upgrade，以及社团任务结算），但玩家此前只能看到
`user_xiuxian.sect_contribution` 这一个当前余额。

本套件用**真实 `economy_log` 列集**（15 列，含 `sect_contribution_delta`）钉住聚合口径、
时间窗、玩家隔离、分段省略与降级行为。
"""

from __future__ import annotations

import ast
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.economy_ledger.contribution_flow import (
    ContributionFlowApplication,
    format_contribution_flow,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

# 真实 economy_log 列集（features/tasks|dufang migrations 与兼容写入方共建的 15 列）。
ECONOMY_LOG_DDL = (
    "CREATE TABLE economy_log("
    "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,source TEXT,action TEXT,"
    "stone_delta INTEGER NOT NULL DEFAULT 0,exp_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_scale_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_materials_delta INTEGER NOT NULL DEFAULT 0,item_delta TEXT,detail TEXT,trace_id TEXT,"
    "created_at TEXT,event_id TEXT)"
)
USER_XIUXIAN_DDL = (
    "CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT UNIQUE,user_name TEXT,"
    "stone INTEGER,sect_id INTEGER,sect_position INTEGER,sect_contribution INTEGER)"
)
SECTS_DDL = (
    "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT,"
    "sect_scale INTEGER NOT NULL DEFAULT 1)"
)


class _Clock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


NOW = datetime(2026, 10, 30, 12, 0, 0)


def fmt(value) -> str:
    return f"{int(value):,}"


class ContributionFlowTests(unittest.TestCase):
    def build(self, temp: str, *, with_economy_log: bool = True,
              with_sects: bool = True, with_contribution_column: bool = True) -> Path:
        game = Path(temp) / "game.db"
        with DatabaseUnitOfWork(str(game), immediate=True) as uow:
            if with_economy_log:
                uow.execute(ECONOMY_LOG_DDL)
            contrib_col = "sect_contribution INTEGER" if with_contribution_column else "level INTEGER"
            uow.execute(
                "CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT UNIQUE,"
                f"user_name TEXT,stone INTEGER,sect_id INTEGER,sect_position INTEGER,{contrib_col})"
            )
            if with_sects:
                uow.execute(SECTS_DDL)
                uow.execute("INSERT INTO sects(sect_id,sect_name,sect_owner,sect_scale)"
                            " VALUES(7,'青云宗','u1',1)")
            if with_contribution_column:
                uow.execute(
                    "INSERT INTO user_xiuxian(id,user_id,user_name,stone,sect_id,sect_position,"
                    "sect_contribution) VALUES(1,'u1','甲',100,7,0,8800)"
                )
                uow.execute(
                    "INSERT INTO user_xiuxian(id,user_id,user_name,stone,sect_id,sect_position,"
                    "sect_contribution) VALUES(2,'u2','乙',100,7,0,50000)"
                )
            else:
                uow.execute(
                    "INSERT INTO user_xiuxian(id,user_id,user_name,stone,sect_id,sect_position)"
                    " VALUES(1,'u1','甲',100,7,0)"
                )
                uow.execute(
                    "INSERT INTO user_xiuxian(id,user_id,user_name,stone,sect_id,sect_position)"
                    " VALUES(2,'u2','乙',100,7,0)"
                )
            if not with_economy_log:
                return game
            rows = [
                # 目标玩家，近 7 天
                ("u1", "sect", "donate", -1000, 1000, "2026-10-29 10:00:00"),
                ("u1", "sect", "donate", -500, 500, "2026-10-28 10:00:00"),
                ("u1", "sect", "task_complete", 0, 120, "2026-10-30 09:00:00"),
                ("u1", "sect", "shop_exchange", 0, -300, "2026-10-29 15:00:00"),
                ("u1", "sect", "fairyland_upgrade", 0, -200, "2026-10-27 08:00:00"),
                # 零变化行：不计入笔数
                ("u1", "sect", "donate", -50, 0, "2026-10-30 11:00:00"),
                # 他人：绝不能泄漏
                ("u2", "sect", "donate", -9999, 9999, "2026-10-30 10:00:00"),
                # 窗外（7 天以前）
                ("u1", "sect", "donate", -7777, 7777, "2026-10-01 10:00:00"),
            ]
            for user_id, source, action, stone, contrib, ts in rows:
                uow.execute(
                    "INSERT INTO economy_log(user_id,sect_id,source,action,stone_delta,"
                    "sect_contribution_delta,created_at) VALUES(?,7,?,?,?,?,?)",
                    (user_id, source, action, stone, contrib, ts),
                )
        return game

    def app(self, temp: str, **kwargs) -> ContributionFlowApplication:
        return ContributionFlowApplication(str(self.build(temp, **kwargs)), clock=_Clock(NOW))

    # ------------------------------------------------------------ 聚合口径
    def test_summary_totals(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("u1")
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["balance"], 8800)
            self.assertEqual(payload["sect_name"], "青云宗")
            self.assertEqual(payload["gained"], 1620)   # 1000 + 500 + 120
            self.assertEqual(payload["spent"], 500)     # 300 + 200
            self.assertEqual(payload["net"], 1120)
            self.assertEqual(payload["records"], 5)     # 零变化与窗外、他人均不计

    def test_top_sources_and_uses(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("u1")
            self.assertEqual([r["action"] for r in payload["sources"]],
                             ["donate", "task_complete"])
            self.assertEqual([r["gained"] for r in payload["sources"]], [1500, 120])
            self.assertEqual([r["action"] for r in payload["uses"]],
                             ["shop_exchange", "fairyland_upgrade"])
            self.assertEqual([r["spent"] for r in payload["uses"]], [300, 200])

    def test_recent_is_newest_first_and_capped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("u1")
            stamps = [r["created_at"] for r in payload["recent"]]
            self.assertEqual(stamps, sorted(stamps, reverse=True))
            self.assertLessEqual(len(payload["recent"]), 5)

    # ------------------------------------------------------------ 玩家隔离
    def test_other_player_never_leaks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_contribution_flow(self.app(temp).summary("u1"), format_number=fmt)
            for leaked in ("9,999", "7,777", "50,000", "乙"):
                self.assertNotIn(leaked, text)
            self.assertIn("青云宗", text)

    def test_second_player_sees_own_numbers_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("u2")
            self.assertEqual(payload["gained"], 9999)
            self.assertEqual(payload["spent"], 0)
            self.assertEqual(payload["records"], 1)

    # ------------------------------------------------------------ 渲染合同
    def test_full_render_uses_chinese_action_labels(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_contribution_flow(self.app(temp).summary("u1"), format_number=fmt)
            self.assertIn("【我的宗门贡献流水】近7天", text)
            self.assertIn("宗门：青云宗（贡献 8,800）", text)
            self.assertIn("获得：+1,620", text)
            self.assertIn("消耗：-500", text)
            self.assertIn("净变化：+1,120", text)
            self.assertIn("记录：5 笔", text)
            self.assertIn("· 宗门捐献 +1,500（2笔）", text)
            self.assertIn("· 宗门任务 +120（1笔）", text)
            self.assertIn("· 宗门商店兑换 -300（1笔）", text)
            self.assertNotIn("sect·donate", text)

    def test_unknown_action_falls_back_to_source_action(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute(
                    "INSERT INTO economy_log(user_id,sect_id,source,action,"
                    "sect_contribution_delta,created_at) VALUES('u1',7,'sect','weird_new',42,"
                    "'2026-10-30 11:30:00')"
                )
            payload = ContributionFlowApplication(str(game), clock=_Clock(NOW)).summary("u1")
            self.assertIn("sect·weird_new", format_contribution_flow(payload, format_number=fmt))

    def test_no_records_message(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute("DELETE FROM economy_log")
            text = format_contribution_flow(
                ContributionFlowApplication(str(game), clock=_Clock(NOW)).summary("u1"),
                format_number=fmt,
            )
            self.assertIn("近7天没有宗门贡献收支记录。", text)
            self.assertIn("贡献 8,800", text)  # 仍有余额

    def test_player_without_character_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_contribution_flow(self.app(temp).summary("nobody"), format_number=fmt)
            self.assertIn("近7天没有宗门贡献收支记录。", text)

    def test_blank_user_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("   ")
            self.assertFalse(payload["ok"])
            self.assertIn("无法查询", format_contribution_flow(payload, format_number=fmt))

    # ------------------------------------------------------------ 兼容/降级
    def test_missing_economy_log_degrades(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp, with_economy_log=False).summary("u1")
            self.assertFalse(payload["ok"])
            self.assertIn("暂无宗门贡献流水记录", format_contribution_flow(payload, format_number=fmt))

    def test_missing_contribution_column_degrades(self) -> None:
        # 极旧库 user_xiuxian 没有 sect_contribution：流水照常渲染，只是不显示余额。
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp, with_contribution_column=False)
            payload = ContributionFlowApplication(str(game), clock=_Clock(NOW)).summary("u1")
            self.assertIsNone(payload["balance"])
            self.assertTrue(payload["ok"])
            text = format_contribution_flow(payload, format_number=fmt)
            self.assertIn("宗门：青云宗", text)
            self.assertNotIn("贡献 ", text)
            self.assertIn("获得：+1,620", text)

    def test_missing_sects_table_still_shows_balance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_contribution_flow(
                self.app(temp, with_sects=False).summary("u1"), format_number=fmt
            )
            self.assertIn("当前宗门贡献：8,800", text)

    def test_read_only_creates_no_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            before = self._tables(game)
            ContributionFlowApplication(str(game), clock=_Clock(NOW)).summary("u1")
            self.assertEqual(self._tables(game), before)

    @staticmethod
    def _tables(path: Path) -> set[str]:
        with sqlite3.connect(path) as conn:
            return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    def test_old_database_without_sect_contribution_delta(self) -> None:
        # 极旧库：economy_log 没有该列时不报错、不建列。
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute(
                    "CREATE TABLE economy_log(id INTEGER PRIMARY KEY,user_id TEXT,source TEXT,"
                    "action TEXT,stone_delta INTEGER,created_at TEXT)"
                )
                uow.execute(
                    "CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT,"
                    "sect_id INTEGER,sect_contribution INTEGER)"
                )
                uow.execute("INSERT INTO user_xiuxian VALUES(1,'u1',NULL,42)")
            payload = ContributionFlowApplication(str(game), clock=_Clock(NOW)).summary("u1")
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["balance"], 42)
            with sqlite3.connect(game) as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(economy_log)")}
            self.assertNotIn("sect_contribution_delta", cols)


class HandlerSourceContractTests(unittest.TestCase):
    def test_command_and_handler(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_sect"
                  / "__init__.py").read_text(encoding="utf-8")
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
        self.assertEqual(matchers.get("我的宗门贡献流水"), "sect_contribution_flow")

        handler = None
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    seg = ast.get_source_segment(source, dec) or ""
                    if "sect_contribution_flow.handle" in seg:
                        handler = node
        self.assertIsNotNone(handler, "handler must be registered")
        body = ast.get_source_segment(source, handler) or ""
        self.assertIn('summary(str(user_info["user_id"]))', body)
        self.assertIn("format_contribution_flow(", body)
        # 安全：不得用命令参数选择他人。
        self.assertNotIn("extract_plain_text", body)
        self.assertNotIn("args", body)


if __name__ == "__main__":
    unittest.main()
