"""玩家「我的突破统计」派生视图（只读）。

突破的四个原始计数一直在 `player_data.statistics` 里累积
（`突破次数`/`突破成功`/`突破失败`/`突破损失修为`），但玩家侧唯一入口是 `修仙数据`，
而它只是把 55 个字段原样裸罗列（`xiuxian_base/__init__.py:1824`）——没有成功率、
没有累计损失的汇总。

本套件用**真实 59 列 `statistics`**（player_db）与真实应用钉住派生口径：
成功率的分母永远是 `突破次数`；当 `成功+失败 != 次数` 时如实标记不一致，
而不是悄悄改用"成功+失败"当分母。
"""

from __future__ import annotations

import ast
import sqlite3
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.base.breakthrough_stats import (
    BreakthroughStatsApplication,
    format_breakthrough_stats,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

# statistics 的真实形状：user_id 主键 + 业务列由各迁移按需 ALTER 补。
# 这里建最小可用集：主键 + 本切片四列。
STATISTICS_DDL = (
    "CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"
    '"突破次数" INTEGER DEFAULT 0,"突破成功" INTEGER DEFAULT 0,'
    '"突破失败" INTEGER DEFAULT 0,"突破损失修为" INTEGER DEFAULT 0)'
)


def fmt(value) -> str:
    return f"{int(value):,}"


class BreakthroughStatsTests(unittest.TestCase):
    def build(self, temp: str) -> Path:
        player = Path(temp) / "player.db"
        with DatabaseUnitOfWork(str(player), immediate=True) as uow:
            uow.execute(STATISTICS_DDL)
            rows = [
                # 目标玩家：12 次，成功 5，失败 7，损失 3600
                ("u1", 12, 5, 7, 3600),
                # 全成功
                ("u2", 3, 3, 0, 0),
                # 不一致：成功+失败(6) != 次数(10)
                ("u3", 10, 3, 3, 900),
                # 从未突破
                ("u4", 0, 0, 0, 0),
            ]
            for row in rows:
                uow.execute(
                    'INSERT INTO statistics(user_id,"突破次数","突破成功","突破失败",'
                    '"突破损失修为") VALUES(?,?,?,?,?)',
                    row,
                )
        return player

    def app(self, player: Path) -> BreakthroughStatsApplication:
        return BreakthroughStatsApplication(str(player))

    # ------------------------------------------------------------ 汇总
    def test_summary_values_and_rate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(self.build(temp)).summary("u1")
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["total"], 12)
            self.assertEqual(payload["success"], 5)
            self.assertEqual(payload["failure"], 7)
            self.assertEqual(payload["exp_lost"], 3600)
            self.assertEqual(payload["rate"], 41.7)
            self.assertFalse(payload["inconsistent"])

    def test_full_success_rate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(self.build(temp)).summary("u2")
            self.assertEqual(payload["rate"], 100.0)
            self.assertEqual(payload["failure"], 0)

    def test_zero_total_has_no_rate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(self.build(temp)).summary("u4")
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["total"], 0)
            self.assertIsNone(payload["rate"])
            self.assertIn("还没有突破记录", format_breakthrough_stats(payload, format_number=fmt))

    # ------------------------------------------------------------ 一致性
    def test_inconsistent_keeps_total_as_denominator(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(self.build(temp)).summary("u3")
            self.assertTrue(payload["inconsistent"])
            # 分母是突破次数 10，而不是 成功+失败 6
            self.assertEqual(payload["rate"], 30.0)

    def test_inconsistent_render_explains_difference(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_breakthrough_stats(
                self.app(self.build(temp)).summary("u3"), format_number=fmt
            )
            self.assertIn("成功率：30.0%", text)
            self.assertIn("不一致", text)

    def test_consistent_render_has_no_warning(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_breakthrough_stats(
                self.app(self.build(temp)).summary("u1"), format_number=fmt
            )
            self.assertNotIn("不一致", text)

    # ------------------------------------------------------------ 隔离
    def test_other_player_does_not_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            player = self.build(temp)
            payload = self.app(player).summary("u2")
            self.assertEqual(payload["total"], 3)
            self.assertEqual(payload["exp_lost"], 0)
            text = format_breakthrough_stats(payload, format_number=fmt)
            self.assertNotIn("3,600", text)
            self.assertNotIn("12 次", text)

    # ------------------------------------------------------------ 渲染
    def test_render_with_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_breakthrough_stats(
                self.app(self.build(temp)).summary("u1"), format_number=fmt
            )
            self.assertIn("【我的突破统计】", text)
            self.assertIn("突破次数：12 次", text)
            self.assertIn("成功：5 次", text)
            self.assertIn("失败：7 次", text)
            self.assertIn("成功率：41.7%", text)
            self.assertIn("累计损失修为：3,600", text)

    # ------------------------------------------------------------ 降级
    def test_missing_table_degrades(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(str(player), immediate=True):
                pass
            payload = self.app(player).summary("u1")
            self.assertFalse(payload["ok"])
            self.assertIn("暂无突破统计数据", format_breakthrough_stats(payload, format_number=fmt))
            with sqlite3.connect(player) as conn:
                tables = {r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("statistics", tables)

    def test_missing_all_four_columns_degrades(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(str(player), immediate=True) as uow:
                uow.execute("CREATE TABLE statistics(user_id TEXT PRIMARY KEY)")
                uow.execute("INSERT INTO statistics(user_id) VALUES('u1')")
            payload = self.app(player).summary("u1")
            self.assertFalse(payload["ok"])
            self.assertIn("暂无突破统计数据", format_breakthrough_stats(payload, format_number=fmt))
            with sqlite3.connect(player) as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(statistics)")}
            self.assertEqual(cols, {"user_id"})  # 只读路径不补列

    def test_single_missing_column_defaults_to_zero(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(str(player), immediate=True) as uow:
                uow.execute(
                    "CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"
                    '"突破次数" INTEGER DEFAULT 0,"突破成功" INTEGER DEFAULT 0,'
                    '"突破失败" INTEGER DEFAULT 0)'
                )
                uow.execute(
                    'INSERT INTO statistics(user_id,"突破次数","突破成功","突破失败")'
                    " VALUES('u1',4,1,3)"
                )
            payload = self.app(player).summary("u1")
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["total"], 4)
            self.assertEqual(payload["exp_lost"], 0)  # 缺列按 0
            self.assertEqual(payload["rate"], 25.0)

    def test_negative_values_are_clamped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            player = Path(temp) / "player.db"
            with DatabaseUnitOfWork(str(player), immediate=True) as uow:
                uow.execute(STATISTICS_DDL)
                uow.execute(
                    'INSERT INTO statistics(user_id,"突破次数","突破成功","突破失败",'
                    '"突破损失修为") VALUES(\'u1\',5,-2,-1,-100)'
                )
            payload = self.app(player).summary("u1")
            self.assertEqual(payload["success"], 0)
            self.assertEqual(payload["failure"], 0)
            self.assertEqual(payload["exp_lost"], 0)
            self.assertEqual(payload["rate"], 0.0)
            text = format_breakthrough_stats(payload, format_number=fmt)
            self.assertNotIn("-", text.split("【我的突破统计】")[1].replace("累计损失修为：0", ""))

    def test_unknown_player_renders_guidance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(self.build(temp)).summary("nobody")
            self.assertTrue(payload["ok"])
            self.assertIn("还没有突破记录", format_breakthrough_stats(payload, format_number=fmt))

    def test_blank_user_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(self.build(temp)).summary("   ")
            self.assertFalse(payload["ok"])
            self.assertIn("无法查询", format_breakthrough_stats(payload, format_number=fmt))

    def test_read_only_creates_no_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            player = self.build(temp)
            with sqlite3.connect(player) as conn:
                before = {r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            self.app(player).summary("u1")
            with sqlite3.connect(player) as conn:
                after = {r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertEqual(before, after)


class HandlerSourceContractTests(unittest.TestCase):
    def test_command_and_handler(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_base"
                  / "breakthrough_tribulation.py").read_text(encoding="utf-8")
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
        self.assertEqual(matchers.get("我的突破统计"), "breakthrough_stats")

        handler = None
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    seg = ast.get_source_segment(source, dec) or ""
                    if "breakthrough_stats.handle" in seg:
                        handler = node
        self.assertIsNotNone(handler, "handler must be registered")
        body = ast.get_source_segment(source, handler) or ""
        self.assertIn('summary(str(user_info["user_id"]))', body)
        self.assertIn("format_breakthrough_stats(", body)
        self.assertNotIn("extract_plain_text", body)
        self.assertNotIn("args", body)


if __name__ == "__main__":
    unittest.main()
