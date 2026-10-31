"""玩家「我的签到统计」视图（只读累计签到）。

每次 `修仙签到` 成功，`SignInApplicationEffects`
（`features/sign_in/application_effects.py:54`）都会
`statistics.record(..., event_key="修仙签到")`，写入
`sign_in_statistics_events` 并递增 `sign_in_statistics_projection.value`。
但玩家侧从没有命令读它：`SignInStatisticsRepository.value()` 只是为这个视图准备的
读接口，长期只被测试与写入侧使用。

本套件用**真实两表列集**（两者都在 game_db）与真实应用（注入固定时钟）钉住口径：
累计次数必须与 `SignInStatisticsRepository.value()` 一致，本月天数按日期去重，
玩家之间互不干扰，缺表/脏数据只降级不抛错。
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

from nonebot_plugin_xiuxian_2.features.sign_in.statistics import (
    SignInStatisticsRepository,
)
from nonebot_plugin_xiuxian_2.features.sign_in.statistics_view import (
    EVENT_KEY,
    SignInStatisticsViewApplication,
    format_sign_in_statistics,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

EVENTS_DDL = (
    "CREATE TABLE sign_in_statistics_events("
    "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,event_key TEXT NOT NULL,"
    "created_at TEXT NOT NULL)"
)
PROJECTION_DDL = (
    "CREATE TABLE sign_in_statistics_projection("
    "user_id TEXT NOT NULL,event_key TEXT NOT NULL,"
    "value INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(user_id, event_key))"
)


class _Clock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


NOW = datetime(2026, 10, 30, 12, 0, 0)


def fmt(value) -> str:
    return f"{int(value):,}"


class SignInStatisticsViewTests(unittest.TestCase):
    def build(
        self,
        temp: str,
        *,
        with_events: bool = True,
        with_projection: bool = True,
        with_rows: bool = True,
    ) -> Path:
        game = Path(temp) / "game.db"
        with DatabaseUnitOfWork(str(game), immediate=True) as uow:
            if with_events:
                uow.execute(EVENTS_DDL)
            if with_projection:
                uow.execute(PROJECTION_DDL)
            if not with_rows:
                return game
            if with_events:
                rows = [
                    ("op1", "u1", EVENT_KEY, "2026-10-30 09:12:00"),
                    ("op2", "u1", EVENT_KEY, "2026-10-29 08:00:00"),
                    # 同一自然日的第二条：本月"天数"不应重复计
                    ("op3", "u1", EVENT_KEY, "2026-10-29 21:00:00"),
                    ("op4", "u1", EVENT_KEY, "2026-10-02 07:00:00"),
                    # 上月：不计入本月
                    ("op5", "u1", EVENT_KEY, "2026-09-28 07:00:00"),
                    # 他人：绝不能泄漏
                    ("op6", "u2", EVENT_KEY, "2026-10-30 10:00:00"),
                ]
                for row in rows:
                    uow.execute(
                        "INSERT INTO sign_in_statistics_events"
                        "(operation_id,user_id,event_key,created_at) VALUES(?,?,?,?)",
                        row,
                    )
            if with_projection:
                uow.execute(
                    "INSERT INTO sign_in_statistics_projection(user_id,event_key,value)"
                    " VALUES('u1',?,5)",
                    (EVENT_KEY,),
                )
                uow.execute(
                    "INSERT INTO sign_in_statistics_projection(user_id,event_key,value)"
                    " VALUES('u2',?,1)",
                    (EVENT_KEY,),
                )
        return game

    def app(self, game: Path) -> SignInStatisticsViewApplication:
        return SignInStatisticsViewApplication(str(game), clock=_Clock(NOW))

    # ------------------------------------------------------------ 口径
    def test_totals_and_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            payload = self.app(game).summary("u1")
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["total"], 5)
            # 10-30 / 10-29 / 10-02 三天；10-29 两条同日只算一天；9 月不计
            self.assertEqual(payload["month_days"], 3)
            self.assertEqual(payload["last_at"], "2026-10-30 09:12:00")

    def test_total_matches_repository_value(self) -> None:
        """玩家视图的累计次数必须与写入侧读接口完全一致。"""
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            payload = self.app(game).summary("u1")
            expected = SignInStatisticsRepository(str(game)).value(
                user_id="u1", event_key=EVENT_KEY
            )
            self.assertEqual(payload["total"], expected)

    def test_other_event_keys_are_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute(
                    "INSERT INTO sign_in_statistics_events"
                    "(operation_id,user_id,event_key,created_at)"
                    " VALUES('op9','u1','其他事件','2026-10-31 10:00:00')"
                )
            payload = self.app(game).summary("u1")
            self.assertEqual(payload["month_days"], 3)
            self.assertEqual(payload["last_at"], "2026-10-30 09:12:00")

    # ------------------------------------------------------------ 隔离
    def test_other_player_does_not_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            payload = self.app(game).summary("u2")
            self.assertEqual(payload["total"], 1)
            self.assertEqual(payload["month_days"], 1)
            self.assertEqual(payload["last_at"], "2026-10-30 10:00:00")
            text = format_sign_in_statistics(self.app(game).summary("u1"), format_number=fmt)
            self.assertNotIn("10:00", text)

    # ------------------------------------------------------------ 渲染
    def test_render_with_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            text = format_sign_in_statistics(
                self.app(game).summary("u1"), format_number=fmt
            )
            self.assertIn("【我的签到统计】", text)
            self.assertIn("累计签到：5 次", text)
            self.assertIn("本月签到：3 天", text)
            self.assertIn("最近签到：10-30 09:12", text)

    def test_render_never_signed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            text = format_sign_in_statistics(
                self.app(game).summary("u3"), format_number=fmt
            )
            self.assertIn("还没有签到记录", text)
            self.assertIn("修仙签到", text)

    # ------------------------------------------------------------ 降级
    def test_both_tables_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp, with_events=False, with_projection=False)
            payload = self.app(game).summary("u1")
            self.assertFalse(payload["ok"])
            self.assertIn("暂无签到统计数据", format_sign_in_statistics(payload, format_number=fmt))
            with sqlite3.connect(game) as conn:
                tables = {r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("sign_in_statistics_events", tables)
            self.assertNotIn("sign_in_statistics_projection", tables)

    def test_only_projection_present(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp, with_events=False)
            payload = self.app(game).summary("u1")
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["total"], 5)
            self.assertIsNone(payload["month_days"])
            self.assertIsNone(payload["last_at"])
            text = format_sign_in_statistics(payload, format_number=fmt)
            self.assertIn("累计签到：5 次", text)
            self.assertNotIn("本月签到", text)
            self.assertNotIn("最近签到", text)

    def test_only_events_present_falls_back_to_event_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp, with_projection=False)
            payload = self.app(game).summary("u1")
            self.assertTrue(payload["ok"])
            # 没有投影行：累计次数按事件行数兜底（u1 共 5 条事件）
            self.assertEqual(payload["total"], 5)
            self.assertEqual(payload["month_days"], 3)

    def test_blank_user_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            payload = self.app(game).summary("   ")
            self.assertFalse(payload["ok"])
            self.assertIn("无法查询", format_sign_in_statistics(payload, format_number=fmt))

    def test_dirty_created_at_is_not_counted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute(
                    "INSERT INTO sign_in_statistics_events"
                    "(operation_id,user_id,event_key,created_at)"
                    " VALUES('op_bad','u1',?,'')",
                    (EVENT_KEY,),
                )
            payload = self.app(game).summary("u1")
            self.assertEqual(payload["month_days"], 3)
            self.assertEqual(payload["last_at"], "2026-10-30 09:12:00")

    def test_empty_tables_render_guidance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp, with_rows=False)
            payload = self.app(game).summary("u1")
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["total"], 0)
            self.assertIn("还没有签到记录", format_sign_in_statistics(payload, format_number=fmt))

    def test_read_only_creates_no_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            with sqlite3.connect(game) as conn:
                before = {r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            self.app(game).summary("u1")
            with sqlite3.connect(game) as conn:
                after = {r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertEqual(before, after)


class HandlerSourceContractTests(unittest.TestCase):
    def test_command_and_handler(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_base"
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
        self.assertEqual(matchers.get("我的签到统计"), "sign_in_statistics")

        handler = None
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    seg = ast.get_source_segment(source, dec) or ""
                    if "sign_in_statistics.handle" in seg:
                        handler = node
        self.assertIsNotNone(handler, "handler must be registered")
        body = ast.get_source_segment(source, handler) or ""
        self.assertIn('summary(str(user_info["user_id"]))', body)
        self.assertIn("format_sign_in_statistics(", body)
        self.assertNotIn("extract_plain_text", body)
        self.assertNotIn("args", body)


if __name__ == "__main__":
    unittest.main()
