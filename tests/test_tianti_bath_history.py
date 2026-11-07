"""我的药浴记录：只读个人药浴回执视图（P4）。"""

from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from nonebot_plugin_xiuxian_2.features.tianti_training.bath_history import (
    EMPTY_TEXT,
    MISSING_TEXT,
    TiantiBathHistoryApplication,
    format_tianti_bath_history,
)

TABLE_DDL = (
    "CREATE TABLE tianti_medicine_bath_operations ("
    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_json TEXT NOT NULL, "
    "result_json TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
)


class _Clock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


class BathHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "game.db"
        self.now = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)
        with sqlite3.connect(self.db) as conn:
            conn.execute(TABLE_DDL)
            conn.commit()
        self.app = TiantiBathHistoryApplication(self.db, clock=_Clock(self.now))

    # ---------------------------------------------------------------- helpers
    def _insert(self, operation_id, user_id, consumed, effect, created_at=None, end_time=None, days_ago_hours=0):
        payload = {"plan": consumed, "effect": effect}
        plan_text = "、".join(f"{c['name']}x{c['amount']}" for c in consumed)
        result = {
            "consumed": consumed,
            "effect": effect,
            "bath_name": "测试药浴（" + plan_text + "）",
            "end_time": end_time or (self.now + timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S"),
        }
        created = created_at or (self.now - timedelta(hours=days_ago_hours)).strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO tianti_medicine_bath_operations(operation_id,user_id,request_json,result_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (operation_id, user_id, json.dumps(payload, ensure_ascii=False),
                 json.dumps(result, ensure_ascii=False), created),
            )
            conn.commit()

    # ---------------------------------------------------------------- summary
    def test_summary_totals_and_best_effect(self):
        self._insert("op-1", "u1", [{"name": "恒心草", "amount": 20}, {"name": "天青花", "amount": 10}], 1.5, days_ago_hours=1)
        self._insert("op-2", "u1", [{"name": "恒心草", "amount": 5}], 1.9, days_ago_hours=3)
        summary = self.app.summary("u1")
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["total"], 2)
        self.assertEqual(summary["total_units"], 35)
        self.assertEqual(summary["total_kinds"], 2)
        self.assertAlmostEqual(summary["best_effect"], 1.9)

    def test_recent_is_newest_first(self):
        self._insert("old", "u1", [{"name": "恒心草", "amount": 1}], 1.2,
                     created_at="2026-11-08 05:00:00")
        self._insert("new", "u1", [{"name": "恒心草", "amount": 1}], 1.2,
                     created_at="2026-11-08 09:00:00")
        recent = self.app.summary("u1")["recent"]
        self.assertEqual([item["created_at"] for item in recent], ["2026-11-08 09:00:00", "2026-11-08 05:00:00"])

    def test_window_uses_lower_bound_only_within_seven_days(self):
        self._insert("old", "u1", [{"name": "恒心草", "amount": 1}], 1.2,
                     created_at=(self.now - timedelta(days=8)).strftime("%Y-%m-%d %H:%M:%S"))
        self._insert("fresh", "u1", [{"name": "恒心草", "amount": 1}], 1.2,
                     created_at=(self.now - timedelta(days=6)).strftime("%Y-%m-%d %H:%M:%S"))
        summary = self.app.summary("u1")
        self.assertEqual(summary["total"], 1)

    def test_future_timestamp_row_is_not_dropped(self):
        # 本地时间写入方可能写出「未来」行；只用下界，不得被上界误删。
        self._insert("future", "u1", [{"name": "恒心草", "amount": 2}], 1.4,
                     created_at=(self.now + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"))
        self.assertEqual(self.app.summary("u1")["total"], 1)

    def test_player_isolation(self):
        self._insert("mine", "u1", [{"name": "恒心草", "amount": 3}], 1.3, days_ago_hours=1)
        self._insert("theirs", "u2", [{"name": "天青花", "amount": 99}], 2.0, days_ago_hours=1)
        summary = self.app.summary("u1")
        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["total_units"], 3)
        self.assertNotIn("u2", json.dumps(summary, ensure_ascii=False))

    # ---------------------------------------------------------------- degrade
    def test_missing_table_returns_empty_without_creating_it(self):
        with TemporaryDirectory() as other:
            empty = Path(other) / "empty.db"
            sqlite3.connect(empty).close()
            app = TiantiBathHistoryApplication(empty, clock=_Clock(self.now))
            summary = app.summary("u1")
            self.assertFalse(summary["ok"])
            self.assertEqual(summary["notice"], EMPTY_TEXT)
            with sqlite3.connect(empty) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("tianti_medicine_bath_operations", tables)

    def test_missing_database_file_does_not_create_one(self):
        with TemporaryDirectory() as other:
            absent = Path(other) / "nope.db"
            app = TiantiBathHistoryApplication(absent, clock=_Clock(self.now))
            self.assertFalse(app.summary("u1")["ok"])
            self.assertFalse(absent.exists())

    def test_missing_column_degrades(self):
        with TemporaryDirectory() as other:
            partial = Path(other) / "partial.db"
            with sqlite3.connect(partial) as conn:
                conn.execute("CREATE TABLE tianti_medicine_bath_operations (operation_id TEXT PRIMARY KEY)")
                conn.commit()
            app = TiantiBathHistoryApplication(partial, clock=_Clock(self.now))
            self.assertFalse(app.summary("u1")["ok"])

    def test_bad_json_and_missing_consumed_are_skipped(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO tianti_medicine_bath_operations(operation_id,user_id,request_json,result_json,created_at) "
                "VALUES(?,?,?,?,?)",
                ("bad", "u1", "{not json", "{also bad", "2026-11-08 10:00:00"),
            )
            conn.execute(
                "INSERT INTO tianti_medicine_bath_operations(operation_id,user_id,request_json,result_json,created_at) "
                "VALUES(?,?,?,?,?)",
                ("noconsume", "u1", "{}", json.dumps({"effect": 1.5}), "2026-11-08 10:00:00"),
            )
            conn.commit()
        self._insert("good", "u1", [{"name": "恒心草", "amount": 2}], 1.5, created_at="2026-11-08 11:00:00")
        summary = self.app.summary("u1")
        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["recent"][0]["consumed"], [{"name": "恒心草", "amount": 2}])

    def test_plan_fallback_from_request_json(self):
        # result_json 缺 consumed 时回退 request_json.plan。
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO tianti_medicine_bath_operations(operation_id,user_id,request_json,result_json,created_at) "
                "VALUES(?,?,?,?,?)",
                ("fallback", "u1",
                 json.dumps({"plan": [{"item_id": 1, "name": "夜交藤", "amount": 7}], "effect": 1.6}),
                 json.dumps({"effect": 1.6}), "2026-11-08 11:00:00"),
            )
            conn.commit()
        recent = self.app.summary("u1")["recent"]
        self.assertEqual(recent[0]["consumed"], [{"name": "夜交藤", "amount": 7}])
        self.assertAlmostEqual(recent[0]["effect"], 1.6)

    def test_empty_user_id_is_refused(self):
        summary = self.app.summary("")
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["notice"], MISSING_TEXT)

    # ---------------------------------------------------------------- render
    def test_render_full_and_empty(self):
        self._insert("op-1", "u1", [{"name": "恒心草", "amount": 20}], 1.5, days_ago_hours=1)
        text = format_tianti_bath_history(self.app.summary("u1"))
        self.assertIn("近7天药浴记录", text)
        self.assertIn("开启次数：1 次", text)
        self.assertIn("累计消耗药材：20 份（1 种）", text)
        self.assertIn("150%", text)
        self.assertIn("最近：", text)
        self.assertEqual(format_tianti_bath_history({"ok": False, "notice": None}), EMPTY_TEXT)

    def test_render_does_not_mutate_input(self):
        self._insert("op-1", "u1", [{"name": "恒心草", "amount": 1}], 1.2, days_ago_hours=1)
        summary = self.app.summary("u1")
        snapshot = json.dumps(summary, ensure_ascii=False, sort_keys=True)
        format_tianti_bath_history(summary)
        self.assertEqual(json.dumps(summary, ensure_ascii=False, sort_keys=True), snapshot)

    # ---------------------------------------------------------------- wiring
    def test_handler_and_help_contract(self):
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_tianti" / "__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn('on_command(\n    "我的药浴记录", aliases={"药浴记录"}', source)
        self.assertIn("format_tianti_bath_history(summary)", source)
        self.assertIn(
            'tianti_bath_history_application.summary(str(user_info["user_id"]))',
            source,
        )
        self.assertNotIn("args.extract_plain_text", source.split("tianti_bath_history.handle")[1])
        # 帮助块登记（发现性合同由 test_help_discoverability 覆盖）
        self.assertIn("- 我的药浴记录", source)


class BathLifecycleHelperTests(unittest.TestCase):
    """P1–P3 纯渲染 helper 的形态断言。"""

    def test_render_bath_status_three_states(self):
        from nonebot_plugin_xiuxian_2.features.tianti_training.presentation import (
            BathStatus,
            render_bath_remaining_suffix,
            render_bath_status,
        )

        active = BathStatus("active", name="卯午生机药浴", effect=1.5,
                            end_time=datetime(2026, 11, 8, 15, 0), remaining_minutes=180)
        text = render_bath_status(active, percent=lambda value: f"{value * 100:.2f}".rstrip("0").rstrip("."))
        self.assertIn("卯午生机药浴", text)
        self.assertIn("150%", text)
        self.assertIn("剩余 180 分钟", text)
        self.assertIn("剩余 180 分钟", render_bath_remaining_suffix(active))

        expired = BathStatus("expired", name="子卯阴息药浴", effect=1.8,
                             end_time=datetime(2026, 11, 8, 5, 0))
        expired_text = render_bath_status(expired)
        self.assertIn("上次药浴已结束", expired_text)
        self.assertIn("子卯阴息药浴", expired_text)
        self.assertEqual(render_bath_remaining_suffix(expired), "")

        self.assertEqual(render_bath_status(BathStatus("none")), "\n药浴：无")

    def test_render_bath_schedule_marks_current_only(self):
        from nonebot_plugin_xiuxian_2.features.tianti_training.presentation import (
            bath_schedule,
            render_bath_schedule,
        )

        config = (
            {"name": "子卯阴息", "start": 23, "end": 5, "herbs": {"夜交藤"}},
            {"name": "卯午生机", "start": 5, "end": 11, "herbs": {"恒心草"}},
        )
        lines = render_bath_schedule(bath_schedule(config, datetime(2026, 11, 8, 9, 30)))
        self.assertTrue(lines[0].startswith("· 子卯阴息"))
        self.assertTrue(lines[1].startswith("◇ 卯午生机"))
        self.assertIn("本时段剩余 1 小时 30 分钟", lines[1])


if __name__ == "__main__":
    unittest.main()
