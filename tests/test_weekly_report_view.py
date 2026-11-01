"""修行周报：跨来源 7 天汇总的边界与降级行为。

视图只读 `economy_log`，把修为/灵石/宗门贡献/物品收支与活跃度一次汇总。
本套件钉住容易退化的口径：

1. 正负拆分与净额、笔数；
2. 只统计本人；
3. 时间窗**只用下界**——`created_at` 晚于当前时间的行仍计入（不得被上界误删）；
4. 窗外（早于 7 天）不计入；
5. `item_delta` 脏数据逐行跳过，不让整个视图失败；
6. 缺表降级为明确文案，不建表；
7. 旧库缺列时该段为 0，其余段仍输出；
8. 零记录给出明确文案；
9. 活跃天数去重、最忙一天、玩法分布前 3 排序；
10. 命令注册与帮助可发现（源合同）。
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.economy_ledger.weekly_report import (
    WeeklyReportApplication,
    format_weekly_report,
)

NOW = datetime(2026, 11, 1, 12, 0, 0)
ME = "10001"
OTHER = "20002"


def ts(**kwargs) -> str:
    return (NOW - timedelta(**kwargs)).strftime("%Y-%m-%d %H:%M:%S")


def create_log(db_path: Path, *, columns: tuple[str, ...] | None = None) -> None:
    """建最小 economy_log；columns 用于模拟旧库缺列。"""
    cols = columns or (
        "id",
        "user_id",
        "source",
        "action",
        "stone_delta",
        "exp_delta",
        "sect_contribution_delta",
        "item_delta",
        "created_at",
    )
    defs = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "user_id": "TEXT",
        "source": "TEXT",
        "action": "TEXT",
        "stone_delta": "INTEGER NOT NULL DEFAULT 0",
        "exp_delta": "INTEGER NOT NULL DEFAULT 0",
        "sect_contribution_delta": "INTEGER NOT NULL DEFAULT 0",
        "item_delta": "TEXT NOT NULL DEFAULT '[]'",
        "created_at": "TEXT NOT NULL",
    }
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE economy_log(" + ",".join(f'"{c}" {defs[c]}' for c in cols) + ")"
        )
        conn.commit()


def seed(db_path: Path, rows: list[dict]) -> None:
    with sqlite3.connect(db_path) as conn:
        for row in rows:
            keys = ",".join(f'"{k}"' for k in row)
            marks = ",".join("?" for _ in row)
            conn.execute(
                f"INSERT INTO economy_log({keys}) VALUES({marks})", list(row.values())
            )
        conn.commit()


class WeeklyReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "game.db"
        self.app = WeeklyReportApplication(str(self.db), now_fn=lambda: NOW)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def report(self, user_id: str = ME) -> dict:
        return self.app.weekly_report(user_id)

    # -------------------------------------------------- 1. 收支拆分
    def test_income_expense_and_net(self) -> None:
        create_log(self.db)
        seed(self.db, [
            {"user_id": ME, "source": "s", "action": "sign_in",
             "exp_delta": 1000, "stone_delta": 200, "sect_contribution_delta": 30,
             "item_delta": '[{"id":1,"name":"破境丹","type":"丹药","amount":2}]',
             "created_at": ts(days=1)},
            {"user_id": ME, "source": "s", "action": "shop_exchange",
             "exp_delta": -400, "stone_delta": -50, "sect_contribution_delta": -10,
             "item_delta": '[{"id":1,"name":"破境丹","type":"丹药","amount":-1}]',
             "created_at": ts(days=2)},
        ])
        p = self.report()
        self.assertEqual(p["records"], 2)
        self.assertEqual((p["exp"]["gained"], p["exp"]["spent"], p["exp"]["net"]), (1000, 400, 600))
        self.assertEqual((p["stone"]["gained"], p["stone"]["spent"], p["stone"]["net"]), (200, 50, 150))
        self.assertEqual(
            (p["contribution"]["gained"], p["contribution"]["spent"], p["contribution"]["net"]),
            (30, 10, 20),
        )
        self.assertEqual(p["exp"]["records"], 2)
        self.assertEqual(p["items"]["gained_total"], 2)
        self.assertEqual(p["items"]["spent_total"], 1)
        self.assertEqual(p["items"]["top_gained"][0]["name"], "破境丹")

    # -------------------------------------------------- 2. 只统计本人
    def test_only_counts_requested_user(self) -> None:
        create_log(self.db)
        seed(self.db, [
            {"user_id": ME, "source": "s", "action": "sign_in", "exp_delta": 100,
             "item_delta": "[]", "created_at": ts(days=1)},
            {"user_id": OTHER, "source": "s", "action": "sign_in", "exp_delta": 999,
             "item_delta": "[]", "created_at": ts(days=1)},
        ])
        p = self.report()
        self.assertEqual(p["records"], 1)
        self.assertEqual(p["exp"]["gained"], 100)

    # -------------------------------------------------- 3. 只用下界
    def test_future_rows_are_not_dropped(self) -> None:
        """created_at 混用 UTC/本地时区，晚于当前时间的行必须仍计入。"""
        create_log(self.db)
        seed(self.db, [
            {"user_id": ME, "source": "s", "action": "closing", "exp_delta": 500,
             "item_delta": "[]", "created_at": (NOW + timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")},
        ])
        p = self.report()
        self.assertEqual(p["records"], 1, "未来时间的行被上界误删了")
        self.assertEqual(p["exp"]["gained"], 500)

    # -------------------------------------------------- 4. 窗外
    def test_rows_outside_window_are_excluded(self) -> None:
        create_log(self.db)
        seed(self.db, [
            {"user_id": ME, "source": "s", "action": "closing", "exp_delta": 500,
             "item_delta": "[]", "created_at": ts(days=8)},
        ])
        p = self.report()
        self.assertEqual(p["records"], 0)
        self.assertEqual(p["exp"]["gained"], 0)
        self.assertTrue(p["notice"])

    # -------------------------------------------------- 5. 脏 item_delta
    def test_dirty_item_delta_degrades_per_row(self) -> None:
        create_log(self.db)
        bad = ["not-json", "null", '"str"', "[1,2]", '[{"amount":"x"}]', '[{"amount":0}]']
        rows = [
            {"user_id": ME, "source": "s", "action": "item_add", "exp_delta": 0,
             "item_delta": raw, "created_at": ts(days=1)}
            for raw in bad
        ]
        rows.append({"user_id": ME, "source": "s", "action": "item_add", "exp_delta": 0,
                     "item_delta": '[{"id":7,"name":"玄铁","amount":3}]', "created_at": ts(days=1)})
        seed(self.db, rows)
        p = self.report()
        self.assertEqual(p["records"], 7)
        self.assertEqual(p["items"]["gained_total"], 3)
        self.assertEqual(p["items"]["top_gained"], [{"name": "玄铁", "type": "", "amount": 3}])

    # -------------------------------------------------- 6. 缺表降级
    def test_missing_table_degrades_without_creating_it(self) -> None:
        self.db.parent.mkdir(parents=True, exist_ok=True)
        sqlite3.connect(self.db).close()  # 空库，无 economy_log
        p = self.report()
        self.assertEqual(p["records"], 0)
        self.assertIn("账本", p["notice"])
        with sqlite3.connect(self.db) as conn:
            names = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        self.assertNotIn("economy_log", names, "只读视图不得建表")

    # -------------------------------------------------- 7. 缺列降级
    def test_missing_columns_degrade_per_section(self) -> None:
        create_log(self.db, columns=("id", "user_id", "action", "stone_delta", "created_at"))
        seed(self.db, [{"user_id": ME, "action": "xianshi_buy_item", "stone_delta": -30,
                        "created_at": ts(days=1)}])
        p = self.report()
        self.assertEqual(p["records"], 1)
        self.assertEqual(p["exp"], {"gained": 0, "spent": 0, "net": 0, "records": 0})
        self.assertEqual(p["stone"]["spent"], 30)
        self.assertEqual(p["contribution"]["gained"], 0)
        self.assertEqual(p["items"]["gained_total"], 0)
        text = format_weekly_report(p, format_number=lambda v: f"{int(v):,}")
        self.assertIn("灵石", text)
        self.assertNotIn("修为：", text)

    # -------------------------------------------------- 8. 零记录
    def test_empty_report_has_explicit_notice(self) -> None:
        create_log(self.db)
        p = self.report()
        self.assertEqual(p["records"], 0)
        self.assertEqual(p["notice"], "本周暂无修行记录。")
        self.assertEqual(format_weekly_report(p), "本周暂无修行记录。")

    # -------------------------------------------------- 9. 活跃度与玩法分布
    def test_activity_days_and_top_actions(self) -> None:
        create_log(self.db)
        rows = []
        # 同一天两笔（去重后 1 天）、另一天 1 笔
        for action, created in (
            ("sign_in", ts(days=1)),
            ("sign_in", ts(days=1, minutes=30)),
            ("closing", ts(days=2)),
        ):
            rows.append({"user_id": ME, "source": "s", "action": action,
                         "exp_delta": 10, "item_delta": "[]", "created_at": created})
        rows.append({"user_id": ME, "source": "s", "action": "weird_action",
                     "exp_delta": 10, "item_delta": "[]", "created_at": ts(days=3)})
        seed(self.db, rows)
        p = self.report()
        self.assertEqual(p["activity"]["active_days"], 3)
        self.assertEqual(p["activity"]["busiest_day"], ts(days=1)[:10])
        self.assertEqual(p["activity"]["busiest_records"], 2)
        labels = [row["label"] for row in p["top_actions"]]
        self.assertEqual(labels[0], "修仙签到")
        self.assertIn("s·weird_action", labels)

    # -------------------------------------------------- 10. 命令与帮助
    def test_command_and_help_are_registered(self) -> None:
        base = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_base" / "__init__.py"
        src = base.read_text(encoding="utf-8")
        self.assertIn('on_command("修行周报"', src)
        self.assertIn('aliases={"我的周报", "本周修行"}', src)
        self.assertIn("format_weekly_report(payload", src)
        self.assertIn("- 修行周报", src)
        self.assertIn("近7天修为、灵石、贡献、物品收支与活跃天数", src)


if __name__ == "__main__":
    unittest.main()
