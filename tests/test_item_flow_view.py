"""玩家「我的物品流水」视图（只读个人物品收支）。

物品增减已经全量记进共享 `economy_log.item_delta`：背包加减
（`xiuxian2_handle` 的 `item_add`/`item_cost`）、奖励统一发放（`reward_service`）、
宗门商店兑换、仙肆购买。元素形状统一为
`{"id","name","type","amount",...}`，加物品 `amount` 为正、扣物品为负。

本套件用**真实 15 列 `economy_log`**（`item_delta` 为 NOT NULL TEXT）与真实
`ItemFlowApplication`（注入固定时钟）钉住聚合口径、时间窗、玩家隔离、脏数据容错与降级。
"""

from __future__ import annotations

import ast
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.economy_ledger.item_flow import (
    ItemFlowApplication,
    format_item_flow,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

# 真实 economy_log 列集（15 列，item_delta 为 NOT NULL TEXT）。
ECONOMY_LOG_DDL = (
    "CREATE TABLE economy_log("
    "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
    "source TEXT NOT NULL,action TEXT NOT NULL,"
    "stone_delta INTEGER NOT NULL DEFAULT 0,exp_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_scale_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
    "item_delta TEXT NOT NULL,detail TEXT NOT NULL DEFAULT '',trace_id TEXT,"
    "created_at TEXT NOT NULL,event_id TEXT)"
)


class _Clock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


NOW = datetime(2026, 10, 30, 12, 0, 0)


def fmt(value) -> str:
    return f"{int(value):,}"


def items(*rows) -> str:
    return json.dumps(
        [{"id": i, "name": n, "type": t, "amount": a} for i, n, t, a in rows],
        ensure_ascii=False,
    )


class ItemFlowTests(unittest.TestCase):
    def build(self, temp: str) -> Path:
        game = Path(temp) / "game.db"
        with DatabaseUnitOfWork(str(game), immediate=True) as uow:
            uow.execute(ECONOMY_LOG_DDL)
            rows = [
                ("u1", "sect", "shop_exchange", items((1, "破境丹", "丹药", 2)), "2026-10-30 09:00:00"),
                ("u1", "xianshi", "xianshi_buy_item", items((2, "玄铁", "材料", 40)), "2026-10-29 15:00:00"),
                ("u1", "back", "item_cost", items((3, "疗伤丹", "丹药", -5)), "2026-10-29 10:00:00"),
                ("u1", "back", "item_add", items((1, "破境丹", "丹药", 10)), "2026-10-28 10:00:00"),
                ("u1", "reward", "grant_reward",
                 items((2, "玄铁", "材料", 60), (3, "疗伤丹", "丹药", -15)), "2026-10-27 08:00:00"),
                # 无物品变动：各种空形状都不计笔数
                ("u1", "sect", "donate", "", "2026-10-30 11:00:00"),
                ("u1", "sect", "donate", "[]", "2026-10-30 11:01:00"),
                ("u1", "sect", "donate", "{}", "2026-10-30 11:02:00"),
                ("u1", "sect", "donate", "null", "2026-10-30 11:03:00"),
                # 他人：绝不能泄漏
                ("u2", "sect", "shop_exchange", items((9, "他人之物", "丹药", 999)), "2026-10-30 10:00:00"),
                # 窗外（7 天以前）
                ("u1", "sect", "shop_exchange", items((1, "破境丹", "丹药", 7)), "2026-10-01 10:00:00"),
            ]
            for uid, src, act, payload, ts in rows:
                uow.execute(
                    "INSERT INTO economy_log(user_id,source,action,item_delta,created_at)"
                    " VALUES(?,?,?,?,?)",
                    (uid, src, act, payload, ts),
                )
        return game

    def app(self, temp: str) -> ItemFlowApplication:
        return ItemFlowApplication(str(self.build(temp)), clock=_Clock(NOW))

    # ------------------------------------------------------------ 聚合口径
    def test_totals_and_kinds(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("u1")
            self.assertTrue(payload["ok"])
            # 破境丹 2+10=12、玄铁 40+60=100
            self.assertEqual(payload["gained_total"], 112)
            self.assertEqual(payload["gained_kinds"], 2)
            # 疗伤丹 5+15=20
            self.assertEqual(payload["spent_total"], 20)
            self.assertEqual(payload["spent_kinds"], 1)
            # 只有 5 行真正带物品
            self.assertEqual(payload["records"], 5)

    def test_empty_shapes_do_not_count_as_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("u1")
            # 4 行空形状 + 他人 1 行 + 窗外 1 行都不计
            self.assertEqual(payload["records"], 5)

    def test_top_lists_sorted_by_amount(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("u1")
            self.assertEqual([e["name"] for e in payload["top_gained"]], ["玄铁", "破境丹"])
            self.assertEqual([e["amount"] for e in payload["top_gained"]], [100, 12])
            self.assertEqual([e["records"] for e in payload["top_gained"]], [2, 2])
            self.assertEqual([e["name"] for e in payload["top_spent"]], ["疗伤丹"])
            self.assertEqual([e["amount"] for e in payload["top_spent"]], [20])

    def test_recent_newest_first_and_capped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("u1")
            stamps = [row["created_at"] for row in payload["recent"]]
            self.assertEqual(stamps, sorted(stamps, reverse=True))
            self.assertLessEqual(len(payload["recent"]), 5)

    def test_window_excludes_older_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("u1")
            # 窗外那笔是 +7 破境丹；若被计入 gained 会是 119
            self.assertEqual(payload["gained_total"], 112)

    # ------------------------------------------------------------ 玩家隔离
    def test_other_player_never_leaks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_item_flow(self.app(temp).summary("u1"), format_number=fmt)
            self.assertNotIn("他人之物", text)
            self.assertNotIn("999", text)

    def test_second_player_sees_only_own(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("u2")
            self.assertEqual(payload["gained_total"], 999)
            self.assertEqual(payload["spent_total"], 0)
            self.assertEqual(payload["records"], 1)

    # ------------------------------------------------------------ 渲染合同
    def test_full_render(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_item_flow(self.app(temp).summary("u1"), format_number=fmt)
            self.assertIn("【我的物品流水】近7天", text)
            self.assertIn("获得：112 件（2 种）", text)
            self.assertIn("消耗：20 件（1 种）", text)
            self.assertIn("变动记录：5 笔", text)
            self.assertIn("主要获得：", text)
            self.assertIn("· 玄铁 ×100（2笔）", text)
            self.assertIn("· 破境丹 ×12（2笔）", text)
            self.assertIn("主要消耗：", text)
            self.assertIn("· 疗伤丹 ×20（2笔）", text)
            self.assertIn("最近 5 笔：", text)
            self.assertIn("宗门商店兑换 获得 破境丹 ×2", text)
            self.assertIn("仙肆购买 获得 玄铁 ×40", text)
            self.assertIn("物品消耗 失去 疗伤丹 ×5", text)

    def test_mixed_row_marks_both_directions(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_item_flow(self.app(temp).summary("u1"), format_number=fmt)
            self.assertIn("获得 玄铁 ×60、失去 疗伤丹 ×15", text)

    def test_unknown_action_falls_back(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute(
                    "INSERT INTO economy_log(user_id,source,action,item_delta,created_at)"
                    " VALUES('u1','odd','mystery',?,'2026-10-30 11:30:00')",
                    (items((5, "奇物", "其他", 1)),),
                )
            text = format_item_flow(
                ItemFlowApplication(str(game), clock=_Clock(NOW)).summary("u1"),
                format_number=fmt,
            )
            self.assertIn("odd·mystery", text)

    # ------------------------------------------------------------ 脏数据容错
    def test_dirty_payloads_are_skipped_not_raised(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                for raw, ts in (
                    ("{not json", "2026-10-30 11:30:00"),
                    ('"just a string"', "2026-10-30 11:31:00"),
                    ("[123, {\"id\":5,\"amount\":3}]", "2026-10-30 11:32:00"),
                    ("[{\"name\":\"坏数量\",\"amount\":\"abc\"}]", "2026-10-30 11:33:00"),
                ):
                    uow.execute(
                        "INSERT INTO economy_log(user_id,source,action,item_delta,created_at)"
                        " VALUES('u1','x','bad',?,?)",
                        (raw, ts),
                    )
            payload = ItemFlowApplication(str(game), clock=_Clock(NOW)).summary("u1")
            self.assertTrue(payload["ok"])
            # 原 5 笔 + 只有第 3 条里的 dict 元素有效
            self.assertEqual(payload["records"], 6)
            self.assertEqual(payload["gained_total"], 115)  # 112 + 3
            names = [e["name"] for e in payload["top_gained"]]
            self.assertIn("物品#5", names)   # 缺 name 回退
            self.assertNotIn("坏数量", names)  # amount 非法被跳过

    def test_dict_wrapper_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute(ECONOMY_LOG_DDL)
                uow.execute(
                    "INSERT INTO economy_log(user_id,source,action,item_delta,created_at)"
                    " VALUES('u1','back','item_add',?,'2026-10-30 09:00:00')",
                    (json.dumps({"id": 1, "name": "单件", "type": "丹药", "amount": 4},
                                ensure_ascii=False),),
                )
            payload = ItemFlowApplication(str(game), clock=_Clock(NOW)).summary("u1")
            self.assertEqual(payload["gained_total"], 4)
            self.assertEqual(payload["top_gained"][0]["name"], "单件")

    def test_missing_type_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute(ECONOMY_LOG_DDL)
                uow.execute(
                    "INSERT INTO economy_log(user_id,source,action,item_delta,created_at)"
                    " VALUES('u1','back','item_add','[{\"name\":\"无类\",\"amount\":2}]',"
                    "'2026-10-30 09:00:00')"
                )
            payload = ItemFlowApplication(str(game), clock=_Clock(NOW)).summary("u1")
            self.assertEqual(payload["top_gained"][0]["type"], "其他")

    # ------------------------------------------------------------ 降级
    def test_no_records_message(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute("DELETE FROM economy_log WHERE user_id='u1'")
            text = format_item_flow(
                ItemFlowApplication(str(game), clock=_Clock(NOW)).summary("u1"),
                format_number=fmt,
            )
            self.assertIn("近7天没有物品收支记录。", text)

    def test_blank_user_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).summary("  ")
            self.assertFalse(payload["ok"])
            self.assertIn("无法查询", format_item_flow(payload, format_number=fmt))

    def test_missing_table_degrades(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(game), immediate=True):
                pass
            payload = ItemFlowApplication(str(game), clock=_Clock(NOW)).summary("u1")
            self.assertFalse(payload["ok"])
            self.assertIn("暂无物品流水记录", format_item_flow(payload, format_number=fmt))

    def test_missing_item_delta_column_degrades(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute(
                    "CREATE TABLE economy_log(id INTEGER PRIMARY KEY,user_id TEXT,"
                    "source TEXT,action TEXT,created_at TEXT)"
                )
            payload = ItemFlowApplication(str(game), clock=_Clock(NOW)).summary("u1")
            self.assertFalse(payload["ok"])
            self.assertIn("暂无物品流水记录", format_item_flow(payload, format_number=fmt))
            with sqlite3.connect(game) as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(economy_log)")}
            self.assertNotIn("item_delta", cols)  # 只读路径不补列

    def test_read_only_creates_no_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = self.build(temp)
            before = self._tables(game)
            ItemFlowApplication(str(game), clock=_Clock(NOW)).summary("u1")
            self.assertEqual(self._tables(game), before)

    @staticmethod
    def _tables(path: Path) -> set[str]:
        with sqlite3.connect(path) as conn:
            return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


class HandlerSourceContractTests(unittest.TestCase):
    def test_command_and_handler(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_back"
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
        self.assertEqual(matchers.get("我的物品流水"), "item_flow")

        handler = None
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    seg = ast.get_source_segment(source, dec) or ""
                    if "item_flow.handle" in seg:
                        handler = node
        self.assertIsNotNone(handler, "handler must be registered")
        body = ast.get_source_segment(source, handler) or ""
        self.assertIn('summary(str(user_info["user_id"]))', body)
        self.assertIn("format_item_flow(", body)
        self.assertNotIn("extract_plain_text", body)
        self.assertNotIn("args", body)


if __name__ == "__main__":
    unittest.main()
