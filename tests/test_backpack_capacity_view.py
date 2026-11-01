"""背包容量体检：单件口径、警示边界与降级行为。

背包上限 `max_goods_num` 在写入前按**单件物品件数**判定（`当前件数 + 本次发放 > 上限`
即 `inventory_full`），见 tasks/dongfu/combat_settlement/pet 各 repository。
本套件钉住视图必须与同一口径一致——否则给出的"剩余空间"会误导玩家：

1. 占用按单件件数算，与 inventory_full 判定同源同值；
2. 剩余可容纳件数 = 上限 − 该件当前件数，不受其他物品影响；
3. 前 3 件按占用降序，并列按名称稳定；
4. 80% 警示边界（79% 不进、80% 进）；
5. 空背包明确文案且仍显示上限；
6. 缺 back 表 → notice，不建表；
7. 缺 goods_num 列 → notice；
8. 条目数与总件数正确（用于展示，但不冒充上限口径）；
9. 命令/帮助/manifest 合同。
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.back.capacity_view import (
    WARN_RATIO,
    BackpackCapacityApplication,
    format_backpack_capacity,
)

ME = "10001"
OTHER = "20002"
LIMIT = 100


def create_back(path: Path, columns=None) -> None:
    cols = columns or ("user_id", "goods_id", "goods_name", "goods_num")
    defs = {
        "user_id": "TEXT",
        "goods_id": "INTEGER",
        "goods_name": "TEXT",
        "goods_type": "TEXT",
        "goods_num": "INTEGER DEFAULT 0",
    }
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE back(" + ",".join(f'"{c}" {defs[c]}' for c in cols) + ")")
        conn.commit()


def seed(path: Path, rows: list[dict]) -> None:
    with sqlite3.connect(path) as conn:
        for row in rows:
            keys = ",".join(f'"{k}"' for k in row)
            marks = ",".join("?" for _ in row)
            conn.execute(f"INSERT INTO back({keys}) VALUES({marks})", list(row.values()))
        conn.commit()


class BackpackCapacityTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "game.db"
        self.app = BackpackCapacityApplication(str(self.db), LIMIT)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    # -------------------------------------------------- 1. 单件口径
    def test_usage_is_per_item_not_total(self) -> None:
        create_back(self.db)
        seed(self.db, [
            {"user_id": ME, "goods_id": 1, "goods_name": "破境丹", "goods_num": 90},
            {"user_id": ME, "goods_id": 2, "goods_name": "玄铁", "goods_num": 5},
        ])
        p = self.app.capacity(ME)
        self.assertTrue(p["ok"])
        top = p["top"][0]
        self.assertEqual(top["name"], "破境丹")
        self.assertEqual(top["count"], 90)
        # 关键：剩余只与该件当前件数有关，不被另一件 5 件影响
        self.assertEqual(top["remaining"], 10)
        self.assertEqual(p["total_items"], 95)

    def test_matches_inventory_full_rule(self) -> None:
        """与 tasks/dongfu 的判定同值：count + add > limit 即满。"""
        create_back(self.db)
        seed(self.db, [{"user_id": ME, "goods_id": 1, "goods_name": "破境丹", "goods_num": 95}])
        p = self.app.capacity(ME)
        remaining = p["top"][0]["remaining"]
        self.assertEqual(remaining, LIMIT - 95)
        # 恰好还能装 remaining 件，再多 1 件就会被判满
        self.assertFalse(95 + remaining > LIMIT)
        self.assertTrue(95 + remaining + 1 > LIMIT)

    # -------------------------------------------------- 2. 排序与并列
    def test_top_three_ordering_and_tie_break(self) -> None:
        create_back(self.db)
        seed(self.db, [
            {"user_id": ME, "goods_id": 1, "goods_name": "B", "goods_num": 50},
            {"user_id": ME, "goods_id": 2, "goods_name": "A", "goods_num": 50},
            {"user_id": ME, "goods_id": 3, "goods_name": "C", "goods_num": 70},
            {"user_id": ME, "goods_id": 4, "goods_name": "D", "goods_num": 10},
        ])
        names = [item["name"] for item in self.app.capacity(ME)["top"]]
        self.assertEqual(names, ["C", "A", "B"])

    # -------------------------------------------------- 3. 警示边界
    def test_warning_boundary(self) -> None:
        create_back(self.db)
        seed(self.db, [{"user_id": ME, "goods_id": 1, "goods_name": "刚好79", "goods_num": 79}])
        self.assertFalse(self.app.capacity(ME)["has_warning"])

        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE back SET goods_num=80")
            conn.commit()
        p = self.app.capacity(ME)
        self.assertTrue(p["has_warning"], f"阈值应为 {WARN_RATIO}")
        self.assertEqual(p["warnings"][0]["name"], "刚好79")
        self.assertIn("80", format_backpack_capacity(p))

    # -------------------------------------------------- 4. 空背包
    def test_empty_backpack(self) -> None:
        create_back(self.db)
        p = self.app.capacity(ME)
        self.assertTrue(p["ok"])
        self.assertEqual(p["entries"], 0)
        self.assertEqual(p["limit"], LIMIT)
        text = format_backpack_capacity(p)
        self.assertIn("背包空空如也", text)
        self.assertIn("100", text)

    def test_zero_and_negative_rows_are_ignored(self) -> None:
        create_back(self.db)
        seed(self.db, [
            {"user_id": ME, "goods_id": 1, "goods_name": "空", "goods_num": 0},
            {"user_id": ME, "goods_id": 2, "goods_name": "负", "goods_num": -3},
            {"user_id": ME, "goods_id": 3, "goods_name": "正常", "goods_num": 4},
        ])
        p = self.app.capacity(ME)
        self.assertEqual(p["entries"], 1)
        self.assertEqual(p["top"][0]["name"], "正常")
        self.assertEqual(p["top"][0]["count"], 4)

    # -------------------------------------------------- 5. 他人隔离
    def test_other_users_are_excluded(self) -> None:
        create_back(self.db)
        seed(self.db, [
            {"user_id": ME, "goods_id": 1, "goods_name": "我的", "goods_num": 10},
            {"user_id": OTHER, "goods_id": 2, "goods_name": "别人的", "goods_num": 99},
        ])
        p = self.app.capacity(ME)
        self.assertEqual(p["entries"], 1)
        self.assertEqual(p["top"][0]["name"], "我的")

    # -------------------------------------------------- 6. 缺表 / 缺列
    def test_missing_table_degrades_without_creating(self) -> None:
        self.db.parent.mkdir(parents=True, exist_ok=True)
        sqlite3.connect(self.db).close()
        p = self.app.capacity(ME)
        self.assertFalse(p["ok"])
        self.assertIn("背包表", p["notice"])
        with sqlite3.connect(self.db) as conn:
            names = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        self.assertNotIn("back", names, "只读视图不得建表")

    def test_missing_goods_num_column_degrades(self) -> None:
        create_back(self.db, columns=("user_id", "goods_id", "goods_name"))
        seed(self.db, [{"user_id": ME, "goods_id": 1, "goods_name": "破境丹"}])
        p = self.app.capacity(ME)
        self.assertFalse(p["ok"])
        self.assertIn("结构不完整", p["notice"])

    # -------------------------------------------------- 7. 命令与帮助
    def test_command_help_and_manifest_contract(self) -> None:
        root = Path(__file__).resolve().parents[1]
        back = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_back" / "__init__.py").read_text(encoding="utf-8")
        manifest = (root / "nonebot_plugin_xiuxian_2" / "features" / "back" / "manifest.py").read_text(encoding="utf-8")
        self.assertIn("on_command('背包容量'", back)
        self.assertIn("aliases={'我的背包容量', '背包空间'}", back)
        self.assertIn("format_backpack_capacity(payload", back)
        self.assertIn("- 背包容量", back)
        self.assertIn("单件物品上限占用与快满的条目", back)
        self.assertIn('CommandSpec("背包容量", aliases=("我的背包容量", "背包空间")', manifest)


if __name__ == "__main__":
    unittest.main()
