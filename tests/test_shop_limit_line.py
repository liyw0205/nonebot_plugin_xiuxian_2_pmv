"""P2 商店「每周限购」行统一（共享格式化 + 宗门商店补齐已购/剩余）。

四个商店此前各写一套限购文案：宗门商店只写总量，其余写「剩余/总量」，同一规则
四处复制。本套件钉住共享 `features/base/shop_limit.py` 的渲染口径、宗门商店从此
仓储读取本页商品的已购数量（读不到则不显示已购），以及五个视图都改调同一 helper。
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.base.shop_limit import (
    shop_limit_line,
    shop_limit_parts,
)
from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_shop
from nonebot_plugin_xiuxian_2.features.sect.repository import (
    SectRenameSqlRepository,
    weekly_shop_key,
)
from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE = REPO_ROOT / "nonebot_plugin_xiuxian_2"


def _read(relative: str) -> str:
    return (PACKAGE / relative).read_text(encoding="utf-8")


def _handler_body(source: str, decorator: str) -> str:
    """取 `@<decorator>.handle` 装饰器与其函数体，截止到下一个顶层装饰器。"""
    start = source.index(f"@{decorator}.handle")
    define = source.index("\nasync def ", start) + 1
    rest = source[define:]
    match = re.search(r"\n@", rest)
    return source[start:] if match is None else source[start: define + match.start()]


class ShopLimitLineTests(unittest.TestCase):
    def test_remaining_over_limit_shape(self):
        self.assertEqual(shop_limit_line(5, 2), "每周限购：3/5个")
        self.assertEqual(shop_limit_line(1, 0), "每周限购：1/1个")

    def test_sold_out_shape_is_explicit(self):
        self.assertEqual(shop_limit_line(3, 3), "每周限购：已售罄（3/3个）")
        self.assertEqual(shop_limit_line(2, 7), "每周限购：已售罄（2/2个）", "超额也不出现负数")

    def test_unlimited_config_renders_unlimited(self):
        for limit in (0, -1, None, "bad"):
            with self.subTest(limit=limit):
                self.assertEqual(shop_limit_line(limit, 4), "每周限购：不限购")

    def test_dirty_purchased_values_are_tolerated(self):
        self.assertEqual(shop_limit_line(5, "2"), "每周限购：3/5个")
        self.assertEqual(shop_limit_line(5, None), "每周限购：5/5个")
        self.assertEqual(shop_limit_line(5, -3), "每周限购：5/5个")

    def test_parts_matches_the_rendered_line(self):
        self.assertEqual(shop_limit_parts(5, 2), (3, 5))
        self.assertEqual(shop_limit_parts(0, 2), (0, 0))

    def test_prefix_defaults_to_the_shared_wording(self):
        self.assertTrue(shop_limit_line(3, 1).startswith("每周限购："))


class SectWeeklyPurchasedTests(unittest.TestCase):
    def setUp(self):
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.database = Path(self._tmp.name) / "sect.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_sect_shop(uow)

    def test_reads_only_this_week_rows_for_this_user(self):
        week = weekly_shop_key(__import__("datetime").datetime(2026, 7, 14, 20, 0))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany(
                "INSERT INTO sect_shop_weekly_purchases(user_id,week_key,item_id,quantity) "
                "VALUES(?,?,?,?)",
                (
                    ("viewer", week, 1001, 2),
                    ("viewer", week, 1002, 0),
                    ("viewer", "2020-W01", 1003, 9),
                    ("other", week, 1001, 5),
                ),
            )
        repository = SectRenameSqlRepository(self.database)

        purchased = repository.weekly_shop_progress("viewer", week_key=week)

        self.assertEqual(purchased, {"1001": 2, "1002": 0}, "跨周行与他人的行都不计入")

    def test_item_ids_filter_keeps_only_the_page(self):
        week = weekly_shop_key(__import__("datetime").datetime(2026, 7, 14, 20, 0))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany(
                "INSERT INTO sect_shop_weekly_purchases(user_id,week_key,item_id,quantity) "
                "VALUES(?,?,?,?)",
                (("viewer", week, 1001, 2), ("viewer", week, 1002, 1)),
            )

        purchased = SectRenameSqlRepository(self.database).weekly_shop_progress(
            "viewer", item_ids=[1001], week_key=week
        )

        self.assertEqual(purchased, {"1001": 2})

    def test_legacy_fallback_fills_missing_rows_like_the_purchase_path(self):
        purchased = SectRenameSqlRepository(self.database).weekly_shop_progress(
            "viewer", fallback={"1001": 2, "1002": "bad"}, week_key="2026-W29"
        )

        self.assertEqual(purchased, {"1001": 2}, "坏值跳过而不是让整行渲染失败")

    def test_missing_database_and_table_degrade_without_ddl(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.db"
            self.assertEqual(SectRenameSqlRepository(missing).weekly_shop_progress("u"), {})
            self.assertFalse(missing.exists())

            blank = Path(directory) / "blank.db"
            with DatabaseUnitOfWork(blank) as uow:
                uow.execute("CREATE TABLE unrelated(x INTEGER)")
            self.assertEqual(SectRenameSqlRepository(blank).weekly_shop_progress("u"), {})
            with DatabaseUnitOfWork(blank, read_only=True) as uow:
                tables = {
                    str(row["name"])
                    for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertEqual(tables, {"unrelated"})

    def test_application_delegates_without_writing(self):
        application = SectApplication(self.database)

        purchased = application.weekly_shop_progress("viewer", item_ids=[1001])

        self.assertEqual(purchased, {})
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertIsNone(
                uow.query_one(
                    "SELECT 1 AS present FROM sect_shop_purchase_operations"
                )
            )


class ShopLimitWiringTests(unittest.TestCase):
    VIEWS = (
        ("xiuxian/xiuxian_sect/__init__.py", "sect_shop", "宗门商店"),
        ("xiuxian/xiuxian_boss/__init__.py", "boss_integral_store", "世界积分商店"),
        ("xiuxian/xiuxian_tower/__init__.py", "tower_shop", "通天塔商店"),
        ("xiuxian/xiuxian_training/__init__.py", "training_shop", "历练商店"),
        ("xiuxian/xiuxian_arena/__init__.py", "arena_shop", "竞技场商店"),
    )

    def test_every_shop_renders_the_shared_limit_line(self):
        for relative, marker, title in self.VIEWS:
            with self.subTest(view=title):
                source = _read(relative)
                body = _handler_body(source, marker)
                self.assertIn("shop_limit_line(", body)
                self.assertIn("features.base.shop_limit import shop_limit_line", source)
                self.assertNotIn("每周限购：{", body, "不得再手写限购字面量")

    def test_sect_shop_supplies_the_purchased_count_from_the_repository(self):
        source = _read("xiuxian/xiuxian_sect/__init__.py")
        body = _handler_body(source, "sect_shop")

        self.assertIn("sect_application.weekly_shop_progress(", body)
        self.assertIn("get_sect_weekly_purchases_snapshot(user_id)", body)
        self.assertIn("weekly_shop_progress.get(str(item_id), 0)", body)

    def test_sect_shop_does_not_break_its_keyword_paging_contract(self):
        source = _read("xiuxian/xiuxian_sect/__init__.py")
        body = _handler_body(source, "sect_shop")

        self.assertIn('page_command("宗门商店", keyword, page + 1)', body)
        self.assertIn("empty_notice(keyword, \"宗门商店\")", body)

    def test_purchase_paths_are_untouched(self):
        for relative, marker in (
            ("xiuxian/xiuxian_sect/__init__.py", "sect_buy"),
            ("xiuxian/xiuxian_boss/__init__.py", "boss_integral_use"),
            ("xiuxian/xiuxian_tower/__init__.py", "tower_buy"),
            ("xiuxian/xiuxian_training/__init__.py", "training_buy"),
        ):
            with self.subTest(purchase=marker):
                body = _handler_body(_read(relative), marker)
                self.assertNotIn("shop_limit_line(", body)

    def test_purchase_repository_keeps_one_week_key_source(self):
        source = _read("features/sect/repository.py")

        self.assertIn("def weekly_shop_key(moment) -> str:", source)
        self.assertEqual(source.count("weekly_shop_key(self.clock.now())"), 2)


if __name__ == "__main__":
    unittest.main()
