"""宠物背包与饰品背包的关键词筛选（补齐同类缺陷）。

这两处是包内最后仍在用「`int(args)` 失败即第 1 页」的分页入口：
- `xiuxian/xiuxian_pet/__init__.py`（宠物背包）
- `xiuxian/xiuxian_back/accessory.py`（饰品背包）

本套件钉住：页码/关键词解析、宠物按名称类字段过滤、饰品按段过滤、
无命中提示与空列表提示可区分，以及两条 handler 的源合同与全包防回归。
"""

from __future__ import annotations

import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.base.keyword_page import (
    empty_notice,
    filter_rows,
    matches_keyword,
    parse_page_arg,
)
from nonebot_plugin_xiuxian_2.features.back.backpack_search import filter_sections

ROOT = Path(__file__).resolve().parents[1]
PET_SRC = (ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_pet" / "__init__.py").read_text(encoding="utf-8")
ACC_SRC = (ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_back" / "accessory.py").read_text(encoding="utf-8")

PET_ROWS = [
    {"form_name": "赤焰龙", "name": "龙", "rarity": "传说", "type": "攻击", "uid": "p1"},
    {"form_name": "灵狐", "name": "狐", "rarity": "稀有", "type": "辅助", "uid": "p2"},
    {"form_name": "玄龟", "name": "龟", "rarity": "常见", "type": "防御", "uid": "p3"},
]

ACC_SECTIONS = [
    ("烈阳", [{"name": "烈阳戒", "goods_type": "饰品"}, {"name": "玄渊链", "goods_type": "饰品"}]),
    ("其他", [{"name": "散件佩", "goods_type": "饰品"}]),
]


def filter_pets(keyword: str) -> list[dict]:
    return filter_rows(
        PET_ROWS,
        keyword,
        fields=("form_name", "name", "rarity", "type", "uid"),
    )


class ParseTests(unittest.TestCase):
    def test_page_or_keyword(self) -> None:
        self.assertEqual(parse_page_arg(""), {"keyword": "", "page": 1})
        self.assertEqual(parse_page_arg("2"), {"keyword": "", "page": 2})
        self.assertEqual(parse_page_arg("龙"), {"keyword": "龙", "page": 1})
        self.assertEqual(parse_page_arg("龙 2"), {"keyword": "龙", "page": 2})
        self.assertEqual(parse_page_arg("0"), {"keyword": "0", "page": 1})


class PetFilterTests(unittest.TestCase):
    def test_match_by_form_name(self) -> None:
        self.assertEqual([r["uid"] for r in filter_pets("赤焰")], ["p1"])

    def test_match_by_rarity_type_uid(self) -> None:
        self.assertEqual([r["uid"] for r in filter_pets("传说")], ["p1"])
        self.assertEqual([r["uid"] for r in filter_pets("防御")], ["p3"])
        self.assertEqual([r["uid"] for r in filter_pets("p2")], ["p2"])

    def test_empty_keyword_keeps_order(self) -> None:
        self.assertEqual([r["uid"] for r in filter_pets("")], ["p1", "p2", "p3"])

    def test_no_match(self) -> None:
        self.assertEqual(filter_pets("不存在的宠物"), [])


class AccessoryFilterTests(unittest.TestCase):
    def test_filter_by_name_and_drop_empty_sections(self) -> None:
        out = filter_sections(ACC_SECTIONS, "烈阳")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0][0], "烈阳")
        self.assertEqual([row["name"] for row in out[0][1]], ["烈阳戒"])

    def test_empty_keyword_keeps_all(self) -> None:
        self.assertEqual([t for t, _ in filter_sections(ACC_SECTIONS, "")], ["烈阳", "其他"])

    def test_no_match(self) -> None:
        self.assertEqual(filter_sections(ACC_SECTIONS, "不存在"), [])


class NoticeTests(unittest.TestCase):
    def test_notice_variants(self) -> None:
        self.assertEqual(empty_notice("", "宠物背包"), "道友的宠物背包空空如也！")
        self.assertIn("宠物背包里没有", empty_notice("龙", "宠物背包"))
        self.assertIn("饰品背包里没有", empty_notice("烈阳", "饰品背包"))


class SourceContractTests(unittest.TestCase):
    def test_pet_handler_uses_shared_parser_and_filter(self) -> None:
        self.assertIn('parse_page_arg(args.extract_plain_text() if args else "")', PET_SRC)
        self.assertIn("_filter_pet_rows(get_pet_bag_rows(data), keyword)", PET_SRC)
        self.assertIn('empty_notice(keyword, "宠物背包")', PET_SRC)
        self.assertIn("keyword=keyword", PET_SRC)
        self.assertNotIn("current_page = int(args.extract_plain_text()", PET_SRC)

    def test_accessory_handler_uses_shared_parser_and_filter(self) -> None:
        self.assertIn('parse_page_arg(args.extract_plain_text() if args else "")', ACC_SRC)
        self.assertIn("filter_sections(_build_accessory_sections_for_md(user_id), keyword)", ACC_SRC)
        self.assertIn('empty_notice(keyword, "饰品背包")', ACC_SRC)
        self.assertNotIn('current_page = int(args.extract_plain_text().strip() or "1")', ACC_SRC)

    # 宗门成员查看已纳入同一检索契约（关键词 + 翻页续查），全包不得再有静默回退。
    KNOWN_PENDING: set[str] = set()

    def test_no_silent_int_page_fallback_in_package(self) -> None:
        """全包防回归：不得再有 int(args.extract_plain_text()) 静默回退。"""
        offenders = []
        for path in (ROOT / "nonebot_plugin_xiuxian_2").rglob("*.py"):
            if "tests" in path.parts:
                continue
            rel = str(path.relative_to(ROOT))
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "current_page = int(args.extract_plain_text()" in text and rel not in self.KNOWN_PENDING:
                offenders.append(rel)
        self.assertEqual(offenders, [])
        # 已知待迁移项必须仍然只有那一处，避免它悄悄扩散
        pending = [
            str(path.relative_to(ROOT))
            for path in (ROOT / "nonebot_plugin_xiuxian_2").rglob("*.py")
            if "tests" not in path.parts
            and "current_page = int(args.extract_plain_text()" in path.read_text(encoding="utf-8", errors="ignore")
        ]
        self.assertEqual(set(pending), self.KNOWN_PENDING)


if __name__ == "__main__":
    unittest.main()
