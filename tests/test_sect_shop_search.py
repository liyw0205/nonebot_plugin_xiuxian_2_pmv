"""宗门商店的关键词筛选与通用分页参数解析。

宗门商店原本只做 `int(page_input)`，非数字一律静默回退第 1 页：玩家输入
`宗门商店 灵芝` 看到的是第一页商品，关键词被无声吞掉。本套件钉住：

1. 纯数字仍是页码；
2. 关键词按商品名/描述/编号匹配，命中后只显示命中的商品；
3. 关键词 + 页码组合；
4. 无命中提示与"暂无商品"可区分；
5. 未命中不影响编号兑换（兑换走 `宗门兑换 编号`，本切片未改）；
6. handler 源合同：接入通用解析与筛选。
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

SHOP = {
    "1001": {"name": "破境丹", "desc": "提升突破成功率", "cost": 500},
    "1002": {"name": "灵芝", "desc": "炼丹药材", "cost": 300},
    "1003": {"name": "玄铁", "desc": "锻造材料", "cost": 800},
}


def filter_shop(keyword: str) -> dict:
    if not keyword:
        return dict(SHOP)
    return {
        item_id: data
        for item_id, data in SHOP.items()
        if matches_keyword([data["name"], data["desc"], item_id], keyword)
    }


class ParsePageArgTests(unittest.TestCase):
    def test_page_and_keyword(self) -> None:
        self.assertEqual(parse_page_arg(""), {"keyword": "", "page": 1})
        self.assertEqual(parse_page_arg("2"), {"keyword": "", "page": 2})
        self.assertEqual(parse_page_arg("灵芝"), {"keyword": "灵芝", "page": 1})
        self.assertEqual(parse_page_arg("灵芝 2"), {"keyword": "灵芝", "page": 2})
        self.assertEqual(parse_page_arg("0"), {"keyword": "0", "page": 1})


class ShopFilterTests(unittest.TestCase):
    def test_filter_by_name(self) -> None:
        self.assertEqual(list(filter_shop("灵芝")), ["1002"])

    def test_filter_by_desc(self) -> None:
        self.assertEqual(list(filter_shop("锻造")), ["1003"])

    def test_filter_by_id(self) -> None:
        self.assertEqual(list(filter_shop("1001")), ["1001"])

    def test_case_insensitive(self) -> None:
        self.assertEqual(len(filter_shop("灵芝")), 1)

    def test_no_match(self) -> None:
        self.assertEqual(filter_shop("不存在的东西"), {})

    def test_empty_keyword_keeps_all(self) -> None:
        self.assertEqual(len(filter_shop("")), 3)

    def test_notice_distinguishes_empty_from_no_match(self) -> None:
        self.assertIn("空空如也", empty_notice("", "宗门商店"))
        text = empty_notice("灵芝", "宗门商店")
        self.assertIn("灵芝", text)
        self.assertNotIn("空空如也", text)

    def test_filter_rows_helper(self) -> None:
        rows = [{"name": "破境丹"}, {"name": "灵芝"}]
        self.assertEqual(
            [row["name"] for row in filter_rows(rows, "灵芝", fields=("name",))], ["灵芝"]
        )

    def test_handler_contract(self) -> None:
        src = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_sect"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("parse_page_arg(args.extract_plain_text()", src)
        self.assertIn("matches_keyword(", src)
        self.assertIn('empty_notice(keyword, "宗门商店")', src)
        self.assertIn("- 宗门商店 [页码|关键词]", src)
        # 防回归：不得再出现「int(page_input) 失败静默第 1 页」的写法
        self.assertNotIn("page = int(page_input) if page_input else 1", src)


if __name__ == "__main__":
    unittest.main()
