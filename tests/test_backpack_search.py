"""我的背包参数解析与关键词筛选。

原本 `我的背包` 只做 `int(args)`，解析失败就静默当第 1 页：玩家输入
`我的背包 破境丹` 看到的是背包首页——参数被无声吞掉。本套件钉住：

1. 纯数字仍是页码（原有行为不回归）；
2. 中文关键词走筛选，不再被当成第 1 页；
3. `关键词 页码` 组合解析；
4. 页码非法（0 / 负数）不被当作页码；
5. 名称与类型都能命中，大小写不敏感；
6. 空关键词不过滤、保持原顺序；
7. 筛选后空段整段丢弃；
8. 无命中提示与"背包为空"文案可区分；
9. handler 源合同：解析、两条渲染路径都接入筛选。
"""

from __future__ import annotations

import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.back.backpack_search import (
    empty_notice,
    filter_lines,
    filter_sections,
    matches_keyword,
    parse_backpack_arg,
)

SECTIONS = [
    ("法器", [{"name": "青锋剑", "goods_type": "装备"}, {"name": "破境丹", "goods_type": "丹药"}]),
    ("功法", [{"name": "太玄经", "goods_type": "技能"}]),
]


class ParseBackpackArgTests(unittest.TestCase):
    def test_empty_is_first_page(self) -> None:
        self.assertEqual(parse_backpack_arg(""), {"keyword": "", "page": 1})
        self.assertEqual(parse_backpack_arg(None), {"keyword": "", "page": 1})
        self.assertEqual(parse_backpack_arg("   "), {"keyword": "", "page": 1})

    def test_digits_stay_page(self) -> None:
        self.assertEqual(parse_backpack_arg("3"), {"keyword": "", "page": 3})
        self.assertEqual(parse_backpack_arg(" 12 "), {"keyword": "", "page": 12})

    def test_keyword_is_not_page(self) -> None:
        self.assertEqual(parse_backpack_arg("破境丹"), {"keyword": "破境丹", "page": 1})

    def test_keyword_with_page(self) -> None:
        self.assertEqual(parse_backpack_arg("丹药 2"), {"keyword": "丹药", "page": 2})

    def test_invalid_page_token_is_keyword(self) -> None:
        self.assertEqual(parse_backpack_arg("0"), {"keyword": "0", "page": 1})
        self.assertEqual(parse_backpack_arg("-1"), {"keyword": "-1", "page": 1})
        self.assertEqual(parse_backpack_arg("丹药 0"), {"keyword": "丹药 0", "page": 1})


class FilterTests(unittest.TestCase):
    def test_matches_name_and_type(self) -> None:
        self.assertTrue(matches_keyword({"name": "破境丹", "goods_type": "丹药"}, "破境"))
        self.assertTrue(matches_keyword({"name": "破境丹", "goods_type": "丹药"}, "丹"))
        self.assertTrue(matches_keyword({"name": "破境丹", "goods_type": "丹药"}, "丹"))

    def test_case_insensitive(self) -> None:
        self.assertTrue(matches_keyword({"name": "MySword", "goods_type": "装备"}, "mysword"))
        self.assertTrue(matches_keyword({"name": "MySword", "goods_type": "装备"}, "MYSWORD"))

    def test_empty_keyword_keeps_everything_in_order(self) -> None:
        out = filter_sections(SECTIONS, "")
        self.assertEqual([title for title, _ in out], ["法器", "功法"])
        self.assertEqual(len(out[0][1]), 2)

    def test_filters_and_drops_empty_sections(self) -> None:
        out = filter_sections(SECTIONS, "破境")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0][0], "法器")
        self.assertEqual([row["name"] for row in out[0][1]], ["破境丹"])

    def test_no_match_returns_empty(self) -> None:
        self.assertEqual(filter_sections(SECTIONS, "不存在的物品"), [])

    def test_filter_lines(self) -> None:
        lines = ["破境丹 x2", "青锋剑 x1", "太玄经 x1"]
        self.assertEqual(filter_lines(lines, "青锋"), ["青锋剑 x1"])
        self.assertEqual(filter_lines(lines, ""), lines)


class NoticeTests(unittest.TestCase):
    def test_empty_backpack_notice(self) -> None:
        self.assertEqual(empty_notice(""), "道友的背包空空如也！")

    def test_no_match_notice_is_distinct(self) -> None:
        text = empty_notice("破境丹")
        self.assertIn("破境丹", text)
        self.assertNotIn("空空如也", text, "筛选无结果不能误报为背包为空")

    def test_handler_wires_both_paths(self) -> None:
        src = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_back"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("parse_backpack_arg(args.extract_plain_text()", src)
        self.assertIn("filter_sections(sections, keyword)", src)
        self.assertIn("filter_lines(msg_list, keyword)", src)
        self.assertIn("empty_notice(keyword)", src)
        self.assertIn("我的背包 关键词 [页码]", src)


if __name__ == "__main__":
    unittest.main()
