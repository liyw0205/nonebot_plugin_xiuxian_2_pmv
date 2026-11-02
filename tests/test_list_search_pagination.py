"""列表检索的「翻页续查」契约。

已交付的关键词筛选只作用于当次请求：翻页提示、MD 下一页链接与底部按钮仍只拼页码，
总页数却按筛选结果计算，于是第 2 页渲染的是**未筛选**集合的第 2 页。

本套件钉住：
- `features/base/keyword_page.py::page_command` 与 `parse_page_arg` 的往返语义；
- 宠物/饰品/背包五视图/宗门商店/宗门成员查看的每个翻页出口都保留关键词（源合同）；
- 宠物与饰品的真实渲染函数在筛选后第 2 页仍是筛选结果；
- 宗门成员筛选字段与首页职位统计口径；
- 全包不再有 `int(args)` 静默回退。
"""

from __future__ import annotations

import unittest
from pathlib import Path
from urllib.parse import quote

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.back.backpack_search import filter_sections
from nonebot_plugin_xiuxian_2.features.base.keyword_page import (
    page_command,
    parse_page_arg,
)

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"
PET_SRC = (PACKAGE / "xiuxian" / "xiuxian_pet" / "__init__.py").read_text(encoding="utf-8")
ACC_SRC = (PACKAGE / "xiuxian" / "xiuxian_back" / "accessory.py").read_text(encoding="utf-8")
BACK_SRC = (PACKAGE / "xiuxian" / "xiuxian_back" / "__init__.py").read_text(encoding="utf-8")
SECT_SRC = (PACKAGE / "xiuxian" / "xiuxian_sect" / "__init__.py").read_text(encoding="utf-8")


def _args_of(command: str) -> str:
    """把完整命令还原成 handler 收到的 `args.extract_plain_text()`。"""
    return command.split(" ", 1)[1] if " " in command else ""


class PageCommandTests(unittest.TestCase):
    def test_without_keyword_keeps_page_only_form(self) -> None:
        self.assertEqual(page_command("宠物背包", "", 2), "宠物背包 2")
        self.assertEqual(page_command("我的背包", "  ", 3), "我的背包 3")

    def test_with_keyword_carries_it(self) -> None:
        self.assertEqual(page_command("宠物背包", "龙", 2), "宠物背包 龙 2")
        self.assertEqual(page_command("宗门成员查看", "长老", 1), "宗门成员查看 长老 1")

    def test_round_trip_with_parser(self) -> None:
        cases = [("", 2), ("龙", 1), ("宗门 长老", 3), ("A B C", 4), ("烈阳", 15)]
        for keyword, page in cases:
            command = page_command("宗门成员查看", keyword, page)
            parsed = parse_page_arg(_args_of(command))
            with self.subTest(keyword=keyword, page=page):
                self.assertEqual(parsed["keyword"].casefold(), keyword.casefold())
                self.assertEqual(parsed["page"], page)

    def test_page_is_clamped_and_never_zero(self) -> None:
        self.assertEqual(page_command("宠物背包", "", 0), "宠物背包 1")
        self.assertEqual(page_command("宠物背包", "", -3), "宠物背包 1")
        self.assertEqual(page_command("宠物背包", "龙", "bad"), "宠物背包 龙 1")

    def test_page_only_command_has_two_tokens(self) -> None:
        self.assertEqual(len(page_command("饰品背包", "", 2).split(" ")), 2)


class PetRenderTests(unittest.TestCase):
    DOC = {
        "active": None,
        "bag": [
            {"form_name": f"赤焰龙{i}", "name": "龙", "rarity": "传说", "type": "攻击",
             "stars": 1, "uid": f"p{i}"}
            for i in range(18)
        ]
        + [
            {"form_name": f"灵狐{i}", "name": "狐", "rarity": "稀有", "type": "辅助",
             "stars": 1, "uid": f"q{i}"}
            for i in range(5)
        ],
    }

    def _builders(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet import (
            _build_pet_bag_md_text,
            _build_pet_bag_plain_text,
        )

        return _build_pet_bag_md_text, _build_pet_bag_plain_text

    def test_plain_text_next_page_keeps_keyword(self) -> None:
        _, plain = self._builders()
        text = plain("道友的宠物背包", self.DOC, 1, keyword="龙")
        self.assertIn("第 1/2 页", text)
        self.assertIn("输入 宠物背包 龙 2 查看下一页", text)

    def test_md_next_link_keeps_keyword(self) -> None:
        md, _ = self._builders()
        text, current_page, total_pages = md("道友的宠物背包", self.DOC, 1, keyword="龙")
        self.assertEqual((current_page, total_pages), (1, 2))
        self.assertIn(quote("宠物背包 龙 2", safe=""), text)

    def test_second_page_still_filtered(self) -> None:
        _, plain = self._builders()
        text = plain("道友的宠物背包", self.DOC, 2, keyword="龙")
        self.assertIn("第 2/2 页", text)
        self.assertIn("UID:p", text)
        self.assertNotIn("灵狐", text)
        self.assertNotIn("UID:q", text)
        self.assertNotIn("查看下一页", text)

    def test_without_keyword_next_page_is_unchanged(self) -> None:
        _, plain = self._builders()
        text = plain("道友的宠物背包", self.DOC, 1)
        self.assertIn("输入 宠物背包 2 查看下一页", text)


class AccessoryRenderTests(unittest.TestCase):
    SECTIONS = [
        ("烈阳", [
            {"name": f"烈阳戒{i}", "goods_type": "饰品", "part": "戒指",
             "set_type": "烈阳", "quality": 3, "uid": f"a{i}"}
            for i in range(17)
        ]),
        ("玄渊", [
            {"name": f"玄渊链{i}", "goods_type": "饰品", "part": "项链",
             "set_type": "玄渊", "quality": 2, "uid": f"b{i}"}
            for i in range(3)
        ]),
    ]

    def _render(self, keyword: str, page: int, per_page: int = 15):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.accessory_helpers import (
            _build_accessory_plain_text,
            _paginate_sections,
        )

        sections = filter_sections(self.SECTIONS, keyword)
        page_sections, current_page, total_pages = _paginate_sections(
            sections, page, per_page=per_page
        )
        text = _build_accessory_plain_text(
            title="道友的饰品背包",
            sections=page_sections,
            current_page=current_page,
            total_pages=total_pages,
            next_cmd=page_command("饰品背包", keyword, current_page + 1),
            capacity_text="容量：20/1000",
        )
        return text, current_page, total_pages

    def test_filtered_first_page_next_command_keeps_keyword(self) -> None:
        text, current_page, total_pages = self._render("烈阳", 1)
        self.assertEqual((current_page, total_pages), (1, 2))
        self.assertIn("输入 饰品背包 烈阳 2 查看下一页", text)

    def test_filtered_second_page_only_contains_matches(self) -> None:
        text, current_page, total_pages = self._render("烈阳", 2)
        self.assertEqual((current_page, total_pages), (2, 2))
        self.assertIn("烈阳戒15", text)
        self.assertNotIn("玄渊链", text)

    def test_without_keyword_next_command_is_page_only(self) -> None:
        text, _, total_pages = self._render("", 1)
        self.assertEqual(total_pages, 2)
        self.assertIn("输入 饰品背包 2 查看下一页", text)


class SourceContractTests(unittest.TestCase):
    def test_pet_renderers_use_shared_page_command(self) -> None:
        self.assertIn("from ...features.base.keyword_page import page_command", PET_SRC)
        # 纯文本/分页文本路径用 page 变量，MD 与纯文本路径用 current_page
        self.assertIn("page_command('宠物背包', keyword, page + 1)", PET_SRC)
        self.assertIn('page_command("宠物背包", keyword, current_page + 1)', PET_SRC)
        self.assertIn("page_command('宠物背包', keyword, current_page + 1)", PET_SRC)
        self.assertNotIn('f"输入 宠物背包 {page + 1}', PET_SRC)
        self.assertNotIn('f"宠物背包 {current_page + 1}"', PET_SRC)

    def test_accessory_renderers_use_shared_page_command(self) -> None:
        self.assertIn("from ...features.base.keyword_page import page_command", ACC_SRC)
        self.assertEqual(ACC_SRC.count('page_command("饰品背包", keyword, current_page + 1)'), 3)
        self.assertNotIn('f"饰品背包 {current_page + 1}"', ACC_SRC)

    def test_every_backpack_view_carries_keyword(self) -> None:
        for command in ("我的背包", "丹药背包", "药材背包", "我的装备", "药材背包详情"):
            with self.subTest(command=command):
                self.assertIn(
                    f'page_command("{command}", keyword, current_page + 1)', BACK_SRC
                )
                self.assertNotIn(f'f"{command} {{current_page + 1}}"', BACK_SRC)
                self.assertNotIn(f'f"{command} {{page + 1}}"', BACK_SRC)

    def test_backpack_md_next_cmd_passes_keyword(self) -> None:
        # 我的背包/丹药背包/药材背包 的 MD 与回落两条路径都要保留关键词
        for command in ("我的背包", "丹药背包", "药材背包"):
            with self.subTest(command=command):
                self.assertEqual(
                    BACK_SRC.count(f'next_cmd=page_command("{command}", keyword, current_page + 1)'),
                    2,
                )

    def test_sect_shop_pagination_carries_keyword(self) -> None:
        self.assertIn("page_command('宗门商店', keyword, page + 1)", SECT_SRC)
        self.assertIn('page_command("宗门商店", keyword, page + 1)', SECT_SRC)
        self.assertNotIn('f"宗门商店 {page + 1}"', SECT_SRC)
        self.assertNotIn("宗门商店+页码", SECT_SRC)

    def test_sect_member_view_uses_shared_parser_and_filter(self) -> None:
        self.assertIn('parse_page_arg(args.extract_plain_text() if args else "")', SECT_SRC)
        self.assertIn("filter_sect_member_rows(userlist, keyword, position_titles)", SECT_SRC)
        self.assertIn('empty_notice(keyword, "宗门成员")', SECT_SRC)
        self.assertIn("count_sect_positions(sorted_users)", SECT_SRC)
        self.assertIn('page_command("宗门成员查看", keyword, current_page + 1)', SECT_SRC)
        self.assertNotIn("current_page = int(args.extract_plain_text()", SECT_SRC)

    def test_sect_member_help_documents_keyword(self) -> None:
        self.assertIn("- 宗门成员查看 [页码|关键词]", SECT_SRC)
        self.assertIn("可按道号/职位/境界筛选", SECT_SRC)


class SectMemberFilterTests(unittest.TestCase):
    ROWS = [
        {"user_name": "张三", "sect_position": "1", "level": "金丹", "user_id": "u1",
         "sect_contribution": 10},
        {"user_name": "李四", "sect_position": "2", "level": "元婴", "user_id": "u2",
         "sect_contribution": 20},
        {"user_name": "王五", "sect_position": "2", "level": "炼气", "user_id": "u3",
         "sect_contribution": 30},
    ]
    TITLES = {"1": "宗主", "2": "长老", "3": "内门"}

    def _filter(self, keyword: str, titles=None):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_member_utils import (
            filter_sect_member_rows,
        )

        return filter_sect_member_rows(self.ROWS, keyword, self.TITLES if titles is None else titles)

    def test_matches_name_position_level_and_uid(self) -> None:
        self.assertEqual([r["user_name"] for r in self._filter("张三")], ["张三"])
        self.assertEqual([r["user_name"] for r in self._filter("长老")], ["李四", "王五"])
        self.assertEqual([r["user_name"] for r in self._filter("元婴")], ["李四"])
        self.assertEqual([r["user_name"] for r in self._filter("U3")], ["王五"])

    def test_empty_keyword_keeps_order(self) -> None:
        rows = self._filter("   ")
        self.assertEqual([r["user_name"] for r in rows], ["张三", "李四", "王五"])
        self.assertIsNot(rows, self.ROWS)

    def test_no_match_returns_empty(self) -> None:
        self.assertEqual(self._filter("不存在"), [])

    def test_missing_position_titles_still_filters_other_fields(self) -> None:
        self.assertEqual([r["user_name"] for r in self._filter("李四", titles={})], ["李四"])
        self.assertEqual(self._filter("长老", titles={}), [])

    def test_position_counts(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_member_utils import (
            count_sect_positions,
        )

        self.assertEqual(count_sect_positions(self.ROWS), {"1": 1, "2": 2})
        self.assertEqual(count_sect_positions([]), {})


class PackageRegressionTests(unittest.TestCase):
    PATTERN = "current_page = int(args.extract_plain_text("

    def test_no_silent_int_page_fallback_anywhere(self) -> None:
        offenders = []
        for path in PACKAGE.rglob("*.py"):
            if "tests" in path.parts:
                continue
            if self.PATTERN in path.read_text(encoding="utf-8", errors="ignore"):
                offenders.append(str(path.relative_to(ROOT)))
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
