"""列表分页「下一页」统一钳制。

同一仓库里原本有两种写法：六处已用 `min(page + 1, total_pages)`，另外几处直接把
`page + 1` 交给 `page_command`。未钳制的那几处会在最后一页提示/按钮出一个不存在的页
（「共 N 页」却让玩家去 N+1），单页列表还会提示「查看其他页」。

本套件钉住 `next_page_command` 的语义，并用源合同保证未受保护站点不再回归。
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.base.keyword_page import (
    next_page_command,
    page_command,
)

PACKAGE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"


class NextPageCommandTests(unittest.TestCase):
    def test_last_page_does_not_advance(self) -> None:
        self.assertEqual(next_page_command("我的仙肆", "", 2, 2), "我的仙肆 2")

    def test_middle_page_advances_and_keeps_keyword(self) -> None:
        self.assertEqual(
            next_page_command("宗门成员查看", "长老", 1, 3), "宗门成员查看 长老 2"
        )
        self.assertEqual(next_page_command("宗门商店", "灵草", 2, 3), "宗门商店 灵草 3")

    def test_single_page_stays_on_page_one(self) -> None:
        self.assertEqual(next_page_command("宠物背包", "", 1, 1), "宠物背包 1")
        self.assertEqual(next_page_command("饰品背包", "戒指", 1, 1), "饰品背包 戒指 1")

    def test_invalid_inputs_fall_back_to_page_one(self) -> None:
        self.assertEqual(next_page_command("我的仙肆", "", 0, 0), "我的仙肆 1")
        self.assertEqual(next_page_command("我的仙肆", "", -3, -1), "我的仙肆 1")
        self.assertEqual(next_page_command("我的仙肆", "", "bad", "bad"), "我的仙肆 1")

    def test_page_beyond_total_is_treated_as_last_page(self) -> None:
        self.assertEqual(next_page_command("宗门商店", "", 9, 3), "宗门商店 3")

    def test_output_matches_page_command_format(self) -> None:
        self.assertEqual(
            next_page_command("宗门商店", "灵草", 1, 2), page_command("宗门商店", "灵草", 2)
        )
        self.assertEqual(next_page_command("宗门商店", "", 1, 2), "宗门商店 2")


class UnguardedCallSiteContractTests(unittest.TestCase):
    """所有「下一页」出口都必须钳制，不再直接把 page + 1 交给 page_command。"""

    FILES = (
        "xiuxian/xiuxian_sect/__init__.py",
        "xiuxian/xiuxian_trade/__init__.py",
        "xiuxian/xiuxian_pet/__init__.py",
        "xiuxian/xiuxian_back/__init__.py",
        "xiuxian/xiuxian_back/accessory.py",
    )

    def test_no_page_command_with_unclamped_plus_one(self) -> None:
        offenders = []
        for relative in self.FILES:
            source = (PACKAGE / relative).read_text(encoding="utf-8")
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if not isinstance(node.func, ast.Name) or node.func.id != "page_command":
                    continue
                for argument in node.args[2:]:
                    if isinstance(argument, ast.BinOp) and isinstance(argument.op, ast.Add):
                        offenders.append(f"{relative}:{node.lineno}")
        self.assertEqual(offenders, [])

    def test_next_page_command_is_used_by_the_fixed_views(self) -> None:
        expectations = {
            "xiuxian/xiuxian_sect/__init__.py": (
                'next_page_command("宗门成员查看"',
                'next_page_command("宗门商店"',
            ),
            "xiuxian/xiuxian_trade/__init__.py": ('next_page_command("我的仙肆"',),
            "xiuxian/xiuxian_pet/__init__.py": ("next_page_command('宠物背包'",),
            "xiuxian/xiuxian_back/accessory.py": ('next_page_command("饰品背包"',),
            "xiuxian/xiuxian_back/__init__.py": (
                'next_page_command("我的背包"',
                'next_page_command("丹药背包"',
                'next_page_command("药材背包"',
                'next_page_command("我的装备"',
            ),
        }
        for relative, fragments in expectations.items():
            source = (PACKAGE / relative).read_text(encoding="utf-8")
            for fragment in fragments:
                with self.subTest(file=relative, fragment=fragment):
                    self.assertIn(fragment, source)

    def test_single_page_views_do_not_advertise_another_page(self) -> None:
        sect = (PACKAGE / "xiuxian/xiuxian_sect/__init__.py").read_text(encoding="utf-8")
        self.assertGreaterEqual(sect.count("if total_pages > 1:"), 2)


if __name__ == "__main__":
    unittest.main()
