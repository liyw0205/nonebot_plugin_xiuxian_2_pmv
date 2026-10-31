"""帮助文本 ↔ 已注册命令的发现性合同。

近四个切片新增的四条只读命令（我的灵石流水 / 我的资产总览 / 我的宗门贡献流水 /
我的物品流水）曾长期"上线即隐身"：matcher 与 handler 都在，但三个帮助入口
（`修仙帮助` / `宗门帮助` / `背包帮助`）里一个字都没有，玩家没有任何途径知道它们存在。

本套件把这件事变成合同：

1. 正向：四条命令既已注册，又出现在其所属帮助文本块里（逐字命中主名，不接受别名顶替）。
2. 不倒退：三个帮助块仍保留各自原有分组名，追加没破坏原结构。
3. 反向防漂移：帮助块里以 ``- `` 开头、形如命令的条目，其命令名必须真实已注册，
   避免帮助写了一条不存在的命令。
4. 分页：`宗门帮助` 走 `paginate_text_blocks`，新增条目后首页仍非空、页数仍 ≥ 1，
   但不钉死具体页数。
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

import nonebot

nonebot.init()

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "nonebot_plugin_xiuxian_2"
XIUXIAN = PKG / "xiuxian"

BASE_SRC = (XIUXIAN / "xiuxian_base" / "__init__.py").read_text(encoding="utf-8")
SECT_SRC = (XIUXIAN / "xiuxian_sect" / "__init__.py").read_text(encoding="utf-8")
BACK_SRC = (XIUXIAN / "xiuxian_back" / "__init__.py").read_text(encoding="utf-8")


def slice_between(source: str, start: str, end: str = '""".strip()') -> str:
    """截取 ``start`` 之后到下一个 ``end`` 之间的文本。"""
    index = source.find(start)
    if index < 0:
        raise AssertionError(f"anchor not found: {start!r}")
    tail = source[index + len(start):]
    stop = tail.find(end)
    if stop < 0:
        raise AssertionError(f"end anchor not found after {start!r}")
    return tail[:stop]


def _literal_strings(node: ast.AST) -> set[str]:
    """从一个 AST 节点里挖出所有字符串字面量（含三元表达式两侧）。"""
    found: set[str] = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            found.add(child.value)
    return found


def registered_commands() -> tuple[set[str], set[str]]:
    """收集包内所有 on_command/on_regex 的首参与 aliases。

    命令可能注册在旧包 `xiuxian/` 下，也可能注册在新适配器层
    `adapters/nonebot/commands.py`，所以扫整个包（排除 tests）。
    首参既可能是字面量，也可能是
    `"修仙签到" if _flag else "__disabled__"` 这类条件表达式，都要能取到。
    """
    names: set[str] = set()
    aliases: set[str] = set()
    for path in PKG.rglob("*.py"):
        if "tests" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = getattr(fn, "id", None) or getattr(fn, "attr", None)
            is_matcher = name in ("on_command", "on_fullmatch", "on_startswith")
            if not is_matcher:
                continue
            if node.args:
                names |= _literal_strings(node.args[0])
            for keyword in node.keywords:
                if keyword.arg == "aliases":
                    aliases |= _literal_strings(keyword.value)
    # 以 __ 开头的是占位符（功能关闭时代替真实命令名），不算真实命令。
    names = {item for item in names if not item.startswith("__")}
    return names, aliases


# 三条已补齐的命令 → (所属帮助块文本, 身份)
HELP_BLOCKS = {
    "修仙帮助": slice_between(BASE_SRC, "**修仙帮助**"),
    "宗门帮助": slice_between(SECT_SRC, '__sect_help__ = f"""'),
    "背包帮助": slice_between(BACK_SRC, "**背包帮助**"),
}

EXPECTED = {
    "我的灵石流水": "修仙帮助",
    "我的资产总览": "修仙帮助",
    "我的签到统计": "修仙帮助",
    "我的突破统计": "修仙帮助",
    "我的宗门贡献流水": "宗门帮助",
    "我的物品流水": "背包帮助",
}


class HelpDiscoverabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registered, self.aliases = registered_commands()

    # ------------------------------------------------------------ 正向
    def test_all_four_commands_are_registered(self) -> None:
        for command in EXPECTED:
            self.assertIn(command, self.registered, f"{command} 未注册")

    def test_each_command_appears_in_its_own_help_block(self) -> None:
        for command, block_name in EXPECTED.items():
            block = HELP_BLOCKS[block_name]
            self.assertIn(
                command, block, f"{command} 未出现在 {block_name} 中（玩家无法发现）"
            )

    def test_help_lists_command_with_a_one_line_hint(self) -> None:
        """每条新增条目都应带一行 `>` 说明，而不是只甩一个命令名。"""
        for command, block_name in EXPECTED.items():
            block = HELP_BLOCKS[block_name]
            match = re.search(
                rf"^- {re.escape(command)}\n> (?P<hint>\S[^\n]*)$", block, re.MULTILINE
            )
            self.assertIsNotNone(
                match, f"{block_name} 里 {command} 缺少 `>` 一行说明"
            )
            self.assertTrue(match.group("hint").strip())

    # ------------------------------------------------------------ 不倒退
    def test_original_sections_survive(self) -> None:
        self.assertIn("**基础指令**", HELP_BLOCKS["修仙帮助"])
        self.assertIn("**功能入口**", HELP_BLOCKS["修仙帮助"])
        self.assertIn("**任务与商店**", HELP_BLOCKS["宗门帮助"])
        self.assertIn("**福利**", HELP_BLOCKS["宗门帮助"])
        self.assertIn("**查看**", HELP_BLOCKS["背包帮助"])
        self.assertIn("**使用与整理**", HELP_BLOCKS["背包帮助"])

    def test_existing_entries_are_not_removed(self) -> None:
        # 抽样固定既有条目，确保只追加、没有顺手删改。
        self.assertIn("我的修仙信息", HELP_BLOCKS["修仙帮助"])
        self.assertIn("修仙签到", HELP_BLOCKS["修仙帮助"])
        self.assertIn("宗门兑换 [物品] [数量]", HELP_BLOCKS["宗门帮助"])
        self.assertIn("宗门周常", HELP_BLOCKS["宗门帮助"])
        self.assertIn("我的背包 [页码]", HELP_BLOCKS["背包帮助"])
        self.assertIn("快速对比 [物品1] [物品2]", HELP_BLOCKS["背包帮助"])

    def test_help_buttons_unchanged(self) -> None:
        """快捷按钮是既有合同，本切片不该动它们。"""
        self.assertIn('k1="存档",', BASE_SRC)
        self.assertIn("v1=\"我的修仙信息\"", BASE_SRC)
        self.assertIn('k1="背包", v1="我的背包",', BACK_SRC)
        self.assertIn("button_id=XiuConfig().button_id2", SECT_SRC)

    # ------------------------------------------------------------ 反向防漂移
    def test_help_never_advertises_unknown_commands(self) -> None:
        """帮助块里的命令条目必须能落到某个真实命令上。

        历史帮助大量使用口语简称（`背包` → 真实命令 `我的背包`），所以命中规则是
        "等于主名/别名，或是某个主名/别名的子串"；完全虚构的命令名仍会被抓到。
        """
        known = self.registered | self.aliases

        def resolvable(token: str) -> bool:
            if token in known:
                return True
            return any(token in candidate for candidate in known)

        for block_name, block in HELP_BLOCKS.items():
            for line in block.splitlines():
                if not line.startswith("- "):
                    continue
                entry = line[2:].strip()
                if not entry or "/" in entry or "[" in entry or "+" in entry:
                    continue
                token = entry.split(" ")[0].strip()
                if not token:
                    continue
                self.assertTrue(
                    resolvable(token),
                    f"{block_name} 宣传了不存在的命令：{token}",
                )

    def test_new_entries_are_exact_command_names(self) -> None:
        """本切片新增的四条必须逐字等于真实命令名，不允许用简称蒙混。"""
        for command, block_name in EXPECTED.items():
            self.assertIn(command, self.registered, f"{command} 不是真实命令名")
            self.assertIn(f"- {command}\n", HELP_BLOCKS[block_name])

    # ------------------------------------------------------------ 分页
    def test_sect_help_still_paginates(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import (
            paginate_text_blocks,
        )

        msg, page, total_pages = paginate_text_blocks(
            HELP_BLOCKS["宗门帮助"], 1, per_page=3
        )
        self.assertTrue(msg.strip())
        self.assertEqual(page, 1)
        self.assertGreaterEqual(total_pages, 1)

    def test_new_entry_is_reachable_through_pagination(self) -> None:
        """新条目必须能在某一页被看到，不能因为分页被截断丢失。"""
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import (
            paginate_text_blocks,
        )

        seen = ""
        for page in range(1, 40):
            msg, current, total = paginate_text_blocks(
                HELP_BLOCKS["宗门帮助"], page, per_page=3
            )
            if current != page:
                break
            seen += msg
            if page >= total:
                break
        self.assertIn("我的宗门贡献流水", seen)


if __name__ == "__main__":
    unittest.main()
