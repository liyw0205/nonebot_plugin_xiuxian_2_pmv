"""修为奖励来源总表防漂移：文档声明必须与代码一致。

`docs/features/xiuxian_progression.md` 登记了全部 `percent_exp_reward` 调用点的
锚点、÷3 压制与压制开关。本套件从代码里重新解析这些调用，逐项与文档声明比对，
任何一方单独改动都会失败——避免再次出现「文档说 gap、代码是 current」的漂移
（世界BOSS 曾经如此）。
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

import nonebot

nonebot.init()

PACKAGE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"
DOC = Path(__file__).resolve().parents[1] / "docs" / "features" / "xiuxian_progression.md"

ANCHOR_RE = re.compile(r"""anchor\s*=\s*["']([a-z_]+)["']""")
SUPPRESS_RE = re.compile(r"apply_rank_suppress\s*=\s*(\w+)")

# 文件 -> 按源码出现顺序的契约列表 (anchor, divide_by_three, apply_rank_suppress)。
# 契约不绑定绝对行号：同一文件内新增无关代码不应让本表漂移。
# 人类可读的行号只保留在 docs/features/xiuxian_progression.md。
EXPECTED: dict[str, list[tuple[str, bool, str]]] = {
    "compatibility/base_breakthrough_effects.py": [("gap", False, "False")],
    "features/boss/exp_reward.py": [("gap", True, "default")],
    "features/training/event_planner.py": [("gap", True, "default")],
    "features/training/event_resolver.py": [
        ("gap", True, "default"),
        ("gap", True, "default"),
    ],
    "xiuxian/xiuxian_Illusion/__init__.py": [("gap", True, "default")],
    "xiuxian/xiuxian_buff/partner.py": [
        ("gap", False, "default"),
        ("gap", False, "False"),
    ],
    "xiuxian/xiuxian_past_life/past_life_events.py": [("gap", True, "default")],
    "xiuxian/xiuxian_sect/__init__.py": [
        ("gap", True, "default"),
        ("gap", True, "default"),
    ],
    "xiuxian/xiuxian_tower/tower_battle.py": [
        ("gap", True, "default"),
        ("gap", True, "default"),
        ("gap", True, "default"),
        ("gap", True, "default"),
    ],
    "xiuxian/xiuxian_world_events/__init__.py": [("gap", False, "default")],
}

# 高频玩法必须 ÷3 压制（文件 -> 该文件内的调用序号）。
REQUIRE_DIV3: dict[str, tuple[int, ...]] = {
    "features/boss/exp_reward.py": (0,),
    "features/training/event_planner.py": (0,),
    "features/training/event_resolver.py": (0, 1),
    "xiuxian/xiuxian_Illusion/__init__.py": (0,),
    "xiuxian/xiuxian_past_life/past_life_events.py": (0,),
    "xiuxian/xiuxian_sect/__init__.py": (0, 1),
    "xiuxian/xiuxian_tower/tower_battle.py": (0, 1, 2, 3),
}


def _code_only(src: str) -> str:
    """把注释与文档字符串替换成等长空白（保留换行，行号不变）。

    否则 `features/boss/exp_reward.py` 模块文档里那句
    ``percent_exp_reward(..., divide_by_three=True, anchor="gap")`` 会被当成真实调用。
    """
    out: list[str] = []
    i, n = 0, len(src)
    while i < n:
        ch = src[i]
        if ch == "#":
            while i < n and src[i] != "\n":
                out.append(" ")
                i += 1
        elif src.startswith('"""', i) or src.startswith("'''", i):
            quote = src[i:i + 3]
            end = src.find(quote, i + 3)
            end = n if end == -1 else end + 3
            out.append("".join("\n" if c == "\n" else " " for c in src[i:end]))
            i = end
        elif ch in "\"'":
            quote = ch
            out.append(ch)
            i += 1
            while i < n:
                if src[i] == "\\" and i + 1 < n:
                    out.append(src[i:i + 2])
                    i += 2
                    continue
                out.append(src[i])
                if src[i] == quote:
                    i += 1
                    break
                i += 1
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def scan_calls() -> dict[str, list[tuple[int, str, bool, str]]]:
    """解析包内所有真实 percent_exp_reward 调用（跳过定义、注释与文档字符串）。

    返回 `文件 -> [(行号, anchor, divide_by_three, apply_rank_suppress), ...]`，
    列表顺序即源码出现顺序；行号只用于失败信息，不参与契约比对。
    """
    found: dict[str, list[tuple[int, str, bool, str]]] = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        if "tests" in path.parts:
            continue
        src = _code_only(path.read_text(encoding="utf-8", errors="ignore"))
        rel = str(path.relative_to(PACKAGE))
        for match in re.finditer(r"percent_exp_reward\s*\(", src):
            head = src[max(0, match.start() - 16):match.start()]
            if head.rstrip().endswith("def"):
                continue
            i, depth, buf = match.end(), 1, []
            while i < len(src) and depth:
                ch = src[i]
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                if depth:
                    buf.append(ch)
                i += 1
            body = " ".join("".join(buf).split())
            if body.count(",") < 1:  # 文档字符串里的示例，不是调用
                continue
            anchor = ANCHOR_RE.search(body)
            suppress = SUPPRESS_RE.search(body)
            line = src[:match.start()].count("\n") + 1
            found.setdefault(rel, []).append((
                line,
                anchor.group(1) if anchor else "current",
                "divide_by_three=True" in body.replace(" ", ""),
                suppress.group(1) if suppress else "default",
            ))
    return found


class ProgressionSourceTableTests(unittest.TestCase):
    def test_no_call_site_missing_from_table(self) -> None:
        found = scan_calls()
        self.assertEqual(
            sorted(set(found) - set(EXPECTED)),
            [],
            "代码里出现了总表未登记的 percent_exp_reward 调用点（新增文件）",
        )
        for path, calls in sorted(found.items()):
            with self.subTest(path=path):
                self.assertEqual(
                    len(calls),
                    len(EXPECTED.get(path, [])),
                    f"{path} 的调用点数与总表声明不一致",
                )

    def test_no_table_entry_missing_from_code(self) -> None:
        found = scan_calls()
        self.assertEqual(
            sorted(set(EXPECTED) - set(found)),
            [],
            "总表登记的调用点所在文件在代码中不存在",
        )

    def test_call_counts_match_table(self) -> None:
        found = scan_calls()
        by_file = {path: len(calls) for path, calls in found.items()}
        expected_by_file = {path: len(contracts) for path, contracts in EXPECTED.items()}
        self.assertEqual(by_file, expected_by_file, "各文件调用点数量与总表声明不一致")

    def test_every_call_matches_documented_contract(self) -> None:
        found = scan_calls()
        for path, contracts in EXPECTED.items():
            actual = found.get(path, [])
            with self.subTest(path=path):
                self.assertEqual(len(actual), len(contracts), f"{path} 调用点数量与总表不一致")
            for expected, entry in zip(contracts, actual):
                line, anchor, div3, suppress = entry
                with self.subTest(site=f"{path}:{line}"):
                    self.assertEqual(
                        (anchor, div3, suppress),
                        expected,
                        f"{path}:{line} 的 anchor/÷3/压制与总表声明不一致",
                    )

    def test_no_reward_site_uses_current_anchor(self) -> None:
        """修为**发放**绝不使用 current 锚定（会随累计修为膨胀）。"""
        offenders = [
            f"{path}:{line}"
            for path, calls in scan_calls().items()
            for line, anchor, _div3, _supp in calls
            if anchor == "current"
        ]
        self.assertEqual(offenders, [], "存在 current 锚定的修为发放点")

    def test_high_frequency_repeatables_divide_by_three(self) -> None:
        found = scan_calls()
        for path, indices in REQUIRE_DIV3.items():
            calls = found.get(path, [])
            for index in indices:
                with self.subTest(site=f"{path}[{index}]"):
                    self.assertLess(index, len(calls), f"{path} 缺少第 {index} 处调用点")
                    line, _anchor, div3, _supp = calls[index]
                    self.assertTrue(div3, f"{path}:{line} 缺少 ÷3 压制")

    def test_document_lists_every_registered_site(self) -> None:
        text = DOC.read_text(encoding="utf-8")
        for path, contracts in EXPECTED.items():
            with self.subTest(path=path):
                self.assertIn(f"`{path}:", text, "总表未写出该调用点所在文件")
                self.assertGreaterEqual(
                    text.count(f"`{path}:"),
                    len(contracts),
                    "总表列出的调用点数少于代码",
                )

    def test_document_states_no_current_anchor_rule(self) -> None:
        text = DOC.read_text(encoding="utf-8")
        self.assertIn("绝不使用 `current` 锚定发放修为", text)

    def test_boss_reward_is_wired_from_documented_helper(self) -> None:
        src = (PACKAGE / "xiuxian" / "xiuxian_boss" / "__init__.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("from ...features.boss.exp_reward import boss_exp_reward", src)
        self.assertIn("boss_exp_reward(", src)


if __name__ == "__main__":
    unittest.main()
