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

# (相对包路径, 行号) -> (anchor, divide_by_three, apply_rank_suppress)
EXPECTED = {
    ("compatibility/base_breakthrough_effects.py", 193): ("gap", False, "False"),
    ("features/boss/exp_reward.py", 61): ("gap", True, "default"),
    ("features/training/event_planner.py", 83): ("gap", True, "default"),
    ("features/training/event_resolver.py", 398): ("gap", True, "default"),
    ("features/training/event_resolver.py", 542): ("gap", True, "default"),
    ("xiuxian/xiuxian_Illusion/__init__.py", 244): ("gap", True, "default"),
    ("xiuxian/xiuxian_buff/partner.py", 2537): ("gap", False, "default"),
    ("xiuxian/xiuxian_buff/partner.py", 2630): ("gap", False, "False"),
    ("xiuxian/xiuxian_past_life/past_life_events.py", 713): ("gap", True, "default"),
    ("xiuxian/xiuxian_sect/__init__.py", 2028): ("gap", True, "default"),
    ("xiuxian/xiuxian_sect/__init__.py", 2111): ("gap", True, "default"),
    ("xiuxian/xiuxian_tower/tower_battle.py", 203): ("gap", True, "default"),
    ("xiuxian/xiuxian_tower/tower_battle.py", 224): ("gap", True, "default"),
    ("xiuxian/xiuxian_tower/tower_battle.py", 370): ("gap", True, "default"),
    ("xiuxian/xiuxian_tower/tower_battle.py", 390): ("gap", True, "default"),
    ("xiuxian/xiuxian_world_events/__init__.py", 1518): ("gap", False, "default"),
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


def scan_calls() -> dict[tuple[str, int], tuple[str, bool, str]]:
    """解析包内所有真实 percent_exp_reward 调用（跳过定义、注释与文档字符串）。"""
    found: dict[tuple[str, int], tuple[str, bool, str]] = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        if "tests" in path.parts:
            continue
        src = _code_only(path.read_text(encoding="utf-8", errors="ignore"))
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
            key = (str(path.relative_to(PACKAGE)), src[:match.start()].count("\n") + 1)
            found[key] = (
                anchor.group(1) if anchor else "current",
                "divide_by_three=True" in body.replace(" ", ""),
                suppress.group(1) if suppress else "default",
            )
    return found


class ProgressionSourceTableTests(unittest.TestCase):
    def test_no_call_site_missing_from_table(self) -> None:
        found = scan_calls()
        self.assertEqual(
            sorted(set(found) - set(EXPECTED)), [],
            "代码里出现了总表未登记的 percent_exp_reward 调用点",
        )

    def test_no_table_entry_missing_from_code(self) -> None:
        found = scan_calls()
        self.assertEqual(
            sorted(set(EXPECTED) - set(found)), [],
            "总表登记的调用点在代码中不存在（行号漂移或已删除）",
        )

    def test_every_call_matches_documented_contract(self) -> None:
        found = scan_calls()
        for key, (anchor, div3, suppress) in EXPECTED.items():
            with self.subTest(site="%s:%d" % key):
                self.assertIn(key, found)
                self.assertEqual(
                    found[key], (anchor, div3, suppress),
                    f"{key[0]}:{key[1]} 的 anchor/÷3/压制与总表声明不一致",
                )

    def test_no_reward_site_uses_current_anchor(self) -> None:
        """修为**发放**绝不使用 current 锚定（会随累计修为膨胀）。"""
        offenders = [
            f"{path}:{line}"
            for (path, line), (anchor, _div3, _supp) in scan_calls().items()
            if anchor == "current"
        ]
        self.assertEqual(offenders, [], "存在 current 锚定的修为发放点")

    def test_high_frequency_repeatables_divide_by_three(self) -> None:
        require_div3 = {
            ("features/boss/exp_reward.py", 61),              # 世界BOSS
            ("features/training/event_planner.py", 83),       # 历练
            ("features/training/event_resolver.py", 398),     # 历练奖励
            ("features/training/event_resolver.py", 542),     # 历练惩罚
            ("xiuxian/xiuxian_Illusion/__init__.py", 244),    # 幻境
            ("xiuxian/xiuxian_past_life/past_life_events.py", 713),
            ("xiuxian/xiuxian_sect/__init__.py", 2028),
            ("xiuxian/xiuxian_sect/__init__.py", 2111),
            ("xiuxian/xiuxian_tower/tower_battle.py", 203),
            ("xiuxian/xiuxian_tower/tower_battle.py", 224),
            ("xiuxian/xiuxian_tower/tower_battle.py", 370),
            ("xiuxian/xiuxian_tower/tower_battle.py", 390),
        }
        found = scan_calls()
        for key in sorted(require_div3):
            with self.subTest(site="%s:%d" % key):
                self.assertTrue(found[key][1], f"{key[0]}:{key[1]} 缺少 ÷3 压制")

    def test_document_lists_every_registered_site(self) -> None:
        text = DOC.read_text(encoding="utf-8")
        for path, line in EXPECTED:
            with self.subTest(site=f"{path}:{line}"):
                self.assertIn(
                    f"{path}:{line}", text,
                    "总表未写出该调用点的位置",
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
