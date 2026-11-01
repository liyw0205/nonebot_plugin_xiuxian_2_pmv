"""修仙帮助关键词检索：匹配强度、别名命中、空/无结果与索引一致性。

`修仙帮助` 原本接受参数却完全忽略，玩家面对 650+ 条命令只能翻长文本。
本套件钉住检索行为，并防止索引与运行期注册漂移：

1. 完全命中排在前缀命中之前，前缀命中排在包含命中之前；
2. 别名能命中（用 manifest 里真实存在的别名）；
3. 大小写/空白归一化；
4. 空关键词 → 提示而非空结果；
5. 无匹配 → 明确提示且不抛错；
6. 命中数超过上限时截断并提示；
7. 索引覆盖 platform_manifest 与 features/*/manifest.py，且与
   `plugin.build_registry()` 登记的命令集合一致（防漂移）；
8. 帮助页与 handler 的源合同（含关键词分支）。
"""

from __future__ import annotations

import unittest
from pathlib import Path

import nonebot

nonebot.init()

import nonebot_plugin_xiuxian_2 as ROOT_PACKAGE
from nonebot_plugin_xiuxian_2.features.base.help_search import (
    MAX_MATCHES,
    build_entries,
    format_help_search,
    iter_feature_manifests,
    search,
)


def entries() -> list[dict]:
    return build_entries(iter_feature_manifests(ROOT_PACKAGE))


class HelpSearchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.entries = entries()

    # -------------------------------------------------- 1. 匹配强度
    def test_exact_beats_prefix_beats_contains(self) -> None:
        sample = [
            {"command": "签到", "aliases": (), "feature": ""},
            {"command": "签到统计", "aliases": (), "feature": ""},
            {"command": "每日签到提醒", "aliases": (), "feature": ""},
            {"command": "修仙签到", "aliases": (), "feature": ""},
        ]
        p = search(sample, "签到")
        self.assertEqual([m["command"] for m in p["matches"]][:2], ["签到", "签到统计"])
        self.assertEqual(p["matches"][0]["matched_by"], "命令")
        self.assertEqual(p["matches"][1]["matched_by"], "前缀")

    # -------------------------------------------------- 2. 别名命中
    def test_alias_match(self) -> None:
        p = search(self.entries, "道具流水")
        commands = [m["command"] for m in p["matches"]]
        self.assertIn("我的物品流水", commands)

    # -------------------------------------------------- 3. 归一化
    def test_case_and_whitespace_normalized(self) -> None:
        sample = [{"command": "我的ID", "aliases": ("MyId",), "feature": ""}]
        self.assertEqual(search(sample, "  myid ")["total"], 1)
        self.assertEqual(search(sample, "  MYID ")["total"], 1)

    # -------------------------------------------------- 4. 空关键词
    def test_empty_keyword_prompts(self) -> None:
        p = search(self.entries, "   ")
        self.assertEqual(p["total"], 0)
        self.assertIn("请给出要查找的关键词", format_help_search(p))

    # -------------------------------------------------- 5. 无匹配
    def test_no_match_message(self) -> None:
        p = search(self.entries, "完全不存在的命令xyz")
        self.assertEqual(p["total"], 0)
        text = format_help_search(p)
        self.assertIn("没有找到", text)
        self.assertIn("修仙帮助", text)

    # -------------------------------------------------- 6. 截断
    def test_truncates_and_hints(self) -> None:
        sample = [
            {"command": f"测试命令{i:02d}", "aliases": (), "feature": ""} for i in range(15)
        ]
        p = search(sample, "测试命令", limit=5)
        self.assertEqual(len(p["matches"]), 5)
        self.assertEqual(p["total"], 15)
        self.assertIn("仅显示前 5 条", format_help_search(p))

    # -------------------------------------------------- 7. 索引覆盖与一致性
    def test_index_matches_registered_registry(self) -> None:
        from nonebot_plugin_xiuxian_2.plugin import build_registry

        registry = build_registry()
        registered = {
            name.strip().casefold()
            for feature in registry.features
            for command in feature.commands
            for name in (command.name, *command.aliases)
        }
        indexed = {
            name.strip().casefold()
            for entry in self.entries
            for name in (entry["command"], *entry["aliases"])
        }
        self.assertEqual(
            sorted(registered - indexed), [],
            "运行期已登记但帮助索引缺失的命令（索引漏扫 manifest）",
        )

    def test_index_is_not_empty_and_covers_multiple_features(self) -> None:
        self.assertGreater(len(self.entries), 100)
        features = {entry["feature"] for entry in self.entries if entry["feature"]}
        self.assertGreater(len(features), 5)

    def test_known_commands_are_findable(self) -> None:
        for keyword, expected in (
            ("修行周报", "修行周报"),
            ("战力拆解", "战力拆解"),
            ("背包容量", "背包容量"),
            ("签到", "修仙签到"),
        ):
            with self.subTest(keyword=keyword):
                commands = [m["command"] for m in search(self.entries, keyword)["matches"]]
                self.assertIn(expected, commands)

    # -------------------------------------------------- 8. 源合同
    def test_help_handler_uses_keyword_branch(self) -> None:
        base = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_base"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("keyword = args.extract_plain_text().strip()", base)
        self.assertIn("search_help_commands(keyword)", base)
        self.assertIn("format_help_search(payload)", base)
        self.assertIn("修仙帮助 关键词", base)
        self.assertIn("_HELP_SEARCH_ENTRIES", base, "索引应惰性缓存，避免每次检索重扫 manifest")


if __name__ == "__main__":
    unittest.main()
