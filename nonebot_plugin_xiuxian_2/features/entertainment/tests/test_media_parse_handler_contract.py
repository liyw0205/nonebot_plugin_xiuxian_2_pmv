from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
ENTERTAINMENT = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment"


def _function(source: str, name: str):
    tree = ast.parse(source)
    return next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    )


class MediaParseHandlerContractTests(unittest.TestCase):
    def test_command_and_regex_matchers_keep_names_priorities_and_cooldowns(self):
        source = (ENTERTAINMENT / "mod/media_parse_link.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('"链接解析"', source)
        for alias in ("流媒体解析", "视频解析", "解析视频", "解析链接"):
            self.assertIn(f'"{alias}"', source)
        self.assertIn("priority=5", source)
        self.assertIn("priority=6", source)
        self.assertIn("priority=10", source)
        self.assertIn("priority=11", source)
        self.assertIn("Cooldown(cd_time=8)", source)
        self.assertIn("Cooldown(cd_time=10)", source)

    def test_matcher_filters_and_parse_output_all_reach_feature_application(self):
        matcher_source = (ENTERTAINMENT / "mod/media_parse_link.py").read_text(
            encoding="utf-8"
        )
        for contract in (
            "cfg.should_parse_message(text)",
            "cfg.auto_parse",
            '"原始链接：" in text',
            "fun_media_message_has_embedded_share_url(text)",
            "fun_media_has_supported_link(text)",
            "fun_media_send_parse_result(bot, event, text)",
        ):
            self.assertIn(contract, matcher_source)

        command_source = (ENTERTAINMENT / "command.py").read_text(encoding="utf-8")
        send_handler = _function(command_source, "fun_media_send_parse_result")
        attributes = {
            node.attr for node in ast.walk(send_handler) if isinstance(node, ast.Attribute)
        }
        self.assertIn("parse_and_build_messages", attributes)
        self.assertIn(
            "fun_media_should_skip_duplicate_event",
            {
                node.func.id
                for node in ast.walk(send_handler)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            },
        )
        dedupe = _function(command_source, "fun_media_should_skip_duplicate_event")
        self.assertIn(
            "should_skip_duplicate",
            {node.attr for node in ast.walk(dedupe) if isinstance(node, ast.Attribute)},
        )
        self.assertNotIn("run_parse_and_build_messages", command_source)

    def test_parser_service_is_only_a_compatibility_facade(self):
        source = (
            ENTERTAINMENT / "media_parser/service.py"
        ).read_text(encoding="utf-8")
        self.assertIn("entertainment_application.media_parser.parse_and_build_messages", source)
        self.assertNotIn("await run_blocking_io", source)


if __name__ == "__main__":
    unittest.main()
