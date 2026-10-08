from __future__ import annotations

import unittest
import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
ENTERTAINMENT = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment"


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
            "handle_command_url(bot, event)",
            "handle_embedded_share(",
            "handle_any_http(bot, event)",
        ):
            self.assertIn(contract, matcher_source)

        owner_source = (
            ROOT / "nonebot_plugin_xiuxian_2/features/entertainment/media_parser_messages.py"
        ).read_text(encoding="utf-8")
        for contract in (
            "should_parse_message(text)",
            "self._config().auto_parse",
            '"原始链接：" in text',
            "should_skip_duplicate(self._event_key(event))",
            "parse_and_build_messages(",
            "send_card_with_text",
            "send_markdown",
        ):
            self.assertIn(contract, owner_source)

    def test_explicit_command_is_a_thin_forwarder_to_the_shared_message_owner(self):
        command_source = (ENTERTAINMENT / "command.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(command_source)
        helper = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "fun_media_send_parse_result"
        )
        statements = [
            statement
            for statement in helper.body
            if not (
                isinstance(statement, ast.Expr)
                and isinstance(statement.value, ast.Constant)
                and isinstance(statement.value.value, str)
            )
        ]
        self.assertEqual(len(statements), 1)
        statement = statements[0]
        self.assertIsInstance(statement, ast.Expr)
        self.assertIsInstance(statement.value, ast.Await)
        call = statement.value.value
        self.assertIsInstance(call, ast.Call)
        self.assertEqual(
            ast.unparse(call.func),
            "entertainment_application.media_parser_messages.send_parse_result",
        )
        self.assertEqual(
            [ast.unparse(argument) for argument in call.args],
            ["bot", "event", "source_text"],
        )
        self.assertNotIn("parse_and_build_messages", ast.unparse(helper))

        matcher_source = (ENTERTAINMENT / "mod/media_parse_link.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "fun_media_send_parse_result(bot, event, arg_text)", matcher_source
        )
        self.assertIn(
            "fun_media_send_parse_result(bot, event, plain)", matcher_source
        )

    def test_parser_service_is_only_a_compatibility_facade(self):
        source = (
            ENTERTAINMENT / "media_parser/service.py"
        ).read_text(encoding="utf-8")
        self.assertIn("entertainment_application.media_parser.parse_and_build_messages", source)
        self.assertNotIn("await run_blocking_io", source)


if __name__ == "__main__":
    unittest.main()
