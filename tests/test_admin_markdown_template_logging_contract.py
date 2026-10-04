from __future__ import annotations

import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
ADMIN_MODULE = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"


class AdminMarkdownTemplateLoggingContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tree = ast.parse(ADMIN_MODULE.read_text(encoding="utf-8"))
        cls.handler = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "mb_template_test_"
        )

    def test_handler_does_not_log_template_values_or_exception_text(self):
        logger_calls = [
            node
            for node in ast.walk(self.handler)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "logger"
        ]

        self.assertEqual(len(logger_calls), 1)
        call = logger_calls[0]
        self.assertEqual(call.func.attr, "warning")
        self.assertIsInstance(call.args[0], ast.JoinedStr)
        self.assertEqual(
            ast.unparse(call.args[0]),
            "f'dm发送markdown模板失败 ({type(e).__name__})'",
        )
        self.assertEqual(len(call.args), 1)

    def test_handler_keeps_template_delivery_and_plain_text_error_reply(self):
        call_targets = {
            ast.unparse(node.func)
            for node in ast.walk(self.handler)
            if isinstance(node, ast.Call)
        }

        self.assertIn("MessageSegment.markdown_template", call_targets)
        self.assertIn("delivery_service.reply", call_targets)
        self.assertNotIn("handle_send", call_targets)


if __name__ == "__main__":
    unittest.main()
