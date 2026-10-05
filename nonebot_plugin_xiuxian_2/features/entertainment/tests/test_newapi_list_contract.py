from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
COMMANDS_PATH = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/newapi_commands.py"
STORE_PATH = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/newapi_store.py"


def _function(source: str, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    tree = ast.parse(source)
    return next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
    )


def _called_names(node: ast.AST) -> set[str]:
    return {
        call.func.id
        for call in ast.walk(node)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    }


def _referenced_names(node: ast.AST) -> set[str]:
    return {child.id for child in ast.walk(node) if isinstance(child, ast.Name)}


class NewApiListCommandContractTests(unittest.TestCase):
    def test_formatter_uses_only_feature_owned_summaries(self):
        source = COMMANDS_PATH.read_text(encoding="utf-8")
        formatter = _function(source, "_format_list_message")
        called = _called_names(formatter)

        self.assertIn("list_account_summaries", called)
        self.assertIn("escape_markdown_text", called)
        self.assertNotIn("load_accounts", called)
        self.assertIn("_MAX_NEWAPI_LIST_RESPONSE_BYTES", source)

    def test_handler_formats_and_sends_the_feature_owned_query_result(self):
        source = COMMANDS_PATH.read_text(encoding="utf-8")
        handler = _function(source, "newapi_list_")
        referenced = _referenced_names(handler)

        self.assertIn("_format_list_message", referenced)
        self.assertIn("run_blocking_io", _called_names(handler))
        self.assertIn("handle_send", referenced)

    def test_store_wrapper_calls_application_query_without_file_access(self):
        source = STORE_PATH.read_text(encoding="utf-8")
        wrapper = _function(source, "list_account_summaries")
        self.assertNotIn("open", _called_names(wrapper))
        self.assertIn("list_account_summaries", {
            node.attr
            for node in ast.walk(wrapper)
            if isinstance(node, ast.Attribute)
        })

    def test_checkin_history_wrapper_forwards_to_application_without_file_access(self):
        source = STORE_PATH.read_text(encoding="utf-8")
        wrapper = _function(source, "append_checkin_history")
        self.assertNotIn("open", _called_names(wrapper))
        self.assertIn("append_checkin_history", {
            node.attr
            for node in ast.walk(wrapper)
            if isinstance(node, ast.Attribute)
        })

    def test_info_handler_uses_feature_owned_targets_and_bounded_remote_helper(self):
        source = COMMANDS_PATH.read_text(encoding="utf-8")
        handler = _function(source, "newapi_info_")
        called = _called_names(handler)

        self.assertIn("resolve_info_targets", {
            node.attr
            for node in ast.walk(handler)
            if isinstance(node, ast.Attribute)
        })
        self.assertNotIn("resolve_targets", called)
        self.assertNotIn("load_accounts", called)
        self.assertIn("_MAX_NEWAPI_INFO_REPLY_BYTES", source)
        self.assertIn("_fetch_info_for_target", {
            node.id
            for node in ast.walk(handler)
            if isinstance(node, ast.Name)
        })
        fetcher = _function(source, "_fetch_info_for_target")
        self.assertIn("detect_auth_mode", _called_names(fetcher))
        self.assertIn("format_user_info_reply", called)


if __name__ == "__main__":
    unittest.main()
