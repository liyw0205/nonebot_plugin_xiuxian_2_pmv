from __future__ import annotations

import ast
import copy
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
HELPERS = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/admin_helpers.py"


def _load_parser():
    tree = ast.parse(HELPERS.read_text(encoding="utf-8"))
    wanted = {"MarkdownTemplateInputError", "parse_markdown_template_args"}
    nodes = [copy.deepcopy(node) for node in tree.body if getattr(node, "name", None) in wanted]
    namespace = {
        "re": re,
        "MARKDOWN_TEMPLATE_MAX_INPUT_BYTES": 64 * 1024,
        "MARKDOWN_TEMPLATE_MAX_PARAMS": 32,
        "MARKDOWN_TEMPLATE_MAX_LIST_VALUES": 32,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(HELPERS), "exec"), namespace)
    return namespace["MarkdownTemplateInputError"], namespace["parse_markdown_template_args"]


MarkdownTemplateInputError, parse_markdown_template_args = _load_parser()


class AdminMarkdownTemplateBudgetTests(unittest.TestCase):
    def test_utf8_input_budget_is_checked_before_normalization(self):
        with self.assertRaises(MarkdownTemplateInputError) as raised:
            parse_markdown_template_args("mid=template k=" + "中" * (64 * 1024))
        self.assertEqual(raised.exception.reason, "input_bytes")

    def test_parameter_and_list_budgets_bound_materialized_work(self):
        many_params = "mid=template " + " ".join(f"k{index}=v" for index in range(33))
        with self.assertRaises(MarkdownTemplateInputError) as raised:
            parse_markdown_template_args(many_params)
        self.assertEqual(raised.exception.reason, "param_count")

        many_values = "mid=template values=[" + ",".join("v" for _ in range(33)) + "]"
        with self.assertRaises(MarkdownTemplateInputError) as raised:
            parse_markdown_template_args(many_values)
        self.assertEqual(raised.exception.reason, "list_count")

    def test_existing_alias_and_url_normalization_are_preserved(self):
        template_id, button_id, params = parse_markdown_template_args(
            r"mid=1 bid=2 link=[label](command)]",
            markdown_id="markdown-one",
            button_id2="button-two",
        )
        self.assertEqual((template_id, button_id), ("markdown-one", "button-two"))
        self.assertEqual(params[0]["key"], "link")
        self.assertEqual(
            params[0]["values"],
            ["label](mqqapi://aio/inlinecmd?command=command&enter=false&reply=false)"],
        )


if __name__ == "__main__":
    unittest.main()
