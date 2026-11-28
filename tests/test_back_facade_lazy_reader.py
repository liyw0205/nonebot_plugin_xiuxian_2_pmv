import ast
from pathlib import Path
import unittest
from unittest.mock import Mock


class BackFacadeLazyReaderTests(unittest.TestCase):
    def test_back_facade_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_back/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().get_back_msg(", source)
        self.assertIn("_sql_message().goods_num(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        accessor = next(node for node in ast.parse(source).body
                        if isinstance(node, ast.FunctionDef) and node.name == "_sql_message")
        manager = object()
        factory = Mock(return_value=manager)
        namespace = {"_sql_message_instance": None, "XiuxianDateManage": factory}
        exec(compile(ast.Module(body=[accessor], type_ignores=[]), "<lazy-reader>", "exec"), namespace)
        factory.assert_not_called()
        self.assertIs(manager, namespace["_sql_message"]())
        self.assertIs(manager, namespace["_sql_message"]())
        factory.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
