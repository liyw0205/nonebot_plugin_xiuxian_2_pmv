import ast
from pathlib import Path
import unittest


class UtilsLazyReaderTests(unittest.TestCase):
    def test_utils_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/utils.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().get_user_cd(", source)
        self.assertIn("_sql_message().get_user_info_with_id(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("player_data_manager = PlayerDataManager()", source)
        reader = next(node for node in ast.parse(source).body
                      if isinstance(node, ast.FunctionDef) and node.name == "get_statistics_data")
        bindings = [node for node in ast.walk(reader) if isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == "manager" for target in node.targets)]
        self.assertEqual([ast.unparse(node.value) for node in bindings], ["_player_data_manager()"])
        calls = {ast.unparse(node.func): node for node in ast.walk(reader) if isinstance(node, ast.Call)}
        self.assertEqual([ast.unparse(arg) for arg in calls["manager.get_field_data"].args],
                         ["str(user_id_for_stats)", "'statistics'", "key"])
        self.assertEqual([ast.unparse(arg) for arg in calls["manager.get_fields"].args],
                         ["str(user_id_for_stats)", "'statistics'"])
        self.assertEqual({kw.arg: ast.unparse(kw.value) for kw in calls["manager.get_fields"].keywords},
                         {"raise_errors": "raise_errors"})
        self.assertIn("_player_data_manager().update_or_write_data(", source)


if __name__ == "__main__":
    unittest.main()
