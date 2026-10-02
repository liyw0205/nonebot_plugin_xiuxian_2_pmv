import ast
from pathlib import Path
import unittest


class SchedulerFacadeLazyReaderTests(unittest.TestCase):
    def test_scheduler_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_scheduler/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertIn("_run_job(\"每日修仙签到重置\", _sql_message().sign_remake)", source)
        self.assertIn("_run_job(\"仙途奇缘重置\", _sql_message().beg_remake)", source)
        self.assertIn("_run_job(\"每日丹药使用次数重置\", _sql_message().day_num_reset)", source)
        self.assertIn("_run_job(\"每日炼丹次数重置\", _sql_message().mixelixir_num_reset)", source)

    def test_stone_limit_reset_is_gated_by_legacy_handler(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_scheduler/__init__.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(source)
        legacy_setting = next(
            node
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name)
                and target.id == "_LEGACY_STONE_GIFT_HANDLER_ENABLED"
                for target in node.targets
            )
        )
        job = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "daily_reset_stone_limits_job"
        )
        legacy_gate = next(
            node
            for node in job.body
            if isinstance(node, ast.If)
            and isinstance(node.test, ast.UnaryOp)
            and isinstance(node.test.op, ast.Not)
            and isinstance(node.test.operand, ast.Name)
            and node.test.operand.id == "_LEGACY_STONE_GIFT_HANDLER_ENABLED"
        )
        self.assertTrue(any(isinstance(node, ast.Return) for node in legacy_gate.body))
        reset_call = next(
            node
            for node in job.body
            if isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Await)
            and isinstance(node.value.value, ast.Call)
            and isinstance(node.value.value.func, ast.Name)
            and node.value.value.func.id == "_run_job"
        )
        self.assertLess(job.body.index(legacy_gate), job.body.index(reset_call))
        self.assertIn("XIUXIAN_STONE_GIFT_LEGACY_HANDLER", ast.get_source_segment(source, legacy_setting))
        self.assertIn("reset_stone_limits", ast.get_source_segment(source, job))


if __name__ == "__main__":
    unittest.main()
