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


if __name__ == "__main__":
    unittest.main()
