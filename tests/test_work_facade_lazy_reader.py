from pathlib import Path
import unittest


class WorkFacadeLazyReaderTests(unittest.TestCase):
    def test_work_facade_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_work/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().get_user_cd(", source)
        self.assertIn("_sql_message().get_work_num(", source)
        self.assertIn("update_last_check_info_time(", source)
        self.assertNotIn("_sql_message().update_last_check_info_time(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)

    def test_status_and_delayed_reminder_use_the_work_status_application(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_work/__init__.py"
        ).read_text(encoding="utf-8")
        status_reader = source.split("def get_user_work_status(", 1)[1].split(
            "async def get_work_status_message", 1
        )[0]
        reminder = source.split("async def delayed_reminder(", 1)[1].split(
            "__work_help__", 1
        )[0]

        self.assertIn("work_status_application.get_user_work_status(user_id)", status_reader)
        self.assertNotIn("_sql_message()", status_reader)
        self.assertIn("status, work_data = get_user_work_status(user_id)", reminder)
        self.assertIn("if status == 3:", reminder)
        self.assertNotIn("has_unaccepted_work(", reminder)

    def test_facade_uses_central_offer_reader_instead_of_direct_json_reads(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_work/__init__.py"
        ).read_text(encoding="utf-8")

        self.assertNotIn("readf(", source)
        self.assertEqual(source.count("work_status_application.get_offer(user_id)"), 4)
        self.assertIn(
            "active_snapshot\n        if active_snapshot is not None\n        else work_status_application.get_offer(user_id)",
            source,
        )


if __name__ == "__main__":
    unittest.main()
