from __future__ import annotations

import unittest
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect import sect_member_utils


class _TaskManager:
    def __init__(self, task):
        self.task = task

    def accept_task(self, user_id, sect_id, task_config):
        return dict(self.task)

    def get_active_task(self, user_id):
        return dict(self.task)


class SectTaskCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.task = {
            "任务名称": "试炼",
            "任务内容": {"type": 1, "cost": 0.2, "give": 0.1, "sect": 10},
            "sect_id": 1,
            "period": "2026-07-11",
            "status": "accepted",
            "progress": 0,
            "target": 1,
        }
        self.cache = {}
        self.patches = (
            patch.object(sect_member_utils, "sect_task_state_manager", _TaskManager(self.task)),
            patch.object(sect_member_utils, "userstask", self.cache),
            patch.object(
                sect_member_utils,
                "config",
                {"宗门任务": {"试炼": self.task["任务内容"]}},
            ),
        )
        for current_patch in self.patches:
            current_patch.start()

    def tearDown(self) -> None:
        for current_patch in reversed(self.patches):
            current_patch.stop()

    def test_accept_caches_period_required_by_settlement(self) -> None:
        task = sect_member_utils.create_user_sect_task("user", 1)

        self.assertEqual(task["period"], "2026-07-11")
        self.assertEqual(task["sect_id"], 1)

    def test_database_restore_caches_period_required_by_settlement(self) -> None:
        self.assertTrue(sect_member_utils.isUserTask("user"))
        self.assertEqual(self.cache["user"]["period"], "2026-07-11")


if __name__ == "__main__":
    unittest.main()
