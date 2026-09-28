from __future__ import annotations

import unittest
from unittest.mock import patch
from types import SimpleNamespace
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect import sect_member_utils


class _TaskManager:
    def __init__(self, task):
        self.task = task
        self.period = task["period"]

    def accept_task(self, user_id, sect_id, task_config):
        return dict(self.task)

    def get_active_task(self, user_id):
        return dict(self.task) if self.task is not None else None

    def current_task_period(self):
        return self.period

    def get_active_task(self, user_id):
        return dict(self.task) if self.task is not None else None

    def claim_task(self, operation_id, user_id, sect_id, task_config, daily_limit, *, replace_existing=False):
        self.claim_call = (operation_id, user_id, sect_id, daily_limit, replace_existing)
        key = list(task_config)[-1]
        return SimpleNamespace(
            applied=True, task_key=key, task_data=task_config[key], sect_id=sect_id,
            period=self.period, status="claimed",
        )

    def refresh_task(self, operation_id, user_id, sect_id, current_task, task_config, daily_limit):
        self.refresh_call = (operation_id, user_id, sect_id, daily_limit)
        key = list(task_config)[-1]
        return SimpleNamespace(
            applied=True, task_key=key, task_data=task_config[key], sect_id=sect_id,
            period=current_task["period"], status="claimed",
        )

    def accept_task(self, user_id, sect_id, task_config):
        return dict(self.task)


class SectTaskProjectionTests(unittest.TestCase):
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
        self.manager = _TaskManager(self.task)
        self.patches = (
            patch.object(sect_member_utils, "sect_application", self.manager),
            patch.object(
                sect_member_utils,
                "config",
                {"宗门任务": {"试炼": self.task["任务内容"]}, "每日宗门任务次上限": 3},
            ),
        )
        for current_patch in self.patches:
            current_patch.start()

    def tearDown(self) -> None:
        for current_patch in reversed(self.patches):
            current_patch.stop()

    def test_accept_returns_task_without_retaining_process_state(self) -> None:
        task = sect_member_utils.create_user_sect_task("user", 1)

        self.assertEqual(task["period"], "2026-07-11")
        self.assertEqual(task["sect_id"], 1)
        self.assertFalse(hasattr(sect_member_utils, "userstask"))

    def test_operation_aware_accept_uses_application_claim(self) -> None:
        task = sect_member_utils.create_user_sect_task("user", 1, "claim-op")

        self.assertEqual(task["任务名称"], "试炼")
        self.assertEqual(sect_member_utils.sect_application.claim_call, ("claim-op", "user", 1, 3, False))

    def test_refresh_uses_application_without_legacy_membership_service(self) -> None:
        task = sect_member_utils.refresh_user_sect_task("user", 1, "refresh-op")

        self.assertEqual(task["任务名称"], "试炼")
        self.assertEqual(sect_member_utils.sect_application.refresh_call, ("refresh-op", "user", 1, 3))

    def test_task_handlers_have_no_undefined_membership_service(self) -> None:
        package = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2"
        facade = (package / "xiuxian/xiuxian_sect/__init__.py").read_text(encoding="utf-8")
        helpers = (package / "xiuxian/xiuxian_sect/sect_member_utils.py").read_text(encoding="utf-8")

        self.assertNotIn("sect_membership_service", facade)
        self.assertNotIn("membership_service", helpers)
        self.assertIn("sect_application.claim_task(", helpers)
        self.assertIn("sect_application.refresh_task(", helpers)

    def test_task_query_returns_fresh_projection_without_retaining_it(self) -> None:
        self.assertTrue(sect_member_utils.isUserTask("user"))
        self.assertEqual(
            self.task,
            sect_member_utils.get_user_sect_task("user"),
        )
        self.manager.task = None
        self.assertFalse(sect_member_utils.isUserTask("user"))
        self.assertIsNone(sect_member_utils.get_user_sect_task("user"))
        self.assertFalse(hasattr(sect_member_utils, "userstask"))


if __name__ == "__main__":
    unittest.main()
