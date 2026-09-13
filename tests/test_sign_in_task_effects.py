from __future__ import annotations

import unittest
from unittest.mock import Mock

from nonebot_plugin_xiuxian_2.features.sign_in.tasks import SignInTaskEffects


class SignInTaskEffectsTests(unittest.TestCase):
    def test_maps_committed_sign_in_to_idempotent_task_operation(self) -> None:
        record = Mock(return_value=["完成签到任务"])
        effects = SignInTaskEffects(record)

        self.assertEqual(effects.record(user_id="u1", operation_id="sign-1"), ["完成签到任务"])
        record.assert_called_once_with("u1", "sign_in", 1, operation_id="task-progress:sign-1")


if __name__ == "__main__":
    unittest.main()
