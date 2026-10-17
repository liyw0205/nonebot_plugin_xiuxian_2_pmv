"""NewAPI checkin history recovery: one local retry, never a remote replay.

The Phase 2 backlog ``newapi-checkin-remote-history-window`` records that a
process crash between the remote check-in POST and the local history write
leaves the remote effect without a local record. The remote service answers a
duplicate same-day check-in with an explicit already-signed message, but the
compensation here only retries the LOCAL history write, so it stays free of
remote side effects. Written as ``unittest.TestCase`` because the CI quality
job collects tests with ``python -m unittest discover``.
"""

from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.entertainment.schemas import (
    NewApiCheckinTarget,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod import (
    newapi_commands,
)


def _target() -> NewApiCheckinTarget:
    return NewApiCheckinTarget(
        index=1,
        api_user_id="10086",
        secret="token",
        base_url="https://api.example.com",
        mode="token",
    )


def _success_data() -> dict:
    return {"success": True, "message": "签到成功", "quota_awarded": 100}


class NewApiCheckinHistoryRecoveryTests(unittest.TestCase):
    def _run(self) -> tuple[int, dict]:
        return newapi_commands._run_checkin_for_target("u1", _target())

    def test_history_retry_after_first_failure_records_without_warning(self) -> None:
        """First history write fails, the compensation retry succeeds."""
        calls = []

        def flaky_append(*args, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise sqlite3.OperationalError("database is locked")

        with (
            patch.object(newapi_commands, "do_checkin", return_value=_success_data()),
            patch.object(newapi_commands, "append_checkin_history", side_effect=flaky_append),
        ):
            index, data = newapi_commands._run_checkin_for_target("u1", _target())

        self.assertEqual(index, 1)
        self.assertNotIn("_history_warning", data)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["account_index"], 1)

    def test_double_failure_keeps_the_checkin_result_and_a_categorized_warning(self) -> None:
        calls = []

        def failing_append(*args, **kwargs):
            calls.append(kwargs)
            raise sqlite3.OperationalError("database unavailable")

        with (
            patch.object(newapi_commands, "do_checkin", return_value=_success_data()),
            patch.object(newapi_commands, "append_checkin_history", side_effect=failing_append),
        ):
            _, data = newapi_commands._run_checkin_for_target("u1", _target())

        self.assertEqual(len(calls), 2)
        warning = data.get("_history_warning", "")
        self.assertIn("数据库不可用", warning)
        self.assertIn("签到结果不受影响", warning)
        # The checkin result itself is untouched.
        self.assertTrue(data["success"])

    def test_non_database_failure_uses_the_writable_category(self) -> None:
        with (
            patch.object(newapi_commands, "do_checkin", return_value=_success_data()),
            patch.object(
                newapi_commands,
                "append_checkin_history",
                side_effect=OSError("disk full"),
            ),
        ):
            _, data = newapi_commands._run_checkin_for_target("u1", _target())

        self.assertIn("历史不可写", data.get("_history_warning", ""))

    def test_successful_history_write_is_never_retried(self) -> None:
        calls = []

        def good_append(*args, **kwargs):
            calls.append(kwargs)

        with (
            patch.object(newapi_commands, "do_checkin", return_value=_success_data()),
            patch.object(newapi_commands, "append_checkin_history", side_effect=good_append),
        ):
            _, data = newapi_commands._run_checkin_for_target("u1", _target())

        self.assertEqual(len(calls), 1)
        self.assertNotIn("_history_warning", data)

    def test_remote_error_still_records_the_failure_summary(self) -> None:
        captured = {}

        def good_append(qq_id, **kwargs):
            captured["summary"] = kwargs["summary"]

        with (
            patch.object(
                newapi_commands,
                "do_checkin",
                return_value={"_error": "连接超时"},
            ),
            patch.object(newapi_commands, "append_checkin_history", side_effect=good_append),
        ):
            _, data = newapi_commands._run_checkin_for_target("u1", _target())

        self.assertNotIn("_history_warning", data)
        self.assertIn("失败", captured["summary"])

    def test_compensation_never_replays_the_remote_post(self) -> None:
        """The retry path calls the local history writer, not do_checkin."""
        checkin_calls = []

        def flaky_append(*args, **kwargs):
            if len(checkin_calls) < 2:
                raise sqlite3.OperationalError("locked")

        def counting_checkin(*args, **kwargs):
            checkin_calls.append(args)
            return _success_data()

        with (
            patch.object(newapi_commands, "do_checkin", side_effect=counting_checkin),
            patch.object(newapi_commands, "append_checkin_history", side_effect=flaky_append),
        ):
            newapi_commands._run_checkin_for_target("u1", _target())

        self.assertEqual(len(checkin_calls), 1)


if __name__ == "__main__":
    unittest.main()
