from __future__ import annotations

import asyncio
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import lay_out


class _Finished(BaseException):
    pass


class _PrivateEvent:
    to_me = False

    def __init__(self, user_id: str):
        self.user_id = user_id

    def get_user_id(self) -> str:
        return self.user_id


class _Matcher:
    plugin_name = ""
    module_name = ""
    commands: set[str] = set()

    def __init__(self):
        self.finish_calls = 0

    async def finish(self, *args, **kwargs):
        self.finish_calls += 1
        raise _Finished


class LayoutRateLimitStateTests(unittest.TestCase):
    def setUp(self) -> None:
        with lay_out._limit_all_data_lock:
            lay_out.limit_all_data.clear()
            lay_out._limit_all_capacity_warned = False

    def tearDown(self) -> None:
        with lay_out._limit_all_data_lock:
            lay_out.limit_all_data.clear()
            lay_out._limit_all_capacity_warned = False

    def test_global_rate_limit_warns_once_then_blocks_until_reset(self) -> None:
        with patch.object(lay_out, "limit_num", 2):
            self.assertIsNone(lay_out.limit_all_run("user-a"))
            self.assertIsNone(lay_out.limit_all_run("user-a"))
            self.assertIs(lay_out.limit_all_run("user-a"), True)
            self.assertIs(lay_out.limit_all_run("user-a"), False)

    def test_capacity_preserves_tracked_users_and_reset_releases_slots(self) -> None:
        with patch.object(lay_out, "_LIMIT_ALL_DATA_MAX_KEYS", 1):
            self.assertIsNone(lay_out.limit_all_run("user-a"))
            self.assertIs(lay_out.limit_all_run("user-b"), False)
            self.assertEqual(lay_out.limit_all_data, {"user-a": 1})
            self.assertIsNone(lay_out.limit_all_run("user-a"))

            with patch.object(lay_out.logger, "opt") as opt:
                opt.return_value.success = Mock()
                lay_out.limit_all_message_()

            self.assertIsNone(lay_out.limit_all_run("user-b"))

    def test_oversized_user_keys_are_not_retained(self) -> None:
        key = "u" * (lay_out._RATE_LIMIT_KEY_MAX_LENGTH + 1)

        self.assertIs(lay_out.limit_all_run(key), False)
        self.assertEqual(lay_out.limit_all_data, {})

    def test_reset_and_increment_are_safe_under_thread_contention(self) -> None:
        errors: list[BaseException] = []
        errors_lock = threading.Lock()

        def record_users() -> None:
            try:
                for index in range(1500):
                    lay_out.limit_all_run(f"user-{index % 128}")
            except BaseException as exc:
                with errors_lock:
                    errors.append(exc)

        def reset_window() -> None:
            try:
                for _ in range(100):
                    lay_out.limit_all_message_()
            except BaseException as exc:
                with errors_lock:
                    errors.append(exc)

        with patch.object(lay_out, "_LIMIT_ALL_DATA_MAX_KEYS", 64):
            with patch.object(lay_out.logger, "opt") as opt:
                opt.return_value.success = Mock()
                workers = [threading.Thread(target=record_users) for _ in range(4)]
                workers.append(threading.Thread(target=reset_window))
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join(timeout=3)

        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertEqual(errors, [])
        self.assertLessEqual(len(lay_out.limit_all_data), 64)
        self.assertTrue(all(isinstance(value, int) for value in lay_out.limit_all_data.values()))

    def test_cooldown_budget_releases_only_after_its_timer_completes(self) -> None:
        budget = lay_out._CooldownKeyBudget(max_keys=1)
        callbacks = []
        fake_loop = SimpleNamespace(
            time=lambda: 100.0,
            call_later=lambda delay, callback: callbacks.append(callback),
        )
        dependency = lay_out.Cooldown(cd_time=1).dependency

        async def invoke(user_id: str, matcher: _Matcher) -> None:
            await dependency(bot=SimpleNamespace(), matcher=matcher, event=_PrivateEvent(user_id))

        with (
            patch.object(lay_out, "_cooldown_key_budget", budget),
            patch.object(lay_out, "PrivateMessageEvent", _PrivateEvent),
            patch.object(lay_out, "patch_context", side_effect=lambda bot, event: (bot, event)),
            patch.object(
                lay_out,
                "XiuConfig",
                return_value=SimpleNamespace(at_response=False, admin_debug=False),
            ),
            patch.object(lay_out, "JsonConfig", return_value=SimpleNamespace(read_data=lambda: {"private": True})),
            patch.object(lay_out, "limit_all_run", return_value=None),
            patch.object(lay_out, "get_running_loop", return_value=fake_loop),
            patch.object(lay_out, "ADMIN_IDS", set()),
        ):
            first_matcher = _Matcher()
            asyncio.run(invoke("user-a", first_matcher))
            self.assertEqual(budget.active_keys, 1)
            self.assertEqual(first_matcher.finish_calls, 0)

            rejected_matcher = _Matcher()
            with self.assertRaises(_Finished):
                asyncio.run(invoke("user-b", rejected_matcher))
            self.assertEqual(rejected_matcher.finish_calls, 1)
            self.assertEqual(budget.active_keys, 1)

            callbacks[0]()
            callbacks[0]()
            self.assertEqual(budget.active_keys, 0)

            next_matcher = _Matcher()
            asyncio.run(invoke("user-b", next_matcher))
            self.assertEqual(next_matcher.finish_calls, 0)
            self.assertEqual(budget.active_keys, 1)
            callbacks[1]()
            self.assertEqual(budget.active_keys, 0)

    def test_shutdown_cancels_cooldown_timers_and_releases_budget(self) -> None:
        budget = lay_out._CooldownKeyBudget(max_keys=1)
        callbacks = []
        handles = []

        class _Handle:
            def __init__(self):
                self.cancelled = False

            def cancel(self):
                self.cancelled = True

        def call_later(_delay, callback):
            handle = _Handle()
            callbacks.append(callback)
            handles.append(handle)
            return handle

        fake_loop = SimpleNamespace(time=lambda: 100.0, call_later=call_later)
        dependency = lay_out.Cooldown(cd_time=60).dependency

        async def invoke() -> None:
            await dependency(
                bot=SimpleNamespace(),
                matcher=_Matcher(),
                event=_PrivateEvent("user-a"),
            )

        with (
            patch.object(lay_out, "_cooldown_key_budget", budget),
            patch.object(lay_out, "PrivateMessageEvent", _PrivateEvent),
            patch.object(lay_out, "patch_context", side_effect=lambda bot, event: (bot, event)),
            patch.object(
                lay_out,
                "XiuConfig",
                return_value=SimpleNamespace(at_response=False, admin_debug=False),
            ),
            patch.object(lay_out, "JsonConfig", return_value=SimpleNamespace(read_data=lambda: {"private": True})),
            patch.object(lay_out, "limit_all_run", return_value=None),
            patch.object(lay_out, "get_running_loop", return_value=fake_loop),
            patch.object(lay_out, "ADMIN_IDS", set()),
        ):
            asyncio.run(invoke())
            self.assertEqual(budget.active_keys, 1)
            self.assertEqual(len(handles), 1)

            lay_out._shutdown_cooldown_runtimes()

            self.assertTrue(handles[0].cancelled)
            self.assertEqual(budget.active_keys, 0)
            callbacks[0]()
            self.assertEqual(budget.active_keys, 0)

    def test_shutdown_is_idempotent_for_parallel_cooldown_state(self) -> None:
        budget = lay_out._CooldownKeyBudget(max_keys=2)
        callbacks = []
        handles = []

        class _Handle:
            def __init__(self):
                self.cancelled = False

            def cancel(self):
                self.cancelled = True

        def call_later(_delay, callback):
            handle = _Handle()
            callbacks.append(callback)
            handles.append(handle)
            return handle

        fake_loop = SimpleNamespace(time=lambda: 100.0, call_later=call_later)
        dependency = lay_out.Cooldown(cd_time=60, parallel=2).dependency

        async def invoke() -> None:
            await dependency(
                bot=SimpleNamespace(),
                matcher=_Matcher(),
                event=_PrivateEvent("user-a"),
            )

        with (
            patch.object(lay_out, "_cooldown_key_budget", budget),
            patch.object(lay_out, "PrivateMessageEvent", _PrivateEvent),
            patch.object(lay_out, "patch_context", side_effect=lambda bot, event: (bot, event)),
            patch.object(
                lay_out,
                "XiuConfig",
                return_value=SimpleNamespace(at_response=False, admin_debug=False),
            ),
            patch.object(lay_out, "JsonConfig", return_value=SimpleNamespace(read_data=lambda: {"private": True})),
            patch.object(lay_out, "limit_all_run", return_value=None),
            patch.object(lay_out, "get_running_loop", return_value=fake_loop),
            patch.object(lay_out, "ADMIN_IDS", set()),
        ):
            asyncio.run(invoke())
            asyncio.run(invoke())
            self.assertEqual(budget.active_keys, 1)
            self.assertEqual(len(handles), 2)

            # One timer may have fired before shutdown; the remaining timer
            # still owns the key and must be released exactly once.
            callbacks[0]()
            self.assertEqual(budget.active_keys, 1)
            lay_out._shutdown_cooldown_runtimes()
            lay_out._shutdown_cooldown_runtimes()

            self.assertFalse(handles[0].cancelled)
            self.assertTrue(handles[1].cancelled)
            self.assertEqual(budget.active_keys, 0)
            callbacks[1]()
            self.assertEqual(budget.active_keys, 0)


if __name__ == "__main__":
    unittest.main()
