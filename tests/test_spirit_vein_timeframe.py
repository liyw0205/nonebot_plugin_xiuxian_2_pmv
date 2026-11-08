"""灵脉时间帧回归：库内 naive 墙钟 vs `_now()` aware UTC。

线上 `/root/xiu2` 重启（23:19:51）后，23:39:35 的「虚神界出关」失败链是：

    xiuxian_impart_pk.impart_pk_out_closing_
      → xiuxian_utils.spirit_vein.apply_spirit_vein_exp_bonus
      → xiuxian_world_events.get_spirit_vein_exp_multiplier
      → _is_spirit_vein_active
      → _ensure_spirit_vein_state
      → `now >= ends_at`
    TypeError: can't compare offset-naive and offset-aware datetimes

`ends_at` 由 `_format_time`（strftime）落库时丢掉 tz，读回是 naive；`_now()` 是
aware UTC。本套件锁定修正后的单一时间帧，并覆盖调度任务入口与结算法调用方。
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import spirit_vein as spirit_vein_utils
import nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events as world_events

NOW = datetime(2026, 11, 8, 23, 39, 35, tzinfo=timezone.utc)


def _state(**overrides) -> dict:
    state = {
        "active": 0,
        "status": "idle",
        "event_id": "",
        "event_type": "spirit_vein",
        "name": "天降灵脉",
        "period": "2026-11-08",
        "manual": 0,
        "bosses": {},
        "participants": {},
        "claimed": {},
        "started_at": "",
        "ends_at": "",
        "last_result": "",
    }
    state.update(overrides)
    return state


def _active(ends_at: str) -> dict:
    return _state(
        active=1,
        status="active",
        event_id="spirit_vein:202611082330",
        started_at="2026-11-08 23:30:00",
        ends_at=ends_at,
        last_result="天降灵脉已开启。",
    )


class ParseTimeFrameTests(unittest.TestCase):
    def test_naive_stored_wall_clock_becomes_aware_utc(self) -> None:
        parsed = world_events._parse_time("2026-11-08 23:30:00")

        self.assertIsNotNone(parsed)
        self.assertIsNotNone(parsed.tzinfo)
        self.assertEqual(parsed, datetime(2026, 11, 8, 23, 30, tzinfo=timezone.utc))

    def test_fractional_and_aware_inputs_land_in_utc(self) -> None:
        self.assertEqual(
            datetime(2026, 11, 8, 23, 30, 0, 500000, tzinfo=timezone.utc),
            world_events._parse_time("2026-11-08 23:30:00.500000"),
        )
        aware = datetime(2026, 11, 9, 7, 30, tzinfo=timezone(timedelta(hours=8)))
        self.assertEqual(
            datetime(2026, 11, 8, 23, 30, tzinfo=timezone.utc),
            world_events._parse_time(aware),
        )

    def test_bad_values_stay_none(self) -> None:
        for bad in (None, "", "not-a-time", 0):
            with self.subTest(bad=bad):
                self.assertIsNone(world_events._parse_time(bad))


class SpiritVeinActiveTests(unittest.TestCase):
    def _patched(self, state: dict):
        return patch.object(world_events, "_load_state", return_value=dict(state))

    def test_active_window_inside_the_frame(self) -> None:
        with self._patched(_active("2026-11-08 23:50:00")):
            self.assertTrue(world_events._is_spirit_vein_active(NOW))

    def test_expired_window_is_inactive_without_type_error(self) -> None:
        with self._patched(_active("2026-11-08 23:35:00")), patch.object(
            world_events, "spirit_vein_lifecycle_application"
        ) as lifecycle:
            lifecycle.replay.return_value = None
            lifecycle.transition.return_value = SimpleNamespace(
                state=_state(status="finished"), status="applied"
            )

            self.assertFalse(world_events._is_spirit_vein_active(NOW))

            lifecycle.transition.assert_called_once()
            applied_target = lifecycle.transition.call_args[0][4]
            self.assertEqual("finished", applied_target["status"])
            self.assertEqual(0, applied_target["active"])

    def test_multiplier_and_messages_follow_the_same_gate(self) -> None:
        with self._patched(_active("2026-11-08 23:50:00")):
            self.assertGreater(world_events.get_spirit_vein_exp_multiplier(), 1.0)
            self.assertIn("灵脉加成", world_events.get_spirit_vein_exp_bonus_msg())
        with self._patched(_state(status="finished")), patch.object(
            world_events, "spirit_vein_lifecycle_application"
        ):
            self.assertEqual(1.0, world_events.get_spirit_vein_exp_multiplier())
            self.assertEqual("", world_events.get_spirit_vein_exp_bonus_msg())

    def test_ensure_state_expires_stale_active_state_once(self) -> None:
        stale = _active("2026-11-08 23:35:00")
        with self._patched(stale), patch.object(
            world_events, "spirit_vein_lifecycle_application"
        ) as lifecycle:
            lifecycle.replay.return_value = None
            lifecycle.transition.return_value = SimpleNamespace(
                state=_state(status="finished"), status="applied"
            )

            state = world_events._ensure_spirit_vein_state(NOW)

            self.assertEqual("finished", state["status"])
            # 回执键包含到期时刻，重复调用不会重复结算
            operation_id = lifecycle.transition.call_args[0][0]
            self.assertIn("expire", operation_id)
            self.assertIn("20261108233500", operation_id)

    def test_replay_short_circuits_expiry(self) -> None:
        with self._patched(_active("2026-11-08 23:35:00")), patch.object(
            world_events, "spirit_vein_lifecycle_application"
        ) as lifecycle:
            lifecycle.replay.return_value = SimpleNamespace(
                state=_state(status="finished"), action="expire"
            )

            state = world_events._ensure_spirit_vein_state(NOW)

            self.assertEqual("finished", state["status"])
            lifecycle.transition.assert_not_called()


class ScheduledJobEntryTests(unittest.TestCase):
    """`spirit_vein_schedule_job` 每小时 :30 走同一入口。"""

    def test_scheduled_job_entry_never_raises_on_stale_state(self) -> None:
        stale = _active("2026-11-08 23:35:00")
        with patch.object(world_events, "_load_state", return_value=dict(stale)), patch.object(
            world_events, "spirit_vein_lifecycle_application"
        ) as lifecycle, patch.object(
            world_events, "runtime_random"
        ) as rng, patch.object(
            world_events, "_now", return_value=NOW
        ):
            lifecycle.replay.return_value = None
            lifecycle.transition.return_value = SimpleNamespace(
                state=_state(status="finished"), status="applied", action="auto_skip"
            )
            rng.random.return_value = 1.0

            state, log_text = world_events._try_start_auto_spirit_vein()

            self.assertIsInstance(log_text, str)
            self.assertIn("天降灵脉", log_text)
            self.assertIn("status", state)

    def test_real_scheduler_job_callable_is_frame_safe(self) -> None:
        """直接调用 apscheduler 注册的协程函数本身（线上 raised an exception 的入口）。"""
        import asyncio

        stale = _active("2026-11-08 23:35:00")
        with patch.object(world_events, "_load_state", return_value=dict(stale)), patch.object(
            world_events, "spirit_vein_lifecycle_application"
        ) as lifecycle, patch.object(
            world_events, "runtime_random"
        ) as rng, patch.object(
            world_events, "_now", return_value=NOW
        ), patch.object(
            world_events, "logger"
        ) as logger:
            lifecycle.replay.return_value = None
            lifecycle.transition.return_value = SimpleNamespace(
                state=_state(status="finished"), status="applied", action="auto_skip"
            )
            rng.random.return_value = 1.0

            asyncio.run(world_events.spirit_vein_schedule_job())

            logger.info.assert_called_once()
            logged = logger.info.call_args[0][0]
            self.assertIn("天降灵脉", logged)


class ClosingChainCallerTests(unittest.TestCase):
    """结算法 `apply_spirit_vein_exp_bonus` 是线上崩溃的调用方。"""

    def test_closing_handler_uses_the_same_bonus_helper(self) -> None:
        """handler 里 `_apply_spirit_vein_exp_bonus` 就是 spirit_vein 的实现。"""
        import nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk as impart_pk

        self.assertIs(
            impart_pk._apply_spirit_vein_exp_bonus,
            spirit_vein_utils.apply_spirit_vein_exp_bonus,
        )

    def test_closing_handler_symbol_runs_the_real_chain(self) -> None:
        """通过 handler 实际使用的符号执行整链，不再出现 naive/aware 崩溃。"""
        import nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk as impart_pk

        with patch.object(
            world_events, "_load_state", return_value=_active("2026-11-08 23:50:00")
        ), patch.object(world_events, "_now", return_value=NOW):
            exp, msg = impart_pk._apply_spirit_vein_exp_bonus(1000, 5000)

            self.assertEqual(1200, exp)
            self.assertIn("灵脉加成", msg)

    def test_bonus_is_applied_when_vein_active(self) -> None:
        with patch.object(
            spirit_vein_utils, "get_spirit_vein_exp_multiplier", return_value=1.2
        ), patch.object(spirit_vein_utils, "get_spirit_vein_exp_bonus_msg", return_value="\n天降灵脉加成：修为+20%"):
            exp, msg = spirit_vein_utils.apply_spirit_vein_exp_bonus(1000, 5000)

            self.assertEqual(1200, exp)
            self.assertIn("灵脉加成", msg)

    def test_bonus_respects_cap_and_never_goes_negative(self) -> None:
        with patch.object(
            spirit_vein_utils, "get_spirit_vein_exp_multiplier", return_value=1.2
        ), patch.object(spirit_vein_utils, "get_spirit_vein_exp_bonus_msg", return_value=""):
            self.assertEqual((5000, ""), spirit_vein_utils.apply_spirit_vein_exp_bonus(4999, 5000))
            self.assertEqual((0, ""), spirit_vein_utils.apply_spirit_vein_exp_bonus(-50, 5000))

    def test_bonus_keeps_values_beyond_int64_exact(self) -> None:
        """加成后可能超过 int64：保持 Python int 精度，不截断、不 clamp。"""
        huge = 10**19
        with patch.object(
            spirit_vein_utils, "get_spirit_vein_exp_multiplier", return_value=1.2
        ), patch.object(spirit_vein_utils, "get_spirit_vein_exp_bonus_msg", return_value=""):
            exp, _ = spirit_vein_utils.apply_spirit_vein_exp_bonus(huge, None)

            self.assertEqual(int(huge * 1.2), exp)
            self.assertGreater(exp, 2**63 - 1)

    def test_end_to_end_with_real_state_read_is_frame_safe(self) -> None:
        """真实 `_is_spirit_vein_active` + 加成：不再抛 naive/aware TypeError。"""
        with patch.object(
            world_events, "_load_state", return_value=_active("2026-11-08 23:50:00")
        ), patch.object(world_events, "_now", return_value=NOW):
            exp, msg = spirit_vein_utils.apply_spirit_vein_exp_bonus(1000, 5000)

            self.assertGreater(exp, 1000)
            self.assertIn("灵脉加成", msg)
            self.assertEqual(1.2, round(world_events.get_spirit_vein_exp_multiplier(), 2))


if __name__ == "__main__":
    unittest.main()
