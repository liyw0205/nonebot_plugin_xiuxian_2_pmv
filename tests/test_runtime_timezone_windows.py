"""Timezone-frame regressions for the three live naive/aware crashes.

Production (xiu2.log 12:12 / 17:01-20:51):

* ``开始历练`` — ``TypeError: can't subtract offset-naive and offset-aware
  datetimes`` (training hour window).
* ``日常`` 灵田行 — ``TypeError: can't compare offset-naive and offset-aware
  datetimes`` (mix_elixir 收取时间 vs runtime clock).
* ``check_auction_end_job`` 每 5 分钟 — same comparison error (auction session
  ``end_time`` epoch vs aware clock).

Convention pinned here: DB text timestamps are naive-UTC wall clock, the runtime
clock is aware UTC, and stored epochs are UTC seconds.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from nonebot_plugin_xiuxian_2.features.auction.domain import (
    session_end_datetime,
    session_window,
)
from nonebot_plugin_xiuxian_2.features.mixelixir.harvest_rules import field_status_text
from nonebot_plugin_xiuxian_2.features.training.hourly_window import (
    to_utc,
    training_hour_window,
)

CST = timezone(timedelta(hours=8))


class TrainingHourWindowTests(unittest.TestCase):
    def test_naive_stored_time_and_aware_clock_share_one_hour(self) -> None:
        window = training_hour_window(
            last_time=datetime(2026, 11, 8, 10, 0, 0),  # naive UTC wall clock
            now=datetime(2026, 11, 8, 10, 30, tzinfo=timezone.utc),
        )

        self.assertTrue(window["in_same_hour"])
        self.assertEqual(
            datetime(2026, 11, 8, 11, 0, tzinfo=timezone.utc), window["next_time"]
        )
        self.assertEqual(30, window["wait_minutes"])

    def test_offset_aware_stored_time_compares_on_the_utc_frame(self) -> None:
        window = training_hour_window(
            last_time=datetime(2026, 11, 8, 18, 5, tzinfo=CST),  # == 10:05 UTC
            now=datetime(2026, 11, 8, 10, 30, tzinfo=timezone.utc),
        )

        self.assertTrue(window["in_same_hour"])
        self.assertEqual(30, window["wait_minutes"])

    def test_hour_boundary_is_not_the_same_window(self) -> None:
        window = training_hour_window(
            last_time=datetime(2026, 11, 8, 10, 59, 59),
            now=datetime(2026, 11, 8, 11, 0, tzinfo=timezone.utc),
        )

        self.assertFalse(window["in_same_hour"])
        self.assertEqual(0, window["wait_minutes"])
        self.assertEqual("现在", window["next_time_local"])

    def test_cross_midnight_gap_is_not_the_same_window(self) -> None:
        window = training_hour_window(
            last_time=datetime(2026, 11, 7, 23, 30),
            now=datetime(2026, 11, 8, 0, 10, tzinfo=timezone.utc),
        )

        self.assertFalse(window["in_same_hour"])

    def test_missing_and_unusable_values_allow_training(self) -> None:
        for last_time in (None, "", "2026-11-08 10:00:00", 0):
            window = training_hour_window(
                last_time=last_time,
                now=datetime(2026, 11, 8, 10, 30, tzinfo=timezone.utc),
            )
            self.assertFalse(window["in_same_hour"], last_time)
            self.assertEqual(0, window["wait_minutes"])

    def test_wait_minutes_never_negative_and_local_label_matches(self) -> None:
        window = training_hour_window(
            last_time=datetime(2026, 11, 8, 10, 59, 30),
            now=datetime(2026, 11, 8, 10, 59, 45, tzinfo=timezone.utc),
        )

        self.assertTrue(window["in_same_hour"])
        self.assertEqual(0, window["wait_minutes"])
        self.assertEqual(
            window["next_time"].astimezone().strftime("%H:%M"),
            window["next_time_local"],
        )

    def test_to_utc_normalizes_both_frames(self) -> None:
        self.assertEqual(
            datetime(2026, 11, 8, 10, 0, tzinfo=timezone.utc),
            to_utc(datetime(2026, 11, 8, 10, 0)),
        )
        self.assertEqual(
            datetime(2026, 11, 8, 10, 0, tzinfo=timezone.utc),
            to_utc(datetime(2026, 11, 8, 18, 0, tzinfo=CST)),
        )
        self.assertIsNone(to_utc("2026-11-08 10:00:00"))


class AuctionSessionWindowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.end = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)
        self.session = {"end_time": self.end.timestamp()}

    def test_running_session_reports_remaining_seconds(self) -> None:
        window = session_window(
            self.session, datetime(2026, 11, 8, 11, 30, tzinfo=timezone.utc)
        )

        self.assertFalse(window["expired"])
        self.assertEqual(1800, window["remaining_seconds"])
        self.assertEqual(self.end, window["end_time"])

    def test_expired_session_at_and_after_the_boundary(self) -> None:
        for now in (
            datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 8, 12, 30, tzinfo=timezone.utc),
        ):
            window = session_window(self.session, now)
            self.assertTrue(window["expired"], now)
            self.assertEqual(0, window["remaining_seconds"])

    def test_reconcile_and_end_check_agree_on_the_same_clock_frame(self) -> None:
        now = datetime(2026, 11, 8, 11, 59, tzinfo=CST)
        end_dt = session_end_datetime(self.session["end_time"], now)

        self.assertIsNotNone(end_dt.tzinfo)
        self.assertEqual(now.utcoffset(), end_dt.utcoffset())
        self.assertEqual(self.end, end_dt)

    def test_naive_clock_keeps_local_semantics_without_raising(self) -> None:
        now = datetime(2026, 11, 8, 11, 30)
        window = session_window(self.session, now)

        self.assertIsNone(window["end_time"].tzinfo)
        # The comparison happens on one frame (local wall clock), so no
        # naive/aware TypeError; the end instant is the local rendering of the
        # stored UTC epoch.
        self.assertEqual(self.end.astimezone().replace(tzinfo=None), window["end_time"])
        self.assertFalse(window["expired"])
        self.assertEqual(
            int((window["end_time"] - now).total_seconds()), window["remaining_seconds"]
        )


class DailyFieldStatusTests(unittest.TestCase):
    def test_naive_utc_stored_time_with_aware_clock(self) -> None:
        # 收取时间 2026-11-08 10:00 + 23h → 2026-11-09 09:00.
        self.assertEqual(
            "⏳ 1时0分后",
            field_status_text(
                "2026-11-08 10:00:00", datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)
            ),
        )

    def test_mature_at_the_boundary_and_after(self) -> None:
        for now in (
            datetime(2026, 11, 9, 9, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc),
        ):
            self.assertEqual(
                "🌱 已成熟", field_status_text("2026-11-08 10:00:00", now)
            )

    def test_offset_aware_clock_is_normalized_to_the_stored_frame(self) -> None:
        self.assertEqual(
            field_status_text(
                "2026-11-08 10:00:00", datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)
            ),
            field_status_text(
                "2026-11-08 10:00:00", datetime(2026, 11, 9, 16, 0, tzinfo=CST)
            ),
        )

    def test_missing_zero_and_garbage_never_claim_maturity(self) -> None:
        now = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        for value in (None, "", "0", 0, "not-a-time", "2026-13-40 99:99:99"):
            self.assertEqual("⬜ 未开启", field_status_text(value, now), value)

    def test_datetime_value_is_accepted(self) -> None:
        self.assertEqual(
            "🌱 已成熟",
            field_status_text(
                datetime(2026, 11, 8, 10, 0), datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
            ),
        )


if __name__ == "__main__":
    unittest.main()
