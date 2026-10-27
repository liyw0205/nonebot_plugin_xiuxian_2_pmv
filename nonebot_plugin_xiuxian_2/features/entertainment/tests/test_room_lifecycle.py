"""Room lifetime rules: expiry boundaries, anchors and the per-type grouping.

These pin the pure rules the restart-restore path uses to decide whether a
persisted room is still live or must be released. The runtime constants are
asserted against the game modules so the copies cannot drift silently.

Written as ``unittest.TestCase`` classes because the CI quality job collects
tests with ``python -m unittest discover``.
"""

from __future__ import annotations

import unittest
from datetime import datetime

from ..room_lifecycle import (
    GOMOKU_MOVE_TIMEOUT_SECONDS,
    GOMOKU_ROOM_TIMEOUT_SECONDS,
    HALF_TEN_ROOM_TIMEOUT_SECONDS,
    MINESWEEPER_GAME_TIMEOUT_SECONDS,
    expired_rooms,
    is_expired,
    remaining_seconds,
    room_deadline,
    timeout_seconds,
)

NOW = datetime(2026, 10, 27, 12, 0, 0)


def gomoku(**overrides):
    state = {
        "room_id": "g1",
        "status": "waiting",
        "create_time": "2026-10-27 11:55:00",
        "last_move_time": None,
    }
    state.update(overrides)
    return state


def half_ten(**overrides):
    state = {
        "room_id": "h1",
        "status": "waiting",
        "create_time": "2026-10-27 11:55:00",
    }
    state.update(overrides)
    return state


def minesweeper(**overrides):
    state = {
        "game_id": "m1",
        "status": "playing",
        "create_time": "2026-10-27 11:00:00",
        "last_action_time": "2026-10-27 11:55:00",
    }
    state.update(overrides)
    return state


class TimeoutAnchorTests(unittest.TestCase):
    def test_waiting_gomoku_uses_the_join_clock(self) -> None:
        state = gomoku(create_time="2026-10-27 11:55:00")
        self.assertEqual(GOMOKU_ROOM_TIMEOUT_SECONDS, 180)
        self.assertEqual(timeout_seconds("gomoku", state), 180)
        self.assertEqual(room_deadline("gomoku", state), datetime(2026, 10, 27, 11, 58, 0))

    def test_playing_gomoku_uses_the_move_clock(self) -> None:
        state = gomoku(
            status="playing",
            create_time="2026-10-27 10:00:00",
            last_move_time="2026-10-27 11:58:30",
        )
        self.assertEqual(GOMOKU_MOVE_TIMEOUT_SECONDS, 120)
        self.assertEqual(timeout_seconds("gomoku", state), 120)
        self.assertEqual(room_deadline("gomoku", state), datetime(2026, 10, 27, 12, 0, 30))

    def test_half_ten_uses_the_create_clock(self) -> None:
        self.assertEqual(HALF_TEN_ROOM_TIMEOUT_SECONDS, 180)
        self.assertEqual(
            room_deadline("half_ten", half_ten()), datetime(2026, 10, 27, 11, 58, 0)
        )

    def test_minesweeper_uses_the_action_clock_not_the_create_clock(self) -> None:
        """A long game must be measured from the last action, not its start."""
        state = minesweeper(
            create_time="2026-10-27 10:00:00",
            last_action_time="2026-10-27 11:58:00",
        )
        self.assertEqual(MINESWEEPER_GAME_TIMEOUT_SECONDS, 600)
        self.assertEqual(room_deadline("minesweeper", state), datetime(2026, 10, 27, 12, 8, 0))

    def test_minesweeper_falls_back_to_create_time_when_action_is_absent(self) -> None:
        state = minesweeper(create_time="2026-10-27 11:55:00", last_action_time=None)
        self.assertEqual(room_deadline("minesweeper", state), datetime(2026, 10, 27, 12, 5, 0))


class NonCandidateTests(unittest.TestCase):
    def test_settled_and_closed_states_never_time_out(self) -> None:
        for game_type, status in (
            ("gomoku", "finished"),
            ("half_ten", "finished"),
            ("half_ten", "closed"),
            ("minesweeper", "win"),
            ("minesweeper", "lose"),
            ("minesweeper", "closed"),
        ):
            with self.subTest(game_type=game_type, status=status):
                state = {
                    "room_id": "r",
                    "game_id": "r",
                    "status": status,
                    "create_time": "2000-01-01 00:00:00",
                    "last_move_time": "2000-01-01 00:00:00",
                    "last_action_time": "2000-01-01 00:00:00",
                }
                self.assertIsNone(timeout_seconds(game_type, state))
                self.assertIsNone(room_deadline(game_type, state))
                self.assertFalse(is_expired(game_type, state, NOW))
                self.assertIsNone(remaining_seconds(game_type, state, NOW))

    def test_unknown_game_type_is_not_a_candidate(self) -> None:
        state = gomoku()
        self.assertIsNone(timeout_seconds("chess", state))
        self.assertFalse(is_expired("chess", state, NOW))

    def test_missing_or_malformed_timestamps_are_never_expired(self) -> None:
        for value in (None, "", "   ", "not-a-time", "2026/10/27 12:00:00", 12345):
            with self.subTest(value=value):
                state = gomoku(create_time=value)
                self.assertIsNone(room_deadline("gomoku", state))
                self.assertFalse(is_expired("gomoku", state, NOW))

    def test_gomoku_falls_back_to_create_time_when_move_time_is_missing(self) -> None:
        """A room restored mid-play with no move clock still has a usable anchor.

        ``playing`` keeps the MOVE timeout (120s), so the fallback anchor is the
        create clock plus two minutes, not the join-clock three minutes.
        """
        state = gomoku(
            status="playing",
            create_time="2026-10-27 11:55:00",
            last_move_time=None,
        )
        self.assertEqual(room_deadline("gomoku", state), datetime(2026, 10, 27, 11, 57, 0))


class ExpiryBoundaryTests(unittest.TestCase):
    def test_a_room_exactly_at_its_deadline_is_not_yet_expired(self) -> None:
        """The live task sleeps the whole timeout, so only a later instant expires."""
        state = gomoku(create_time="2026-10-27 11:57:00")
        deadline = datetime(2026, 10, 27, 12, 0, 0)
        self.assertEqual(room_deadline("gomoku", state), deadline)
        self.assertFalse(is_expired("gomoku", state, deadline))
        self.assertEqual(remaining_seconds("gomoku", state, deadline), 0)

    def test_one_second_past_the_deadline_expires(self) -> None:
        state = gomoku(create_time="2026-10-27 11:57:00")
        self.assertTrue(is_expired("gomoku", state, datetime(2026, 10, 27, 12, 0, 1)))

    def test_stale_room_from_a_previous_run_is_expired(self) -> None:
        """Three hours old, against a 180s window."""
        state = gomoku(create_time="2026-10-27 09:00:00")
        self.assertTrue(is_expired("gomoku", state, NOW))

    def test_remaining_time_is_a_remainder_not_a_fresh_timeout(self) -> None:
        """A restart must not hand the player a full new window."""
        state = gomoku(create_time="2026-10-27 11:59:00")
        # join clock 11:59:00 + 180s = 12:02:00; at 12:00:00 two minutes are left
        self.assertEqual(remaining_seconds("gomoku", state, NOW), 120)
        self.assertNotEqual(remaining_seconds("gomoku", state, NOW), GOMOKU_ROOM_TIMEOUT_SECONDS)

    def test_expired_rooms_report_zero_remaining(self) -> None:
        state = gomoku(create_time="2026-10-27 09:00:00")
        self.assertEqual(remaining_seconds("gomoku", state, NOW), 0)


class ExpiredRoomsGroupingTests(unittest.TestCase):
    def test_groups_ids_per_game_type_using_each_identity_field(self) -> None:
        snapshots = {
            "gomoku": [gomoku(room_id="g-old", create_time="2026-10-27 09:00:00")],
            "half_ten": [half_ten(room_id="h-old", create_time="2026-10-27 09:00:00")],
            "minesweeper": [
                minesweeper(game_id="m-old", last_action_time="2026-10-27 09:00:00")
            ],
        }
        self.assertEqual(
            expired_rooms(snapshots, NOW),
            {"gomoku": ["g-old"], "half_ten": ["h-old"], "minesweeper": ["m-old"]},
        )

    def test_live_rooms_are_excluded(self) -> None:
        snapshots = {
            "gomoku": [gomoku(room_id="g-live", create_time="2026-10-27 11:59:30")],
            "half_ten": [],
            "minesweeper": [minesweeper(game_id="m-live")],
        }
        self.assertEqual(
            expired_rooms(snapshots, NOW),
            {"gomoku": [], "half_ten": [], "minesweeper": []},
        )

    def test_every_known_type_is_present_even_when_absent_from_input(self) -> None:
        self.assertEqual(
            expired_rooms({}, NOW), {"gomoku": [], "half_ten": [], "minesweeper": []}
        )

    def test_unknown_types_and_identityless_states_are_skipped(self) -> None:
        snapshots = {
            "chess": [{"room_id": "x", "status": "waiting", "create_time": "2000-01-01 00:00:00"}],
            "gomoku": [
                {"status": "waiting", "create_time": "2000-01-01 00:00:00"},
                {"room_id": "", "status": "waiting", "create_time": "2000-01-01 00:00:00"},
                "not-a-mapping",
            ],
        }
        self.assertEqual(
            expired_rooms(snapshots, NOW),
            {"gomoku": [], "half_ten": [], "minesweeper": []},
        )

    def test_input_order_is_preserved_within_a_type(self) -> None:
        snapshots = {
            "gomoku": [
                gomoku(room_id="first", create_time="2026-10-27 09:00:00"),
                gomoku(room_id="second", create_time="2026-10-27 09:30:00"),
            ]
        }
        self.assertEqual(expired_rooms(snapshots, NOW)["gomoku"], ["first", "second"])


class RuntimeConstantParityTests(unittest.TestCase):
    def test_module_constants_match_the_runtime_game_modules(self) -> None:
        """The pure copy must not drift from the values the live tasks sleep on."""
        import nonebot

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

        from ....xiuxian.xiuxian_entertainment.mod import (
            gomoku,
            half_ten,
            minesweeper,
        )

        self.assertEqual(gomoku.ROOM_TIMEOUT, GOMOKU_ROOM_TIMEOUT_SECONDS)
        self.assertEqual(gomoku.MOVE_TIMEOUT, GOMOKU_MOVE_TIMEOUT_SECONDS)
        self.assertEqual(half_ten.ROOM_TIMEOUT, HALF_TEN_ROOM_TIMEOUT_SECONDS)
        self.assertEqual(minesweeper.GAME_TIMEOUT, MINESWEEPER_GAME_TIMEOUT_SECONDS)


if __name__ == "__main__":
    unittest.main()
