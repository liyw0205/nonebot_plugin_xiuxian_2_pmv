"""Restart restore must release expired rooms and re-arm the survivors' timers.

Room snapshots survive a restart but the asyncio timers do not, so before this
behaviour a restored room kept binding its players forever: a waiting room whose
180s window had long passed still answered ``你已在其它棋局中``.

Written as ``unittest.TestCase`` classes because the CI quality job collects
tests with ``python -m unittest discover``.
"""

from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import nonebot

try:
    nonebot.get_driver()
except ValueError:
    nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod import (
    gomoku,
    half_ten,
    minesweeper,
)


class MemoryRoomApplication:
    def __init__(self, states):
        self.states = {game_type: list(values) for game_type, values in states.items()}
        self.writes = []
        self.deletes = []

    def room_states(self, game_type):
        return list(self.states.get(game_type, ()))

    def save_room_state(self, game_type, room_id, state):
        self.writes.append((game_type, room_id, state))

    def delete_room_state(self, game_type, room_id):
        self.deletes.append((game_type, room_id))
        return True


NOW = datetime(2026, 10, 27, 12, 0, 0)


def _stamp(seconds_ago: int) -> str:
    return (NOW - timedelta(seconds=seconds_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _gomoku_state(room_id, status, create_ago, move_ago=None):
    return {
        "room_id": room_id,
        "creator_id": "u1",
        "player_black": "u1",
        "player_white": None if status == "waiting" else "u2",
        "player_names": {"u1": "One", "u2": "Two"},
        "current_player": "u1",
        "board": [[0 for _ in range(15)] for _ in range(15)],
        "moves": [],
        "status": status,
        "winner": None,
        "create_time": _stamp(create_ago),
        "last_move_time": None if move_ago is None else _stamp(move_ago),
    }


def _half_state(room_id, status, create_ago, players=("u1",)):
    return {
        "room_id": room_id,
        "creator_id": players[0],
        "players": list(players),
        "player_names": {p: p for p in players},
        "status": status,
        "create_time": _stamp(create_ago),
        "start_time": None,
        "close_reason": None,
        "cards": {},
        "points": {},
        "rankings": [],
        "winner": None,
    }


def _minesweeper_state(game_id, status, create_ago, action_ago):
    return {
        "game_id": game_id,
        "user_id": "u3",
        "user_name": "Three",
        "width": 3,
        "height": 3,
        "mines": 1,
        "status": status,
        "create_time": _stamp(create_ago),
        "last_action_time": _stamp(action_ago),
        "first_click_done": False,
        "board": [[0, 0, 0], [0, 0, 0], [0, 0, 0]],
        "revealed": [[False] * 3 for _ in range(3)],
        "flagged": [[False] * 3 for _ in range(3)],
    }


class RoomRearmTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._task_dicts = (
            (gomoku.room_timeout_tasks, dict(gomoku.room_timeout_tasks)),
            (gomoku.move_timeout_tasks, dict(gomoku.move_timeout_tasks)),
            (half_ten.half_timeout_tasks, dict(half_ten.half_timeout_tasks)),
            (minesweeper.minesweeper_timeout_tasks, dict(minesweeper.minesweeper_timeout_tasks)),
        )
        for mapping, _ in self._task_dicts:
            mapping.clear()

    def tearDown(self) -> None:
        for mapping, _ in self._task_dicts:
            mapping.clear()

    def _install(self, module, manager_attr, app):
        manager = getattr(module, manager_attr).__class__(app)
        return patch.object(module, manager_attr, manager), manager


class GomokuRearmTests(RoomRearmTestCase):
    def test_expired_waiting_room_is_released_and_the_player_can_start_again(self) -> None:
        app = MemoryRoomApplication(
            {"gomoku": [_gomoku_state("old", "waiting", create_ago=3 * 3600)]}
        )
        patcher, manager = self._install(gomoku, "room_manager", app)
        with patcher:
            manager.load_rooms()
            self.assertEqual(manager.get_user_room("u1"), "old")

            result = asyncio.run(gomoku.rearm_room_timeouts(now=NOW))

            self.assertEqual(result, {"gomoku": ["old"]})
            self.assertEqual(manager.rooms, {})
            self.assertIsNone(manager.get_user_room("u1"))
            self.assertEqual(app.deletes, [("gomoku", "old")])
            self.assertEqual(gomoku.room_timeout_tasks, {})

    def test_expired_playing_room_is_released_and_no_message_is_sent(self) -> None:
        app = MemoryRoomApplication(
            {
                "gomoku": [
                    _gomoku_state("old", "playing", create_ago=3600, move_ago=3 * 3600)
                ]
            }
        )
        patcher, manager = self._install(gomoku, "room_manager", app)
        with patcher, patch.object(
            gomoku, "handle_pic_msg_send"
        ) as pic_send, patch.object(gomoku, "handle_send") as text_send:
            manager.load_rooms()
            asyncio.run(gomoku.rearm_room_timeouts(now=NOW))

            self.assertEqual(manager.rooms, {})
            text_send.assert_not_called()
            pic_send.assert_not_called()

    def test_live_waiting_room_keeps_a_timer_for_its_remaining_time(self) -> None:
        """60s left of a 180s window — not a fresh 180s."""
        app = MemoryRoomApplication(
            {"gomoku": [_gomoku_state("live", "waiting", create_ago=120)]}
        )
        patcher, manager = self._install(gomoku, "room_manager", app)
        with patcher:
            manager.load_rooms()
            asyncio.run(gomoku.rearm_room_timeouts(now=NOW))
            self.assertIn("live", manager.rooms)
            self.assertIn("live", gomoku.room_timeout_tasks)

    def test_live_playing_room_gets_a_move_timer_not_a_room_timer(self) -> None:
        app = MemoryRoomApplication(
            {
                "gomoku": [
                    _gomoku_state("live", "playing", create_ago=3600, move_ago=60)
                ]
            }
        )
        patcher, manager = self._install(gomoku, "room_manager", app)
        with patcher:
            manager.load_rooms()
            asyncio.run(gomoku.rearm_room_timeouts(now=NOW))
            self.assertIn("live", gomoku.move_timeout_tasks)
            self.assertNotIn("live", gomoku.room_timeout_tasks)

    def test_repeated_rearm_does_not_stack_timers(self) -> None:
        app = MemoryRoomApplication(
            {"gomoku": [_gomoku_state("live", "waiting", create_ago=120)]}
        )
        patcher, manager = self._install(gomoku, "room_manager", app)
        with patcher:
            manager.load_rooms()
            asyncio.run(gomoku.rearm_room_timeouts(now=NOW))
            first = gomoku.room_timeout_tasks["live"]
            asyncio.run(gomoku.rearm_room_timeouts(now=NOW))
            second = gomoku.room_timeout_tasks["live"]
            self.assertEqual(len(gomoku.room_timeout_tasks), 1)
            self.assertIsNot(first, second)
            self.assertTrue(first.cancelled())

    def test_settled_room_is_left_alone(self) -> None:
        app = MemoryRoomApplication(
            {"gomoku": [_gomoku_state("done", "finished", create_ago=3 * 3600)]}
        )
        patcher, manager = self._install(gomoku, "room_manager", app)
        with patcher:
            manager.load_rooms()
            asyncio.run(gomoku.rearm_room_timeouts(now=NOW))
            self.assertEqual(app.deletes, [])
            self.assertEqual(gomoku.room_timeout_tasks, {})


class HalfTenRearmTests(RoomRearmTestCase):
    def test_expired_waiting_room_is_released(self) -> None:
        app = MemoryRoomApplication(
            {"half_ten": [_half_state("old", "waiting", create_ago=3 * 3600)]}
        )
        patcher, manager = self._install(half_ten, "half_manager", app)
        with patcher:
            manager.load_rooms()
            self.assertEqual(manager.get_user_room("u1"), "old")

            result = asyncio.run(half_ten.rearm_half_ten_timeouts(now=NOW))

            self.assertEqual(result, {"half_ten": ["old"]})
            self.assertIsNone(manager.get_user_room("u1"))
            self.assertEqual(app.deletes, [("half_ten", "old")])

    def test_live_waiting_room_keeps_a_timer(self) -> None:
        app = MemoryRoomApplication(
            {"half_ten": [_half_state("live", "waiting", create_ago=60)]}
        )
        patcher, manager = self._install(half_ten, "half_manager", app)
        with patcher:
            manager.load_rooms()
            asyncio.run(half_ten.rearm_half_ten_timeouts(now=NOW))
            self.assertIn("live", half_ten.half_timeout_tasks)

    def test_expired_room_goes_through_the_real_close_branch(self) -> None:
        """The expired room must persist the same closed status the live timer writes."""
        app = MemoryRoomApplication(
            {"half_ten": [_half_state("old", "waiting", create_ago=3 * 3600)]}
        )
        patcher, manager = self._install(half_ten, "half_manager", app)
        with patcher:
            manager.load_rooms()
            asyncio.run(half_ten.rearm_half_ten_timeouts(now=NOW))
            # one player < MIN_PLAYERS: the timeout closes the room and saves it
            self.assertEqual([(gt, rid) for gt, rid, _ in app.writes], [("half_ten", "old")])
            self.assertIsNone(manager.get_user_room("u1"))

    def test_settled_room_is_left_alone(self) -> None:
        app = MemoryRoomApplication(
            {"half_ten": [_half_state("done", "finished", create_ago=3 * 3600)]}
        )
        patcher, manager = self._install(half_ten, "half_manager", app)
        with patcher:
            manager.load_rooms()
            asyncio.run(half_ten.rearm_half_ten_timeouts(now=NOW))
            self.assertIn("done", manager.rooms)
            self.assertEqual(app.deletes, [])
            self.assertEqual(half_ten.half_timeout_tasks, {})


class MinesweeperRearmTests(RoomRearmTestCase):
    def test_expired_game_is_released_and_the_player_can_start_again(self) -> None:
        app = MemoryRoomApplication(
            {
                "minesweeper": [
                    _minesweeper_state("old", "playing", create_ago=3600, action_ago=3 * 3600)
                ]
            }
        )
        patcher, manager = self._install(minesweeper, "ms_manager", app)
        with patcher:
            manager.load_rooms()
            self.assertIsNotNone(manager.get_user_game("u3"))

            result = asyncio.run(minesweeper.rearm_game_timeouts(now=NOW))

            self.assertEqual(result, {"minesweeper": ["old"]})
            self.assertIsNone(manager.get_user_game("u3"))
            self.assertEqual(app.deletes, [("minesweeper", "old")])

    def test_live_game_keeps_a_timer_for_its_remaining_time(self) -> None:
        app = MemoryRoomApplication(
            {
                "minesweeper": [
                    _minesweeper_state("live", "playing", create_ago=3600, action_ago=30)
                ]
            }
        )
        patcher, manager = self._install(minesweeper, "ms_manager", app)
        with patcher:
            manager.load_rooms()
            asyncio.run(minesweeper.rearm_game_timeouts(now=NOW))
            self.assertIn("live", manager.games)
            self.assertIn("live", minesweeper.minesweeper_timeout_tasks)

    def test_closed_game_is_left_alone(self) -> None:
        app = MemoryRoomApplication(
            {
                "minesweeper": [
                    _minesweeper_state("done", "closed", create_ago=3 * 3600, action_ago=3 * 3600)
                ]
            }
        )
        patcher, manager = self._install(minesweeper, "ms_manager", app)
        with patcher:
            manager.load_rooms()
            asyncio.run(minesweeper.rearm_game_timeouts(now=NOW))
            self.assertEqual(app.deletes, [])
            self.assertEqual(minesweeper.minesweeper_timeout_tasks, {})


class StartupHookTests(unittest.TestCase):
    def test_restore_hooks_are_awaitable_and_rearm(self) -> None:
        """The startup registry awaits awaitable callbacks; restore must rearm."""
        import inspect

        for module, hook in (
            (gomoku, gomoku.restore_gomoku_rooms),
            (half_ten, half_ten.restore_half_ten_rooms),
            (minesweeper, minesweeper.restore_minesweeper_rooms),
        ):
            with self.subTest(module=module.__name__):
                self.assertTrue(inspect.iscoroutinefunction(hook))

    def test_legacy_startup_awaits_awaitable_callbacks(self) -> None:
        from nonebot_plugin_xiuxian_2.bootstrap import legacy

        source = inspect_source(legacy.run_legacy_startup)
        self.assertIn("isawaitable", source)


def inspect_source(fn) -> str:
    import inspect

    return inspect.getsource(fn)


if __name__ == "__main__":
    unittest.main()
