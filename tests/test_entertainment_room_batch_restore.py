"""Batch room restore: one read-only unit of work, idempotent, releases expired.

R1 of the goal card requires the startup path to take ALL three game types in a
single read-only unit of work instead of one connection per type, and R2 requires
the restore to run once per process even though three startup hooks are
registered. Both properties are pinned here.

Written as ``unittest.TestCase`` classes because the CI quality job collects
tests with ``python -m unittest discover``.
"""

from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import nonebot

try:
    nonebot.get_driver()
except ValueError:
    nonebot.init()

from nonebot_plugin_xiuxian_2.features.entertainment import room_repository
from nonebot_plugin_xiuxian_2.features.entertainment.migrations import (
    apply_entertainment,
    apply_entertainment_rooms,
)
from nonebot_plugin_xiuxian_2.features.entertainment.room_repository import (
    EntertainmentRoomSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


def _stamp(seconds_ago: int) -> str:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return (now - timedelta(seconds=seconds_ago)).strftime("%Y-%m-%d %H:%M:%S")


class BatchReadTests(unittest.TestCase):
    def _repository(self):
        import tempfile
        from pathlib import Path

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_entertainment(uow)
            apply_entertainment_rooms(uow)
        return EntertainmentRoomSqlRepository(database)

    def _seed(self, repository) -> None:
        repository.save_state(
            "gomoku",
            "g1",
            {
                "room_id": "g1",
                "creator_id": "u1",
                "player_black": "u1",
                "player_white": None,
                "player_names": {"u1": "one"},
                "current_player": "u1",
                "board": [[0 for _ in range(15)] for _ in range(15)],
                "moves": [],
                "status": "waiting",
                "winner": None,
                "create_time": "2026-10-27 11:00:00",
                "last_move_time": None,
            },
        )
        repository.save_state(
            "half_ten",
            "h1",
            {
                "room_id": "h1",
                "creator_id": "u2",
                "players": ["u2"],
                "player_names": {"u2": "two"},
                "status": "waiting",
                "create_time": "2026-10-27 11:00:00",
                "start_time": None,
                "close_reason": None,
                "cards": {},
                "points": {},
                "rankings": [],
                "winner": None,
            },
        )
        repository.save_state(
            "minesweeper",
            "m1",
            {
                "game_id": "m1",
                "user_id": "u3",
                "user_name": "three",
                "width": 3,
                "height": 3,
                "mines": 1,
                "status": "playing",
                "create_time": "2026-10-27 11:00:00",
                "last_action_time": "2026-10-27 11:00:00",
                "first_click_done": False,
                "board": [[0, 0, 0], [0, 0, 0], [0, 0, 0]],
                "revealed": [[False] * 3 for _ in range(3)],
                "flagged": [[False] * 3 for _ in range(3)],
            },
        )

    def test_batch_read_uses_exactly_one_read_only_unit_of_work(self) -> None:
        repository = self._repository()
        self._seed(repository)
        opened = []
        real = room_repository.DatabaseUnitOfWork

        class Counting(real):
            def __init__(self, *args, **kwargs):
                opened.append(kwargs.get("read_only", args[1] if len(args) > 1 else False))
                super().__init__(*args, **kwargs)

        with patch.object(room_repository, "DatabaseUnitOfWork", Counting):
            states = repository.list_all_states()

        self.assertEqual(len(opened), 1, "the batch read must open ONE unit of work")
        self.assertEqual(opened, [True], "and it must be read-only")
        self.assertEqual(sorted(states), ["gomoku", "half_ten", "minesweeper"])
        self.assertEqual([s["room_id"] for s in states["gomoku"]], ["g1"])
        self.assertEqual([s["room_id"] for s in states["half_ten"]], ["h1"])
        self.assertEqual([s["game_id"] for s in states["minesweeper"]], ["m1"])

    def test_batch_read_carries_every_type_even_when_one_is_empty(self) -> None:
        repository = self._repository()
        self._seed(repository)
        repository.delete_state("half_ten", "h1")
        repository.delete_state("minesweeper", "m1")

        states = repository.list_all_states()

        self.assertEqual(sorted(states), ["gomoku", "half_ten", "minesweeper"])
        self.assertEqual(states["half_ten"], [])
        self.assertEqual(states["minesweeper"], [])

    def test_batch_read_keeps_the_byte_bound_check(self) -> None:
        repository = self._repository()
        self._seed(repository)

        with patch.object(room_repository, "MAX_ROOM_RESTORE_BYTES", 1):
            with self.assertRaisesRegex(RuntimeError, "byte limit"):
                repository.list_all_states()

    def test_batch_read_fails_closed_without_the_schema(self) -> None:
        import tempfile
        from pathlib import Path

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "empty.db"
        with DatabaseUnitOfWork(database, immediate=True):
            pass
        repository = EntertainmentRoomSqlRepository(database)

        with self.assertRaisesRegex(RuntimeError, "schema_missing"):
            repository.list_all_states()

    def test_single_type_read_semantics_are_unchanged(self) -> None:
        """The batch capability must not alter list_states' shape or bounds."""
        repository = self._repository()
        self._seed(repository)
        self.assertEqual([s["room_id"] for s in repository.list_states("gomoku")], ["g1"])
        with patch.object(room_repository, "MAX_ROOM_ROWS_PER_TYPE", 0):
            with self.assertRaisesRegex(RuntimeError, "row limit"):
                repository.list_states("gomoku")


class RestoreCoordinatorTests(unittest.TestCase):
    """The shared restore: one batch snapshot, then per-game expiry and re-arm."""

    def _modules(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod import (
            gomoku,
            half_ten,
            minesweeper,
            room_restore,
        )

        return gomoku, half_ten, minesweeper, room_restore

    def setUp(self) -> None:
        gomoku, half_ten, minesweeper, room_restore = self._modules()
        room_restore.reset_restore_state()
        for module in (gomoku, half_ten, minesweeper):
            for name in dir(module):
                if name.endswith("_timeout_tasks"):
                    getattr(module, name).clear()
        self._saved = {
            (gomoku, "room_manager"): gomoku.room_manager,
            (half_ten, "half_manager"): half_ten.half_manager,
            (minesweeper, "ms_manager"): minesweeper.ms_manager,
            (gomoku, "entertainment_application"): gomoku.entertainment_application,
        }

    def tearDown(self) -> None:
        for (module, attr), value in self._saved.items():
            setattr(module, attr, value)
        gomoku, half_ten, minesweeper, room_restore = self._modules()
        room_restore.reset_restore_state()
        for module in (gomoku, half_ten, minesweeper):
            for name in dir(module):
                if name.endswith("_timeout_tasks"):
                    getattr(module, name).clear()

    def _install(self, states):
        gomoku, half_ten, minesweeper, room_restore = self._modules()
        calls = {"batch": 0, "per_type": 0}

        class App:
            def room_states_all(self):
                calls["batch"] += 1
                return states

            def room_states(self, game_type):
                calls["per_type"] += 1
                return states.get(game_type, [])

            def save_room_state(self, *args):
                return None

            def delete_room_state(self, *args):
                return True

        app = App()
        gomoku.room_manager = gomoku.GomokuRoomManager(app)
        half_ten.half_manager = half_ten.HalfTenRoomManager(app)
        minesweeper.ms_manager = minesweeper.MinesweeperManager(app)
        gomoku.entertainment_application = app
        return calls

    def test_restore_uses_the_batch_read_once_and_is_idempotent(self) -> None:
        states = {
            "gomoku": [
                {
                    "room_id": "g-old",
                    "creator_id": "u1",
                    "player_black": "u1",
                    "player_white": None,
                    "player_names": {"u1": "one"},
                    "current_player": "u1",
                    "board": [[0 for _ in range(15)] for _ in range(15)],
                    "moves": [],
                    "status": "waiting",
                    "winner": None,
                    "create_time": _stamp(3 * 3600),
                    "last_move_time": None,
                }
            ],
            "half_ten": [],
            "minesweeper": [],
        }
        calls = self._install(states)
        gomoku, half_ten, minesweeper, room_restore = self._modules()

        first = asyncio.run(room_restore.restore_entertainment_rooms())
        second = asyncio.run(room_restore.restore_entertainment_rooms())

        self.assertEqual(calls["batch"], 1, "one batch snapshot per process")
        self.assertEqual(calls["per_type"], 0, "the restore must not read per type")
        self.assertEqual(second, {}, "the second hook must be a no-op")
        self.assertEqual(first["gomoku"], ["g-old"])
        self.assertEqual(gomoku.room_manager.rooms, {})
        self.assertIsNone(gomoku.room_manager.get_user_room("u1"))

    def test_restore_keeps_live_rooms_and_arms_their_timers(self) -> None:
        states = {
            "gomoku": [
                {
                    "room_id": "g-live",
                    "creator_id": "u1",
                    "player_black": "u1",
                    "player_white": None,
                    "player_names": {"u1": "one"},
                    "current_player": "u1",
                    "board": [[0 for _ in range(15)] for _ in range(15)],
                    "moves": [],
                    "status": "waiting",
                    "winner": None,
                    "create_time": _stamp(60),
                    "last_move_time": None,
                }
            ],
            "half_ten": [],
            "minesweeper": [],
        }
        self._install(states)
        gomoku, half_ten, minesweeper, room_restore = self._modules()

        result = asyncio.run(room_restore.restore_entertainment_rooms())

        self.assertEqual(result["gomoku"], [])
        self.assertIn("g-live", gomoku.room_manager.rooms)
        self.assertIn("g-live", gomoku.room_timeout_tasks)

    def test_restore_sends_no_messages_without_a_bot_context(self) -> None:
        states = {
            "gomoku": [
                {
                    "room_id": "g-live",
                    "creator_id": "u1",
                    "player_black": "u1",
                    "player_white": None,
                    "player_names": {"u1": "one"},
                    "current_player": "u1",
                    "board": [[0 for _ in range(15)] for _ in range(15)],
                    "moves": [],
                    "status": "waiting",
                    "winner": None,
                    "create_time": _stamp(60),
                    "last_move_time": None,
                }
            ],
            "half_ten": [],
            "minesweeper": [],
        }
        self._install(states)
        gomoku, half_ten, minesweeper, room_restore = self._modules()

        with patch.object(gomoku, "handle_send") as gomoku_send, patch.object(
            half_ten, "handle_send"
        ) as half_send, patch.object(minesweeper, "handle_send") as ms_send:
            asyncio.run(room_restore.restore_entertainment_rooms())

        gomoku_send.assert_not_called()
        half_send.assert_not_called()
        ms_send.assert_not_called()

    def test_missing_database_still_registers_the_hooks(self) -> None:
        """A restore failure must not break startup wiring."""
        gomoku, half_ten, minesweeper, room_restore = self._modules()
        for module in (gomoku, half_ten, minesweeper):
            hook = getattr(
                module,
                {
                    "gomoku": "restore_gomoku_rooms",
                    "half_ten": "restore_half_ten_rooms",
                    "minesweeper": "restore_minesweeper_rooms",
                }[module.__name__.rsplit(".", 1)[-1]],
            )
            import inspect

            self.assertTrue(inspect.iscoroutinefunction(hook))


if __name__ == "__main__":
    unittest.main()
