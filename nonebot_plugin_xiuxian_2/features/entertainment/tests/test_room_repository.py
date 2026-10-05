from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_entertainment, apply_entertainment_rooms
from ..room_repository import EntertainmentRoomSqlRepository


def _room_snapshots() -> dict[str, dict]:
    return {
        "gomoku": {
            "room_id": "shared-id",
            "creator_id": "u1",
            "player_black": "u1",
            "player_white": None,
            "player_names": {"u1": "one"},
            "current_player": "u1",
            "board": [[0 for _ in range(15)] for _ in range(15)],
            "moves": [],
            "status": "waiting",
            "winner": None,
            "create_time": "2026-10-05 00:00:00",
            "last_move_time": None,
        },
        "half_ten": {
            "room_id": "shared-id",
            "creator_id": "u2",
            "players": ["u2"],
            "player_names": {"u2": "two"},
            "status": "waiting",
            "create_time": "2026-10-05 00:00:00",
            "start_time": None,
            "close_reason": None,
            "cards": {},
            "points": {},
            "rankings": [],
            "winner": None,
        },
        "minesweeper": {
            "game_id": "shared-id",
            "user_id": "u3",
            "user_name": "three",
            "width": 2,
            "height": 2,
            "mines": 1,
            "status": "playing",
            "create_time": "2026-10-05 00:00:00",
            "last_action_time": "2026-10-05 00:00:00",
            "first_click_done": False,
            "board": [[0, 0], [0, 0]],
            "revealed": [[False, False], [False, False]],
            "flagged": [[False, False], [False, False]],
        },
    }


class EntertainmentRoomMigrationTests(unittest.TestCase):
    def test_imports_distinct_schemas_once_and_preserves_legacy_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rooms = root / "rooms"
            rooms.mkdir()
            snapshots = _room_snapshots()
            originals = {}
            for game_type, state in snapshots.items():
                path = rooms / f"{game_type}.json"
                path.write_text(json.dumps(state), encoding="utf-8")
                originals[path] = path.read_bytes()
            (rooms / "ignored.json").write_text("not-json", encoding="utf-8")
            ignored_original = (rooms / "ignored.json").read_bytes()
            database = root / "game.db"

            with DatabaseUnitOfWork(database) as uow:
                apply_entertainment(uow)
                apply_entertainment_rooms(uow, rooms, "2026-10-05 00:00:00")
            repository = EntertainmentRoomSqlRepository(database)

            for game_type, state in snapshots.items():
                self.assertEqual([state], repository.list_states(game_type))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                receipt = uow.query_one(
                    "SELECT imported_count,ignored_count FROM entertainment_game_room_migrations "
                    "WHERE migration_key=?",
                    ("legacy.entertainment.rooms-json-v1",),
                )
            self.assertEqual({"imported_count": 3, "ignored_count": 1}, receipt)
            for path, original in originals.items():
                self.assertEqual(original, path.read_bytes())
            self.assertEqual(ignored_original, (rooms / "ignored.json").read_bytes())

            (rooms / "gomoku.json").write_text("{}", encoding="utf-8")
            with DatabaseUnitOfWork(database) as uow:
                apply_entertainment_rooms(uow, rooms, "2026-10-05 00:00:00")
            self.assertEqual([snapshots["gomoku"]], repository.list_states("gomoku"))

    def test_invalid_recognized_snapshot_rolls_back_without_removing_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rooms = root / "rooms"
            rooms.mkdir()
            state = _room_snapshots()["gomoku"]
            state["board"] = [[0]]
            path = rooms / "bad.json"
            path.write_text(json.dumps(state), encoding="utf-8")
            original = path.read_bytes()
            database = root / "game.db"

            with self.assertRaisesRegex(ValueError, "gomoku"):
                with DatabaseUnitOfWork(database) as uow:
                    apply_entertainment_rooms(uow, rooms)

            self.assertEqual(original, path.read_bytes())
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                tables = {
                    str(row["name"])
                    for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertNotIn("entertainment_game_room_migrations", tables)
            self.assertNotIn("entertainment_game_rooms", tables)


class EntertainmentRoomRepositoryTests(unittest.TestCase):
    def test_state_writes_are_type_scoped_and_delete_isolated(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_entertainment(uow)
                apply_entertainment_rooms(uow, root / "missing-rooms")
            repository = EntertainmentRoomSqlRepository(database)
            snapshots = _room_snapshots()
            repository.save_state("gomoku", "shared-id", snapshots["gomoku"])
            repository.save_state("half_ten", "shared-id", snapshots["half_ten"])

            self.assertEqual([snapshots["gomoku"]], repository.list_states("gomoku"))
            self.assertEqual([snapshots["half_ten"]], repository.list_states("half_ten"))
            self.assertTrue(repository.delete_state("gomoku", "shared-id"))
            self.assertEqual([], repository.list_states("gomoku"))
            self.assertEqual([snapshots["half_ten"]], repository.list_states("half_ten"))

    def test_missing_schema_fails_closed_without_creating_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "missing.db"
            repository = EntertainmentRoomSqlRepository(database)

            with self.assertRaisesRegex(RuntimeError, "schema_missing"):
                repository.list_states("gomoku")
            with self.assertRaisesRegex(RuntimeError, "schema_missing"):
                repository.save_state("gomoku", "room", _room_snapshots()["gomoku"])
            self.assertFalse(database.exists())

    def test_restore_checks_byte_budget_before_loading_room_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_entertainment(uow)
                apply_entertainment_rooms(uow, root / "missing-rooms")
            repository = EntertainmentRoomSqlRepository(database)
            repository.save_state("gomoku", "shared-id", _room_snapshots()["gomoku"])

            with patch("nonebot_plugin_xiuxian_2.features.entertainment.room_repository.MAX_ROOM_RESTORE_BYTES", 1):
                with self.assertRaisesRegex(RuntimeError, "byte limit"):
                    repository.list_states("gomoku")

    def test_existing_database_without_room_schema_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with DatabaseUnitOfWork(database):
                pass
            repository = EntertainmentRoomSqlRepository(database)

            with self.assertRaisesRegex(RuntimeError, "schema_missing"):
                repository.save_state("gomoku", "room", _room_snapshots()["gomoku"])


if __name__ == "__main__":
    unittest.main()
