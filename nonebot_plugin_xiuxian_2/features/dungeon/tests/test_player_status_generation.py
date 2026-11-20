from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....core.errors import DomainError
from ..migrations import apply_dungeon_explore_player_schema
from ..reset_repository import DungeonResetSqlRepository


class DungeonPlayerStatusGenerationTests(unittest.TestCase):
    def prepare(self, directory):
        database = Path(directory) / "player.db"
        with DatabaseUnitOfWork(database) as uow:
            apply_dungeon_explore_player_schema(uow)
        repository = DungeonResetSqlRepository(database)
        repository.reset("generation-1", "2026-11-20", "manual", lambda: {
            "dungeon_id": "trial", "dungeon_name": "Trial", "total_layers": 5,
        })
        repository.ensure_player_status("player")
        return repository

    def test_same_generation_preserves_each_phase_and_progress_without_writes(self):
        for phase, layer in (("not_started", 0), ("exploring", 2), ("exited", 2), ("completed", 5)):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                repository = self.prepare(directory)
                with DatabaseUnitOfWork(repository.database) as uow:
                    uow.execute("UPDATE player_dungeon_status SET dungeon_status=?,current_layer=? WHERE user_id='player'", (phase, layer))
                    before = uow.query_one("SELECT * FROM player_dungeon_status WHERE user_id='player'")
                    uow.execute("CREATE TRIGGER forbid_status_write BEFORE UPDATE ON player_dungeon_status "
                                "BEGIN SELECT RAISE(ABORT,'same generation must be read only'); END")
                for _ in range(2):
                    actual = repository.ensure_player_status("player")
                    self.assertEqual(actual, {key: value for key, value in before.items() if key != "user_id"})
                with DatabaseUnitOfWork(repository.database, read_only=True) as uow:
                    self.assertEqual(uow.query_one("SELECT * FROM player_dungeon_status WHERE user_id='player'"), before)

    def test_only_different_publication_identity_initializes_existing_player(self):
        for field, value in (("dungeon_id", "old"), ("last_reset_date", "2026-11-19"),
                             ("reset_generation", 0), ("reset_operation_id", "old-generation")):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                repository = self.prepare(directory)
                with DatabaseUnitOfWork(repository.database) as uow:
                    uow.execute(f"UPDATE player_dungeon_status SET dungeon_status='exploring',current_layer=2,{field}=? WHERE user_id='player'", (value,))
                actual = repository.ensure_player_status("player")
                self.assertEqual(actual, {"dungeon_id": "trial", "dungeon_name": "Trial", "dungeon_status": "not_started",
                                          "current_layer": 0, "total_layers": 5, "last_reset_date": "2026-11-20",
                                          "reset_generation": 1, "reset_operation_id": "generation-1"})
                with DatabaseUnitOfWork(repository.database, read_only=True) as uow:
                    self.assertEqual(uow.query_one("SELECT * FROM player_dungeon_status WHERE user_id='player'"),
                                     {"user_id": "player", **actual})

    def test_initialization_write_failures_roll_back_and_recover(self):
        for missing, timing, body, reason in (
            (True, "BEFORE INSERT", "SELECT RAISE(IGNORE)", "write_rejected"),
            (True, "AFTER INSERT", "UPDATE player_dungeon_status SET dungeon_name='changed' WHERE user_id=NEW.user_id", "projection_mismatch"),
            (False, "BEFORE UPDATE", "SELECT RAISE(IGNORE)", "write_rejected"),
            (False, "AFTER UPDATE", "UPDATE player_dungeon_status SET current_layer=99 WHERE user_id=NEW.user_id", "projection_mismatch"),
        ):
            with self.subTest(missing=missing, reason=reason), tempfile.TemporaryDirectory() as directory:
                repository = self.prepare(directory)
                initial = repository.ensure_player_status("player")
                repository.ensure_player_status("other")
                with DatabaseUnitOfWork(repository.database) as uow:
                    if missing:
                        uow.execute("DELETE FROM player_dungeon_status WHERE user_id='player'")
                    else:
                        uow.execute("UPDATE player_dungeon_status SET dungeon_status='exploring',current_layer=2,reset_generation=0 WHERE user_id='player'")
                    before = uow.query_all("SELECT * FROM player_dungeon_status ORDER BY user_id")
                    uow.execute(f"CREATE TRIGGER status_fault {timing} ON player_dungeon_status WHEN NEW.user_id='player' BEGIN "
                                f"UPDATE player_dungeon_status SET current_layer=99 WHERE user_id='other'; {body}; END")
                with self.assertRaises(DomainError) as caught:
                    repository.ensure_player_status("player")
                self.assertEqual(caught.exception.code, "dungeon_status_initialization_" + reason)
                self.assertEqual(caught.exception.details["subject"], "player")
                with DatabaseUnitOfWork(repository.database) as uow:
                    self.assertEqual(uow.query_all("SELECT * FROM player_dungeon_status ORDER BY user_id"), before)
                    uow.execute("DROP TRIGGER status_fault")
                self.assertEqual(repository.ensure_player_status("player"), initial)
                self.assertEqual(repository.ensure_player_status("player"), initial)

    def test_initialization_abort_keeps_exception_and_rolls_back(self):
        for missing, timing in ((True, "BEFORE INSERT"), (False, "BEFORE UPDATE")):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as directory:
                repository = self.prepare(directory)
                initial = repository.ensure_player_status("player")
                repository.ensure_player_status("other")
                with DatabaseUnitOfWork(repository.database) as uow:
                    if missing:
                        uow.execute("DELETE FROM player_dungeon_status WHERE user_id='player'")
                    else:
                        uow.execute("UPDATE player_dungeon_status SET reset_generation=0 WHERE user_id='player'")
                    before = uow.query_all("SELECT * FROM player_dungeon_status ORDER BY user_id")
                    uow.execute(f"CREATE TRIGGER status_abort {timing} ON player_dungeon_status WHEN NEW.user_id='player' BEGIN "
                                "UPDATE player_dungeon_status SET current_layer=99 WHERE user_id='other'; "
                                "SELECT RAISE(ABORT,'isolated status initialization abort'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated status initialization abort"):
                    repository.ensure_player_status("player")
                with DatabaseUnitOfWork(repository.database) as uow:
                    self.assertEqual(uow.query_all("SELECT * FROM player_dungeon_status ORDER BY user_id"), before)
                    uow.execute("DROP TRIGGER status_abort")
                self.assertEqual(repository.ensure_player_status("player"), initial)


if __name__ == "__main__":
    unittest.main()
