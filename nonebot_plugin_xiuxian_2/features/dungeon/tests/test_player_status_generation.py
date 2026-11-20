from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
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


if __name__ == "__main__":
    unittest.main()
