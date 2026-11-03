import tempfile
import unittest
from pathlib import Path

from ..migrations import apply_boss_punishment_operations
from ..punishment_repository import WorldBossPunishmentSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class WorldBossPunishmentRepositoryTests(unittest.TestCase):
    def test_single_punishment_replay_and_revision_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "player.db"
            with DatabaseUnitOfWork(str(db), immediate=True) as uow:
                apply_boss_punishment_operations(uow)
            with db_backend.transaction(db) as conn:
                conn.execute("INSERT INTO world_boss_state VALUES('global','[{\"id\":1},{\"id\":2}]','now',0)")
            repo = WorldBossPunishmentSqlRepository(db)
            bosses, revision = repo.snapshot()
            first = repo.punish("p1", "single", revision, bosses, 1)
            duplicate = repo.punish("p1", "single", revision, bosses, 1)
            stale = repo.punish("p2", "single", revision, bosses, 1)
            self.assertEqual((first.status, duplicate.status, stale.status), ("punished", "duplicate", "session_changed"))
