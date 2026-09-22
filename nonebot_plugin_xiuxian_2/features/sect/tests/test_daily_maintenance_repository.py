import tempfile
import unittest
from pathlib import Path

from ..daily_maintenance_repository import SectDailyMaintenanceSqlRepository
from tests.test_db_backend import db_backend


class SectDailyMaintenanceRepositoryTests(unittest.TestCase):
    def test_resets_and_replays(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "sect.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_task INTEGER,sect_elixir_get INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',2,1)")
                conn.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT,elixir_room_level INTEGER,sect_materials INTEGER)")
                conn.execute("INSERT INTO sects VALUES(1,'宗门','owner',1,200)")
            repo = SectDailyMaintenanceSqlRepository(db)
            first = repo.settle("2026-09-23", {1: 200})
            duplicate = repo.settle("2026-09-23", {1: 200})
            self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
