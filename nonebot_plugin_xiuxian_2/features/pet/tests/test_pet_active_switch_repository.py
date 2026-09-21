import tempfile
import unittest
from pathlib import Path

from ..repository import PetActiveSwitchSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class PetActiveSwitchSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "player.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, active TEXT)")
            uow.execute("INSERT INTO player_pet VALUES('u','old','old')")
            uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, is_active INTEGER, updated_at INTEGER)")
            uow.execute("INSERT INTO player_pet_item VALUES('u','old',1,0)")
            uow.execute("INSERT INTO player_pet_item VALUES('u','new',0,0)")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_switch_and_replay(self) -> None:
        repository = PetActiveSwitchSqlRepository(self.database)
        self.assertEqual(repository.switch("op", "u", "old", "new").status, "applied")
        self.assertEqual(repository.switch("op", "u", "old", "new").status, "duplicate")
        with DatabaseUnitOfWork(self.database) as uow:
            metadata = uow.query_one("SELECT active_uid,active FROM player_pet WHERE user_id='u'")
            active = uow.query_all("SELECT uid,is_active FROM player_pet_item WHERE user_id='u' ORDER BY uid")
        self.assertEqual((metadata["active_uid"], metadata["active"]), ("new", "new"))
        self.assertEqual([(row["uid"], row["is_active"]) for row in active], [("new", 1), ("old", 0)])

    def test_rejections_preserve_state(self) -> None:
        repository = PetActiveSwitchSqlRepository(self.database)
        self.assertEqual(repository.switch("missing", "u", "old", "missing").status, "pet_missing")
        self.assertEqual(repository.switch("travel", "u", "old", "new", "new").status, "pet_traveling")
        self.assertEqual(repository.switch("already", "u", "old", "old").status, "already_active")
        self.assertEqual(repository.switch("stale", "u", "wrong", "new").status, "state_changed")

    def test_operation_conflict_and_trigger_rollback(self) -> None:
        repository = PetActiveSwitchSqlRepository(self.database)
        self.assertEqual(repository.switch("conflict", "u", "old", "new").status, "applied")
        self.assertEqual(repository.switch("conflict", "u", "new", "old").status, "operation_conflict")
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE player_pet SET active_uid='old', active='old' WHERE user_id='u'")
            uow.execute("UPDATE player_pet_item SET is_active=CASE uid WHEN 'old' THEN 1 ELSE 0 END WHERE user_id='u'")
            uow.execute("CREATE TRIGGER fail_pet_active_switch BEFORE INSERT ON pet_active_switch_operations BEGIN SELECT RAISE(ABORT, 'operation failed'); END")
        with self.assertRaises(Exception):
            repository.switch("rollback", "u", "old", "new")
        with DatabaseUnitOfWork(self.database) as uow:
            metadata = uow.query_one("SELECT active_uid,active FROM player_pet WHERE user_id='u'")
            active = uow.query_all("SELECT uid,is_active FROM player_pet_item WHERE user_id='u' ORDER BY uid")
        self.assertEqual((metadata["active_uid"], metadata["active"]), ("old", "old"))
        self.assertEqual([(row["uid"], row["is_active"]) for row in active], [("new", 0), ("old", 1)])


if __name__ == "__main__":
    unittest.main()
