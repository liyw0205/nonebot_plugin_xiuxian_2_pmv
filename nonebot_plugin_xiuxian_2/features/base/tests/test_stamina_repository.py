import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..stamina_application import PlayerStaminaApplication
from ..stamina_repository import PlayerStaminaSqlRepository


class PlayerStaminaRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT,user_stamina INTEGER)"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES('u',10)")
        self.repository = PlayerStaminaSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _stamina(self):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return int(uow.query_one("SELECT user_stamina FROM user_xiuxian WHERE user_id='u'")["user_stamina"])

    def test_consume_uses_expected_snapshot_and_cas(self):
        result = self.repository.consume("u", 3, expected_stamina=10)
        stale = self.repository.consume("u", 3, expected_stamina=10)
        self.assertEqual((result["status"], result["stamina"]), ("applied", 7))
        self.assertEqual((stale["status"], stale["stamina"]), ("state_changed", 7))
        self.assertEqual(self._stamina(), 7)

    def test_duplicate_user_id_updates_only_the_first_projection_row(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("INSERT INTO user_xiuxian VALUES('u',10)")
        result = self.repository.consume("u", 3, expected_stamina=10)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT user_stamina FROM user_xiuxian WHERE user_id='u' ORDER BY rowid"
            )
        self.assertEqual(result["status"], "applied")
        self.assertEqual([int(row["user_stamina"]) for row in rows], [7, 10])

    def test_insufficient_and_missing_schema_fail_closed(self):
        insufficient = self.repository.consume("u", 11, expected_stamina=10)
        self.assertEqual((insufficient["status"], insufficient["stamina"]), ("stamina_insufficient", 10))
        with tempfile.TemporaryDirectory() as temp:
            missing = Path(temp) / "missing.sqlite3"
            result = PlayerStaminaSqlRepository(missing).consume("u", 1)
            self.assertEqual(result["status"], "schema_missing")
            self.assertFalse(missing.exists())

    def test_application_delegates_to_feature_repository(self):
        application = PlayerStaminaApplication(self.database, repository=self.repository)
        result = application.consume("u", 2, expected_stamina=10)
        self.assertEqual((result["status"], result["stamina"]), ("applied", 8))

    def test_recover_updates_in_bounded_batches_and_caps_at_maximum(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("INSERT INTO user_xiuxian VALUES('u2',0)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u3',9)")

        result = self.repository.recover(10, 3, batch_size=1)

        self.assertEqual((result["status"], result["updated"]), ("applied", 2))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT user_id,user_stamina FROM user_xiuxian ORDER BY rowid"
            )
        self.assertEqual(
            [(str(row["user_id"]), int(row["user_stamina"])) for row in rows],
            [("u", 10), ("u2", 3), ("u3", 10)],
        )

    def test_recover_zero_points_is_a_noop(self):
        result = self.repository.recover(10, 0, batch_size=1)

        self.assertEqual((result["status"], result["updated"]), ("applied", 0))
        self.assertEqual(self._stamina(), 10)

    def test_recover_missing_schema_fails_closed_without_creating_database(self):
        with tempfile.TemporaryDirectory() as temp:
            missing = Path(temp) / "missing.sqlite3"
            result = PlayerStaminaSqlRepository(missing).recover(10, 1)
            self.assertEqual((result["status"], result["updated"]), ("schema_missing", 0))
            self.assertFalse(missing.exists())

        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "empty.sqlite3"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                uow.execute("CREATE TABLE unrelated(value INTEGER)")
            result = PlayerStaminaSqlRepository(database).recover(10, 1)
            self.assertEqual((result["status"], result["updated"]), ("schema_missing", 0))

    def test_recover_query_is_bounded_and_does_not_load_user_rows(self):
        source = Path(__file__).parents[1].joinpath("stamina_repository.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("ORDER BY rowid LIMIT ?", source)
        self.assertNotIn("SELECT * FROM user_xiuxian", source)


if __name__ == "__main__":
    unittest.main()
