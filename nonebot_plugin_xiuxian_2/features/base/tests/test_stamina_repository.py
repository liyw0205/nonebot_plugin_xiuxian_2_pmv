import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.features.base.stamina_application import PlayerStaminaApplication
from nonebot_plugin_xiuxian_2.features.base.stamina_repository import PlayerStaminaSqlRepository


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


if __name__ == "__main__":
    unittest.main()
