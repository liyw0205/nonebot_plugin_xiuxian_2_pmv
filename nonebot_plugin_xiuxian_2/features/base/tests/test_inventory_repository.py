import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..inventory_application import PlayerInventoryApplication
from ..inventory_repository import PlayerInventorySqlRepository


class PlayerInventoryRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE back("
                "user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER)"
            )
            uow.execute(
                "INSERT INTO back VALUES('u',7,'旧物品','材料',9,'','',4)"
            )
        self.repository = PlayerInventorySqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _row(self):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_all(
                "SELECT rowid,goods_num,bind_num FROM back "
                "WHERE user_id='u' AND goods_id=7 ORDER BY rowid"
            )

    def test_grant_caps_quantity_and_bound_items(self):
        result = self.repository.grant(
            "u", 7, "新物品", "材料", 5, bind_flag=1, max_goods_num=10
        )

        self.assertEqual(
            (result.status, result.requested, result.applied, result.final_quantity),
            ("applied", 5, 1, 10),
        )
        self.assertEqual([(int(row["goods_num"]), int(row["bind_num"])) for row in self._row()], [(10, 5)])

    def test_duplicate_projection_updates_only_first_row(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("INSERT INTO back VALUES('u',7,'重复','材料',2,'','',1)")

        result = self.repository.grant(
            "u", 7, "奖励", "材料", 1, bind_flag=0, max_goods_num=10
        )

        self.assertEqual(result.status, "applied")
        self.assertEqual(
            [(int(row["goods_num"]), int(row["bind_num"])) for row in self._row()],
            [(10, 4), (2, 1)],
        )

    def test_missing_schema_fails_closed_without_creating_database(self):
        with tempfile.TemporaryDirectory() as temp:
            missing = Path(temp) / "missing.sqlite3"
            result = PlayerInventorySqlRepository(missing).grant(
                "u", 7, "奖励", "材料", 1, max_goods_num=10
            )
            self.assertEqual(result.status, "schema_missing")
            self.assertFalse(missing.exists())

    def test_application_delegates_to_feature_repository(self):
        result = PlayerInventoryApplication(
            self.database, repository=self.repository
        ).grant_item("u", 8, "奖励", "材料", 2, max_goods_num=10)
        self.assertEqual((result.status, result.applied), ("applied", 2))


if __name__ == "__main__":
    unittest.main()
