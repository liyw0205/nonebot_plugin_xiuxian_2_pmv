import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.inventory import read_inventory_rows


class InventoryBatchProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "inventory.db"

    def seed(self, uow, affinity="INTEGER", index=None, bind=True):
        extra = ",bind_num" if bind else ""
        uow.execute(f"CREATE TABLE back(user_id TEXT,goods_id {affinity},goods_name,goods_type,goods_num{extra})")
        if index is not None:
            uow.execute(f"CREATE {index} INDEX inventory_key ON back(user_id,goods_id)")
        rows = [("u", "01", "first", "pill", 7), ("u", 2, None, None, None),
                ("other", 1, "other", "pill", 999)]
        if index != "UNIQUE":
            rows.append(("u", 1, "last", "pill", 8))
        placeholders = "?,?,?,?,?,?" if bind else "?,?,?,?,?"
        for row in rows:
            uow.execute(f"INSERT INTO back VALUES({placeholders})", row + (None,) if bind else row)

    def test_matches_point_reads_for_affinity_duplicates_null_and_missing(self):
        for affinity in ("INTEGER", "TEXT", ""):
            for index in (None, "", "UNIQUE"):
                with self.subTest(affinity=affinity, index=index), DatabaseUnitOfWork(self.database) as uow:
                    self.seed(uow, affinity, index)
                    ids = [1, "01", 2, 999, 1, "1"]
                    expected = [uow.query_one(
                        "SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?",
                        ("u", item_id)) for item_id in ids]
                    actual = read_inventory_rows(uow, "u", ids, projection="details")
                    self.assertEqual(actual, expected)
                    self.assertIsNotNone(actual[2])
                    self.assertIsNone(actual[3])
                    uow.execute("DROP TABLE back")

    def test_empty_is_zero_io_and_projection_is_controlled(self):
        with DatabaseUnitOfWork(self.database) as uow, patch.object(uow, "query_all") as query:
            self.assertEqual(read_inventory_rows(uow, "u", []), [])
            query.assert_not_called()
            with self.assertRaises(ValueError):
                read_inventory_rows(uow, "u", [1], projection="goods_num FROM secrets")
            query.assert_not_called()

    def test_chunks_under_legacy_bind_limit_and_keeps_order(self):
        with DatabaseUnitOfWork(self.database) as uow:
            self.seed(uow, index="UNIQUE")
            ids = [1, 999] * 401
            with patch.object(uow, "query_all", wraps=uow.query_all) as query:
                actual = read_inventory_rows(uow, "u", ids, projection="quantity")
            self.assertEqual(actual, [{"goods_num": 7}, None] * 401)
            self.assertEqual(query.call_count, 3)
            self.assertLessEqual(max(len(call.args[1]) for call in query.call_args_list), 801)

    def test_old_schema_and_write_followed_by_fresh_projection(self):
        with DatabaseUnitOfWork(self.database) as uow:
            self.seed(uow, bind=False)
            before = read_inventory_rows(uow, "u", [1, 1], projection="details", has_bind_num=False)
            self.assertEqual(before[0]["bind_num"], 0)
            before[0]["goods_num"] = -1
            self.assertEqual(before[1]["goods_num"], 7)
            uow.execute("UPDATE back SET goods_num=19 WHERE user_id='u' AND goods_id=1")
            self.assertEqual(read_inventory_rows(uow, "u", [1], projection="quantity"), [{"goods_num": 19}])

    def test_schema_errors_are_not_silenced(self):
        with DatabaseUnitOfWork(self.database) as uow:
            with self.assertRaises(sqlite3.OperationalError):
                read_inventory_rows(uow, "u", [1])


if __name__ == "__main__":
    unittest.main()
