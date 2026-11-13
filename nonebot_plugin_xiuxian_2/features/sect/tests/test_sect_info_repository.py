from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....core.errors import DomainError
from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SectApplication
from ..sect_info_repository import SectInfoSqlRepository


class SectInfoRepositoryTests(unittest.TestCase):
    def _numeric_fixture(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        database = Path(directory.name) / "sect.db"
        with sqlite3.connect(database) as connection:
            # No affinity: preserve each historical storage representation.
            connection.execute(
                "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,"
                "sect_materials,sect_used_stone,stone)"
            )
            connection.execute("INSERT INTO sects VALUES(1,'fixture-sect',0,0,0)")
        return database, SectInfoSqlRepository(database)

    def test_name_projection_does_not_read_or_normalize_treasury(self):
        database, repository = self._numeric_fixture()
        with sqlite3.connect(database) as connection:
            connection.execute(
                "UPDATE sects SET sect_materials=?,sect_used_stone=?,stone=?",
                (float(2**53), "invalid", 1.5),
            )
            before = list(connection.iterdump())
        self.assertEqual("fixture-sect", repository.get_name_by_id(1))
        self.assertIsNone(repository.get_name_by_id(2))
        with self.assertRaises(DomainError) as caught:
            repository.get_by_id(1)
        self.assertEqual("sect_materials_precision_lost_real", caught.exception.details["reason"])
        with sqlite3.connect(database) as connection:
            self.assertEqual(before, list(connection.iterdump()))

    def test_full_financial_read_accepts_exact_legacy_forms_field_by_field(self):
        database, repository = self._numeric_fixture()
        forms = ((None, 0), (1000, 1000), (1000.0, 1000), ("1e3", 1000),
                 ("1000.000", 1000), (b"100000000000000000001", 100000000000000000001))
        for field in ("sect_materials", "sect_used_stone", "stone"):
            for value, expected in forms:
                with self.subTest(field=field, value=value):
                    with sqlite3.connect(database) as connection:
                        connection.execute("UPDATE sects SET sect_materials=0,sect_used_stone=0,stone=0")
                        connection.execute(f"UPDATE sects SET {field}=?", (value,))
                    self.assertEqual(expected, repository.get_by_id(1)[field])

    def test_full_financial_read_still_refuses_invalid_values_field_by_field(self):
        database, repository = self._numeric_fixture()
        for field in ("sect_materials", "sect_used_stone", "stone"):
            for value, suffix in ((float(2**53), "precision_lost_real"),
                                  (1.5, "invalid_numeric_value"),
                                  ("invalid", "invalid_numeric_value"),
                                  ("1e-2", "invalid_numeric_value")):
                with self.subTest(field=field, value=value):
                    with sqlite3.connect(database) as connection:
                        connection.execute("UPDATE sects SET sect_materials=0,sect_used_stone=0,stone=0")
                        connection.execute(f"UPDATE sects SET {field}=?", (value,))
                        before = list(connection.iterdump())
                    self.assertEqual("fixture-sect", repository.get_name_by_id(1))
                    with self.assertRaises(DomainError) as caught:
                        repository.get_by_id(1)
                    self.assertEqual("currency_precision_unavailable", caught.exception.code)
                    self.assertEqual({"reason": f"{field}_{suffix}", "subject": "sect", "field": field},
                                     caught.exception.details)
                    with sqlite3.connect(database) as connection:
                        self.assertEqual(before, list(connection.iterdump()))

    def test_reads_full_row_and_preserves_sect_numeric_normalization(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,"
                    "sect_scale TEXT,sect_materials TEXT,sect_contribution TEXT)"
                )
                uow.execute(
                    "INSERT INTO sects VALUES(1,'青云',?,?,?)",
                    (str(2**70), str(2**70 + 1), str(2**70 + 2)),
                )

            application = SectApplication(database)
            sect = application.get_sect_info(1)

            self.assertEqual("青云", sect["sect_name"])
            self.assertEqual(2**70, sect["sect_scale"])
            self.assertEqual(2**70 + 1, sect["sect_materials"])
            self.assertEqual(str(2**70 + 2), sect["sect_contribution"])
            self.assertIsNone(application.get_sect_info(2))

    def test_resolves_name_to_first_sect_id_and_preserves_missing_behavior(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE sects(sect_id INTEGER,sect_name TEXT)")
                uow.execute("INSERT INTO sects VALUES(1,'青云')")
                uow.execute("INSERT INTO sects VALUES(2,'青云')")

            application = SectApplication(database)

            self.assertEqual(1, application.get_sect_id_by_name("青云"))
            self.assertIsNone(application.get_sect_id_by_name("missing"))


if __name__ == "__main__":
    unittest.main()
