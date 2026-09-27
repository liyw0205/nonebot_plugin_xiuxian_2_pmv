from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SectApplication


class SectInfoRepositoryTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
