from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SectApplication


class SectDirectoryRepositoryTests(unittest.TestCase):
    def test_lists_all_sects_with_owner_name_and_member_count(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,"
                    "sect_scale INTEGER,sect_owner TEXT)"
                )
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,sect_id INTEGER)"
                )
                uow.execute("INSERT INTO sects VALUES(1,'青云',100,'owner')")
                uow.execute("INSERT INTO sects VALUES(2,'无主',40,NULL)")
                uow.execute("INSERT INTO user_xiuxian VALUES('owner','掌门',1)")
                uow.execute("INSERT INTO user_xiuxian VALUES('member','弟子',1)")

            result = SectApplication(database).list_sects_with_member_count()

            self.assertEqual(
                [
                    (1, "青云", 100, "掌门", 2),
                    (2, "无主", 40, None, 0),
                ],
                result,
            )


if __name__ == "__main__":
    unittest.main()
