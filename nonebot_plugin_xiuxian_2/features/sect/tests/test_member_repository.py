from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SectApplication


class SectMemberRepositoryTests(unittest.TestCase):
    def test_lists_complete_normalized_member_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,"
                    "sect_id INTEGER,sect_position INTEGER,sect_contribution TEXT,stone TEXT)"
                )
                uow.execute(
                    "INSERT INTO user_xiuxian VALUES('member','弟子',1,2,?,?)",
                    (str(2**70), str(2**70 + 1)),
                )

            application = SectApplication(database)
            members = application.list_sect_members(1)

            self.assertEqual(1, len(members))
            self.assertEqual("member", members[0]["user_id"])
            self.assertEqual(2, members[0]["sect_position"])
            self.assertEqual(2**70, members[0]["sect_contribution"])
            self.assertEqual(2**70 + 1, members[0]["stone"])
            self.assertEqual([], application.list_sect_members(2))


if __name__ == "__main__":
    unittest.main()
