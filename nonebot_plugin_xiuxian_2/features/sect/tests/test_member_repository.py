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

    def test_user_profile_preserves_first_duplicate_row_and_missing_behavior(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT,"
                    "sect_id INTEGER,sect_contribution TEXT)"
                )
                uow.execute("INSERT INTO user_xiuxian VALUES('u','first',1,?)", (str(2**70),))
                uow.execute("INSERT INTO user_xiuxian VALUES('u','second',2,'0')")

            application = SectApplication(database)
            profile = application.get_user_profile("u")

            self.assertEqual("first", profile["user_name"])
            self.assertEqual(1, profile["sect_id"])
            self.assertEqual(2**70, profile["sect_contribution"])
            self.assertIsNone(application.get_user_profile("missing"))


if __name__ == "__main__":
    unittest.main()
