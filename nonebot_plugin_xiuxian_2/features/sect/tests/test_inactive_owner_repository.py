from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SectApplication


class SectInactiveOwnerRepositoryTests(unittest.TestCase):
    def test_reads_only_state_fields_and_returns_none_for_missing_sect(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,closed INTEGER)"
                )
                uow.execute("INSERT INTO sects VALUES(1,'owner',1)")

            application = SectApplication(database)

            self.assertEqual(
                {"closed": 1, "sect_owner": "owner"},
                application.get_inactive_owner_sect_state(1),
            )
            self.assertIsNone(application.get_inactive_owner_sect_state(2))

    def test_owner_profile_preserves_first_row_and_missing_behavior(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)")
                uow.execute("INSERT INTO user_xiuxian VALUES('owner','first')")
                uow.execute("INSERT INTO user_xiuxian VALUES('owner','second')")

            application = SectApplication(database)

            self.assertEqual(
                {"user_name": "first"},
                application.get_inactive_owner_user_profile("owner"),
            )
            self.assertIsNone(application.get_inactive_owner_user_profile("missing"))


if __name__ == "__main__":
    unittest.main()
