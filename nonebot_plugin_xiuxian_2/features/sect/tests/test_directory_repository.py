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
                    "sect_scale INTEGER,sect_owner TEXT,combat_power INTEGER)"
                )
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,sect_id INTEGER)"
                )
                uow.execute("INSERT INTO sects VALUES(1,'青云',100,'owner',300)")
                uow.execute("INSERT INTO sects VALUES(2,'无主',40,NULL,900)")
                uow.execute("INSERT INTO sects VALUES(3,'赤霄',250,'master',200)")
                uow.execute("INSERT INTO user_xiuxian VALUES('owner','掌门',1)")
                uow.execute("INSERT INTO user_xiuxian VALUES('member','弟子',1)")
                uow.execute("INSERT INTO user_xiuxian VALUES('master','宗主',3)")

            application = SectApplication(database)
            result = application.list_sects_with_member_count()
            active_names = application.list_active_sect_names()
            scale_rank = application.list_sect_scale_rank()
            power_rank = application.list_sect_combat_power_rank()

            self.assertEqual(
                [
                    (1, "青云", 100, "掌门", 2),
                    (2, "无主", 40, None, 0),
                    (3, "赤霄", 250, "宗主", 1),
                ],
                result,
            )
            self.assertEqual(["青云", "赤霄"], active_names)
            self.assertEqual([(3, "赤霄", 250), (1, "青云", 100)], scale_rank)
            self.assertEqual([(1, "青云", 300), (3, "赤霄", 200)], power_rank)

    def test_combat_power_rank_is_limited_to_fifty_active_sects(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "sect.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,"
                    "combat_power INTEGER,sect_owner TEXT)"
                )
                uow.executemany(
                    "INSERT INTO sects VALUES(?,?,?,?)",
                    (
                        (sect_id, f"sect-{sect_id}", sect_id, "owner")
                        for sect_id in range(1, 53)
                    ),
                )
            rank = SectApplication(database).list_sect_combat_power_rank()
            self.assertEqual(50, len(rank))
            self.assertEqual((52, "sect-52", 52), rank[0])
            self.assertEqual((3, "sect-3", 3), rank[-1])


if __name__ == "__main__":
    unittest.main()
