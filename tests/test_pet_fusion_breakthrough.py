from __future__ import annotations

import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.transaction_service import (  # noqa: E402
    PetFusionBreakthroughService,
)
from tests.test_db_backend import db_backend  # noqa: E402


class PetFusionBreakthroughTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "player.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE player_pet_item("
                "user_id TEXT,uid TEXT,pet_id TEXT,stars INTEGER,exp INTEGER,total_exp INTEGER,"
                "skill_id TEXT,is_active INTEGER,updated_at INTEGER)"
            )
            conn.executemany(
                "INSERT INTO player_pet_item VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0)",
                [
                    ("u", "main", "fox", 4, 100, 500, "old", 1),
                    ("u", "body", "fox", 1, 0, 0, "body-skill", 0),
                ],
            )
        self.service = PetFusionBreakthroughService(self.database)
        self.main = ("main", "fox", 4, 100, 500, "old", 1)
        self.materials = [("body", "fox", 1, 0, 0, "body-skill", 0)]

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_breakthrough_consumes_material_and_is_idempotent(self) -> None:
        result = self.service.breakthrough("op", "u", self.main, self.materials, 5, 0, {"skill": "fixed"})
        self.assertEqual(result.status, "applied")
        duplicate = self.service.breakthrough("op", "u", self.main, self.materials, 5, 0, {"skill": "fixed"})
        self.assertEqual(duplicate.status, "duplicate")
        with closing(db_backend.connect(self.database)) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stars,exp FROM player_pet_item WHERE uid='main'").fetchone()), (5, 0))
            self.assertIsNone(conn.execute("SELECT 1 FROM player_pet_item WHERE uid='body'").fetchone())

    def test_rejects_changed_snapshot_and_operation_payload(self) -> None:
        changed = [(*self.materials[0][:2], 2, *self.materials[0][3:])]
        self.assertEqual(self.service.breakthrough("changed", "u", self.main, changed, 5, 0).status, "state_changed")
        self.assertEqual(self.service.breakthrough("op", "u", self.main, self.materials, 5, 0).status, "applied")
        self.assertEqual(self.service.breakthrough("op", "u", self.main, self.materials, 6, 0).status, "state_changed")

    def test_operation_failure_rolls_back_material_and_main_pet(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE pet_fusion_breakthrough_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,stars INTEGER,exp INTEGER)"
            )
            conn.execute(
                "CREATE TRIGGER fail_fusion_operation BEFORE INSERT ON pet_fusion_breakthrough_operations "
                "BEGIN SELECT RAISE(ABORT,'boom'); END"
            )
        with self.assertRaises(db_backend.Error):
            self.service.breakthrough("rollback", "u", self.main, self.materials, 5, 0)
        with closing(db_backend.connect(self.database)) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stars,exp FROM player_pet_item WHERE uid='main'").fetchone()), (4, 100))
            self.assertIsNotNone(conn.execute("SELECT 1 FROM player_pet_item WHERE uid='body'").fetchone())


if __name__ == "__main__":
    unittest.main()
