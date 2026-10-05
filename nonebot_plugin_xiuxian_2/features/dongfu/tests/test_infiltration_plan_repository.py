from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..infiltration_plan_repository import DongfuInfiltrationPlanSqlRepository
from ..migrations import apply_dongfu_event_replay, apply_dongfu_operations


class DongfuInfiltrationPlanRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_dongfu_operations(uow)
            apply_dongfu_event_replay(uow)
        self.repository = DongfuInfiltrationPlanSqlRepository(self.database)
        self.request = {"random": True, "target_name": ""}
        self.plan = {
            "visitor_id": "v",
            "target_id": "t",
            "target_name": "道友",
            "settlement": "success",
            "rewards": [[1, "灵草", "药材", 2]],
            "slot_no": 1,
        }

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_first_plan_is_immutable_and_replay_uses_it(self) -> None:
        first = self.repository.prepare("op", "v", self.request, self.plan)
        changed_random_result = dict(self.plan, target_id="other", slot_no=2)
        replay = self.repository.prepare("op", "v", self.request, changed_random_result)
        read = self.repository.get("op", "v", self.request)

        self.assertEqual(first.status, "planned")
        self.assertEqual(replay.status, "existing")
        self.assertEqual(read.status, "existing")
        self.assertEqual(read.plan, self.plan)

    def test_reused_id_with_changed_request_or_user_is_rejected(self) -> None:
        self.repository.prepare("op", "v", self.request, self.plan)

        changed_request = self.repository.get(
            "op", "v", {"random": False, "target_name": "道友"}
        )
        changed_user = self.repository.get("op", "other", self.request)

        self.assertEqual(changed_request.status, "operation_conflict")
        self.assertEqual(changed_user.status, "operation_conflict")

    def test_missing_plan_schema_fails_closed(self) -> None:
        database = Path(self.temp_dir.name) / "empty.db"
        with DatabaseUnitOfWork(database):
            pass

        result = DongfuInfiltrationPlanSqlRepository(database).prepare(
            "op", "v", self.request, self.plan
        )

        self.assertEqual(result.status, "schema_missing")
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            tables = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
        self.assertNotIn("dongfu_infiltration_operations", tables)


if __name__ == "__main__":
    unittest.main()
